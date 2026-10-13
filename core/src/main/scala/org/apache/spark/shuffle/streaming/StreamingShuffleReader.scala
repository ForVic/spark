/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.streaming

import java.io.InputStream
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, LinkedBlockingQueue, Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import io.netty.buffer.ByteBufInputStream

import org.apache.spark.{ShuffleLocationResponse, SparkEnv, SparkRuntimeException, TaskContext}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config.{STREAMING_SHUFFLE_CHECKSUM_ENABLED, STREAMING_SHUFFLE_READER_MAX_MEMORY}
import org.apache.spark.memory.{MemoryConsumer, MemoryMode}
import org.apache.spark.network.client.{TransportClient, TransportClientFactory}
import org.apache.spark.network.shuffle.streaming.{DataMessage, ShuffleChecksum, StreamingShuffleMessage, TerminationControlMessage}
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.{ErrorNotifier, NextIterator, ThreadUtils, Utils}

/**
 * Default factory used by `StreamingShuffleReader` to create its output iterator.
 */
class StreamingShuffleReaderIteratorFactory {
  protected def nanoTime(): Long = System.nanoTime()

  protected def recordCpuResumeWait(nanos: Long): Unit = ()

  def create[K, C](
      messageQueue: LinkedBlockingQueue[StreamingShuffleMessage],
      handleTerminationMessage: TerminationControlMessage => Boolean,
      handleDataMessage: DataMessage => Iterator[(K, C)],
      checkTaskFailure: () => Unit,
      recordWait: Long => Unit = _ => ()
    ): Iterator[Product2[K, C]] = {
    new NextIterator[Product2[K, C]] {
      // Iterator that iterates through multiple rows in data message buffer. When the iterator
      // does not have any more rows, we should fetch another message from message queue.
      private var rowIterator: Iterator[(K, C)] = Iterator.empty

      def getNext(): Product2[K, C] = {
        while (!rowIterator.hasNext) {
          checkTaskFailure()
          val waitStartNs = nanoTime()
          var message = messageQueue.poll()
          if (message == null) {
            def waitForMessage(): Unit = {
              while (message == null) {
                checkTaskFailure()
                message = messageQueue.poll(10, TimeUnit.MILLISECONDS)
              }
            }
            val context = TaskContext.get()
            try {
              if (context == null) {
                waitForMessage()
              } else {
                var messageReadyNs = 0L
                context.withReleasedCpu {
                  waitForMessage()
                  messageReadyNs = nanoTime()
                }
                // The message is already available while the task awaits permission to compute.
                // Keep this interval inside total fetch wait, but expose it separately as well.
                recordCpuResumeWait(nanoTime() - messageReadyNs)
              }
            } catch {
              case error: Throwable =>
                // A grant wait can be canceled after removing a message from the queue.
                if (message != null) message.release()
                throw error
            }
          }
          recordWait(nanoTime() - waitStartNs)
          message match {
            case msg: TerminationControlMessage =>
              if (handleTerminationMessage(msg)) {
                finished = true
                return null.asInstanceOf[Product2[K, C]]
              }
            case dataMessage: DataMessage =>
              rowIterator = handleDataMessage(dataMessage)
            case null => // no message received, continue to poll
            case other =>
              throw new IllegalArgumentException(
                s"Unexpected message type in reader queue: ${other.getClass.getName}")
          }
        }
        rowIterator.next()
      }

      override def close(): Unit = {
        // no-op. handleTerminationMessage will take care of final cleanup.
      }
    }
  }
}

class StreamingShuffleReader[K, C](
    handle: ShuffleHandle,
    val context: TaskContext,
    clientHandler: Option[StreamingShuffleClientHandler] = None,
    private[streaming] val errorNotifier: ErrorNotifier = new ErrorNotifier(),
    sharedClientFactory: Option[TransportClientFactory] = None,
    reducePartitionId: Option[Int] = None,
    readMetrics: Option[ShuffleReadMetricsReporter] = None)
    extends ShuffleReader[K, C] with TaskContextAwareLogging {
  private val setupStartNs = System.nanoTime()
  assert(SparkEnv.get.streamingShuffleOutputTracker.isDefined)
  private val conf = SparkEnv.get.conf
  private val serializerManager = SparkEnv.get.serializerManager

  private val streamingShuffleHandle = handle.asInstanceOf[StreamingShuffleHandle[K, _, C]]
  private val diagnostics = new StreamingShuffleMetrics(streamingShuffleHandle.diagnostics)
  private var fetchWaitNs = 0L
  setShuffleIdForLogging(streamingShuffleHandle.shuffleId)
  // a mapping of mapId and client
  private[spark] val clientMap = new ConcurrentHashMap[Long, TransportClient]()
  // Directly constructed readers own one factory; manager-created readers share executor I/O.
  // In either case each writer connection gets a distinct handler and is closed by this reader.
  private val ownedClientContext = if (sharedClientFactory.isDefined) None else {
    Some(StreamingShuffleManager.createClientContext(conf, 1))
  }
  private[streaming] val clientFactory = sharedClientFactory.getOrElse {
    ownedClientContext.get.createClientFactory()
  }
  private var resourcesClosed = false // guarded by clientMap
  private val tracker = SparkEnv.get.streamingShuffleOutputTracker.get

  private[spark] val taskDiscoveryExecutor =
    ThreadUtils.newDaemonSingleThreadExecutor(
      s"streaming-shuffle-task-discovery-thread-" +
        s"${streamingShuffleHandle.shuffleId}-${context.partitionId()}")

  private val totalNumShuffleWriters: AtomicInteger = new AtomicInteger(-1)
  private var perWriterByteLimit: Long = 1

  // We might need to revisit if the size limit is enough. If not, there should be a way to tie it
  // to the per-task memory limit from the task context.
  private val MAX_MEMORY = conf.get(STREAMING_SHUFFLE_READER_MAX_MEMORY)
  // Data from all writers and a single final termination message are put into this queue.
  private[spark] val messageQueue = new LinkedBlockingQueue[StreamingShuffleMessage]()

  private val memoryConsumer =
    new MemoryConsumer(context.taskMemoryManager(), MemoryMode.OFF_HEAP) {
      override def spill(size: Long, trigger: MemoryConsumer): Long = 0
    }

  private val shuffleChecksum = if (conf.get(STREAMING_SHUFFLE_CHECKSUM_ENABLED)) {
    new ShuffleChecksum()
  } else {
    null
  }

  // The set of shuffle writers that this reader has successfully received
  // termination ack messages from.  This is used to make sure all term ack messages
  // are successfully sent before exiting.
  private[spark] val terminationAckControlMessageSet = ConcurrentHashMap.newKeySet[Long]()

  // Guarded by the set itself: separate writer connections can finish concurrently.
  private val completedWriters = collection.mutable.Set.empty[Int]

  private val allTermAcksSentNotice = new Semaphore(0)

  private[spark] val clientCreationExecutor = ThreadUtils.newDaemonFixedThreadPool(
    Runtime.getRuntime.availableProcessors,
    s"streaming-shuffle-async-client-creation-${context.partitionId()}")

  // Signals to other threads that task discovery should stop. For example, we may receive all
  // the termination messages before we actually put the clients in the client map. In that
  // scenario we can just exit without checking the client map for whether all the clients were
  // created.
  @volatile private var taskDiscoveryShouldStop = false

  private var currentDataMessage: StreamingShuffleMessage = _
  private var currentDataStream: InputStream = _

  private def shutdownExecutorService(
      executor: java.util.concurrent.ExecutorService,
      name: String): Unit = {
    executor.shutdownNow()
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        logWarning(log"${MDC(LogKeys.NAME, name)} did not shut down within the timeout.")
      } else {
        logInfo(log"${MDC(LogKeys.NAME, name)} shut down successfully.")
      }
    } catch {
      case _: InterruptedException =>
        // make sure we clean up even if the task thread is interrupted during shutdown
        shutdownExecutorService(executor, name)
        // Restore the interrupt flag so downstream code knows we were interrupted
        Thread.currentThread().interrupt()
    }
  }

  /**
   * Cleans up all reader resources. This method should be idempotent and
   * can be called multiple times without issue.
   */
  private[streaming] def cleanupResources(): Unit = {
    val cleanupStartTime = System.currentTimeMillis()
    clientMap.synchronized {
      resourcesClosed = true
    }

    Utils.tryLogNonFatalError {
      stopTaskDiscovery()
    }
    Utils.tryLogNonFatalError {
      shutdownExecutorService(clientCreationExecutor, "Client Creation Executor")
    }
    Utils.tryLogNonFatalError {
      clientMap.forEach((_, client) => client.close())
    }
    Utils.tryLogNonFatalError {
      if (ownedClientContext.isDefined) clientFactory.close()
    }
    Utils.tryLogNonFatalError {
      ownedClientContext.foreach(_.close())
    }
    Utils.tryLogNonFatalError {
      if (currentDataStream != null) {
        currentDataStream.close()
        currentDataStream = null
      }
    }
    Utils.tryLogNonFatalError {
      if (currentDataMessage != null) {
        currentDataMessage.release()
        currentDataMessage = null
      }
    }
    Utils.tryLogNonFatalError {
      val list = new java.util.ArrayList[StreamingShuffleMessage]()
      messageQueue.drainTo(list)
      list.forEach(_.release())
    }
    Utils.tryLogNonFatalError {
      memoryConsumer.freeMemory(memoryConsumer.getUsed())
    }
    logInfo(log"Resource cleanup took ${MDC(LogKeys.DURATION,
      System.currentTimeMillis() - cleanupStartTime)} ms")
  }

  // Register a task completion listener to make sure we clean up resources
  // when the task is completed.  Without this, threads, client connections/factories,
  // message buffers, and memory allocations could be leaked.
  context.addTaskCompletionListener[Unit] { _ =>
    val start = System.nanoTime()
    try {
      cleanupResources()
    } finally {
      diagnostics.add("readerCleanupNs", System.nanoTime() - start)
      readMetrics.foreach { metrics =>
        metrics.incFetchWaitTime(fetchWaitNs / 1000000)
        // Short tasks can finish before a heartbeat merges their temporary read counters.
        context.taskMetrics().mergeShuffleReadMetrics()
      }
      diagnostics.report()
    }
  }

  taskDiscoveryExecutor.execute(() => {
    val discoveryStartNs = System.nanoTime()
    val startTime = System.currentTimeMillis()
    try {
      logInfo(log"Task discovery thread started.")
      // a mapping of mapId and client creation future.  Used for parallel client creation
      val clientFutureMap = new ConcurrentHashMap[Long, CompletableFuture[Void]]()

      var isDone = false

      def shouldStop(): Boolean = {
        (
          // Got all locations for shuffle writers
          // and started creating the clients to communicate with them.
          isDone
          // signal from main thread that task discovery should terminate.
          // For example, we got all the term messages from all clients
          || taskDiscoveryShouldStop
          // task has failed or been interrupted.
          || context.isFailed()
          || context.isInterrupted()
          )
      }

      var retryCount = 0
      var retryDelayMs = 1L
      while (!shouldStop()) {
        val knownWriters = clientFutureMap.size()
        diagnostics.add("readerDiscoveryPolls", 1)
        val shuffleLocationResponseOption =
          tracker.getAvailableShuffleWriterTaskLocations(
            streamingShuffleHandle.shuffleId, knownWriters)

        shuffleLocationResponseOption.foreach {
          case ShuffleLocationResponse(shuffleWriterLocations, numShuffleWriters) =>
            diagnostics.add("readerLocationEntries", shuffleWriterLocations.size)
            if (!totalNumShuffleWriters.compareAndSet(-1, numShuffleWriters)) {
              assert(totalNumShuffleWriters.get() == numShuffleWriters)
            } else {
              perWriterByteLimit = Math.max(MAX_MEMORY / numShuffleWriters, 1)
              memoryConsumer.acquireMemory(MAX_MEMORY)
            }
            shuffleWriterLocations
              .foreach {
                case (mapId, location) =>
                  if (!clientFutureMap.containsKey(mapId)) {
                    val future = createClientAsync(mapId.toInt, location.host, location.port)
                      .thenAccept((shuffleClient: TransportClient) => {
                        logInfo(
                          log"Created shuffle client to shuffle " +
                            log"writer with id ${MDC(LogKeys.MAP_ID, mapId)} and location ${MDC(
                              LogKeys.TASK_LOCATION, location)}. ${MDC(
                              LogKeys.NUM_CONNECTED_SHUFFLE_WRITERS, clientMap.size)} / ${MDC(
                              LogKeys.NUM_SHUFFLE_WRITERS, numShuffleWriters)} shuffle writer " +
                            log"tasks connected.")
                      }).exceptionally(th => {
                        val errorMsg = s"Error creating transport client to shuffle writer" +
                          s" with id ${mapId} and location ${location}."
                        // The real exception is likely wrapped in CompletionException
                        logError(errorMsg, th.getCause)
                        errorNotifier.markError(th.getCause)
                        throw new RuntimeException(errorMsg)
                      })
                    clientFutureMap.put(mapId, future)
                  }
              }

            val numClients = clientFutureMap.size()
            // we should not be getting more clients than expected
            assert(!(numClients > numShuffleWriters))
            isDone = numClients == numShuffleWriters
        }
        if (!isDone) {
          // New source waves should not incur a fixed 10 ms discovery delay. Back off when
          // no writers appear, including an unregistered shuffle, instead of busy polling it.
          if (clientFutureMap.size() > knownWriters) retryDelayMs = 1L
          Thread.sleep(retryDelayMs)
          retryDelayMs = math.min(retryDelayMs * 2, 10L)
          retryCount += 1
          if (retryCount % 100 == 0) {
            logInfo(log"Still attempting to get shuffle writer locations." +
              log" Got the location of ${MDC(LogKeys.NUM_CONNECTED_SHUFFLE_WRITERS,
                clientFutureMap.size)} / ${MDC(LogKeys.NUM_SHUFFLE_WRITERS,
                totalNumShuffleWriters.get())} shuffle writers")
          }
        }
      }

      if (isDone && !taskDiscoveryShouldStop && !context.isFailed() && !context.isInterrupted()) {
        // wait for all futures to finish
        CompletableFuture.allOf(clientFutureMap.values().asScala.toSeq: _*).get()
        assert(
          clientMap.size() == totalNumShuffleWriters.get(),
          s"actual num of clients: ${clientMap.size()} " +
          s"expected num clients: ${totalNumShuffleWriters.get()}"
        )
      }
    } catch {
      case th: Throwable =>
        logError(log"Task discovery thread failed.", th)
        errorNotifier.markError(th)
    } finally {
      diagnostics.add("readerDiscoveryNs", System.nanoTime() - discoveryStartNs)
      clientCreationExecutor.shutdown()
      logInfo(log"Task discovery thread exited. Took ${MDC(LogKeys.DURATION,
        System.currentTimeMillis() - startTime)} ms")
    }
  })
  diagnostics.add("readerSetupNs", System.nanoTime() - setupStartNs)

  private def stopTaskDiscovery(): Unit = {
    taskDiscoveryShouldStop = true
    shutdownExecutorService(taskDiscoveryExecutor, "Task Discovery Executor")
  }

  private def createClientAsync(
      mapId: Int,
      remoteHost: String,
      remotePort: Int): CompletableFuture[TransportClient] = {
    val future = new CompletableFuture[TransportClient]()
    clientCreationExecutor.execute(() => {
      val connectStartNs = System.nanoTime()
      try {
        val shuffleClient = createClient(mapId, remoteHost, remotePort)
        future.complete(shuffleClient)
      } catch {
        case th: Throwable =>
          future.completeExceptionally(th)
      } finally {
        diagnostics.add("readerConnectNs", System.nanoTime() - connectStartNs)
      }
    })
    future
  }

  protected def onTermAckResponse(shuffleWriterId: Int): Unit = {
    terminationAckControlMessageSet.add(shuffleWriterId.toLong)
    val curSent = terminationAckControlMessageSet.size()
    val totalSentNeeded = totalNumShuffleWriters.get()
    logInfo(log"Termination ack message sent successfully to shuffle writer " +
      log"${MDC(LogKeys.SHUFFLE_WRITER_ID, shuffleWriterId)}." +
      log" ${MDC(LogKeys.NUM_TERMINATION_ACKS, curSent)} / " +
      log"${MDC(LogKeys.NUM_SHUFFLE_WRITERS, totalSentNeeded)} sent successfully.")

    // if we have sent all term acks successfully
    if (totalSentNeeded > 0 && curSent == totalSentNeeded) {
      allTermAcksSentNotice.release()
    }
  }

  private def shouldEnqueueTermination(writerId: Int): Boolean = completedWriters.synchronized {
    // A connection enqueues all of its data before reporting completion. Holding this lock
    // makes exactly one connection publish the final marker after every writer's data.
    completedWriters.add(writerId) && completedWriters.size == totalNumShuffleWriters.get()
  }

  /**
   * Verifies the checksum of a DataMessage if checksum is enabled.
   * @throws SparkRuntimeException if checksum verification fails
   */
  private def verifyDataMessageChecksum(dataMessage: DataMessage): Unit = {
    if (shuffleChecksum != null) {
      val data = dataMessage.data
      shuffleChecksum.reset()
      shuffleChecksum.updateChecksum(data, data.readerIndex(), data.readableBytes())
      val calculatedChecksum = shuffleChecksum.getValue()
      if (dataMessage.checksum != calculatedChecksum) {
        throw new SparkRuntimeException(
          errorClass = "STREAMING_SHUFFLE_CHECKSUM_VERIFICATION_FAILED",
          messageParameters = Map(
            "writerId" -> dataMessage.shuffleWriterId.toString,
            "expectedChecksum" -> dataMessage.checksum.toString,
            "calculatedChecksum" -> calculatedChecksum.toString,
            "dataLength" -> data.readableBytes().toString
          )
        )
      }
    }
  }

  // visible for test
  protected def createClient(mapId: Int, remoteHost: String, remotePort: Int): TransportClient = {
    val handler = clientHandler.getOrElse(new StreamingShuffleClientHandler(
      mapId,
      reducePartitionId.getOrElse(context.partitionId()),
      messageQueue,
      streamingShuffleHandle.shuffleId,
      perWriterByteLimit,
      context,
      errorNotifier
    ))
    handler.setOnTermAckResponseHandler(onTermAckResponse)
    handler.shouldEnqueueTermination = shouldEnqueueTermination
    handler.registerClient = client => clientMap.synchronized {
      if (resourcesClosed) {
        false
      } else {
        clientMap.put(mapId.toLong, client)
        true
      }
    }
    val client = clientFactory.createUnmanagedClient(remoteHost, remotePort, handler)
    // Netty can complete the connect future before channelActive runs. Discovery completion
    // must imply registered ownership, too. Registration from either path is idempotent.
    if (!handler.registerClient(client)) client.close()
    client
  }

  private def checkTaskFailure(): Unit = {
    context.getTaskFailure.foreach(throw _)
    // Surface any background error on the task thread so that markTaskFailed and
    // markTaskCompleted are called from this thread, ensuring completion listeners
    // (including cleanupResources) run without contention.
    errorNotifier.throwErrorIfExists()
    if (context.isInterrupted()) {
      throw new InterruptedException("Task interrupted. Exiting read loop.")
    }
  }

  override def read(): Iterator[Product2[K, C]] = {
    val serializerInstance = streamingShuffleHandle.dependency.serializer.newInstance()
    def handleTerminationMessage(msg: TerminationControlMessage): Boolean = {
      // The network handlers enqueue this marker only after all writers have finished.
      // Wait for their acknowledgments before releasing the reader's connections.
      val ackStartNs = System.nanoTime()
      if (!allTermAcksSentNotice.tryAcquire()) {
        context.withReleasedCpu {
          while (!allTermAcksSentNotice.tryAcquire(100, TimeUnit.MILLISECONDS)) {
            checkTaskFailure()
          }
        }
      }
      diagnostics.add("readerAckWaitNs", System.nanoTime() - ackStartNs)
      true
    }

    def handleDataMessage(dataMessage: DataMessage): Iterator[(K, C)] = {
      currentDataMessage = dataMessage
      verifyDataMessageChecksum(dataMessage)
      val recordData = dataMessage.getRecordData()
      val bytes = recordData.readableBytes()
      diagnostics.add("readerBytes", bytes)
      diagnostics.add("readerMessages", 1)
      // These bytes arrive over the transport even for another task on the same executor.
      readMetrics.foreach(_.incRemoteBytesRead(bytes))
      val input = new ByteBufInputStream(recordData)
      val blockId = ShuffleBlockId(streamingShuffleHandle.shuffleId,
        dataMessage.shuffleWriterId, dataMessage.shuffleReaderId)
      currentDataStream = serializerManager.wrapForCompression(blockId, input)
      val deserializedIterator = serializerInstance
        .deserializeStream(currentDataStream)
        .asKeyValueIterator
        .asInstanceOf[Iterator[(K, C)]]
      assert(
        deserializedIterator.hasNext,
        formatMessage(
          s"data message that had no data to deserialize:" +
            s" ${dataMessage.data}. Readable bytes: " +
            s"${dataMessage.data.readableBytes()}"
        )
      )
      new NextIterator[(K, C)] {
        override def getNext(): (K, C) = {
          if (!deserializedIterator.hasNext) {
            finished = true
            null.asInstanceOf[(K, C)]
          } else {
            readMetrics.foreach(_.incRecordsRead(1))
            deserializedIterator.next()
          }
        }
        override def close(): Unit = {
          try {
            if (currentDataStream != null) currentDataStream.close()
          } finally {
            currentDataStream = null
            dataMessage.release()
            currentDataMessage = null
          }
        }
      }
    }

    val iteratorFactory = new StreamingShuffleReaderIteratorFactory {
      override protected def recordCpuResumeWait(nanos: Long): Unit = {
        diagnostics.add("readerResumeWaitNs", nanos)
        diagnostics.add("readerMessageWaits", 1)
      }
    }
    iteratorFactory.create(
      messageQueue,
      handleTerminationMessage,
      handleDataMessage,
      checkTaskFailure,
      nanos => {
        fetchWaitNs += nanos
        diagnostics.add("readerWaitNs", nanos)
      }
    )
  }
}
