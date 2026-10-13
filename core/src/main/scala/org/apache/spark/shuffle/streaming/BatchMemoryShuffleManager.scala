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

import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{ShuffleDependency, SparkConf, SparkContext, SparkEnv, StreamingShuffleOutputTrackerMaster, StreamingShuffleTaskLocation, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient, TransportClientBootstrap}
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.{RpcHandler, StreamManager, TransportServerBootstrap}
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.MemoryShuffleBlockResolver
import org.apache.spark.util.LongAccumulator

/** One endpoint per executor; streams and buffers outlive the task that produced them. */
private[spark] class BatchMemoryShuffleManager(conf: SparkConf)
  extends PipelinedShuffleManager with Logging {
  import BatchMemoryShuffleManager._

  override def supportsBatchExecution: Boolean = true
  override def supportsIndependentProducerCompletion: Boolean = true
  override def supportsLiveReducePartitionHints: Boolean = true

  private val outputs = mutable.HashMap.empty[(Int, Int, Int), LiveShuffleOutput]
  private val closedEpochs = mutable.HashSet.empty[Int]
  private val locations =
    mutable.HashMap.empty[(Int, Int), Map[Long, StreamingShuffleTaskLocation]]
  private val clients = mutable.HashMap.empty[(String, Int), TransportClient]
  private var stopped = false
  private var initialized = false
  private var clientInitialized = false
  private var serverInitialized = false

  private[spark] def liveOutputCount: Int = synchronized { outputs.size }

  private[streaming] lazy val resolver = {
    SparkEnv.get.blockingShuffleManager.shuffleBlockResolver match {
      case memory: MemoryShuffleBlockResolver => memory
      case _ => throw new IllegalStateException(
        "Batch memory shuffle requires spark.shuffle.memory.enabled and SortShuffleManager")
    }
  }
  private[streaming] val pageSize = conf.get(STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE)
  require(pageSize >= 1024, "Batch memory shuffle needs pages of at least 1024 bytes")

  private[streaming] lazy val transportConf = SparkTransportConf.fromSparkConf(conf,
    "batch-memory-shuffle", 2,
    sslOptions = Some(SparkEnv.get.securityManager.getRpcSSLOptions()))

  private lazy val transport = synchronized {
    require(!stopped)
    initialized = true
    new TransportContext(transportConf, new RpcHandler {
      override def getStreamManager: StreamManager = null
      override def receive(
          client: TransportClient,
          request: ByteBuffer,
          callback: RpcResponseCallback): Unit = {
        val operation = request.getInt()
        val epoch = request.getInt()
        if (operation == CLOSE_EPOCH) {
          closeEpoch(epoch)
          callback.onSuccess(ByteBuffer.allocate(0))
        } else {
          val shuffle = request.getInt()
          val map = request.getInt()
          val partition = request.getInt()
          val sequence = request.getInt()
          val maxPages = request.getInt()
          val output = BatchMemoryShuffleManager.this.synchronized {
            outputs.getOrElse((epoch, shuffle, map),
              throw new IOException("Missing live shuffle attempt"))
          }
          if (operation == ABANDON) {
            output.abandon(partition)
            callback.onSuccess(ByteBuffer.allocate(0))
          } else {
            require(operation == READ)
            output.readBatch(partition, sequence, maxPages).whenComplete { (buffer, error) =>
              if (error != null) callback.onFailure(error)
              else {
                try callback.onSuccess(buffer) finally buffer.release()
              }
            }
          }
        }
      }
    })
  }

  private lazy val clientFactory = synchronized {
    require(!stopped)
    val security = SparkEnv.get.securityManager
    val bootstrap: Seq[TransportClientBootstrap] = if (security.isAuthenticationEnabled()) {
      Seq(new AuthClientBootstrap(transportConf, conf.getAppId, security))
    } else Nil
    val factory = transport.createClientFactory(bootstrap.asJava)
    clientInitialized = true
    factory
  }

  private lazy val server = synchronized {
    require(!stopped)
    val security = SparkEnv.get.securityManager
    val bootstrap: Seq[TransportServerBootstrap] = if (security.isAuthenticationEnabled()) {
      Seq(new AuthServerBootstrap(transportConf, security))
    } else Nil
    val endpoint = transport.createServer(0, bootstrap.asJava)
    serverInitialized = true
    endpoint
  }

  private[streaming] def client(
      location: StreamingShuffleTaskLocation,
      context: TaskContext = null): TransportClient = {
    val key = (location.host, location.port)
    synchronized { clients.get(key).filter(_.isActive) } match {
      case Some(ready) => ready
      case None =>
        def connect(): TransportClient = clientFactory.createClient(location.host, location.port)
        val connected = if (context == null) connect() else {
          context.withReleasedCpu { connect() }
        }
        synchronized { if (!stopped) clients(key) = connected }
        connected
    }
  }

  private[streaming] def location(
      shuffle: Int,
      map: Int,
      context: TaskContext): StreamingShuffleTaskLocation = {
    val key = (epoch(context), shuffle)
    locations.synchronized { locations.get(key).flatMap(_.get(map.toLong)) } match {
      case Some(known) => return known
      case None =>
    }
    def lookup(): Option[StreamingShuffleTaskLocation] = locations.synchronized {
      val known = locations.getOrElse(key, Map.empty)
      known.get(map.toLong).orElse {
        SparkEnv.get.streamingShuffleOutputTracker.get
          .getAvailableShuffleWriterTaskLocations(shuffle, known.size).flatMap { response =>
            val updated = known ++ response.shuffleTaskLocations
            locations(key) = updated
            updated.get(map.toLong)
          }
      }
    }
    context.withReleasedCpu {
      var found = lookup()
      while (found.isEmpty) {
        context.killTaskIfInterrupted()
        Thread.sleep(5)
        found = lookup()
      }
      found.get
    }
  }

  private[streaming] def readLocal(
      epoch: Int, shuffle: Int, map: Int, partition: Int, sequence: Int)
      : CompletableFuture[ManagedBuffer] = synchronized {
    outputs.getOrElse((epoch, shuffle, map),
      throw new IOException("Missing local live shuffle attempt")).read(partition, sequence)
  }

  private[streaming] def abandonLocal(
      epoch: Int, shuffle: Int, map: Int, partition: Int): Unit = synchronized {
    outputs.get((epoch, shuffle, map)).foreach(_.abandon(partition))
  }

  private[streaming] def newOutput(
      handle: BatchMemoryShuffleHandle[_, _, _],
      mapId: Long,
      context: TaskContext): LiveShuffleOutput = {
    val run = epoch(context)
    val key = (run, handle.shuffleId, context.partitionId())
    val output = synchronized {
      require(!stopped && !closedEpochs.contains(run) && !outputs.contains(key),
        "Live shuffle attempt is closed or already registered")
      val created = new LiveShuffleOutput(resolver, handle.shuffleId, mapId,
        handle.dependency.partitioner.numPartitions, pageSize, transportConf,
        handle.memoryBytes.add, handle.diskBytes.add, () => {
          val start = System.nanoTime()
          context.withReleasedCpu { resolver.awaitLiveCapacity(pageSize) }
          handle.backpressureNs.add(System.nanoTime() - start)
        })
      outputs(key) = created
      created
    }
    context.addTaskCompletionListener[Unit] { _ =>
      output.abort(new IOException("Producer ended without committing live output"))
    }
    try {
      val location = StreamingShuffleTaskLocation(SparkEnv.get.executorId,
        SparkEnv.get.blockManager.blockManagerId.host, server.getPort)
      val registered = SparkEnv.get.streamingShuffleOutputTracker.get
        .registerShuffleWriterTask(handle.shuffleId, context.partitionId(), location, run)
      require(registered, "Live shuffle run ended before producer registration")
      output
    } catch {
      case error: Throwable =>
        synchronized { outputs.remove(key) }
        output.dispose()
        throw error
    }
  }

  private def closeEpoch(epoch: Int): Unit = {
    locations.synchronized { locations.filterInPlace { case ((run, _), _) => run != epoch } }
    val removed = synchronized {
      closedEpochs += epoch
      val keys = outputs.keys.filter(_._1 == epoch).toList
      keys.flatMap(outputs.remove)
    }
    removed.foreach(_.dispose())
  }

  override def endRun(epoch: Int): Unit = {
    SparkEnv.get.streamingShuffleOutputTracker.foreach {
      case master: StreamingShuffleOutputTrackerMaster =>
        master.closeRun(epoch).foreach { location =>
          try {
            client(location).sendRpcSync(request(CLOSE_EPOCH, epoch), 10000)
          } catch {
            case NonFatal(error) => logWarning("Could not retire live shuffle output", error)
          }
        }
      case _ =>
    }
    closeEpoch(epoch)
  }

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new BatchMemoryShuffleHandle(shuffleId, dependency, dependency.rdd.getNumPartitions)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    new BatchMemoryShuffleWriter(handle.asInstanceOf[BatchMemoryShuffleHandle[K, V, _]],
      mapId, context, metrics, this)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    require(endPartition == startPartition + 1, "Live readers require one reduce partition")
    val h = handle.asInstanceOf[BatchMemoryShuffleHandle[K, _, C]]
    new BatchMemoryShuffleReader(h, startMapIndex, math.min(endMapIndex, h.numMaps),
      startPartition, context, metrics, this)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    locations.synchronized {
      locations.filterInPlace { case ((_, shuffle), _) => shuffle != shuffleId }
    }
    val removed = synchronized {
      outputs.keys.filter(_._2 == shuffleId).toList.flatMap(outputs.remove)
    }
    removed.foreach(_.dispose())
    true
  }

  override def stop(): Unit = {
    val (removed, closeClient, closeServer, closeTransport) = synchronized {
      stopped = true
      val all = outputs.values.toList
      outputs.clear()
      clients.clear()
      (all, clientInitialized, serverInitialized, initialized)
    }
    removed.foreach(_.dispose())
    if (closeClient) clientFactory.close()
    if (closeServer) server.close()
    if (closeTransport) transport.close()
  }
}

private[spark] object BatchMemoryShuffleManager {
  val READ = 0
  val ABANDON = 1
  val CLOSE_EPOCH = 2

  def epoch(context: TaskContext): Int = {
    Option(context.getLocalProperty(SparkContext.SPARK_PIPELINED_RUN_EPOCH))
      .getOrElse(throw new IOException("Missing live shuffle execution epoch")).toInt
  }

  def request(
      operation: Int,
      epoch: Int,
      shuffle: Int = 0,
      map: Int = 0,
      partition: Int = 0,
      sequence: Int = 0,
      maxPages: Int = 1): ByteBuffer = {
    val buffer = ByteBuffer.allocate(28)
    buffer.putInt(operation).putInt(epoch).putInt(shuffle).putInt(map)
      .putInt(partition).putInt(sequence).putInt(maxPages).flip()
    buffer
  }
}

private[spark] class BatchMemoryShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C],
    val numMaps: Int) extends BaseShuffleHandle[K, V, C](shuffleId, dependency) {
  val memoryBytes: LongAccumulator =
    dependency.rdd.context.longAccumulator(s"memoryShuffle.$shuffleId.liveMemoryBytes")
  val diskBytes: LongAccumulator =
    dependency.rdd.context.longAccumulator(s"memoryShuffle.$shuffleId.liveDiskBytes")
  val earlyReadPages: LongAccumulator =
    dependency.rdd.context.longAccumulator(s"memoryShuffle.$shuffleId.pagesReadBeforeCommit")
  val backpressureNs: LongAccumulator =
    dependency.rdd.context.longAccumulator(s"memoryShuffle.$shuffleId.writerBackpressureNs")
  val sinkNs: LongAccumulator =
    dependency.rdd.context.longAccumulator(s"memoryShuffle.$shuffleId.writerSinkNs")
  val readRequests: LongAccumulator =
    dependency.rdd.context.longAccumulator(s"memoryShuffle.$shuffleId.readRequests")
  val readPages: LongAccumulator =
    dependency.rdd.context.longAccumulator(s"memoryShuffle.$shuffleId.readPages")
  val discoveryNs: LongAccumulator =
    dependency.rdd.context.longAccumulator(s"memoryShuffle.$shuffleId.readerDiscoveryNs")
}
