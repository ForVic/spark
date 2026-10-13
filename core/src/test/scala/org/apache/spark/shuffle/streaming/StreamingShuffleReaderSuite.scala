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

import java.io.{FilterInputStream, InputStream}
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel
import org.mockito.ArgumentMatchers.{any, anyInt, anyString}
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.executor.{ExecutorBackend, TaskCpuLease}
import org.apache.spark.internal.config.{IO_COMPRESSION_CODEC, SHUFFLE_COMPRESS, SHUFFLE_MANAGER_INCREMENTAL}
import org.apache.spark.io.LZ4CompressionCodec
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.client.{TransportClient, TransportClientFactory}
import org.apache.spark.network.server.RpcHandler
import org.apache.spark.network.shuffle.streaming.{CreditControlMessage, DataMessage, StreamingShuffleMessage, TerminationAckMessage, TerminationControlMessage}
import org.apache.spark.shuffle.streaming.StreamingShuffleManager.QUERY_ID_PROPERTY_KEY
import org.apache.spark.util.ErrorNotifier

/**
 * Reader-side unit tests that do not require a shuffle writer. End-to-end writer <-> reader
 * behavior is covered by `StreamingShuffleSuite` in the tests PR of this stack.
 */
class StreamingShuffleReaderSuite
  extends SparkFunSuite
  with LocalSparkContext
  with Matchers
  with MockitoSugar {

  private def newConf(): SparkConf =
    // StreamingShuffleManager is pipelined, so it belongs in the incremental slot (the default
    // spark.shuffle.manager must be a BlockingShuffleManager). This is what initializes the
    // streaming output tracker that the reader constructor asserts.
    new SparkConf().set(SHUFFLE_MANAGER_INCREMENTAL, classOf[StreamingShuffleManager].getName)

  private def createTaskContext(conf: SparkConf, partitionId: Int): TaskContextImpl = {
    val properties = new Properties()
    properties.setProperty(QUERY_ID_PROPERTY_KEY, "test-query-id")
    val taskMemoryManager = new TaskMemoryManager(new TestMemoryManager(conf), 0)
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId,
      taskAttemptId = 0,
      attemptNumber = 0,
      numPartitions = 1,
      taskMemoryManager = taskMemoryManager,
      localProperties = properties,
      metricsSystem = mock[MetricsSystem],
      cpuAmount = 1)
  }

  // A minimal DataMessage that is only used to exercise the iterator factory's type dispatch;
  // its contents are never decoded here because handleDataMessage is stubbed in these tests.
  private def emptyDataMessage(): DataMessage =
    new DataMessage(0, 0, 0, Unpooled.EMPTY_BUFFER, 0L)

  // The iterator factory drives four collaborators; these tests supply in-memory fakes for all of
  // them so the reader's consumer-loop control flow can be verified without Netty or a SparkEnv.
  private val factory = new StreamingShuffleReaderIteratorFactory()

  test("reader wait metrics separate receiving data from regaining the CPU") {
    val context = TaskContext.empty()
    val messages = new LinkedBlockingQueue[StreamingShuffleMessage]()
    var now = 0L
    var totalWait = 0L
    var resumeWaits = Seq.empty[Long]
    val measuredFactory = new StreamingShuffleReaderIteratorFactory {
      override protected def nanoTime(): Long = now
      override protected def recordCpuResumeWait(nanos: Long): Unit = {
        resumeWaits :+= nanos
      }
    }
    var lease: TaskCpuLease = null
    val backend = new ExecutorBackend {
      override def statusUpdate(id: Long, state: TaskState.TaskState, data: ByteBuffer): Unit = ()
      override def releaseTaskCpu(id: Long, sequence: Long): Unit = {
        now += 10000000L
        messages.put(emptyDataMessage())
      }
      override def requestTaskCpu(id: Long, sequence: Long): Unit = {
        now += 3000000L
        lease.resume(sequence)
      }
    }
    lease = new TaskCpuLease(0, backend)
    TaskContext.withTaskContext(context) {
      TaskCpuLease.install(lease)
      try {
        val reader = measuredFactory.create[Int, Int](messages, _ => true,
          _ => Iterator((1, 1)), () => (), nanos => totalWait += nanos)
        assert(reader.next() == ((1, 1)))
        assert(totalWait == 13000000L)
        assert(resumeWaits == Seq(3000000L))
        messages.put(new TerminationControlMessage(0, 0))
        assert(!reader.hasNext)
        assert(totalWait == 13000000L)
        assert(resumeWaits == Seq(3000000L))
      } finally {
        TaskCpuLease.clear()
      }
    }
  }

  test("canceling a CPU grant releases a message already removed from the reader queue") {
    val context = TaskContext.empty()
    val messages = new LinkedBlockingQueue[StreamingShuffleMessage]()
    val buffer = Unpooled.buffer(8)
    val message = new DataMessage(0, 0, 0, buffer, 0L)
    buffer.release() // The message retains its own reference.
    val backend = new ExecutorBackend {
      override def statusUpdate(id: Long, state: TaskState.TaskState, data: ByteBuffer): Unit = ()
      override def releaseTaskCpu(id: Long, sequence: Long): Unit = messages.put(message)
      override def requestTaskCpu(id: Long, sequence: Long): Unit = {
        context.markInterrupted("cancel before CPU grant")
      }
    }
    TaskContext.withTaskContext(context) {
      TaskCpuLease.install(new TaskCpuLease(0, backend))
      try {
        val reader = factory.create[Int, Int](messages, _ => false,
          _ => fail("canceled task must not deserialize data"), () => ())
        intercept[TaskKilledException](reader.hasNext)
        assert(messages.isEmpty)
        assert(buffer.refCnt() == 0)
      } finally {
        TaskCpuLease.clear()
        if (buffer.refCnt() > 0) buffer.release()
      }
    }
  }

  test("a connection rejected by its reader closes before advertising readiness") {
    val messages = new LinkedBlockingQueue[StreamingShuffleMessage]()
    val handler = new StreamingShuffleClientHandler(
      0, 0, messages, 0, 1024, createTaskContext(newConf(), 0), new ErrorNotifier())
    handler.registerClient = _ => false
    val channel = new EmbeddedChannel()
    val client = mock[TransportClient]
    when(client.getChannel).thenReturn(channel)
    try {
      handler.channelActive(client)
      assert(!channel.isActive)
      verify(client, never()).send(any[ByteBuf]())
      assert(messages.isEmpty)
    } finally {
      channel.finishAndReleaseAll()
    }
  }

  test("TCP flow control drains frames with only initial credit and final acknowledgment") {
    val messages = new LinkedBlockingQueue[StreamingShuffleMessage]()
    val errors = new ErrorNotifier()
    val handler = new StreamingShuffleClientHandler(
      0, 0, messages, 0, 1, createTaskContext(newConf(), 0), errors)
    val channel = new EmbeddedChannel()
    val client = mock[TransportClient]
    var sent = Vector.empty[StreamingShuffleMessage]
    when(client.getChannel).thenReturn(channel)
    when(client.send(any[ByteBuf]())).thenAnswer { invocation =>
      val buffer = invocation.getArgument[ByteBuf](0)
      try {
        sent :+= StreamingShuffleMessage.decode(buffer)
      } finally {
        buffer.release()
      }
      channel.newSucceededFuture()
    }
    def receive(message: StreamingShuffleMessage): Unit = {
      val buffer = Unpooled.compositeBuffer().capacity(message.headerLength())
      try {
        message.encode(buffer)
        handler.receive(client, buffer.nioBuffer(), null)
        errors.throwErrorIfExists()
      } finally {
        buffer.release()
        message.release()
      }
    }
    try {
      handler.channelActive(client)
      assert(sent.size == 1 && sent.head.isInstanceOf[CreditControlMessage])
      val payload = Unpooled.buffer(4).writeInt(7)
      val frame = new DataMessage(0, 0, 4, payload, 0L)
      payload.release()
      frame.setSeqNum(0)
      receive(frame)
      assert(!channel.config().isAutoRead)
      val received = messages.remove().asInstanceOf[DataMessage]
      assert(received.data.getInt(received.data.readerIndex()) == 7)
      received.release()
      assert(channel.config().isAutoRead)
      assert(sent.size == 1)
      val termination = new TerminationControlMessage(0, 0)
      termination.setSeqNum(1)
      receive(termination)
      assert(sent.size == 2 && sent.last.isInstanceOf[TerminationAckMessage])
      assert(sent.last.getSeqNum == 1)
      assert(messages.remove().isInstanceOf[TerminationControlMessage])
      handler.channelInactive(client)
      errors.throwErrorIfExists()
    } finally {
      messages.asScala.foreach(_.release())
      sent.foreach(_.release())
      channel.finishAndReleaseAll()
    }
  }

  test("connect completion owns the client even before channelActive is delivered") {
    withSpark(new SparkContext("local", "StreamingShuffleReaderSuite", newConf())) { sc =>
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId = 0, numMaps = 1, numReduces = 1, jobId = 0)
      val dep = new ShuffleDependency[Int, Int, Int](
        sc.parallelize(Seq((1, 1))), new HashPartitioner(1))
      val handle = new StreamingShuffleHandle(0, dep)
      val context = createTaskContext(sc.conf, 0)
      val clients = mock[TransportClientFactory]
      val client = mock[TransportClient]
      // A successful connect future need not have delivered the channelActive callback yet.
      when(clients.createUnmanagedClient(anyString(), anyInt(), any[RpcHandler]()))
        .thenReturn(client)
      class TestReader extends StreamingShuffleReader[Int, Int](
          handle, context, sharedClientFactory = Some(clients)) {
        def connect(): TransportClient = createClient(0, "localhost", 1234)
      }
      val reader = new TestReader
      try {
        assert(reader.connect() eq client)
        assert(reader.clientMap.get(0L) eq client)
      } finally {
        context.markTaskCompleted(None)
      }
      verify(client).close()
      verify(clients, never()).close()
    }
  }

  test("reader discovery waits for registration without busy polling") {
    withSpark(new SparkContext("local", "StreamingShuffleReaderSuite", newConf())) { sc =>
      val tracker = SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
      val dep = new ShuffleDependency[Int, Int, Int](
        sc.parallelize(Seq((1, 1))), new HashPartitioner(1))
      val polls = sc.longAccumulator("readerDiscoveryPolls")
      val handle = new StreamingShuffleHandle(37, dep, Map("readerDiscoveryPolls" -> polls))
      val context = createTaskContext(sc.conf, 0)
      val clients = mock[TransportClientFactory]
      val client = mock[TransportClient]
      when(clients.createUnmanagedClient(anyString(), anyInt(), any[RpcHandler]()))
        .thenReturn(client)
      val reader = new StreamingShuffleReader[Int, Int](handle, context,
        sharedClientFactory = Some(clients))
      try {
        eventually(Timeout(10.seconds)) {
          assert(Thread.getAllStackTraces.keySet().asScala.exists { thread =>
            thread.getName == "streaming-shuffle-task-discovery-thread-37-0" &&
              thread.getState == Thread.State.TIMED_WAITING
          })
        }
        tracker.registerShuffle(37, numMaps = 1, numReduces = 1, jobId = 0)
        tracker.registerShuffleWriterTask(37, 0,
          StreamingShuffleTaskLocation("0", "localhost", 1234))
        eventually(Timeout(10.seconds)) { assert(reader.clientMap.size() == 1) }
      } finally {
        context.markTaskCompleted(None)
      }
      assert(polls.value > 0)
      verify(client).close()
    }
  }

  test("task completion closes a partially consumed compressed frame") {
    val conf = newConf().set(SHUFFLE_COMPRESS, true)
      .set(IO_COMPRESSION_CODEC, classOf[StreamingShuffleCloseTrackingCodec].getName)
    withSpark(new SparkContext("local", "StreamingShuffleReaderSuite", conf)) { sc =>
      StreamingShuffleReaderSuite.closedCodecStreams.set(0)
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId = 0, numMaps = 1, numReduces = 1, jobId = 0)
      val rdd = sc.parallelize(Seq((7, 7)))
      val dependency = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(1))
      val context = createTaskContext(sc.conf, 0)
      try {
        val reader = new StreamingShuffleReader[Int, Int](
          new StreamingShuffleHandle(0, dependency), context)
        val newMessage = StreamingShuffleSuite.newDataMsgFunc(dependency, 0)
        reader.messageQueue.add(newMessage(0, 7))
        assert(reader.read().next() == (7, 7))
        assert(StreamingShuffleReaderSuite.closedCodecStreams.get() == 0)
        context.markTaskCompleted(None)
        assert(StreamingShuffleReaderSuite.closedCodecStreams.get() == 1)
        reader.cleanupResources()
        assert(StreamingShuffleReaderSuite.closedCodecStreams.get() == 1)
      } finally {
        context.markTaskCompleted(None)
      }
    }
  }

  test("iterator emits all rows from data messages then stops on termination") {
    val queue = new LinkedBlockingQueue[StreamingShuffleMessage]()
    queue.put(emptyDataMessage())
    queue.put(emptyDataMessage())
    queue.put(new TerminationControlMessage(0, 0))

    // Each data message yields a fixed pair of rows; the single termination message ends the read.
    val rowsPerMessage = Seq(Iterator((1, 1), (2, 2)), Iterator((3, 3), (4, 4)))
    val nextRows = rowsPerMessage.iterator
    val it = factory.create[Int, Int](
      queue,
      handleTerminationMessage = _ => true,
      handleDataMessage = _ => nextRows.next(),
      checkTaskFailure = () => ())

    it.toSeq should contain theSameElementsInOrderAs Seq((1, 1), (2, 2), (3, 3), (4, 4))
  }

  test("iterator does not stop until handleTerminationMessage returns true") {
    val queue = new LinkedBlockingQueue[StreamingShuffleMessage]()
    // Two writers: the first termination message must not end the read, only the second.
    queue.put(new TerminationControlMessage(0, 0))
    queue.put(emptyDataMessage())
    queue.put(new TerminationControlMessage(1, 0))

    var terminationsSeen = 0
    val it = factory.create[Int, Int](
      queue,
      handleTerminationMessage = _ => {
        terminationsSeen += 1
        terminationsSeen == 2 // only the second termination completes the read
      },
      handleDataMessage = _ => Iterator((1, 1)),
      checkTaskFailure = () => ())

    it.toSeq should contain theSameElementsInOrderAs Seq((1, 1))
    terminationsSeen should be(2)
  }

  test("iterator surfaces a background error before dequeuing the next message") {
    val queue = new LinkedBlockingQueue[StreamingShuffleMessage]()
    // A message is available in the queue, but a background error has already been recorded
    // (e.g. the channelInactive premature-disconnect path). Because checkTaskFailure runs before
    // every dequeue, the iterator must throw before this message is dequeued and handled, rather
    // than emitting stale data and only failing later.
    queue.put(emptyDataMessage())

    var dataHandled = false
    val it = factory.create[Int, Int](
      queue,
      handleTerminationMessage = _ => true,
      handleDataMessage = _ => {
        dataHandled = true
        Iterator((1, 1))
      },
      checkTaskFailure = () => throw new SparkException("background failure"))

    val e = intercept[SparkException] {
      it.hasNext
    }
    e.getMessage should include("background failure")
    // The pending message must not have been processed: the error was surfaced first. This is
    // what would keep the reader from emitting rows after a writer has already failed.
    dataHandled should be(false)
  }

  test("getReader routes to a StreamingShuffleReader wired with the given context") {
    // A construction/routing smoke test: the manager must dispatch to the streaming reader (not a
    // fallback shuffle reader), the reader must construct successfully (its constructor asserts the
    // output tracker, starts the task-discovery and client-creation executors, and registers the
    // task-completion listener), and it must be wired with the context we passed in.
    withSpark(new SparkContext("local", "StreamingShuffleReaderSuite", newConf())) { sc =>
      SparkEnv.get.streamingShuffleOutputTracker.get
        .asInstanceOf[StreamingShuffleOutputTrackerMaster]
        .registerShuffle(shuffleId = 0, numMaps = 1, numReduces = 1, jobId = 0)
      val rdd = sc.parallelize(1 to 4).map(x => (x, x))
      val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(1))
      val handle = new StreamingShuffleHandle(0, dep)
      val context = createTaskContext(sc.conf, 0)
      try {
        val reader = new StreamingShuffleManager()
          .getReader[Int, Int](handle, 0, 1, 0, 1, context, null)
        reader shouldBe a[StreamingShuffleReader[_, _]]
        val streamingReader = reader.asInstanceOf[StreamingShuffleReader[_, _]]
        streamingReader.context should be theSameInstanceAs context
      } finally {
        // Constructing the reader starts a background task-discovery thread; the task-completion
        // listener (cleanupResources) shuts it down.
        context.markTaskCompleted(None)
      }
    }
  }
}

private[streaming] object StreamingShuffleReaderSuite {
  val closedCodecStreams = new AtomicInteger()
}

class StreamingShuffleCloseTrackingCodec(conf: SparkConf) extends LZ4CompressionCodec(conf) {
  override def compressedInputStream(input: InputStream): InputStream = {
    new FilterInputStream(super.compressedInputStream(input)) {
      private var closed = false
      override def close(): Unit = if (!closed) {
        closed = true
        try {
          super.close()
        } finally {
          StreamingShuffleReaderSuite.closedCodecStreams.incrementAndGet()
        }
      }
    }
  }
}
