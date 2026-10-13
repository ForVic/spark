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

import org.apache.spark.{ShuffleDependency, SparkConf, SparkContext, SparkEnv, SparkException, SparkRuntimeException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{EXECUTOR_ID, STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE}
import org.apache.spark.internal.config.Network.RPC_IO_THREADS
import org.apache.spark.network.TransportContext
import org.apache.spark.network.client.TransportClientFactory
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.{NoOpRpcHandler, TransportServerFactory}
import org.apache.spark.network.shuffle.streaming.{DataMessage, StreamingShuffleMessage, StreamingShuffleMessageType, TerminationControlMessage}
import org.apache.spark.shuffle._
import org.apache.spark.util.LongAccumulator

class StreamingShuffleHandle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C],
    private[streaming] val diagnostics: Map[String, LongAccumulator] = Map.empty)
  extends BaseShuffleHandle[K, V, C](shuffleId, dependency)

object StreamingShuffleManager extends Logging {
  private[streaming] def createClientContext(
      conf: SparkConf,
      numThreads: Int): TransportContext = {
    val role = conf.get(EXECUTOR_ID).map { id =>
      if (SparkContext.isDriver(id)) "driver" else "executor"
    }
    val transportConf = SparkTransportConf.fromSparkConf(
      conf, "streaming-shuffle-reader", numThreads, role)
    new TransportContext(transportConf, new NoOpRpcHandler())
  }

  // Exposed for testing
  private[spark] val QUERY_ID_PROPERTY_KEY = "sql.streaming.queryId"
  // Since above is not applicable for batch query, we use below id to track error for batch
  // query with streaming shuffle
  private val QUERY_EXECUTION_ID_PROPERTY_KEY = "spark.sql.execution.id"

  def getQueryId(context: TaskContext): String = {
    Option(context.getLocalProperty(QUERY_ID_PROPERTY_KEY))
      .orElse(Option(context.getLocalProperty(QUERY_EXECUTION_ID_PROPERTY_KEY)))
      .getOrElse(throw SparkException.internalError(
        "Streaming shuffle requires the query id or SQL execution id local property to be set"))
  }

  /* Called from the reader side to get the writerId associated with a message */
  def getWriterId(message: StreamingShuffleMessage): Int = {
    message.messageType() match {
      case StreamingShuffleMessageType.DATA_MESSAGE_UNSAFE_ROW =>
        message.asInstanceOf[DataMessage].shuffleWriterId
      case StreamingShuffleMessageType.TERMINATION_CONTROL_MESSAGE =>
        message.asInstanceOf[TerminationControlMessage].shuffleWriterId
      case _ =>
        // Should not reach here
        throw streamingShuffleUnexpectedMessageType(message.messageType());
    }
  }

  def streamingShuffleIncorrectSequenceNumber(
      messageType: StreamingShuffleMessageType,
      writerId: Int,
      readerId: Int,
      expSeqNum: Long,
      actSeqNum: Long): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "STREAMING_SHUFFLE_INCORRECT_SEQUENCE_NUMBER",
      messageParameters = Map(
        "messageType" -> messageType.toString,
        "writerId" -> writerId.toString,
        "readerId" -> readerId.toString,
        "expSeqNum" -> expSeqNum.toString,
        "actSeqNum" -> actSeqNum.toString))
  }

  def streamingShuffleUnexpectedMessageType(
      messageType: StreamingShuffleMessageType): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "STREAMING_SHUFFLE_UNEXPECTED_MESSAGE_TYPE",
      messageParameters = Map("messageType" -> messageType.toString))
  }

  def streamingShuffleWriterConnectionTimeout(
      shuffleId: Int,
      writerId: Int,
      readerId: Int,
      timeoutMs: Long): RuntimeException = {
    new SparkRuntimeException(
      errorClass = "STREAMING_SHUFFLE_WRITER_CONNECTION_TIMEOUT",
      messageParameters = Map(
        "shuffleId" -> shuffleId.toString,
        "writerId" -> writerId.toString,
        "readerId" -> readerId.toString,
        "timeoutMs" -> timeoutMs.toString))
  }
}

private[spark] class StreamingShuffleManager extends PipelinedShuffleManager with Logging {

  logInfo(log"Using StreamingShuffleManager")

  override def supportsBatchExecution: Boolean = true
  override def supportsLiveReducePartitionHints: Boolean = true
  override def advisoryBytesPerStream: Long =
    SparkEnv.get.conf.get(STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE).toLong

  // Connections and their protocol state belong to readers; I/O workers and the allocator are
  // shared for the executor's lifetime. Initialize on first use so ordinary batch jobs pay nothing.
  private var clientContext: TransportContext = _
  private var clientFactory: TransportClientFactory = _
  private var serverContext: TransportContext = _
  private var serverFactory: TransportServerFactory = _
  private var stopped = false

  private[streaming] def sharedClientFactory: TransportClientFactory = synchronized {
    require(!stopped, "Streaming shuffle manager has stopped")
    if (clientFactory == null) {
      val conf = SparkEnv.get.conf
      clientContext = StreamingShuffleManager.createClientContext(
        conf, conf.get(RPC_IO_THREADS).getOrElse(0))
      try {
        clientFactory = clientContext.createClientFactory()
      } catch {
        case t: Throwable =>
          clientContext.close()
          clientContext = null
          throw t
      }
    }
    clientFactory
  }

  private[streaming] def sharedServerFactory: TransportServerFactory = synchronized {
    require(!stopped, "Streaming shuffle manager has stopped")
    if (serverFactory == null) {
      val conf = SparkEnv.get.conf
      val role = conf.get(EXECUTOR_ID).map { id =>
        if (SparkContext.isDriver(id)) "driver" else "executor"
      }
      val transportConf = SparkTransportConf.fromSparkConf(
        conf, "streaming-shuffle-writer", conf.get(RPC_IO_THREADS).getOrElse(0), role)
      serverContext = new TransportContext(transportConf, new NoOpRpcHandler())
      try {
        serverFactory = serverContext.createServerFactory()
      } catch {
        case t: Throwable =>
          serverContext.close()
          serverContext = null
          throw t
      }
    }
    serverFactory
  }

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    val diagnostics = StreamingShuffleMetrics.names.map { name =>
      name -> dependency.rdd.context.longAccumulator(s"streamingShuffle.$shuffleId.$name")
    }.toMap
    new StreamingShuffleHandle(shuffleId, dependency, diagnostics)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val streamingShuffleHandle = handle.asInstanceOf[StreamingShuffleHandle[K, V, _]]
    new StreamingShuffleWriter[K, V](streamingShuffleHandle, mapId, context,
      sharedServerFactory = Some(sharedServerFactory), writeMetrics = Option(metrics))
  }

  /** Each reader consumes one complete reduce partition, independent of its task's index. */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    require(endPartition == startPartition + 1,
      "Streaming shuffle requires exactly one complete reduce partition per reader")
    val streamingShuffleHandle = handle.asInstanceOf[StreamingShuffleHandle[K, _, C]]
    new StreamingShuffleReader[K, C](streamingShuffleHandle, context,
      sharedClientFactory = Some(sharedClientFactory), reducePartitionId = Some(startPartition),
      readMetrics = Option(metrics))
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    // No manager-side state to release here: the driver's StreamingShuffleOutputTracker is
    // unregistered in ContextCleaner.doCleanupShuffle (its state is driver-only), and per-task
    // writer and reader resources are released via task completion listeners.
    true
  }

  override def stop(): Unit = synchronized {
    stopped = true
    try {
      if (clientFactory != null) {
        try {
          clientFactory.close()
        } finally {
          clientContext.close()
          clientFactory = null
          clientContext = null
        }
      }
    } finally {
      if (serverFactory != null) {
        try {
          serverFactory.close()
        } finally {
          serverContext.close()
          serverFactory = null
          serverContext = null
        }
      }
    }
  }
}
