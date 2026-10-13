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

import org.apache.spark.{Aggregator, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.checksum.RowBasedChecksum
import org.apache.spark.storage.{ShuffleBlockId, TimeTrackingOutputStream}

private[spark] final class BatchMemoryShuffleWriter[K, V](
    handle: BatchMemoryShuffleHandle[K, V, _],
    mapId: Long,
    context: TaskContext,
    metrics: ShuffleWriteMetricsReporter,
    manager: BatchMemoryShuffleManager) extends ShuffleWriter[K, V] {
  private val env = SparkEnv.get
  private val partitioner = handle.dependency.partitioner
  private val serializer = handle.dependency.serializer.newInstance()
  private val rowChecksums = handle.dependency.rowBasedChecksums
  private val live = Option(context.getLocalProperty(
    SparkContext.SPARK_PIPELINED_LIVE_REDUCE_PARTITIONS))
    .map(_.split(",").filter(_.nonEmpty).map(_.toInt).toSet)
  private val output = manager.newOutput(handle, mapId, context)
  private val streams = new Array[SerializationStream](partitioner.numPartitions)
  private var status: MapStatus = _
  private var stopped = false

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    try {
      val input = if (handle.dependency.mapSideCombine) {
        handle.dependency.aggregator.get.asInstanceOf[Aggregator[K, V, Any]]
          .combineValuesByKey(records.asInstanceOf[Iterator[(K, V)]], context)
      } else records
      var rows = 0
      while (input.hasNext) {
        if ((rows & 1023) == 0) {
          context.killTaskIfInterrupted()
          context.yieldExecution()
        }
        rows += 1
        val record = input.next()
        val partition = partitioner.getPartition(record._1)
        if (live.forall(_.contains(partition))) {
          if (streams(partition) == null) {
            val block = ShuffleBlockId(handle.shuffleId, mapId, partition)
            val raw = new TimeTrackingOutputStream(metrics, output.output(partition))
            streams(partition) = serializer.serializeStream(
              env.serializerManager.wrapStream(block, raw),
              env.serializerManager.serializerNeedsBuffering(block))
          }
          streams(partition).writeKey[Any](record._1).writeValue[Any](record._2)
          if (rowChecksums.nonEmpty) rowChecksums(partition).update(record._1, record._2)
          metrics.incRecordsWritten(1)
        }
      }
      streams.filter(_ != null).foreach(_.close())
      val commitStart = System.nanoTime()
      output.commit()
      metrics.incWriteTime(System.nanoTime() - commitStart)
      handle.sinkNs.add(context.taskMetrics().shuffleWriteMetrics.writeTime)
      handle.earlyReadPages.add(output.pagesReadBeforeCommit)
      val lengths = output.lengths
      metrics.incBytesWritten(lengths.sum)
      status = MapStatus(env.blockManager.shuffleServerId, lengths, mapId,
        RowBasedChecksum.getAggregatedChecksumValue(rowChecksums))
    } catch {
      case error: Throwable =>
        output.fail(error)
        streams.filter(_ != null).foreach { stream =>
          try stream.close() catch { case secondary: Throwable => error.addSuppressed(secondary) }
        }
        throw error
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopped) return None
    stopped = true
    if (success) Option(status) else {
      output.fail(new java.io.IOException("Live shuffle producer failed"))
      None
    }
  }

  override def getPartitionLengths(): Array[Long] = output.lengths
}
