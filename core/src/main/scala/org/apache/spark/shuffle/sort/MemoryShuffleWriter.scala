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

package org.apache.spark.shuffle.sort

import java.util.zip.CheckedOutputStream

import org.apache.spark.{ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.shuffle.checksum.{RowBasedChecksum, ShuffleChecksumSupport}
import org.apache.spark.storage.{ShuffleBlockId, TimeTrackingOutputStream}
import org.apache.spark.util.LongAccumulator

/** Same partition streams as bypass merge sort, with output owned by the executor resolver. */
private[spark] class MemoryShuffleWriter[K, V](
    handle: MemoryShuffleHandle[K, V],
    mapId: Long,
    context: TaskContext,
    metrics: ShuffleWriteMetricsReporter,
    resolver: MemoryShuffleBlockResolver)
  extends ShuffleWriter[K, V] with ShuffleChecksumSupport {

  private val env = SparkEnv.get
  private val dependency = handle.dependency
  private val partitioner = dependency.partitioner
  private val serializer = dependency.serializer.newInstance()
  private val checksums = createPartitionChecksums(partitioner.numPartitions, env.conf)
  private val rowChecksums = dependency.rowBasedChecksums
  private val streams = new Array[SerializationStream](partitioner.numPartitions)
  private val output = resolver.newMapOutput(handle.shuffleId, mapId, partitioner.numPartitions,
    bytes => handle.evictedBytes.add(bytes))
  private var lengths: Array[Long] = _
  private var status: MapStatus = _
  private var stopped = false

  context.addTaskCompletionListener[Unit] { _ => output.abort() }

  override def write(records: Iterator[Product2[K, V]]): Unit = {
    try {
      while (records.hasNext) {
        val record = records.next()
        val partition = partitioner.getPartition(record._1)
        if (streams(partition) == null) {
          val blockId = ShuffleBlockId(handle.shuffleId, mapId, partition)
          val raw = new TimeTrackingOutputStream(metrics, output.output(partition))
          val checked = if (checksums.isEmpty) raw else {
            new CheckedOutputStream(raw, checksums(partition))
          }
          streams(partition) = serializer.serializeStream(env.serializerManager.wrapStream(
            blockId, checked), env.serializerManager.serializerNeedsBuffering(blockId))
        }
        streams(partition).writeKey[Any](record._1).writeValue[Any](record._2)
        if (rowChecksums.nonEmpty) rowChecksums(partition).update(record._1, record._2)
        metrics.incRecordsWritten(1)
      }
      streams.filter(_ != null).foreach(_.close())
      val start = System.nanoTime()
      lengths = output.commit(getChecksumValues(checksums))
      var totalBytes = 0L
      var index = 0
      while (index < lengths.length) {
        totalBytes += lengths(index)
        index += 1
      }
      handle.pageBytes.add(output.pageBytesAllocated)
      metrics.incBytesWritten(totalBytes)
      metrics.incWriteTime(System.nanoTime() - start)
      if (output.isInMemory) handle.memoryBytes.add(totalBytes)
      else handle.diskBytes.add(totalBytes)
      status = MapStatus(env.blockManager.shuffleServerId, lengths, mapId,
        RowBasedChecksum.getAggregatedChecksumValue(rowChecksums))
    } catch {
      case error: Throwable =>
        // Closing a serializer may emit trailers. Do that before invalidating its output.
        streams.filter(_ != null).foreach { stream =>
          try stream.close() catch { case secondary: Throwable => error.addSuppressed(secondary) }
        }
        output.abort()
        throw error
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopped) return None
    stopped = true
    if (success) Option(status) else {
      output.abort()
      resolver.removeDataByMap(handle.shuffleId, mapId)
      None
    }
  }

  override def getPartitionLengths(): Array[Long] = lengths
}

private[spark] class MemoryShuffleHandle[K, V](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, V])
  extends BypassMergeSortShuffleHandle[K, V](shuffleId, dependency) {

  override def supportsHostLocalDiskRead: Boolean = false

  val memoryBytes: LongAccumulator =
    dependency.rdd.context.longAccumulator(s"memoryShuffle.$shuffleId.memoryBytesWritten")
  val diskBytes: LongAccumulator =
    dependency.rdd.context.longAccumulator(s"memoryShuffle.$shuffleId.diskBytesWritten")
  val evictedBytes: LongAccumulator =
    dependency.rdd.context.longAccumulator(s"memoryShuffle.$shuffleId.evictedBytes")
  val pageBytes: LongAccumulator =
    dependency.rdd.context.longAccumulator(s"memoryShuffle.$shuffleId.pageBytesAllocated")
}
