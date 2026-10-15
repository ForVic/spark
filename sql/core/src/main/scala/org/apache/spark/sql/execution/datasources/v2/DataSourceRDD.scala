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

package org.apache.spark.sql.execution.datasources.v2

import java.util.concurrent.ConcurrentHashMap

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.morsel.{Morsel, MorselPartition, MorselSource}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{HasPartitionStatistics, InputPartition, PartitionReader, PartitionReaderFactory, SplittableInputPartition}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.ArrayImplicits._

class DataSourceRDDPartition(val index: Int, val inputPartition: Option[InputPartition])
  extends Partition with Serializable

/**
 * Holds the state for a reader in a task, used by the completion listener to access the most
 * recently created reader and iterator for final metrics updates and cleanup.
 *
 * When `compute()` is called multiple times for the same task (e.g., when DataSourceRDD is
 * coalesced), this state is updated on each call to track the most recent reader. The task
 * completion listener then uses this most recent reader for final cleanup and metrics reporting.
 *
 * @param reader The partition reader
 * @param iterator The metrics iterator wrapping the reader
 */
private case class ReaderState(reader: PartitionReader[_], iterator: MetricsIterator[_])

// TODO: we should have 2 RDDs: an RDD[InternalRow] for row-based scan, an `RDD[ColumnarBatch]` for
// columnar scan.
/**
 * An RDD that reads data from a V2 data source.
 *
 * This RDD handles both row-based and columnar reads, tracks custom metrics from the data source,
 * and ensures that task completion listeners are added only once per thread to avoid duplicate
 * metric updates and resource cleanup.
 *
 * @param sc The Spark context
 * @param inputPartitions The input partitions to read from
 * @param partitionReaderFactory Factory for creating partition readers
 * @param columnarReads Whether to use columnar reads
 * @param customMetrics Custom metrics defined by the data source
 */
class DataSourceRDD(
    sc: SparkContext,
    @transient private val inputPartitions: Seq[Option[InputPartition]],
    partitionReaderFactory: PartitionReaderFactory,
    columnarReads: Boolean,
    customMetrics: Map[String, SQLMetric])
  extends RDD[InternalRow](sc, Nil) with MorselSource {

  /**
   * Splits a file of a [[FilePartition]] (of a V2 file source) into morsels of about the given
   * size, returning each with its size. Whole files by default.
   */
  @volatile private[sql] var fileSplitter: (PartitionedFile, Long) => Seq[(PartitionedFile, Long)] =
    (file, _) => Seq((file, file.length))

  // Map from task attempt ID to the most recently created ReaderState for that task.
  // When compute() is called multiple times for the same task (due to coalescing), the map entry
  // is updated each time so the completion listener always closes the last reader.
  @transient private lazy val taskReaderStates = new ConcurrentHashMap[Long, ReaderState]()

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex.map {
      case (inputPartition, index) => new DataSourceRDDPartition(index, inputPartition)
    }.toArray
  }

  private def castPartition(split: Partition): DataSourceRDDPartition = split match {
    case p: DataSourceRDDPartition => p
    case _ => throw QueryExecutionErrors.notADatasourceRDDPartitionError(split)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val taskAttemptId = context.taskAttemptId()

    // Add completion listener only once per task attempt. When compute() is called a second time
    // for the same task (e.g., due to coalescing), the first call will have already put a
    // ReaderState into taskReaderStates, so containsKey returns true and we skip this block.
    if (!taskReaderStates.containsKey(taskAttemptId)) {
      context.addTaskCompletionListener[Unit] { ctx =>
        // In case of early stopping before consuming the entire iterator,
        // we need to do one more metric update at the end of the task.
        try {
          val readerState = taskReaderStates.get(ctx.taskAttemptId())
          if (readerState != null) {
            CustomMetrics.updateMetrics(
              readerState.reader.currentMetricsValues.toImmutableArraySeq, customMetrics)
            readerState.iterator.forceUpdateMetrics()
            readerState.reader.close()
          }
        } finally {
          taskReaderStates.remove(ctx.taskAttemptId())
        }
      }
    }

    val inputPartitions = split match {
      case morsels: MorselPartition => morsels.morsels.map(_.asInstanceOf[InputPartition])
      case _ => castPartition(split).inputPartition.iterator
    }
    inputPartitions.flatMap { inputPartition =>
      val (iter, reader) = if (columnarReads) {
        val batchReader = partitionReaderFactory.createColumnarReader(inputPartition)
        val iter = new MetricsBatchIterator(
          new PartitionIterator[ColumnarBatch](batchReader, customMetrics))
        (iter, batchReader)
      } else {
        val rowReader = partitionReaderFactory.createReader(inputPartition)
        val iter = new MetricsRowIterator(
          new PartitionIterator[InternalRow](rowReader, customMetrics))
        (iter, rowReader)
      }

      // Flush metrics and close the previous reader before advancing to the next one.
      // Pass the accumulated metrics to the new reader so they carry forward correctly.
      val prevState = taskReaderStates.get(taskAttemptId)
      if (prevState != null) {
        val metrics = prevState.reader.currentMetricsValues
        CustomMetrics.updateMetrics(metrics.toImmutableArraySeq, customMetrics)
        reader.initMetricsValues(metrics)
        prevState.reader.close()
      }

      // Update the map so the completion listener always references the latest reader.
      taskReaderStates.put(taskAttemptId, ReaderState(reader, iter))

      // TODO: SPARK-25083 remove the type erasure hack in data source scan
      new InterruptibleIterator(context, iter.asInstanceOf[Iterator[InternalRow]])
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    castPartition(split).inputPartition.toSeq.flatMap(_.preferredLocations())
  }

  /**
   * Splits the input partition of a task into morsels: the files of a V2 file source's
   * [[FilePartition]], further split by `fileSplitter`, or the pieces of a
   * [[SplittableInputPartition]]. Other input partitions are a single morsel.
   */
  override private[spark] def splitIntoMorsels(
      split: Partition,
      targetSize: Long): IndexedSeq[Morsel] = castPartition(split).inputPartition match {
    case Some(partition: FilePartition) =>
      partition.files.toIndexedSeq.flatMap(fileSplitter(_, targetSize)).map { case (file, size) =>
        Morsel(FilePartition(partition.index, Array(file)), size)
      }
    case Some(partition: SplittableInputPartition) =>
      val pieces = partition.split(targetSize).toIndexedSeq
      // Each piece must be readable like the partition, see SplittableInputPartition.
      if (pieces.forall(partitionReaderFactory.supportColumnarReads(_) == columnarReads)) {
        pieces.map(piece => Morsel(piece, sizeInBytes(piece).getOrElse(targetSize)))
      } else {
        IndexedSeq(Morsel(partition, sizeInBytes(partition).getOrElse(targetSize)))
      }
    case Some(partition) => IndexedSeq(Morsel(partition, sizeInBytes(partition).getOrElse(0L)))
    case None => IndexedSeq.empty
  }

  /** Reads adjacent files of a [[FilePartition]] together. */
  override private[spark] def mergeMorsels(first: AnyRef, next: AnyRef): Option[AnyRef] =
    (first, next) match {
      case (a: FilePartition, b: FilePartition) => Some(FilePartition(a.index, a.files ++ b.files))
      case _ => None
    }

  private def sizeInBytes(partition: InputPartition): Option[Long] = partition match {
    case p: HasPartitionStatistics if p.sizeInBytes().isPresent => Some(p.sizeInBytes().getAsLong)
    case _ => None
  }
}

private class PartitionIterator[T](
    reader: PartitionReader[T],
    customMetrics: Map[String, SQLMetric]) extends Iterator[T] {
  private[this] var valuePrepared = false
  private[this] var hasMoreInput = true

  private var numRow = 0L

  override def hasNext: Boolean = {
    if (!valuePrepared && hasMoreInput) {
      hasMoreInput = reader.next()
      valuePrepared = hasMoreInput
    }
    valuePrepared
  }

  override def next(): T = {
    if (!hasNext) {
      throw QueryExecutionErrors.endOfStreamError()
    }
    if (numRow % CustomMetrics.NUM_ROWS_PER_UPDATE == 0) {
      CustomMetrics.updateMetrics(reader.currentMetricsValues.toImmutableArraySeq, customMetrics)
    }
    numRow += 1
    valuePrepared = false
    reader.get()
  }
}

private class MetricsHandler extends Logging with Serializable {
  private val inputMetrics = TaskContext.get().taskMetrics().inputMetrics
  private val startingBytesRead = inputMetrics.bytesRead
  private val getBytesRead = SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()

  def updateMetrics(numRows: Int, force: Boolean = false): Unit = {
    inputMetrics.incRecordsRead(numRows)
    val shouldUpdateBytesRead =
      inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0
    if (shouldUpdateBytesRead || force) {
      inputMetrics.setBytesRead(startingBytesRead + getBytesRead())
    }
  }
}

private abstract class MetricsIterator[I](iter: Iterator[I]) extends Iterator[I] {
  protected val metricsHandler = new MetricsHandler

  override def hasNext: Boolean = {
    if (iter.hasNext) {
      true
    } else {
      forceUpdateMetrics()
      false
    }
  }

  def forceUpdateMetrics(): Unit = metricsHandler.updateMetrics(0, force = true)
}

private class MetricsRowIterator(
    iter: Iterator[InternalRow]) extends MetricsIterator[InternalRow](iter) {
  override def next(): InternalRow = {
    val item = iter.next()
    metricsHandler.updateMetrics(1)
    item
  }
}

private class MetricsBatchIterator(
    iter: Iterator[ColumnarBatch]) extends MetricsIterator[ColumnarBatch](iter) {
  override def next(): ColumnarBatch = {
    val batch: ColumnarBatch = iter.next()
    metricsHandler.updateMetrics(batch.numRows)
    batch
  }
}
