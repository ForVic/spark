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

package org.apache.spark.sql.execution

import java.util.Arrays

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.morsel.{Morsel, MorselPartition, MorselSource}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.ShuffleBlockId

sealed trait ShufflePartitionSpec

// A partition that reads data of one or more reducers, from `startReducerIndex` (inclusive) to
// `endReducerIndex` (exclusive).
case class CoalescedPartitionSpec(
    startReducerIndex: Int,
    endReducerIndex: Int,
    @transient dataSize: Option[Long] = None) extends ShufflePartitionSpec

object CoalescedPartitionSpec {
  def apply(startReducerIndex: Int,
            endReducerIndex: Int,
            dataSize: Long): CoalescedPartitionSpec = {
    CoalescedPartitionSpec(startReducerIndex, endReducerIndex, Some(dataSize))
  }
}

// A partition that reads partial data of one reducer, from `startMapIndex` (inclusive) to
// `endMapIndex` (exclusive).
case class PartialReducerPartitionSpec(
    reducerIndex: Int,
    startMapIndex: Int,
    endMapIndex: Int,
    @transient dataSize: Long) extends ShufflePartitionSpec

// A partition that reads partial data of one mapper, from `startReducerIndex` (inclusive) to
// `endReducerIndex` (exclusive).
case class PartialMapperPartitionSpec(
    mapIndex: Int,
    startReducerIndex: Int,
    endReducerIndex: Int) extends ShufflePartitionSpec

// TODO(SPARK-36234): Consider mapper location and shuffle block size when coalescing mappers
case class CoalescedMapperPartitionSpec(
    startMapIndex: Int,
    endMapIndex: Int,
    numReducers: Int) extends ShufflePartitionSpec

/**
 * The [[Partition]] used by [[ShuffledRowRDD]].
 */
private final case class ShuffledRowRDDPartition(
  index: Int, spec: ShufflePartitionSpec) extends Partition

/**
 * A Partitioner that might group together one or more partitions from the parent.
 *
 * @param parent a parent partitioner
 * @param partitionStartIndices indices of partitions in parent that should create new partitions
 *   in child (this should be an array of increasing partition IDs). For example, if we have a
 *   parent with 5 partitions, and partitionStartIndices is [0, 2, 4], we get three output
 *   partitions, corresponding to partition ranges [0, 1], [2, 3] and [4] of the parent partitioner.
 */
class CoalescedPartitioner(val parent: Partitioner, val partitionStartIndices: Array[Int])
  extends Partitioner {

  @transient private lazy val parentPartitionMapping: Array[Int] = {
    val n = parent.numPartitions
    val result = new Array[Int](n)
    for (i <- partitionStartIndices.indices) {
      val start = partitionStartIndices(i)
      val end = if (i < partitionStartIndices.length - 1) partitionStartIndices(i + 1) else n
      for (j <- start until end) {
        result(j) = i
      }
    }
    result
  }

  override def numPartitions: Int = partitionStartIndices.length

  override def getPartition(key: Any): Int = {
    parentPartitionMapping(parent.getPartition(key))
  }

  override def equals(other: Any): Boolean = other match {
    case c: CoalescedPartitioner =>
      c.parent == parent && Arrays.equals(c.partitionStartIndices, partitionStartIndices)
    case _ =>
      false
  }

  override def hashCode(): Int = 31 * parent.hashCode() + Arrays.hashCode(partitionStartIndices)
}

/**
 * This is a specialized version of [[org.apache.spark.rdd.ShuffledRDD]] that is optimized for
 * shuffling rows instead of Java key-value pairs. Note that something like this should eventually
 * be implemented in Spark core, but that is blocked by some more general refactorings to shuffle
 * interfaces / internals.
 *
 * This RDD takes a [[ShuffleDependency]] (`dependency`),
 * and an array of [[ShufflePartitionSpec]] as input arguments.
 *
 * The `dependency` has the parent RDD of this RDD, which represents the dataset before shuffle
 * (i.e. map output). Elements of this RDD are (partitionId, Row) pairs.
 * Partition ids should be in the range [0, numPartitions - 1].
 * `dependency.partitioner` is the original partitioner used to partition
 * map output, and `dependency.partitioner.numPartitions` is the number of pre-shuffle partitions
 * (i.e. the number of partitions of the map output).
 */
class ShuffledRowRDD(
    var dependency: ShuffleDependency[Int, InternalRow, InternalRow],
    metrics: Map[String, SQLMetric],
    partitionSpecs: Array[ShufflePartitionSpec])
  extends RDD[InternalRow](dependency.rdd.context, Nil) with MorselSource {

  def this(
      dependency: ShuffleDependency[Int, InternalRow, InternalRow],
      metrics: Map[String, SQLMetric]) = {
    this(dependency, metrics,
      Array.tabulate(dependency.partitioner.numPartitions)(i => CoalescedPartitionSpec(i, i + 1)))
  }

  dependency.rdd.context.setLocalProperty(
    SortShuffleManager.FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY,
    SQLConf.get.fetchShuffleBlocksInBatch.toString)

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override val partitioner: Option[Partitioner] =
    if (partitionSpecs.forall(_.isInstanceOf[CoalescedPartitionSpec])) {
      val indices = partitionSpecs.map(_.asInstanceOf[CoalescedPartitionSpec].startReducerIndex)
      // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
      if (indices.toSet.size == partitionSpecs.length) {
        Some(new CoalescedPartitioner(dependency.partitioner, indices))
      } else {
        None
      }
    } else {
      None
    }

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](partitionSpecs.length) { i =>
      ShuffledRowRDDPartition(i, partitionSpecs(i))
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    partition.asInstanceOf[ShuffledRowRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        // TODO order by partition size.
        startReducerIndex.until(endReducerIndex).flatMap { reducerIndex =>
          tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)
        }

      case PartialReducerPartitionSpec(_, startMapIndex, endMapIndex, _) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)

      case PartialMapperPartitionSpec(mapIndex, _, _) =>
        tracker.getMapLocation(dependency, mapIndex, mapIndex + 1)

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)
    }
  }

  /**
   * Splits a partition into morsels whose payloads are the [[ShufflePartitionSpec]]s they read:
   *  - a read of several reducer partitions (AQE coalescing) into one morsel per reducer
   *    partition. Each holds every row of its keys, so key-grouped operators (final aggregation,
   *    windows, joins) can process it on its own.
   *  - a local read of several map outputs into one morsel per map output, and a local read of
   *    one map output into one morsel per reducer partition. These only suit row-wise pipelines.
   * Empty morsels are dropped: they produce no rows.
   */
  override private[spark] def splitIntoMorsels(
      split: Partition,
      targetSize: Long): IndexedSeq[Morsel] = {
    morselsOf(split.asInstanceOf[ShuffledRowRDDPartition].spec)
      .map(_.filter(_.sizeInBytes > 0))
      .getOrElse(IndexedSeq(Morsel(split, 0L)))
  }

  /** Reducer partitions of different shuffles hold the same keys, so they align across sides. */
  override private[spark] def splitIntoAlignedMorsels(
      split: Partition): Option[IndexedSeq[Morsel]] = {
    split.asInstanceOf[ShuffledRowRDDPartition].spec match {
      case spec: CoalescedPartitionSpec => morselsOf(spec)
      case _ => None
    }
  }

  /** Merges adjacent morsels into one read; empty reducer partitions in between hold no rows. */
  override private[spark] def mergeMorsels(first: AnyRef, next: AnyRef): Option[AnyRef] =
    (first, next) match {
      case (CoalescedPartitionSpec(start, end, _), CoalescedPartitionSpec(nextStart, nextEnd, _))
          if end <= nextStart =>
        Some(CoalescedPartitionSpec(start, nextEnd))
      case (CoalescedMapperPartitionSpec(start, end, numReducers),
          CoalescedMapperPartitionSpec(nextStart, nextEnd, nextNumReducers))
          if end <= nextStart && numReducers == nextNumReducers =>
        Some(CoalescedMapperPartitionSpec(start, nextEnd, numReducers))
      case (PartialMapperPartitionSpec(mapIndex, start, end),
          PartialMapperPartitionSpec(nextMapIndex, nextStart, nextEnd))
          if mapIndex == nextMapIndex && end <= nextStart =>
        Some(PartialMapperPartitionSpec(mapIndex, start, nextEnd))
      case (PartialReducerPartitionSpec(reducer, start, end, size),
          PartialReducerPartitionSpec(nextReducer, nextStart, nextEnd, nextSize))
          if reducer == nextReducer && end == nextStart =>
        Some(PartialReducerPartitionSpec(reducer, start, nextEnd, size + nextSize))
      case _ => None
    }

  override private[spark] def joinSideId: Option[Int] = Some(dependency.shuffleId)

  override private[spark] def partitionSize(split: Partition): Option[Long] =
    split.asInstanceOf[ShuffledRowRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        blockSizes(0, Int.MaxValue, startReducerIndex, endReducerIndex, 1)((_, _) => 0)
          .map(_.head)
      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        blockSizes(startMapIndex, endMapIndex, reducerIndex, reducerIndex + 1, 1)((_, _) => 0)
          .map(_.head)
      case _ => None
    }

  /**
   * Splits each reducer partition that a partition reads into ranges of map outputs of about
   * `targetSize` bytes, like AQE's skew join splitting: a range of a reducer partition joins with
   * the whole reducer partition of the other side.
   */
  override private[spark] def splitIntoOneSidedMorsels(
      split: Partition,
      targetSize: Long): Option[IndexedSeq[Morsel]] =
    split.asInstanceOf[ShuffledRowRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        mapRangeMorsels(startReducerIndex, endReducerIndex, 0, Int.MaxValue, targetSize)
      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        mapRangeMorsels(reducerIndex, reducerIndex + 1, startMapIndex, endMapIndex, targetSize)
      case _ => None
    }

  private def mapRangeMorsels(
      startReducerIndex: Int,
      endReducerIndex: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      targetSize: Long): Option[IndexedSeq[Morsel]] = {
    // Block sizes by reducer partition and map index.
    val sizes = Array.fill(endReducerIndex - startReducerIndex)(mutable.TreeMap.empty[Int, Long])
    var onlyShuffleBlocks = true
    SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(dependency.shuffleId,
        startMapIndex, endMapIndex, startReducerIndex, endReducerIndex)
      .foreach { case (_, blocks) =>
        blocks.foreach {
          case (block: ShuffleBlockId, size, mapIndex) =>
            val reducerSizes = sizes(block.reduceId - startReducerIndex)
            reducerSizes(mapIndex) = reducerSizes.getOrElse(mapIndex, 0L) + size
          case _ => onlyShuffleBlocks = false
        }
      }
    if (!onlyShuffleBlocks) {
      return None
    }
    Some(sizes.indices.flatMap { i =>
      val reducer = startReducerIndex + i
      val ranges = new mutable.ArrayBuffer[Morsel]
      var rangeStart = startMapIndex
      var rangeSize = 0L
      sizes(i).foreach { case (mapIndex, size) =>
        if (rangeSize > 0 && rangeSize + size > targetSize) {
          ranges += Morsel(
            PartialReducerPartitionSpec(reducer, rangeStart, mapIndex, rangeSize), rangeSize)
          rangeStart = mapIndex
          rangeSize = 0L
        }
        rangeSize += size
      }
      if (ranges.isEmpty && startMapIndex == 0 && endMapIndex == Int.MaxValue) {
        ranges += Morsel(CoalescedPartitionSpec(reducer, reducer + 1), rangeSize)
      } else {
        ranges += Morsel(
          PartialReducerPartitionSpec(reducer, rangeStart, endMapIndex, rangeSize), rangeSize)
      }
      ranges
    })
  }

  private def morselsOf(spec: ShufflePartitionSpec): Option[IndexedSeq[Morsel]] = spec match {
    case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
      blockSizes(0, Int.MaxValue, startReducerIndex, endReducerIndex,
          endReducerIndex - startReducerIndex)((block, _) => block.reduceId - startReducerIndex)
        .map(sizes => sizes.indices.map { i =>
          val r = startReducerIndex + i
          Morsel(CoalescedPartitionSpec(r, r + 1), sizes(i))
        })
    case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
      blockSizes(startMapIndex, endMapIndex, 0, numReducers,
          endMapIndex - startMapIndex)((_, mapIndex) => mapIndex - startMapIndex)
        .map(sizes => sizes.indices.map { i =>
          val m = startMapIndex + i
          Morsel(CoalescedMapperPartitionSpec(m, m + 1, numReducers), sizes(i))
        })
    case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
      blockSizes(mapIndex, mapIndex + 1, startReducerIndex, endReducerIndex,
          endReducerIndex - startReducerIndex)((block, _) => block.reduceId - startReducerIndex)
        .map(sizes => sizes.indices.map { i =>
          val r = startReducerIndex + i
          Morsel(PartialMapperPartitionSpec(mapIndex, r, r + 1), sizes(i))
        })
    case _ => None
  }

  /**
   * Sizes of map output blocks, from the map statuses the reader fetches anyway. None if the read
   * includes other kinds of blocks (e.g. push-merged ones), whose rows morsels can't attribute.
   */
  private def blockSizes(
      startMapIndex: Int,
      endMapIndex: Int,
      startReducerIndex: Int,
      endReducerIndex: Int,
      numSlots: Int)(slot: (ShuffleBlockId, Int) => Int): Option[Array[Long]] = {
    val sizes = new Array[Long](numSlots)
    var onlyShuffleBlocks = true
    SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(dependency.shuffleId,
        startMapIndex, endMapIndex, startReducerIndex, endReducerIndex)
      .foreach { case (_, blocks) =>
        blocks.foreach {
          case (block: ShuffleBlockId, size, mapIndex) => sizes(slot(block, mapIndex)) += size
          case _ => onlyShuffleBlocks = false
        }
      }
    if (onlyShuffleBlocks) Some(sizes) else None
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] =
    split match {
      // Read the claimed morsels one at a time.
      case morsels: MorselPartition =>
        morsels.morsels.flatMap(spec => read(spec.asInstanceOf[ShufflePartitionSpec], context))
      case _ => read(split.asInstanceOf[ShuffledRowRDDPartition].spec, context)
    }

  private def read(spec: ShufflePartitionSpec, context: TaskContext): Iterator[InternalRow] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    // `SQLShuffleReadMetricsReporter` will update its own metrics for SQL exchange operator,
    // as well as the `tempMetrics` for basic shuffle metrics.
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)
    val reader = spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)

      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          reducerIndex,
          reducerIndex + 1,
          context,
          sqlMetricsReporter)

      case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          mapIndex,
          mapIndex + 1,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          0,
          numReducers,
          context,
          sqlMetricsReporter)
    }
    reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    dependency = null
  }
}
