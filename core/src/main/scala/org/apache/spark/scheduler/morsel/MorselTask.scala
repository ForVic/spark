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

package org.apache.spark.scheduler.morsel

import java.nio.ByteBuffer

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext, TaskContextImpl}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.{MapPartitionsRDD, RDD, UnionPartition, UnionRDD, ZippedPartitionsPartition, ZippedPartitionsRDD2}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.sort.{MorselShuffleWriter, MorselSpills, SortShuffleManager}
import org.apache.spark.storage.StorageLevel

/**
 * Runs a task as morsels when its RDD chain allows it: the chain must consist of
 * order-insensitive [[MapPartitionsRDD]]s, [[UnionRDD]]s and [[ZippedPartitionsRDD2]]s over
 * [[MorselSource]]s, and the task must write its records to a [[MorselSink]]: a shuffle map task
 * whose [[ShuffleDependency]] the planner marked as morsel-safe, or a result task whose RDD is a
 * [[MorselSinkRDD]].
 *
 * The task's thread and any helpers each run the pipeline over the morsels they claim and write
 * their records as their own part of the task's output; the task's thread then commits all parts
 * as the task's output. The output, the task's result and the task attempt are those of the task
 * as a whole, so retries, speculation and fetch failures behave as for any other task.
 */
private[spark] object MorselTask extends Logging {

  private type AnySink = MorselSink[Any, Any, Any, Any]

  /** Runs a shuffle map task as morsels; None if it must take the normal path. */
  def tryRunShuffleMapTask(
      taskBinary: Broadcast[Array[Byte]],
      rdd: RDD[_],
      dep: ShuffleDependency[_, _, _],
      partition: Partition,
      mapId: Long,
      context: TaskContext,
      isBarrier: Boolean,
      setHelperCpuTime: Long => Unit): Option[MapStatus] = {
    if (!dep.morselSafe || dep.shuffleMergeAllowed || !canConcatenateOutput(dep)) {
      return None
    }
    SparkEnv.get.shuffleManager match {
      case manager: SortShuffleManager =>
        tryRun(taskBinary, rdd, new ShuffleMorselSink(dep, mapId, manager), partition, context,
            isBarrier, dep.morselJoinSplits, setHelperCpuTime) {
          case (rdd: RDD[_], dep: ShuffleDependency[_, _, _]) =>
            (rdd, new ShuffleMorselSink(dep, mapId, manager))
        }.map(_.asInstanceOf[MapStatus])
      case _ => None
    }
  }

  /**
   * Runs a result task as morsels if its RDD is a [[MorselSinkRDD]], returning the RDD's records
   * for the task's function; None if the task must take the normal path.
   */
  def tryRunResultTask(
      taskBinary: Broadcast[Array[Byte]],
      rdd: RDD[_],
      partition: Partition,
      context: TaskContext,
      isBarrier: Boolean,
      setHelperCpuTime: Long => Unit): Option[Iterator[Any]] = rdd match {
    case sinkRdd: MorselSinkRDD[_, _] if isComputed(sinkRdd) =>
      tryRun(taskBinary, sinkRdd.prev, sinkRdd.sink, partition, context, isBarrier,
          sinkRdd.joinSplits, setHelperCpuTime) {
        case (rdd: MorselSinkRDD[_, _], _) => (rdd.prev, rdd.sink)
      }.map(Iterator(_))
    case _ => None
  }

  private def tryRun(
      taskBinary: Broadcast[Array[Byte]],
      pipeline: RDD[_],
      sink: MorselSink[_, _, _, _],
      partition: Partition,
      context: TaskContext,
      isBarrier: Boolean,
      joinSplits: Seq[MorselJoinSplit],
      setHelperCpuTime: Long => Unit)(
      participantTask: PartialFunction[AnyRef, (RDD[_], MorselSink[_, _, _, _])]): Option[Any] = {
    if (isBarrier) return None
    (MorselScheduler.get, context) match {
      case (Some(scheduler), owner: TaskContextImpl)
          if owner.getClass == classOf[TaskContextImpl] =>
        morselsOf(pipeline, partition, scheduler.targetMorselSize, aligned = false, joinSplits)
          .map(_.grouped(scheduler.targetMorselSize))
          .flatMap { split =>
            if (split.morsels.length >= 2) {
              val job = new SinkMorselJob(owner, split, taskBinary, partition,
                sink.asInstanceOf[AnySink], participantTask)
              Some(run(scheduler, owner, job, pipeline, partition, setHelperCpuTime))
            } else {
              // Nothing to share: compute the morsel, if any, on the task's thread and write it
              // the normal way, reusing what splitting found out (e.g. a Parquet footer, or that
              // a file range holds no row group).
              val morsels = split.morsels.iterator.map(_.payload)
              val records = pipeline.iterator(
                new MorselPartition(partition, morsels, split.zippedSide), owner)
              Some(sink.asInstanceOf[AnySink].write(records.asInstanceOf[Iterator[Any]], owner))
            }
          }
      case _ => None
    }
  }

  /** Whether the shuffle output of different participants can be concatenated. */
  private def canConcatenateOutput(dep: ShuffleDependency[_, _, _]): Boolean = {
    val env = SparkEnv.get
    SortShuffleManager.canUseSerializedShuffle(dep) &&
      !env.serializerManager.encryptionEnabled &&
      (!env.conf.get(config.SHUFFLE_COMPRESS) ||
        CompressionCodec.supportsConcatenationOfSerializedStreams(
          CompressionCodec.createCodec(env.conf)))
  }

  /**
   * The morsels of a task and how to merge adjacent ones (see [[MorselSource.mergeMorsels]]).
   *
   * @param zippedSide see [[MorselPartition]].
   */
  private case class MorselSplit(
      morsels: IndexedSeq[Morsel],
      merge: (AnyRef, AnyRef) => Option[AnyRef] = (_, _) => None,
      zippedSide: Option[Int] = None) {

    /**
     * Merges runs of adjacent small morsels into morsels of up to `targetSize`, so that
     * per-morsel costs (opening a file, setting up a shuffle fetch) stay small relative to
     * the work, e.g. for the many small reducer partitions of an AQE-coalesced read.
     */
    def grouped(targetSize: Long): MorselSplit = {
      val out = new scala.collection.mutable.ArrayBuffer[Morsel](morsels.length)
      morsels.foreach { morsel =>
        val merged = out.lastOption
          .filter(_.sizeInBytes + morsel.sizeInBytes <= targetSize)
          .flatMap(last => merge(last.payload, morsel.payload)
            .map(Morsel(_, last.sizeInBytes + morsel.sizeInBytes)))
        merged match {
          case Some(m) => out(out.length - 1) = m
          case None => out += morsel
        }
      }
      copy(morsels = out.toIndexedSeq)
    }
  }

  private def isComputed(rdd: RDD[_]): Boolean =
    rdd.getStorageLevel == StorageLevel.NONE && !rdd.isCheckpointedAndMaterialized

  /**
   * The morsels of the task's partition, if every RDD between the task's RDD and its sources
   * passes a [[MorselPartition]] down to its parents.
   *
   * @param aligned whether the partition is zipped with partitions of other sources, so its
   *                morsels must be aligned with theirs.
   * @param joinSplits which zipped partitions may be split one side only.
   */
  private def morselsOf(
      rdd: RDD[_],
      split: Partition,
      targetSize: Long,
      aligned: Boolean,
      joinSplits: Seq[MorselJoinSplit]): Option[MorselSplit] = {
    if (!isComputed(rdd)) {
      None
    } else {
      rdd match {
        case source: MorselSource =>
          if (aligned) {
            source.splitIntoAlignedMorsels(split).map(MorselSplit(_, source.mergeMorsels))
          } else {
            Some(MorselSplit(source.splitIntoMorsels(split, targetSize), source.mergeMorsels))
          }
        case m: MapPartitionsRDD[_, _]
            if m.getClass == classOf[MapPartitionsRDD[_, _]] && !m.isOrderSensitive &&
              m.prev != null =>
          morselsOf(m.prev, split, targetSize, aligned, joinSplits)
        case u: UnionRDD[_] if u.getClass == classOf[UnionRDD[_]] =>
          val part = split.asInstanceOf[UnionPartition[_]]
          morselsOf(u.rdds(part.parentRddIndex), part.parentPartition, targetSize, aligned,
            joinSplits)
        case z: ZippedPartitionsRDD2[_, _, _]
            if z.getClass == classOf[ZippedPartitionsRDD2[_, _, _]] =>
          val parts = split.asInstanceOf[ZippedPartitionsPartition].partitions
          val alignedSplit = for {
            left <- morselsOf(z.rdd1, parts(0), targetSize, aligned = true, joinSplits)
            right <- morselsOf(z.rdd2, parts(1), targetSize, aligned = true, joinSplits)
            if left.morsels.map(_.payload) == right.morsels.map(_.payload)
          } yield {
            val morsels = left.morsels.zip(right.morsels).map { case (l, r) =>
              Morsel(l.payload, l.sizeInBytes + r.sizeInBytes)
            }
            // Morsels that are empty on every side produce no rows. Aligned payloads are the same
            // on both sides, so merging them by one side's rules suits both.
            MorselSplit(if (aligned) morsels else morsels.filter(_.sizeInBytes > 0), left.merge)
          }
          // Aligned morsels need no side to be read more than once, but leave a task with one
          // dominant part (e.g. a skewed reducer partition) to one participant.
          val balanced = alignedSplit.exists { a =>
            val sizes = a.morsels.map(_.sizeInBytes)
            sizes.length >= 2 && sizes.max * 2 <= sizes.sum
          }
          if (aligned || balanced) {
            alignedSplit
          } else {
            oneSidedSplit(z, parts, targetSize, joinSplits).orElse(alignedSplit)
          }
        case _ => None
      }
    }
  }

  /**
   * Splits one side of a zipped partition into morsels that each join with the other side's
   * whole partition, which every participant reads, if the planner allows it for that side (see
   * [[MorselJoinSplit]]) and the other side is at most half as large, so that reading it once per
   * participant costs less than splitting saves.
   */
  private def oneSidedSplit(
      z: ZippedPartitionsRDD2[_, _, _],
      parts: Seq[Partition],
      targetSize: Long,
      joinSplits: Seq[MorselJoinSplit]): Option[MorselSplit] = {
    for {
      (left, leftPart) <- sourceOf(z.rdd1, parts(0))
      (right, rightPart) <- sourceOf(z.rdd2, parts(1))
      leftId <- left.joinSideId
      rightId <- right.joinSideId
      hints = joinSplits.filter(h => h.leftSideId == leftId && h.rightSideId == rightId)
      if hints.nonEmpty
      split <- Seq(
          (0, hints.forall(_.splitLeft), left, leftPart, right, rightPart),
          (1, hints.forall(_.splitRight), right, rightPart, left, leftPart))
        .filter(_._2)
        .flatMap { case (side, _, source, part, other, otherPart) =>
          for {
            morsels <- source.splitIntoOneSidedMorsels(part, targetSize)
            otherSize <- other.partitionSize(otherPart)
            size = morsels.iterator.map(_.sizeInBytes).sum
            if morsels.length >= 2 && otherSize * 2 <= size
          } yield (size, MorselSplit(morsels.filter(_.sizeInBytes > 0), source.mergeMorsels,
            Some(side)))
        }
        .sortBy(-_._1).headOption.map(_._2)
    } yield split
  }

  /** The source under order-insensitive [[MapPartitionsRDD]]s, with its partition. */
  private def sourceOf(rdd: RDD[_], split: Partition): Option[(MorselSource, Partition)] =
    if (!isComputed(rdd)) {
      None
    } else {
      rdd match {
        case source: MorselSource => Some((source, split))
        case m: MapPartitionsRDD[_, _]
            if m.getClass == classOf[MapPartitionsRDD[_, _]] && !m.isOrderSensitive &&
              m.prev != null =>
          sourceOf(m.prev, split)
        case _ => None
      }
    }

  private def run(
      scheduler: MorselScheduler,
      owner: TaskContextImpl,
      job: SinkMorselJob,
      pipeline: RDD[_],
      partition: Partition,
      setHelperCpuTime: Long => Unit): Any = {
    // Registered before the pipeline starts so that it runs after the pipeline's own completion
    // listeners (e.g. the scan's final input metrics update), which run in reverse order.
    owner.addTaskCompletionListener[Unit](_ => job.mergeHelperMetrics())
    val ownerMemory = owner.taskMemoryManager
    var ownerPart: Option[Any] = None
    var success = false
    ownerMemory.setMemoryReclaimer(() => job.reclaimMemory())
    scheduler.register(job)
    try {
      try {
        val records = pipeline.iterator(
          new MorselPartition(partition, job.ownerMorsels, job.zippedSide), owner)
        ownerPart = Some(job.sink.writePart(job.state, records.asInstanceOf[Iterator[Any]], owner))
      } catch {
        case t: Throwable =>
          job.fail(t)
          throw t
      } finally {
        scheduler.unregister(job)
        scheduler.lendSlotWhile(job) {
          job.closeAndAwaitHelpers()
        }
        ownerMemory.setMemoryReclaimer(null)
      }
      job.failureOption.foreach(throw _)
      // Morsels that aborted helpers gave back after the task's own pipeline finished.
      job.runRemainingMorsels()
      job.failureOption.foreach(throw _)
      val result = job.sink.commit(job.state, ownerPart.toSeq ++ job.helperResults, owner)
      setHelperCpuTime(job.helperCpuTimeNs)
      scheduler.jobsRun.incrementAndGet()
      scheduler.morselsStolen.addAndGet(job.morselsStolen)
      scheduler.helperAborts.addAndGet(job.helperAborts)
      logInfo(s"Morsel task ${owner.taskAttemptId} (stage ${owner.stageId}." +
        s"${owner.stageAttemptNumber} partition ${owner.partitionId}): " +
        s"${job.numMorsels} morsels, ${job.morselsStolen} stolen by ${job.helperRuns} helper " +
        s"runs (${job.helperAborts} aborted) using ${job.helperCpuTimeNs / 1000000} ms CPU")
      success = true
      result
    } finally {
      if (!success) {
        try {
          (ownerPart.toSeq ++ job.helperResults).foreach(job.sink.discard(job.state, _))
        } finally {
          job.sink.abort(job.state, owner)
        }
      }
    }
  }

  /** A task's morsels, written to a [[MorselSink]]. */
  private class SinkMorselJob(
      owner: TaskContextImpl,
      split: MorselSplit,
      taskBinary: Broadcast[Array[Byte]],
      partition: Partition,
      val sink: AnySink,
      participantTask: PartialFunction[AnyRef, (RDD[_], MorselSink[_, _, _, _])])
    extends MorselJob[Any](owner, split.morsels, split.merge) {

    val zippedSide: Option[Int] = split.zippedSide

    val state: Any = sink.open(owner)

    override protected def runPart(context: MorselTaskContext, morsels: Iterator[AnyRef]): Any = {
      // Each participant deserializes its own copy of the task so that operator state and metrics
      // are not shared with other threads. Accumulators register with the participant's context
      // and are merged into the owner's when the task completes.
      val ser = SparkEnv.get.closureSerializer.newInstance()
      val task = ser.deserialize[AnyRef](
        ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
      val participant = participantTask(task)
      val records = participant._1.iterator(
        new MorselPartition(partition, morsels, zippedSide), context)
      participant._2.asInstanceOf[AnySink]
        .writePart(state, records.asInstanceOf[Iterator[Any]], context)
    }

    override protected def discardPart(part: Any): Unit = sink.discard(state, part)
  }

  /** The output of a task's shuffle map output, see [[MorselShuffleWriter]]. */
  private case class ShuffleOutput(
      writer: MorselShuffleWriter[Any, Any],
      writeMetrics: ShuffleWriteMetricsReporter)

  /** Writes the map output of a shuffle map task. */
  private class ShuffleMorselSink(
      dep: ShuffleDependency[_, _, _],
      mapId: Long,
      manager: SortShuffleManager)
    extends MorselSink[Product2[Any, Any], ShuffleOutput, MorselSpills, MapStatus] {

    override def write(records: Iterator[Product2[Any, Any]], context: TaskContext): MapStatus =
      dep.shuffleWriterProcessor.write(records, dep, mapId, context.partitionId(), context)

    override def open(context: TaskContext): ShuffleOutput = ShuffleOutput(
      manager.getMorselWriter[Any, Any](dep.shuffleHandle, mapId),
      dep.shuffleWriterProcessor.morselMetricsReporter(context))

    override def writePart(
        output: ShuffleOutput,
        records: Iterator[Product2[Any, Any]],
        context: TaskContext): MorselSpills = {
      // The serializer and checksums of this participant's copy of the dependency.
      val metrics = if (context.isInstanceOf[MorselTaskContext]) {
        dep.shuffleWriterProcessor.morselMetricsReporter(context)
      } else {
        output.writeMetrics
      }
      val spills = output.writer.write(records, context, metrics, dep.serializer,
        dep.rowBasedChecksums)
      context.taskMetrics().incPeakExecutionMemory(spills.peakMemoryUsedBytes)
      spills
    }

    override def commit(
        output: ShuffleOutput,
        parts: Seq[MorselSpills],
        context: TaskContext): MapStatus =
      output.writer.commit(parts.toArray, output.writeMetrics, dep.rowBasedChecksums,
        dep.morselOutputVariesWithSplit)

    override def discard(output: ShuffleOutput, part: MorselSpills): Unit = part.delete()

    override def abort(output: ShuffleOutput, context: TaskContext): Unit = {}
  }
}
