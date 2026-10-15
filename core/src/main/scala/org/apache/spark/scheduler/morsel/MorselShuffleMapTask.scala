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
import org.apache.spark.shuffle.sort.{MorselShuffleWriter, MorselSpills, SortShuffleManager}
import org.apache.spark.storage.StorageLevel

/**
 * Runs a shuffle map task as morsels when its RDD chain allows it: the chain must consist of
 * order-insensitive [[MapPartitionsRDD]]s, [[UnionRDD]]s and [[ZippedPartitionsRDD2]]s over
 * [[MorselSource]]s, and the planner must have marked the [[ShuffleDependency]] as morsel-safe.
 *
 * The task's thread and any helpers each run the pipeline over the morsels they claim and write
 * their output to their own shuffle files; the task's thread then concatenates them into the
 * task's single map output (see [[MorselShuffleWriter]]). The map output, map status and task
 * attempt are unchanged, so retries, speculation and fetch failures behave as for any other map
 * task.
 */
private[spark] object MorselShuffleMapTask extends Logging {

  /** Returns None if the task cannot run as morsels and must take the normal path. */
  def tryRun(
      taskBinary: Broadcast[Array[Byte]],
      rdd: RDD[_],
      dep: ShuffleDependency[_, _, _],
      partition: Partition,
      mapId: Long,
      context: TaskContext,
      isBarrier: Boolean,
      setHelperCpuTime: Long => Unit): Option[MapStatus] = {
    if (!dep.morselSafe || isBarrier || dep.shuffleMergeAllowed || !canConcatenateOutput(dep)) {
      return None
    }
    (MorselScheduler.get, SparkEnv.get.shuffleManager, context) match {
      case (Some(scheduler), manager: SortShuffleManager, owner: TaskContextImpl)
          if owner.getClass == classOf[TaskContextImpl] =>
        morselsOf(rdd, partition, scheduler.targetMorselSize, aligned = false)
          .map(_.grouped(scheduler.targetMorselSize))
          .filter(_.morsels.length >= 2)
          .map(split => run(scheduler, manager, owner, split, taskBinary, rdd, dep, partition,
            mapId, setHelperCpuTime))
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

  /** The morsels of a task and how to merge adjacent ones (see [[MorselSource.mergeMorsels]]). */
  private case class MorselSplit(
      morsels: IndexedSeq[Morsel],
      merge: (AnyRef, AnyRef) => Option[AnyRef] = (_, _) => None) {

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

  /**
   * The morsels of the task's partition, if every RDD between the task's RDD and its sources
   * passes a [[MorselPartition]] down to its parents.
   *
   * @param aligned whether the partition is zipped with partitions of other sources, so its
   *                morsels must be aligned with theirs.
   */
  private def morselsOf(
      rdd: RDD[_],
      split: Partition,
      targetSize: Long,
      aligned: Boolean): Option[MorselSplit] = {
    if (rdd.getStorageLevel != StorageLevel.NONE || rdd.isCheckpointedAndMaterialized) {
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
          morselsOf(m.prev, split, targetSize, aligned)
        case u: UnionRDD[_] if u.getClass == classOf[UnionRDD[_]] =>
          val part = split.asInstanceOf[UnionPartition[_]]
          morselsOf(u.rdds(part.parentRddIndex), part.parentPartition, targetSize, aligned)
        case z: ZippedPartitionsRDD2[_, _, _]
            if z.getClass == classOf[ZippedPartitionsRDD2[_, _, _]] =>
          val parts = split.asInstanceOf[ZippedPartitionsPartition].partitions
          for {
            left <- morselsOf(z.rdd1, parts(0), targetSize, aligned = true)
            right <- morselsOf(z.rdd2, parts(1), targetSize, aligned = true)
            if left.morsels.map(_.payload) == right.morsels.map(_.payload)
          } yield {
            val morsels = left.morsels.zip(right.morsels).map { case (l, r) =>
              Morsel(l.payload, l.sizeInBytes + r.sizeInBytes)
            }
            // Morsels that are empty on every side produce no rows. Aligned payloads are the same
            // on both sides, so merging them by one side's rules suits both.
            MorselSplit(if (aligned) morsels else morsels.filter(_.sizeInBytes > 0), left.merge)
          }
        case _ => None
      }
    }
  }

  private def run(
      scheduler: MorselScheduler,
      manager: SortShuffleManager,
      owner: TaskContextImpl,
      split: MorselSplit,
      taskBinary: Broadcast[Array[Byte]],
      rdd: RDD[_],
      dep: ShuffleDependency[_, _, _],
      partition: Partition,
      mapId: Long,
      setHelperCpuTime: Long => Unit): MapStatus = {
    val writer = manager.getMorselWriter[Any, Any](dep.shuffleHandle, mapId)
    val job = new ShuffleMorselJob(owner, split, taskBinary, writer, partition)
    // Registered before the pipeline starts so that it runs after the pipeline's own completion
    // listeners (e.g. the scan's final input metrics update), which run in reverse order.
    owner.addTaskCompletionListener[Unit](_ => job.mergeHelperMetrics())
    val writeMetrics = dep.shuffleWriterProcessor.morselMetricsReporter(owner)
    var ownerSpills: MorselSpills = null
    var success = false
    scheduler.register(job)
    try {
      try {
        val records = rdd.iterator(new MorselPartition(partition, job.ownerMorsels), owner)
        ownerSpills = writer.write(records.asInstanceOf[Iterator[Product2[Any, Any]]], owner,
          writeMetrics, dep.serializer, dep.rowBasedChecksums)
        owner.taskMetrics.incPeakExecutionMemory(ownerSpills.peakMemoryUsedBytes)
      } catch {
        case t: Throwable =>
          job.fail(t)
          throw t
      } finally {
        scheduler.unregister(job)
        scheduler.lendSlotWhile(owner.cpus) {
          job.closeAndAwaitHelpers()
        }
      }
      job.failureOption.foreach(throw _)
      val parts = (ownerSpills +: job.helperResults).toArray
      ownerSpills = null
      val status = writer.commit(parts, writeMetrics, dep.rowBasedChecksums,
        dep.morselOutputVariesWithSplit)
      setHelperCpuTime(job.helperCpuTimeNs)
      scheduler.jobsRun.incrementAndGet()
      scheduler.morselsStolen.addAndGet(job.morselsStolen)
      logInfo(s"Morsel task ${owner.taskAttemptId} (stage ${owner.stageId}." +
        s"${owner.stageAttemptNumber} partition ${owner.partitionId}): " +
        s"${job.numMorsels} morsels, ${job.morselsStolen} stolen by ${job.helperRuns} helper " +
        s"runs using ${job.helperCpuTimeNs / 1000000} ms CPU")
      success = true
      status
    } finally {
      if (!success) {
        if (ownerSpills != null) ownerSpills.delete()
        job.helperResults.foreach(_.delete())
      }
    }
  }

  private class ShuffleMorselJob(
      owner: TaskContextImpl,
      split: MorselSplit,
      taskBinary: Broadcast[Array[Byte]],
      writer: MorselShuffleWriter[Any, Any],
      partition: Partition) extends MorselJob[MorselSpills](owner, split.morsels, split.merge) {

    override protected def runHelper(
        context: MorselTaskContext,
        morsels: Iterator[AnyRef]): MorselSpills = {
      // Each helper deserializes its own copy of the task so that operator state and metrics are
      // not shared with other threads. Accumulators register with the helper's context and are
      // merged into the owner's when the task completes.
      val ser = SparkEnv.get.closureSerializer.newInstance()
      val rddAndDep = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
        ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
      val rdd = rddAndDep._1
      val dep = rddAndDep._2
      val records = rdd.iterator(new MorselPartition(partition, morsels), context)
      val spills = writer.write(records.asInstanceOf[Iterator[Product2[Any, Any]]], context,
        dep.shuffleWriterProcessor.morselMetricsReporter(context), dep.serializer,
        dep.rowBasedChecksums)
      context.taskMetrics.incPeakExecutionMemory(spills.peakMemoryUsedBytes)
      spills
    }

    override protected def discardResult(result: MorselSpills): Unit = result.delete()
  }
}
