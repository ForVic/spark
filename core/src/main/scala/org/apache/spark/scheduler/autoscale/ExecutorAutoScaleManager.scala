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

package org.apache.spark.scheduler.autoscale

import java.util.Locale

import scala.collection.{immutable, mutable}
import scala.util.matching.Regex

import org.apache.spark.{
  ExceptionFailure,
  ExecutorAllocationManagerWithDrp,
  ExecutorLostFailure,
  SparkConf
}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.resource.{
  ExecutorResourceRequest,
  ResourceProfile,
  ResourceProfileManager,
  TaskResourceRequest
}
import org.apache.spark.resource.ResourceProfile.{
  getResourcesForClusterManager,
  ExecutorResourcesOrDefaults
}
import org.apache.spark.scheduler.{
  DAGScheduler,
  LiveListenerBus,
  SparkListener,
  SparkListenerStageCompleted,
  SparkListenerStageSubmitted,
  SparkListenerTaskEnd
}

/**
 * A module to dynamically scale executor requirements at a per-partition level based on memory
 * related signals. These signals are collected via various events in the SparkListener
 * interface (e.g. SparkListenerTaskEnd). The ResourceProfile class is used to describe the memory
 * requirements of a partition. The [[DAGScheduler.updateStageResourceProfile]] entrypoint is used
 * to update the resource profile for the affected partitions.
 */
private[spark] class ExecutorAutoScaleManager(
    listenerBus: LiveListenerBus,
    sparkConf: SparkConf,
    dagScheduler: DAGScheduler,
    resourceProfileManager: ResourceProfileManager) extends Logging {

  import ExecutorAutoScaleManager._
  import ExecutorAllocationManagerWithDrp.StageAttempt

  private val listener = new ExecutorAutoScaleListener()

  private val isPythonApp = sparkConf.get(IS_PYTHON_APP)
  private val minNumPartitionsScaleUp =
    sparkConf.get(EXECUTOR_AUTOSCALING_MIN_NUM_PARTITIONS_SCALE_UP)
  private val maxOomRatio = sparkConf.get(EXECUTOR_AUTOSCALING_MAX_OOM_RATIO)
  private val scaleUpFactor = sparkConf.get(EXECUTOR_AUTOSCALING_MEMORY_SCALE_UP_FACTOR)
  private val maxScaleUpFactor =
    sparkConf.get(EXECUTOR_AUTOSCALING_MEMORY_MAX_SCALE_UP_FACTOR)
  private val defaultRp = resourceProfileManager.defaultResourceProfile
  private val defaultRpCores = defaultRp.getExecutorCores.getOrElse(sparkConf.get(EXECUTOR_CORES))
  private val appDefaultCpusPerTask = sparkConf.get(CPUS_PER_TASK)
  private val overheadFactor = sparkConf.get(EXECUTOR_MEMORY_OVERHEAD_FACTOR)
  private val minimumOverheadMemory = sparkConf.get(EXECUTOR_MIN_MEMORY_OVERHEAD)

  private val stageToInitialRpId = new mutable.HashMap[Int, Int]
  private val stageAttemptToPartitionToRpId =
    new mutable.HashMap[StageAttempt, mutable.HashMap[Int, Int]]
  private val stageAttemptToOomPartitions =
    new mutable.HashMap[StageAttempt, mutable.HashSet[Int]]
  private val stageAttemptToNumPartitions = new mutable.HashMap[StageAttempt, Int]
  private val resourcesToRpId = new mutable.HashMap[
    (Map[String, ExecutorResourceRequest], Map[String, TaskResourceRequest]), Int]

  def start(): Unit = {
    listenerBus.addToManagementQueue(listener)
  }

  def stop(): Unit = {
    listenerBus.removeListener(listener)
  }

  private def handleTaskFailure(taskEnd: SparkListenerTaskEnd, error: String): Unit = {
    val normalizedError = error.toLowerCase(Locale.ROOT)
    if (!isOomFailure(normalizedError)) {
      logDebug(s"Ignoring non-OOM task failure for stage ${taskEnd.stageId}: $error")
      return
    }

    val stageAttempt = StageAttempt(taskEnd.stageId, taskEnd.stageAttemptId)
    val partitionId = if (taskEnd.taskInfo.partitionId >= 0) {
      taskEnd.taskInfo.partitionId
    } else {
      taskEnd.taskInfo.index
    }
    val failedTaskRpId = taskEnd.taskInfo.resourceProfileId

    synchronized {
      val oomPartitions = stageAttemptToOomPartitions.getOrElseUpdate(
        stageAttempt, new mutable.HashSet[Int])
      val partitionToRpId = stageAttemptToPartitionToRpId.getOrElseUpdate(
        stageAttempt, new mutable.HashMap[Int, Int])
      val numPartitions = stageAttemptToNumPartitions.getOrElse(stageAttempt, 0)

      oomPartitions += partitionId
      val shouldScaleUp = numPartitions > 0 &&
        (oomPartitions.size <= minNumPartitionsScaleUp ||
          oomPartitions.size.toDouble / numPartitions <= maxOomRatio)

      if (!shouldScaleUp) {
        logInfo(
          s"Not scaling stage ${stageAttempt.stageId} attempt ${stageAttempt.stageAttemptId} " +
            s"after OOM for partition $partitionId because OOM partition threshold " +
            "was exceeded.")
        return
      }

      val currentPartitionRpId = partitionToRpId.getOrElse(partitionId, failedTaskRpId)
      val scaledUpRp = scaleUpTaskResourceProfile(
        failedTaskRpId, currentPartitionRpId, stageAttempt.stageId)
      partitionToRpId(partitionId) = scaledUpRp.id
      logInfo(s"Scaling stage ${stageAttempt.stageId} attempt ${stageAttempt.stageAttemptId} " +
        s"partition $partitionId from resource profile $currentPartitionRpId to " +
        s"${scaledUpRp.id} after OOM task failure.")
      dagScheduler.updateStageResourceProfile(
        stageAttempt.stageId,
        stageAttempt.stageAttemptId,
        None,
        immutable.HashMap(partitionId -> scaledUpRp.id))
    }
  }

  private def scaleUpTaskResourceProfile(
      failedTaskRpId: Int,
      currentPartitionRpId: Int,
      stageId: Int): ResourceProfile = {
    val (failedTaskRp, failedTaskRpResources) = getResourceProfileResources(failedTaskRpId)
    val failedTaskRpTaskCpus = failedTaskRp.getTaskCpus.getOrElse(appDefaultCpusPerTask)

    val normalizedFailedTaskRp = failedTaskRp.getExecutorCores match {
      case Some(executorCores)
          if executorCores > failedTaskRpTaskCpus && failedTaskRpTaskCpus > 0 =>
        val ratio = failedTaskRpTaskCpus.toDouble / executorCores.toDouble
        val normalizedHeapMiB = math.max(
          1L,
          math.ceil(failedTaskRpResources.executorMemoryMiB.toDouble * ratio).toLong)
        val normalizedOverheadMiB = math.max(
          minimumOverheadMemory,
          math.ceil(failedTaskRpResources.memoryOverheadMiB.toDouble * ratio).toLong)
        ResourceProfile.updateResourceProfile(
          failedTaskRp,
          memoryOpt = Some(s"${normalizedHeapMiB}m"),
          memoryOverheadOpt = Some(s"${normalizedOverheadMiB}m"),
          coresOpt = Some(failedTaskRpTaskCpus))
      case _ =>
        failedTaskRp
    }

    val (_, normalizedFailedTaskRpResources) = getResourceProfileResources(normalizedFailedTaskRp)
    val (currentPartitionRp, currentPartitionResources) =
      getResourceProfileResources(currentPartitionRpId)
    val initialStageRpId = stageToInitialRpId.getOrElse(stageId, defaultRp.id)
    val (_, initialStageRpResources) = getResourceProfileResources(initialStageRpId)
    val initialStageRpHeapRatio = getHeapRatio(initialStageRpResources)

    val (rpToScaleUp, rpToScaleUpResources) =
      if (
        memoryPerCore(normalizedFailedTaskRpResources) >=
          memoryPerCore(currentPartitionResources)) {
        (normalizedFailedTaskRp, normalizedFailedTaskRpResources)
      } else {
        (currentPartitionRp, currentPartitionResources)
      }

    val scaledTotalMemoryMiB = math.min(
      math.ceil(rpToScaleUpResources.totalMemMiB.toDouble * scaleUpFactor).toLong,
      math.ceil(initialStageRpResources.totalMemMiB.toDouble * maxScaleUpFactor).toLong)
    val (scaledHeapMiB, scaledOverheadMiB) =
      splitMemory(scaledTotalMemoryMiB.toDouble, initialStageRpHeapRatio)
    val scaledUpRp = ResourceProfile.updateResourceProfile(
      rpToScaleUp,
      memoryOpt = Some(s"${heapWithBounds(scaledHeapMiB, initialStageRpResources)}m"),
      memoryOverheadOpt =
        Some(s"${overheadWithBounds(scaledOverheadMiB, initialStageRpResources)}m"))
    addResourceProfile(scaledUpRp)
  }

  private def getResourceProfileResources(
      rpId: Int): (ResourceProfile, ExecutorResourcesOrDefaults) = {
    getResourceProfileResources(resourceProfileManager.resourceProfileFromId(rpId))
  }

  private def getResourceProfileResources(
      rp: ResourceProfile): (ResourceProfile, ExecutorResourcesOrDefaults) = {
    val resources = getResourcesForClusterManager(
      rp.id,
      rp.executorResources,
      minimumOverheadMemory,
      overheadFactor,
      sparkConf,
      isPythonApp,
      Map.empty)
    (rp, resources)
  }

  private def addResourceProfile(rp: ResourceProfile): ResourceProfile = {
    val cacheKey = (rp.executorResources, rp.taskResources)
    val addedRp = resourcesToRpId.get(cacheKey)
      .map(resourceProfileManager.resourceProfileFromId)
      .orElse(resourceProfileManager.getEquivalentProfile(rp))
      .getOrElse {
        resourceProfileManager.addResourceProfile(rp)
        rp
      }
    resourcesToRpId(cacheKey) = addedRp.id
    addedRp
  }

  private def isOomFailure(error: String): Boolean = {
    OOM_STRINGS_SET.exists(error.contains) ||
      OOM_REGEX_SET.exists(_.findFirstIn(error).nonEmpty)
  }

  private def memoryPerCore(resources: ExecutorResourcesOrDefaults): Double = {
    resources.totalMemMiB.toDouble / getExecutorCores(resources).toDouble
  }

  private def getExecutorCores(resources: ExecutorResourcesOrDefaults): Int = {
    resources.cores.getOrElse(defaultRpCores)
  }

  private def getHeapRatio(resources: ExecutorResourcesOrDefaults): Double = {
    val heapAndOverhead = resources.executorMemoryMiB + resources.memoryOverheadMiB
    if (heapAndOverhead <= 0) {
      1.0
    } else {
      resources.executorMemoryMiB.toDouble / heapAndOverhead.toDouble
    }
  }

  private def splitMemory(totalMemoryMiB: Double, heapRatio: Double): (Long, Long) = {
    val heapMemoryMiB = math.max(1L, math.round(totalMemoryMiB * heapRatio))
    val overheadMemoryMiB = math.max(0L, math.round(totalMemoryMiB - heapMemoryMiB))
    (heapMemoryMiB, overheadMemoryMiB)
  }

  private def heapWithBounds(
      heapMemoryMiB: Long,
      resources: ExecutorResourcesOrDefaults): Long = {
    math.max(
      MIN_HEAP_MEMORY_MIB,
      math.max(resources.executorMemoryMiB, math.min(heapMemoryMiB, MAX_HEAP_MEMORY_MIB)))
  }

  private def overheadWithBounds(
      overheadMemoryMiB: Long,
      resources: ExecutorResourcesOrDefaults): Long = {
    math.max(
      minimumOverheadMemory,
      math.max(
        resources.memoryOverheadMiB,
        math.min(overheadMemoryMiB, MAX_MEMORY_OVERHEAD_MIB)))
  }

  /**
   * A listener that processes TaskEnd events and collects memory related signals.
   * If necessary it updates the resource profile of the task/stage.
   */
  private class ExecutorAutoScaleListener extends SparkListener {

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit =
      ExecutorAutoScaleManager.this.synchronized {
      val stageInfo = stageSubmitted.stageInfo
      val stageAttempt = StageAttempt(stageInfo.stageId, stageInfo.attemptNumber())
      stageToInitialRpId(stageInfo.stageId) = stageInfo.resourceProfileId
      val partitionToRpId = stageAttemptToPartitionToRpId.getOrElseUpdate(
        stageAttempt, new mutable.HashMap[Int, Int])
      stageInfo.initialPartitionToRpIdAndTaskIndex.foreach { case (partitionId, (rpId, _)) =>
        partitionToRpId(partitionId) = rpId
      }
      stageAttemptToNumPartitions(stageAttempt) = stageInfo.numTasks
      logDebug(s"Registered stage ${stageInfo.stageId} attempt ${stageInfo.attemptNumber()} " +
        s"for executor autoscaling with ${stageInfo.numTasks} partitions.")
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
      ExecutorAutoScaleManager.this.synchronized {
      val stageInfo = stageCompleted.stageInfo
      val stageAttempt = StageAttempt(stageInfo.stageId, stageInfo.attemptNumber())
      if (stageInfo.failureReason.isEmpty) {
        stageToInitialRpId.remove(stageInfo.stageId)
      }
      stageAttemptToPartitionToRpId.remove(stageAttempt)
      stageAttemptToOomPartitions.remove(stageAttempt)
      stageAttemptToNumPartitions.remove(stageAttempt)
      logDebug(s"Removed autoscaling state for stage ${stageInfo.stageId} " +
        s"attempt ${stageInfo.attemptNumber()}.")
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      taskEnd.reason match {
        case failure: ExecutorLostFailure =>
          handleTaskFailure(taskEnd, failure.toErrorString)
        case failure: ExceptionFailure =>
          handleTaskFailure(taskEnd, failure.toErrorString)
        case _ =>
      }
    }
  }
}

private[spark] object ExecutorAutoScaleManager {
  private val MIN_HEAP_MEMORY_MIB = 512L
  private val MAX_HEAP_MEMORY_MIB = ByteUnit.GiB.toMiB(1024)
  private val MAX_MEMORY_OVERHEAD_MIB = ByteUnit.GiB.toMiB(256)

  private val OOM_STRINGS_SET = Set(
    "outofmemoryerror",
    "out of memory",
    "java heap space",
    "gc overhead limit exceeded",
    "unable to create native thread",
    "direct buffer memory")

  private val OOM_REGEX_SET: Set[Regex] = Set(
    "container killed by .* exceeding memory limits".r,
    "memory limit .* exceeded".r)
}
