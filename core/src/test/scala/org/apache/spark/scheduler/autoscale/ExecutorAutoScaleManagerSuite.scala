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

import scala.collection.mutable

import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{doAnswer, mock}
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.{
  ExceptionFailure,
  SparkConf,
  SparkContext,
  SparkFunSuite,
  TempLocalSparkContext
}
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Python.PYSPARK_EXECUTOR_MEMORY
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.resource.{ResourceProfile, ResourceProfileManager}
import org.apache.spark.scheduler._

class ExecutorAutoScaleManagerSuite extends SparkFunSuite with TempLocalSparkContext {
  private val managers = new mutable.ListBuffer[ExecutorAutoScaleManager]()
  private var listenerBus: LiveListenerBus = _
  private var sparkConf: SparkConf = _
  private var metricsSystem: MetricsSystem = _
  private var resourceProfileManager: ResourceProfileManager = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    managers.clear()
    sparkConf = createConf()
    ResourceProfile.reInitDefaultProfile(sparkConf)
    listenerBus = new LiveListenerBus(sparkConf)
    metricsSystem = mock(classOf[MetricsSystem])
    listenerBus.start(null, metricsSystem)
    resourceProfileManager = new ResourceProfileManager(sparkConf, listenerBus)
  }

  override def afterEach(): Unit = {
    try {
      managers.foreach(_.stop())
      if (listenerBus != null) {
        listenerBus.stop()
      }
    } finally {
      listenerBus = null
      metricsSystem = null
      resourceProfileManager = null
      sparkConf = null
      super.afterEach()
    }
  }

  private def post(event: SparkListenerEvent): Unit = {
    listenerBus.post(event)
    listenerBus.waitUntilEmpty()
  }

  private def createConf(
      minNumPartitionsScaleUp: Int = 1,
      maxOomRatio: Double = 0.2,
      scaleUpFactor: Double = 1.2,
      maxScaleUpFactor: Double = 4.0): SparkConf = {
    new SparkConf()
      .setMaster("local-cluster[1,1,1024]")
      .setAppName(getClass().getName())
      .set(DYN_ALLOCATION_ENABLED, true)
      .set(DYN_ALLOCATION_TESTING, true)
      .set(EXECUTOR_AUTOSCALING_ENABLED, true)
      .set(EXECUTOR_AUTOSCALING_MIN_NUM_PARTITIONS_SCALE_UP, minNumPartitionsScaleUp)
      .set(EXECUTOR_AUTOSCALING_MAX_OOM_RATIO, maxOomRatio)
      .set(EXECUTOR_AUTOSCALING_MEMORY_SCALE_UP_FACTOR, scaleUpFactor)
      .set(EXECUTOR_AUTOSCALING_MEMORY_MAX_SCALE_UP_FACTOR, maxScaleUpFactor)
      .set(MEMORY_OFFHEAP_ENABLED, false)
      .set(EXECUTOR_CORES, 1)
  }

  private def createManager(
      conf: SparkConf,
      dagScheduler: DAGScheduler = mock(classOf[DAGScheduler])): ExecutorAutoScaleManager = {
    new ExecutorAutoScaleManager(listenerBus, conf, dagScheduler, resourceProfileManager)
  }

  test("initialize executor autoscaler") {
    val conf = createConf()
    val sc = new SparkContext(conf)
    managers ++= sc.executorAutoScaleManager.toSeq
    try {
      assert(sc.executorAutoScaleManager.isDefined)
    } finally {
      sc.stop()
    }
  }

  test("invalid executor autoscaling configs") {
    intercept[IllegalArgumentException] {
      createManager(createConf(minNumPartitionsScaleUp = 0))
    }
    intercept[IllegalArgumentException] {
      createManager(createConf(maxOomRatio = 0.0))
    }
  }

  test("scale up partition resource profile on oom task failure") {
    val dagScheduler = mock(classOf[DAGScheduler])
    var update: Option[(Int, Int, Option[Int], scala.collection.Map[Int, Int])] = None
    doAnswer { (invocation: InvocationOnMock) =>
      update = Some((
        invocation.getArgument[Int](0),
        invocation.getArgument[Int](1),
        invocation.getArgument[Option[Int]](2),
        invocation.getArgument[scala.collection.Map[Int, Int]](3)))
      ()
    }.when(dagScheduler).updateStageResourceProfile(anyInt(), anyInt(), any(), any())

    val manager = new ExecutorAutoScaleManager(
      listenerBus, sparkConf, dagScheduler, resourceProfileManager)
    managers += manager
    manager.start()

    post(SparkListenerStageSubmitted(new StageInfo(
      0,
      0,
      "test stage",
      10,
      Seq.empty,
      Seq.empty,
      "details",
      taskLocalityPreferences = Seq.empty,
      resourceProfileId = resourceProfileManager.defaultResourceProfile.id)))

    post(SparkListenerTaskEnd(
      0,
      0,
      "ResultTask",
      new ExceptionFailure(new OutOfMemoryError("Java heap space"), Seq.empty),
      new TaskInfo(
        0L,
        1,
        0,
        1,
        0L,
        "executor-1",
        "host-1",
        TaskLocality.ANY,
        speculative = false,
        resourceProfileManager.defaultResourceProfile.id),
      new ExecutorMetrics,
      null))

    assert(update.isDefined)
    val (stageId, stageAttemptId, stageRpIdOpt, partitionUpdates) = update.get
    assert(stageId === 0)
    assert(stageAttemptId === 0)
    assert(stageRpIdOpt.isEmpty)
    assert(partitionUpdates.keySet === Set(1))
    val scaledUpRpId = partitionUpdates(1)
    assert(scaledUpRpId !== resourceProfileManager.defaultResourceProfile.id)
    val scaledUpRp = resourceProfileManager.resourceProfileFromId(scaledUpRpId)
    assert(scaledUpRp.getExecutorMemory.get >
      resourceProfileManager.defaultResourceProfile.getExecutorMemory.get)
  }

  test("do not keep scaling after oom threshold is exceeded") {
    val conf = createConf(minNumPartitionsScaleUp = 2, maxOomRatio = 0.1)
    listenerBus.stop()
    sparkConf = conf
    ResourceProfile.reInitDefaultProfile(sparkConf)
    listenerBus = new LiveListenerBus(sparkConf)
    listenerBus.start(null, metricsSystem)
    resourceProfileManager = new ResourceProfileManager(sparkConf, listenerBus)

    val dagScheduler = mock(classOf[DAGScheduler])
    var numUpdates = 0
    doAnswer { (_: InvocationOnMock) =>
      numUpdates += 1
      ()
    }.when(dagScheduler).updateStageResourceProfile(anyInt(), anyInt(), any(), any())

    val manager = new ExecutorAutoScaleManager(
      listenerBus, sparkConf, dagScheduler, resourceProfileManager)
    managers += manager
    manager.start()

    post(SparkListenerStageSubmitted(new StageInfo(
      1,
      0,
      "test stage",
      10,
      Seq.empty,
      Seq.empty,
      "details",
      taskLocalityPreferences = Seq.empty,
      resourceProfileId = resourceProfileManager.defaultResourceProfile.id)))

    def postOom(partitionId: Int): Unit = {
      post(SparkListenerTaskEnd(
        1,
        0,
        "ResultTask",
        new ExceptionFailure(new OutOfMemoryError(s"oom-$partitionId"), Seq.empty),
        new TaskInfo(
          partitionId.toLong,
          partitionId,
          0,
          partitionId,
          0L,
          "executor-1",
          "host-1",
          TaskLocality.ANY,
          speculative = false,
          resourceProfileManager.defaultResourceProfile.id),
        new ExecutorMetrics,
        null))
    }

    postOom(0)
    postOom(1)

    assert(numUpdates === 1)
  }

  test("do not initialize executor autoscaler if off heap memory is enabled") {
    val conf = createConf()
    conf.set(MEMORY_OFFHEAP_ENABLED, true)
    conf.set(MEMORY_OFFHEAP_SIZE, 1L)
    val sc = new SparkContext(conf)
    managers ++= sc.executorAutoScaleManager.toSeq
    try {
      assert(sc.executorAutoScaleManager.isEmpty)
    } finally {
      sc.stop()
    }
  }

  test("do not initialize executor autoscaler if pyspark memory is enabled") {
    val conf = createConf().set(PYSPARK_EXECUTOR_MEMORY.key, "1g")
    val sc = new SparkContext(conf)
    managers ++= sc.executorAutoScaleManager.toSeq
    try {
      assert(sc.executorAutoScaleManager.isEmpty)
    } finally {
      sc.stop()
    }
  }
}
