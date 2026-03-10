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

package org.apache.spark

import scala.collection.mutable

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import org.apache.spark.internal.config
import org.apache.spark.internal.config.DECOMMISSION_ENABLED
import org.apache.spark.internal.config.Tests.TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.resource.{ExecutorResourceRequests, ResourceProfile, ResourceProfileBuilder, ResourceProfileManager, TaskResourceRequests}
import org.apache.spark.scheduler.{ExecutorAllocationClient, LiveListenerBus, SparkListenerEvent, SparkListenerStageSubmitted, StageInfo}
import org.apache.spark.util.SystemClock

class ExecutorAllocationManagerWithDrpSuite extends SparkFunSuite {

  import ExecutorAllocationManagerWithDrpSuite._

  private val managers = new mutable.ListBuffer[ExecutorAllocationManagerWithDrp]()
  private var listenerBus: LiveListenerBus = _
  private var client: ExecutorAllocationClient = _
  private var rpManager: ResourceProfileManager = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    managers.clear()
    listenerBus = new LiveListenerBus(new SparkConf())
    listenerBus.start(null, mock(classOf[MetricsSystem]))
    client = mock(classOf[ExecutorAllocationClient])
    when(client.isExecutorActive(any())).thenReturn(true)
  }

  override def afterEach(): Unit = {
    try {
      listenerBus.stop()
      managers.foreach(_.stop())
    } finally {
      listenerBus = null
      super.afterEach()
    }
  }

  private def post(event: SparkListenerEvent): Unit = {
    listenerBus.post(event)
    listenerBus.waitUntilEmpty()
  }

  test("initialize dynamic allocation in SparkContext with drp manager") {
    val conf = createConf(0, 5, 1)
      .setMaster("local-cluster[1,1,1024]")
      .setAppName(getClass().getName())
      .set(config.EXECUTOR_AUTOSCALING_ENABLED, true)
      .set(config.MEMORY_OFFHEAP_ENABLED, false)

    val sc = new SparkContext(conf)
    try {
      assert(sc.executorAllocationManager.exists(_.isInstanceOf[ExecutorAllocationManagerWithDrp]))
    } finally {
      sc.stop()
    }
  }

  test("verify min max executors including non-default resource profile") {
    intercept[SparkException] {
      createManager(
        createConf().set(config.DYN_ALLOCATION_MIN_EXECUTORS_NON_DEFAULT_RESOURCE_PROFILE, -1))
    }

    intercept[SparkException] {
      createManager(
        createConf(1, 4).set(config.DYN_ALLOCATION_MIN_EXECUTORS_NON_DEFAULT_RESOURCE_PROFILE, 5))
    }

    createManager(
      createConf(1, 4)
        .set(config.DYN_ALLOCATION_MIN_EXECUTORS_NON_DEFAULT_RESOURCE_PROFILE, 2)
        .set(config.DYN_ALLOCATION_INITIAL_EXECUTORS_NON_DEFAULT_RESOURCE_PROFILE, 3))
  }

  test("initialize non-default resource profile with configured target and pending add") {
    val manager = createManager(
      createConf(1, 10, 1)
        .set(config.DYN_ALLOCATION_MIN_EXECUTORS_NON_DEFAULT_RESOURCE_PROFILE, 2)
        .set(config.DYN_ALLOCATION_INITIAL_EXECUTORS_NON_DEFAULT_RESOURCE_PROFILE, 3))
    val rp = createNonDefaultResourceProfile()

    post(SparkListenerStageSubmitted(createStageInfo(0, 100, rp = rp)))

    assert(manager.numExecutorsTargetPerResourceProfileId(rp.id) === 3)
    assert(manager.numExecutorsToAddPerResourceProfileId(rp.id) === 0)
  }

  test("reset restores non-default initial target and add count") {
    val manager = createManager(
      createConf(1, 10, 1)
        .set(config.DYN_ALLOCATION_MIN_EXECUTORS_NON_DEFAULT_RESOURCE_PROFILE, 2)
        .set(config.DYN_ALLOCATION_INITIAL_EXECUTORS_NON_DEFAULT_RESOURCE_PROFILE, 4))
    val rp = createNonDefaultResourceProfile()

    post(SparkListenerStageSubmitted(createStageInfo(0, 100, rp = rp)))
    manager.numExecutorsTargetPerResourceProfileId(rp.id) = 7
    manager.numExecutorsToAddPerResourceProfileId(rp.id) = 8

    manager.reset()

    assert(manager.numExecutorsTargetPerResourceProfileId(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID) === 1)
    assert(manager.numExecutorsToAddPerResourceProfileId(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID) === 1)
    assert(manager.numExecutorsTargetPerResourceProfileId(rp.id) === 4)
    assert(manager.numExecutorsToAddPerResourceProfileId(rp.id) === 0)
  }

  test("non-default initial executors respect non-default minimum") {
    val manager = createManager(
      createConf(1, 10, 1)
        .set(config.DYN_ALLOCATION_MIN_EXECUTORS_NON_DEFAULT_RESOURCE_PROFILE, 4)
        .set(config.DYN_ALLOCATION_INITIAL_EXECUTORS_NON_DEFAULT_RESOURCE_PROFILE, 2))
    val rp = createNonDefaultResourceProfile()

    post(SparkListenerStageSubmitted(createStageInfo(0, 100, rp = rp)))

    assert(manager.numExecutorsTargetPerResourceProfileId(rp.id) === 4)
  }

  private def createConf(
      minExecutors: Int = 1,
      maxExecutors: Int = 5,
      initialExecutors: Int = 1,
      decommissioningEnabled: Boolean = false): SparkConf = {
    new SparkConf()
      .set(config.DYN_ALLOCATION_ENABLED, true)
      .set(config.DYN_ALLOCATION_MIN_EXECUTORS, minExecutors)
      .set(config.DYN_ALLOCATION_MAX_EXECUTORS, maxExecutors)
      .set(config.DYN_ALLOCATION_INITIAL_EXECUTORS, initialExecutors)
      .set(config.DYN_ALLOCATION_SCHEDULER_BACKLOG_TIMEOUT.key,
        s"${schedulerBacklogTimeout.toString}s")
      .set(config.DYN_ALLOCATION_SUSTAINED_SCHEDULER_BACKLOG_TIMEOUT.key,
        s"${sustainedSchedulerBacklogTimeout.toString}s")
      .set(config.DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT.key, s"${executorIdleTimeout.toString}s")
      .set(config.EXECUTOR_CORES, 1)
      .set(config.SHUFFLE_SERVICE_ENABLED, true)
      .set(config.DYN_ALLOCATION_TESTING, true)
      .set(TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED, false)
      .set(DECOMMISSION_ENABLED, decommissioningEnabled)
  }

  private def createManager(conf: SparkConf): ExecutorAllocationManagerWithDrp = {
    ResourceProfile.reInitDefaultProfile(conf)
    rpManager = new ResourceProfileManager(conf, listenerBus)
    val manager = new ExecutorAllocationManagerWithDrp(client, listenerBus, conf,
      clock = new SystemClock(), resourceProfileManager = rpManager, reliableShuffleStorage = false)
    managers += manager
    manager.start()
    manager
  }

  private def createNonDefaultResourceProfile(): ResourceProfile = {
    val builder = new ResourceProfileBuilder()
    builder.require(new ExecutorResourceRequests().cores(4).resource("gpu", 4))
    builder.require(new TaskResourceRequests().cpus(1).resource("gpu", 1))
    val rp = builder.build()
    rpManager.addResourceProfile(rp)
    rp
  }
}

private object ExecutorAllocationManagerWithDrpSuite {
  private val schedulerBacklogTimeout = 1L
  private val sustainedSchedulerBacklogTimeout = 2L
  private val executorIdleTimeout = 3L

  private def createStageInfo(
      stageId: Int,
      numTasks: Int,
      attemptId: Int = 0,
      rp: ResourceProfile): StageInfo = {
    new StageInfo(stageId, attemptId, "name", numTasks, Seq.empty, Seq.empty, "no details",
      taskLocalityPreferences = Seq.empty, resourceProfileId = rp.id)
  }
}
