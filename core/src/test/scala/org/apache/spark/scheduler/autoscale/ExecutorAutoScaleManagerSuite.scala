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

import org.mockito.Mockito.mock

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite, TempLocalSparkContext}
import org.apache.spark.internal.config.Python.PYSPARK_EXECUTOR_MEMORY
import org.apache.spark.internal.config._
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler._

class ExecutorAutoScaleManagerSuite extends SparkFunSuite with TempLocalSparkContext {
  private val managers = new mutable.ListBuffer[ExecutorAutoScaleManager]()
  private var listenerBus: LiveListenerBus = _
  private var sparkConf: SparkConf = _
  private var metricsSystem: MetricsSystem = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    managers.clear()
    sparkConf = createConf()
    listenerBus = new LiveListenerBus(sparkConf)
    metricsSystem = mock(classOf[MetricsSystem])
    listenerBus.start(null, metricsSystem)
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
      sparkConf = null
      super.afterEach()
    }
  }

  private def post(event: SparkListenerEvent): Unit = {
    listenerBus.post(event)
    listenerBus.waitUntilEmpty()
  }

  private def createConf(): SparkConf = {
    new SparkConf()
      .setMaster("local-cluster[1,1,1024]")
      .setAppName(getClass().getName())
      .set(DYN_ALLOCATION_ENABLED, true)
      .set(DYN_ALLOCATION_TESTING, true)
      .set(EXECUTOR_AUTOSCALING_ENABLED, true)
      .set(MEMORY_OFFHEAP_ENABLED, false)
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

  test("do not initialize executor autoscaler if off heap memory is enabled") {
    val conf = createConf()
      .set(MEMORY_OFFHEAP_ENABLED, true)
      .set(MEMORY_OFFHEAP_SIZE, "1g")
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
