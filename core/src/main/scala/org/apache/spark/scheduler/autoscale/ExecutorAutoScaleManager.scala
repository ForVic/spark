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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfileManager
import org.apache.spark.scheduler.{DAGScheduler, LiveListenerBus, SparkListener}

/**
 * A module to dynamically scale executor requirements at a per-partition level based on memory
 * related signals. These signals are collected via various events in the SparkListener
 * interface (e.g. SparkListenerTaskEnd). The ResourceProfile class is used to describe the memory
 * requirements of a partition. The [[DAGScheduler.updateResourceProfileForPartition]] is used
 * to update the resource profile for a partition.
 */
private[spark] class ExecutorAutoScaleManager(
    listenerBus: LiveListenerBus,
    sparkConf: SparkConf,
    dagScheduler: DAGScheduler,
    resourceProfileManager: ResourceProfileManager) extends Logging {

  private val listener = new ExecutorAutoScaleListener()

  def start(): Unit = {
    listenerBus.addToManagementQueue(listener)
  }

  def stop(): Unit = {
    listenerBus.removeListener(listener)
  }

  /**
   * A listener that processes TaskEnd events and collects memory related signals.
   * If necessary it updates the resource profile of the task/stage.
   */
  private class ExecutorAutoScaleListener extends SparkListener {

  }
}
