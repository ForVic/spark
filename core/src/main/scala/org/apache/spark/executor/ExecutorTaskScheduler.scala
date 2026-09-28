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

package org.apache.spark.executor

import scala.collection.mutable

/**
 * Executor-local CPU admission for resident tasks. The driver bounds residency separately and
 * retains the executor's entire CPU allocation. A blocked task keeps its stack and memory, but
 * releases its permit directly to ready work without a driver round trip. All tasks, including
 * ordinary tasks sharing this executor, must acquire a permit.
 */
private[spark] final class ExecutorTaskScheduler(
    capacity: BigDecimal) {
  require(capacity > 0)
  private var available = capacity
  private val ready = mutable.ArrayBuffer.empty[Permit]

  def newPermit(cpus: BigDecimal): Permit = {
    require(cpus > 0 && cpus <= capacity, "Task CPU demand exceeds executor CPU capacity")
    new Permit(cpus)
  }

  private[executor] def usedCpus: BigDecimal = synchronized { capacity - available }
  private[executor] def readyCount: Int = synchronized { ready.size }

  final class Permit private[ExecutorTaskScheduler] (cpus: BigDecimal) {
    private var held = false
    private var sliceStart = 0L

    def acquire(checkCanceled: () => Unit): Unit = ExecutorTaskScheduler.this.synchronized {
      require(!held)
      ready += this
      try {
        checkCanceled()
        while ((ready.head ne this) || available < cpus) {
          ExecutorTaskScheduler.this.wait(20)
          checkCanceled()
        }
        available -= cpus
        held = true
        sliceStart = System.nanoTime()
      } finally {
        ready -= this
        ExecutorTaskScheduler.this.notifyAll()
      }
    }

    def yieldIfNeeded(checkCanceled: () => Unit): Unit = {
      val shouldYield = ExecutorTaskScheduler.this.synchronized {
        held && ready.nonEmpty && System.nanoTime() - sliceStart >= 2000000L
      }
      if (shouldYield) {
        release()
        acquire(checkCanceled)
      }
    }

    def release(): Unit = ExecutorTaskScheduler.this.synchronized {
      if (held) {
        held = false
        available += cpus
        assert(available <= capacity)
        ExecutorTaskScheduler.this.notifyAll()
      }
    }
  }
}
