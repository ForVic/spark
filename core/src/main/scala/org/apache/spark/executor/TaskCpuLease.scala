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

import org.apache.spark.TaskContext

/** A task retains its memory and other resources while lending its CPU to the scheduler. */
private[spark] final class TaskCpuLease(
    taskId: Long,
    backend: ExecutorBackend,
    localPermit: Option[ExecutorTaskScheduler#Permit] = None) {
  private var generation = 0L
  private var released = false
  private var resumedGeneration = 0L // guarded by this

  def resume(sequence: Long): Unit = synchronized {
    if (sequence == generation) {
      resumedGeneration = sequence
      notifyAll()
    }
  }

  def yieldIfNeeded(context: TaskContext): Unit = {
    if (!released) localPermit.foreach(_.yieldIfNeeded(() => context.killTaskIfInterrupted()))
  }

  def whileWaiting[T](context: TaskContext)(body: => T): T = {
    if (released) return body
    context.killTaskIfInterrupted()
    if (localPermit.isDefined) {
      released = true
      localPermit.get.release()
      try {
        return body
      } finally {
        localPermit.get.acquire(() => context.killTaskIfInterrupted())
        released = false
      }
    }
    val sequence = synchronized {
      generation += 1
      generation
    }
    released = true
    backend.releaseTaskCpu(taskId, sequence)
    try {
      body
    } finally {
      // A killed task must be able to unwind even if no CPU is available. Its terminal status
      // removes the outstanding reservation; it must not resume normal task computation.
      context.killTaskIfInterrupted()
      backend.requestTaskCpu(taskId, sequence)
      synchronized {
        while (resumedGeneration < sequence) {
          context.killTaskIfInterrupted()
          wait(100)
        }
      }
      context.killTaskIfInterrupted()
      released = false
    }
  }
}

private[spark] object TaskCpuLease {
  val PROPERTY = "spark.internal.pipelinedTaskCpuRelease"
  val QUEUED_SOURCE_PROPERTY = "spark.internal.pipelinedQueuedSource"
  private val current = new ThreadLocal[TaskCpuLease]

  def install(lease: TaskCpuLease): Unit = current.set(lease)
  def clear(): Unit = current.remove()

  def yieldIfNeeded(context: TaskContext): Unit = {
    val lease = current.get()
    if (lease != null && (TaskContext.get() eq context)) lease.yieldIfNeeded(context)
  }

  def whileWaiting[T](context: TaskContext)(body: => T): T = {
    val lease = current.get()
    if (lease == null || (TaskContext.get() ne context)) body else lease.whileWaiting(context)(body)
  }
}
