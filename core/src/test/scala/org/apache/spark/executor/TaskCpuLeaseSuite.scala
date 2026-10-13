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

import java.nio.ByteBuffer
import java.util.concurrent.{CountDownLatch, FutureTask, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{SparkFunSuite, TaskContext, TaskKilledException}
import org.apache.spark.TaskState.TaskState

class TaskCpuLeaseSuite extends SparkFunSuite {
  test("cancellation wins when it arrives together with a CPU grant") {
    val context = TaskContext.empty()
    var lease: TaskCpuLease = null
    val backend = new ExecutorBackend {
      override def statusUpdate(id: Long, state: TaskState, data: ByteBuffer): Unit = ()
      override def requestTaskCpu(id: Long, sequence: Long): Unit = {
        context.markInterrupted("canceled with grant")
        lease.resume(sequence)
      }
    }
    lease = new TaskCpuLease(0, backend)
    intercept[TaskKilledException] {
      lease.whileWaiting(context) { () }
    }
  }

  test("a task waits for its matching CPU grant before computing, including nested waits") {
    val context = TaskContext.empty()
    val requested = new CountDownLatch(1)
    val completed = new CountDownLatch(1)
    val releases = new AtomicInteger
    val backend = new ExecutorBackend {
      override def statusUpdate(id: Long, state: TaskState, data: ByteBuffer): Unit = ()
      override def releaseTaskCpu(id: Long, sequence: Long): Unit = releases.incrementAndGet()
      override def requestTaskCpu(id: Long, sequence: Long): Unit = requested.countDown()
    }
    val lease = new TaskCpuLease(0, backend)
    val work = new FutureTask[Unit](() => {
      TaskContext.withTaskContext(context) {
        TaskCpuLease.install(lease)
        try {
          context.withReleasedCpu {
            context.withReleasedCpu { assert(releases.get() == 1) }
          }
          completed.countDown()
        } finally {
          TaskCpuLease.clear()
        }
      }
    })
    val thread = new Thread(work, "cpu-lease-test")
    thread.start()
    try {
      assert(requested.await(10, TimeUnit.SECONDS))
      lease.resume(0)
      lease.resume(2)
      assert(!completed.await(100, TimeUnit.MILLISECONDS))
      lease.resume(1)
      work.get(10, TimeUnit.SECONDS)
      assert(completed.getCount == 0)
      assert(releases.get() == 1)
    } finally {
      thread.interrupt()
      thread.join(10000)
    }
  }

  test("cancellation while awaiting CPU exits without a grant or further computation") {
    val context = TaskContext.empty()
    val requested = new CountDownLatch(1)
    val backend = new ExecutorBackend {
      override def statusUpdate(id: Long, state: TaskState, data: ByteBuffer): Unit = ()
      override def requestTaskCpu(id: Long, sequence: Long): Unit = requested.countDown()
    }
    val lease = new TaskCpuLease(0, backend)
    val work = new FutureTask[Unit](() => {
      intercept[TaskKilledException] {
        lease.whileWaiting(context) { () }
      }
    })
    val thread = new Thread(work, "cpu-lease-cancel-test")
    thread.start()
    try {
      assert(requested.await(10, TimeUnit.SECONDS))
      context.markInterrupted("test cancellation")
      work.get(10, TimeUnit.SECONDS)
    } finally {
      thread.interrupt()
      thread.join(10000)
    }
  }
}
