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

import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SparkFunSuite, TaskContext, TaskKilledException}
import org.apache.spark.TaskState.TaskState

class ExecutorTaskSchedulerSuite extends SparkFunSuite {
  test("queued sources wait for state admission without taking CPU from live readers") {
    val state = new ExecutorTaskScheduler(1)
    val cpu = new ExecutorTaskScheduler(1)
    val firstState = state.newPermit(1)
    val firstCpu = cpu.newPermit(1)
    firstState.acquire(() => ())
    firstCpu.acquire(() => ())
    val queued = new FutureTask[Unit](() => {
      val nextState = state.newPermit(1)
      val nextCpu = cpu.newPermit(1)
      nextState.acquire(() => ())
      try {
        nextCpu.acquire(() => ())
        try assert(state.usedCpus == 1) finally nextCpu.release()
      } finally {
        nextState.release()
      }
    })
    val thread = new Thread(queued, "queued-source-state-test")
    thread.start()
    try {
      eventually { assert(state.readyCount == 1) }
      assert(cpu.readyCount == 0, "uninitialized producers must not queue for CPU")
      firstCpu.release()
      val reader = cpu.newPermit(1)
      reader.acquire(() => ())
      try {
        assert(!queued.isDone, "lending CPU must not admit more producer operator state")
        assert(state.usedCpus == 1)
      } finally {
        reader.release()
      }
      firstState.release()
      queued.get(10, TimeUnit.SECONDS)
    } finally {
      firstCpu.release()
      firstState.release()
      thread.interrupt()
      thread.join(10000)
    }
    assert(state.usedCpus == 0 && cpu.usedCpus == 0)
  }

  test("a blocked task hands CPU to ready work locally and resumes without driver messages") {
    val scheduler = new ExecutorTaskScheduler(1)
    val first = scheduler.newPermit(1)
    val second = scheduler.newPermit(1)
    val context = TaskContext.empty()
    val backend = new ExecutorBackend {
      override def statusUpdate(id: Long, state: TaskState, data: ByteBuffer): Unit = ()
      override def releaseTaskCpu(id: Long, sequence: Long): Unit = fail("driver CPU release")
      override def requestTaskCpu(id: Long, sequence: Long): Unit = fail("driver CPU request")
    }
    first.acquire(() => ())
    val consumed = new CountDownLatch(1)
    val work = new FutureTask[Unit](() => {
      second.acquire(() => ())
      try {
        assert(scheduler.usedCpus == 1)
        consumed.countDown()
      } finally {
        second.release()
      }
    })
    val thread = new Thread(work, "local-cpu-handoff-test")
    thread.start()
    try {
      eventually { assert(scheduler.readyCount == 1) }
      val lease = new TaskCpuLease(0, backend, Some(first))
      lease.whileWaiting(context) {
        lease.whileWaiting(context) {
          assert(consumed.await(10, TimeUnit.SECONDS))
        }
      }
      assert(scheduler.usedCpus == 1)
      work.get(10, TimeUnit.SECONDS)
    } finally {
      first.release()
      thread.interrupt()
      thread.join(10000)
    }
    assert(scheduler.usedCpus == 0)
  }

  test("cancellation removes a queued permit without leaking or exceeding fractional CPU") {
    val scheduler = new ExecutorTaskScheduler(BigDecimal("1.5"))
    val first = scheduler.newPermit(1)
    val second = scheduler.newPermit(BigDecimal("0.5"))
    val canceled = scheduler.newPermit(1)
    first.acquire(() => ())
    second.acquire(() => ())
    val context = TaskContext.empty()
    val work = new FutureTask[Unit](() => {
      intercept[TaskKilledException] {
        canceled.acquire(() => context.killTaskIfInterrupted())
      }
    })
    val thread = new Thread(work, "local-cpu-cancel-test")
    thread.start()
    try {
      eventually { assert(scheduler.readyCount == 1) }
      assert(scheduler.usedCpus == BigDecimal("1.5"))
      context.markInterrupted("cancel queued work")
      work.get(10, TimeUnit.SECONDS)
      assert(scheduler.readyCount == 0)
    } finally {
      first.release()
      second.release()
      thread.interrupt()
      thread.join(10000)
    }
    assert(scheduler.usedCpus == 0)
    canceled.acquire(() => ())
    canceled.release()
    canceled.release()
    assert(scheduler.usedCpus == 0)
  }
}
