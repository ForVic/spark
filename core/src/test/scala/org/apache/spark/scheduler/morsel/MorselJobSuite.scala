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

import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkEnv, SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager}

class MorselJobSuite extends SparkFunSuite with LocalSparkContext with Eventually {

  override def beforeEach(): Unit = {
    super.beforeEach()
    sc = new SparkContext("local[2]", "morsel-job-suite", new SparkConf())
  }

  private def newOwner(cpus: Int = 1): TaskContextImpl = new TaskContextImpl(
    0, 0, 0, 1L, 0, 1, new TaskMemoryManager(SparkEnv.get.memoryManager, 1L), new Properties,
    SparkEnv.get.metricsSystem, new TaskMetrics, cpus)

  private def morsels(n: Int, size: Long = 100L): IndexedSeq[Morsel] =
    (0 until n).map(i => Morsel(Int.box(i), size))

  /** A job whose parts are the morsels each participant processed. */
  private class TestJob(owner: TaskContextImpl, n: Int)(
      process: (MorselTaskContext, Int) => Unit) extends MorselJob[Seq[Int]](owner, morsels(n)) {
    override protected def runPart(
        context: MorselTaskContext,
        input: Iterator[AnyRef]): Seq[Int] =
      input.map { m =>
        val i = m.asInstanceOf[Int]
        process(context, i)
        i
      }.toList

    override protected def discardPart(part: Seq[Int]): Unit = {}
  }

  private def runHelper(job: MorselJob[_]): Thread = {
    val first = job.tryJoin()
    assert(first != null)
    val thread = new Thread(() => job.runAsHelper(first, () => false))
    thread.start()
    thread
  }

  test("helpers aborted to free memory give their morsels back to the owner") {
    val owner = newOwner()
    val helperWorking = new CountDownLatch(1)
    val job = new TestJob(owner, 10)((context, _) => {
      helperWorking.countDown()
      // Work until aborted.
      while (true) {
        context.killTaskIfInterrupted()
        Thread.sleep(1)
      }
    })
    val helper = runHelper(job)
    assert(helperWorking.await(30, TimeUnit.SECONDS))
    assert(job.reclaimMemory())
    helper.join(30000)
    // No helper joins after the owner ran short of memory.
    assert(job.tryJoin() == null)
    assert(job.ownerMorsels.map(_.asInstanceOf[Int]).toSeq.sorted == (0 until 10))
    job.closeAndAwaitHelpers()
    assert(job.failureOption.isEmpty)
    assert(job.helperResults.isEmpty)
    assert(job.helperAborts == 1)
  }

  test("morsels of helpers that run out of memory after the owner finished run on its thread") {
    val owner = newOwner()
    TaskContext.setTaskContext(owner)
    try {
      val ownerThread = Thread.currentThread()
      val ownerDone = new CountDownLatch(1)
      val contexts = ConcurrentHashMap.newKeySet[TaskContext]()
      val job = new TestJob(owner, 10)((context, _) => {
        contexts.add(TaskContext.get())
        if (Thread.currentThread() ne ownerThread) {
          ownerDone.await(30, TimeUnit.SECONDS)
          // scalastyle:off throwerror
          throw new SparkOutOfMemoryError("UNABLE_TO_ACQUIRE_MEMORY",
            java.util.Map.of("requestedBytes", "1", "receivedBytes", "0"))
          // scalastyle:on throwerror
        }
      })
      val helper = runHelper(job)
      val ownerPart = job.ownerMorsels.map(_.asInstanceOf[Int]).toSeq
      assert(ownerPart == (0 until 9))
      ownerDone.countDown()
      job.closeAndAwaitHelpers()
      helper.join(30000)
      assert(job.failureOption.isEmpty)
      assert(job.helperResults.isEmpty)
      job.runRemainingMorsels()
      assert(job.failureOption.isEmpty)
      assert(job.helperResults == Seq(Seq(9)))
      assert(TaskContext.get() eq owner)
      // The helper and the owner's run of the remaining morsel had contexts of their own.
      assert(contexts.size == 2 && !contexts.contains(owner))
      assert(owner.taskMemoryManager.getMemoryConsumptionForThisTask == 0)
    } finally {
      TaskContext.unset()
    }
  }

  test("other helper failures fail the task") {
    val owner = newOwner()
    val job = new TestJob(owner, 10)((_, _) => throw new IllegalStateException("bad morsel"))
    runHelper(job).join(30000)
    job.closeAndAwaitHelpers()
    assert(job.failureOption.exists(_.getMessage == "bad morsel"))
  }

  test("tasks with spare CPUs use them for their own helpers") {
    // Tiny morsels are worth helping with.
    val conf = new SparkConf().set(config.MORSEL_HELPER_DELAY, 0L)
      .set(config.MORSEL_TARGET_SIZE, 2L)
    val scheduler = new MorselScheduler(4, conf)
    try {
      val owner = newOwner(cpus = 2)
      val threads = ConcurrentHashMap.newKeySet[Thread]()
      val busy = new AtomicInteger
      val maxBusy = new AtomicInteger
      val work = () => {
        threads.add(Thread.currentThread())
        maxBusy.accumulateAndGet(busy.incrementAndGet(), math.max)
        Thread.sleep(5)
        busy.decrementAndGet()
      }
      val job = new TestJob(owner, 200)((_, _) => work())
      // Another task holds the other two slots, so no slot is idle.
      scheduler.taskStarted(2)
      scheduler.taskStarted(2)
      scheduler.register(job)
      eventually(timeout(30.seconds)) {
        assert(scheduler.numActiveHelpers == 1)
      }
      job.ownerMorsels.foreach(_ => work())
      scheduler.unregister(job)
      scheduler.lendSlotWhile(job)(job.closeAndAwaitHelpers())
      scheduler.taskFinished(2)
      scheduler.taskFinished(2)
      assert(job.failureOption.isEmpty)
      assert(job.helperResults.flatten.nonEmpty)
      // The owner and one helper on the task's second CPU.
      assert(threads.size == 2 && maxBusy.get == 2)
      eventually(timeout(30.seconds)) {
        assert(scheduler.numActiveHelpers == 0)
      }
    } finally {
      scheduler.stop()
    }
  }

  test("helpers only join tasks with enough work left") {
    // 8 morsels of 100 bytes: a helper would get 400 bytes.
    val job = new TestJob(newOwner(), 8)((_, _) => ())
    assert(job.stealPriority(500, Long.MaxValue) == 0)
    assert(job.stealPriority(400, Long.MaxValue) == 400)
    // Or enough time, at the rate the task processes its morsels after the first.
    val input = job.ownerMorsels
    input.next()
    Thread.sleep(100)
    input.next()
    assert(job.stealPriority(500, 1L) == 0)
    Thread.sleep(100)
    input.next()
    // 100 bytes in 100 ms, with 250 bytes for the helper.
    assert(job.stealPriority(500, 150000000L) > 0)
    assert(job.stealPriority(500, 350000000L) == 0)
    // The scheduler starts no helper for a task with too little work.
    val conf = new SparkConf().set(config.MORSEL_HELPER_DELAY, 0L)
      .set(config.MORSEL_TARGET_SIZE, 1000L)
    val scheduler = new MorselScheduler(4, conf)
    try {
      val small = new TestJob(newOwner(), 8)((_, _) => ())
      scheduler.taskStarted(1)
      scheduler.register(small)
      Thread.sleep(200)
      assert(scheduler.numActiveHelpers == 0)
      assert(small.helperRuns == 0)
      scheduler.unregister(small)
    } finally {
      scheduler.stop()
    }
  }

  test("tasks with one CPU get no helpers while no slot is idle") {
    val conf = new SparkConf().set(config.MORSEL_HELPER_DELAY, 0L)
      .set(config.MORSEL_TARGET_SIZE, 2L)
    val scheduler = new MorselScheduler(2, conf)
    try {
      val owner = newOwner()
      val job = new TestJob(owner, 50)((_, _) => Thread.sleep(2))
      scheduler.taskStarted(1)
      scheduler.taskStarted(1)
      scheduler.register(job)
      job.ownerMorsels.foreach { _ => }
      scheduler.unregister(job)
      scheduler.lendSlotWhile(job)(job.closeAndAwaitHelpers())
      assert(job.helperRuns == 0)
    } finally {
      scheduler.stop()
    }
  }
}
