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

package org.apache.spark.shuffle.streaming

import java.util.concurrent.{Callable, CountDownLatch, TimeUnit}

import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkFunSuite

class StreamingShuffleClientExecutorsSuite extends SparkFunSuite {
  test("drained readers reuse workers while stalled readers retain isolated queues") {
    val pools = new StreamingShuffleClientExecutors(1)
    val stalled = pools.borrow()
    val started = new CountDownLatch(1)
    val unblock = new CountDownLatch(1)
    try {
      stalled.execute(() => {
        started.countDown()
        unblock.await()
      })
      assert(started.await(10, TimeUnit.SECONDS))
      val other = pools.borrow()
      val worker = other.submit(new Callable[Thread] {
        override def call(): Thread = Thread.currentThread()
      }).get(10, TimeUnit.SECONDS)
      eventually(Timeout(10.seconds)) { assert(other.getActiveCount == 0) }
      assert(pools.release(other, submitterTerminated = true, pendingConnections = 0))
      val next = pools.borrow()
      assert(next.submit(new Callable[Thread] {
        override def call(): Thread = Thread.currentThread()
      }).get(10, TimeUnit.SECONDS) eq worker)
      // Cancelling a stalled reader never interrupts another reader's reused workers.
      assert(!pools.release(stalled, submitterTerminated = true, pendingConnections = 1))
      stalled.shutdownNow()
      assert(stalled.awaitTermination(10, TimeUnit.SECONDS))
      assert(!next.isShutdown)
      pools.stop()
      assert(next.awaitTermination(10, TimeUnit.SECONDS))
      intercept[IllegalArgumentException] { pools.borrow() }
    } finally {
      unblock.countDown()
      stalled.shutdownNow()
      pools.stop()
    }
  }

  test("an empty queue is not reusable until its submitter and connections finish") {
    val pools = new StreamingShuffleClientExecutors(1)
    val executor = pools.borrow()
    try {
      assert(!pools.release(executor, submitterTerminated = false, pendingConnections = 0))
      executor.shutdownNow()
      // A dequeued job may not yet appear in the executor's active count. The reader's
      // outstanding-work count must prevent returning that executor to another reader.
      val dequeued = pools.borrow()
      assert(dequeued.getActiveCount == 0 && dequeued.getQueue.isEmpty)
      assert(!pools.release(dequeued, submitterTerminated = true, pendingConnections = 1))
      dequeued.shutdownNow()
      val next = pools.borrow()
      assert(next ne executor)
      assert(next ne dequeued)
      assert(pools.release(next, submitterTerminated = true, pendingConnections = 0))
      pools.stop()
      assert(next.isShutdown)
      // A reader finishing concurrently with manager shutdown cannot return a live pool.
      assert(!pools.release(executor, submitterTerminated = true, pendingConnections = 0))
    } finally {
      executor.shutdownNow()
      pools.stop()
    }
  }
}
