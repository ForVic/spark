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

import java.util.concurrent.ThreadPoolExecutor

import scala.collection.mutable

import org.apache.spark.util.ThreadUtils

/**
 * Reuse connection workers between readers without sharing their work queues. A reader exclusively
 * borrows one executor, so slow connects cannot occupy another reader's connection capacity and
 * cancellation cannot interrupt a subsequent borrower. Idle worker threads expire after a minute.
 */
private[streaming] final class StreamingShuffleClientExecutors(threadsPerReader: Int) {
  private val idle = mutable.Queue.empty[ThreadPoolExecutor]
  private val all = mutable.HashSet.empty[ThreadPoolExecutor]
  private var stopped = false

  def borrow(): ThreadPoolExecutor = synchronized {
    require(!stopped, "Streaming shuffle connection workers have stopped")
    if (idle.nonEmpty) {
      idle.dequeue()
    } else {
      val executor = ThreadUtils.newDaemonCachedThreadPool(
        "streaming-shuffle-async-client-creation", threadsPerReader)
      all += executor
      executor
    }
  }

  /**
   * Return only an executor whose submitter and connection callbacks have finished. The caller
   * tracks outstanding connections before submitting them: executor activity and queue size alone
   * miss a worker that has dequeued a job but has not started it. Rejected executors must be
   * shut down.
   */
  def release(
      executor: ThreadPoolExecutor,
      submitterTerminated: Boolean,
      pendingConnections: Int): Boolean = synchronized {
    require(!idle.contains(executor), "Connection workers were returned twice")
    if (!stopped && submitterTerminated && pendingConnections == 0 && !executor.isShutdown) {
      idle.enqueue(executor)
      true
    } else {
      all -= executor
      false
    }
  }

  def stop(): Unit = {
    val executors = synchronized {
      stopped = true
      val result = all.toSeq
      all.clear()
      idle.clear()
      result
    }
    executors.foreach(_.shutdownNow())
  }
}
