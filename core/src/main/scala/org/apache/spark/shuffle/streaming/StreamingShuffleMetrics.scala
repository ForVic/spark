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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.util.LongAccumulator

/**
 * Per-exchange diagnostic counters. Background transport threads update local atomic counters;
 * only the task completion thread publishes them to Spark's accumulators. Durations overlap:
 * discovery and send completion are asynchronous, and write duration includes upstream compute.
 * None of these durations is added to the disk/buffer-cache shuffle write timer.
 */
private[streaming] class StreamingShuffleMetrics(accumulators: Map[String, LongAccumulator]) {
  private val counters = accumulators.keys.map(_ -> new AtomicLong()).toMap

  def add(name: String, value: Long): Unit = counters.get(name).foreach(_.addAndGet(value))

  def report(): Unit = counters.foreach { case (name, value) =>
    accumulators(name).add(value.getAndSet(0L))
  }
}

private[streaming] object StreamingShuffleMetrics {
  val names: Seq[String] = Seq(
    "writerSetupNs", "writerWriteNs", "writerBackpressureNs", "writerAckWaitNs",
    "writerSendCompletionNs", "writerCleanupNs", "writerBytes", "writerMessages",
    "writerUncompressedBytes", "writerCompressionNs",
    "readerSetupNs", "readerDiscoveryNs", "readerDiscoveryPolls", "readerConnectNs", "readerWaitNs",
    "readerResumeWaitNs", "readerMessageWaits", "readerLocationEntries",
    "readerAckWaitNs", "readerCleanupNs", "readerBytes", "readerMessages")
}
