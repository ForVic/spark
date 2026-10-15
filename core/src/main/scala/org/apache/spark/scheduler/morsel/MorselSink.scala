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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

/**
 * Where a task writes the records it computes, e.g. its shuffle map output or the files of a
 * write, such that the task can run as morsels.
 *
 * A task that runs as morsels has several participants: its own thread and any helpers. Each
 * writes the records of the morsels it processes as a separate part of the task's output, and
 * once all have finished, the task's thread commits the parts together as the task's output. A
 * part must therefore not be visible as output until the task commits it. Helpers write with
 * their own copy of the sink, deserialized with the task, so that a sink may hold per-thread
 * state such as metrics, and with the state that the task's thread opened.
 *
 * @tparam T the records written.
 * @tparam S the state of a task's output, shared by its participants.
 * @tparam P a part of the output, written by one participant.
 * @tparam U the task's result, e.g. its map status or commit message.
 */
private[spark] trait MorselSink[T, S, P, U] {

  /** Writes all records of a task that does not run as morsels, on the task's thread. */
  def write(records: Iterator[T], context: TaskContext): U

  /** Starts the output of a task that runs as morsels, on the task's thread. */
  def open(context: TaskContext): S

  /**
   * Writes the records of one participant as a part, on the participant's thread and with its
   * context. May run concurrently with other participants. If it fails, it must remove what it
   * wrote.
   */
  def writePart(state: S, records: Iterator[T], context: TaskContext): P

  /**
   * Commits the parts as the task's output, on the task's thread once all participants have
   * finished. The parts are those of the task's thread first, then of helpers, in no particular
   * order.
   */
  def commit(state: S, parts: Seq[P], context: TaskContext): U

  /** Removes a part that will not be committed, e.g. of a helper that was aborted. */
  def discard(state: S, part: P): Unit

  /** Abandons the output of a task that failed, after its parts were discarded. */
  def abort(state: S, context: TaskContext): Unit
}

/**
 * Writes each partition of `prev` to `sink` and returns the sink's result as the partition's only
 * record, e.g. the commit message of a write task. A result task whose RDD is a [[MorselSinkRDD]]
 * can run as morsels when the pipeline of `prev` can, see [[MorselTask]].
 */
private[spark] class MorselSinkRDD[T: ClassTag, U: ClassTag](
    val prev: RDD[T],
    val sink: MorselSink[T, _, _, U] with Serializable)
  extends RDD[U](prev) {

  /** Joins in the pipeline whose morsels may split one side only, see [[MorselJoinSplit]]. */
  @volatile private[spark] var joinSplits: Seq[MorselJoinSplit] = Nil

  override protected def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    Iterator(sink.write(firstParent[T].iterator(split, context), context))
}
