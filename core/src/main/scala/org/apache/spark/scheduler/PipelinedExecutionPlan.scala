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

package org.apache.spark.scheduler

import scala.collection.mutable

/**
 * Execution groups for one finite job. Live edges join stages into one admission unit; durable
 * edges connect those units. Available durable inputs terminate the graph. This is independent
 * of SQL query stages: the same graph can finish in a result stage or a materialized shuffle.
 *
 * The scheduler may admit a group together with its unfinished ancestors when their combined
 * demand fits. This lets an independent live producer run while a sibling materializes, without
 * consuming the slots needed to finish that sibling. Otherwise it admits smaller ancestor groups
 * first. Reservations cover unfinished stages, including their not-yet-started tasks. A completed
 * stage cannot be retried within this execution and releases its reservation immediately.
 * All access is on the DAGScheduler event thread.
 */
private[scheduler] final class PipelinedExecutionPlan(job: ActiveJob) {
  import PipelinedExecutionPlan.Group

  private val stages = mutable.LinkedHashMap.empty[Int, Stage]
  private val pending = mutable.Stack(job.finalStage)
  while (pending.nonEmpty) {
    val stage = pending.pop()
    val available = stage match {
      case s: ShuffleMapStage => s.isAvailable
      case _ => false
    }
    if (!available && !stages.contains(stage.id)) {
      stages(stage.id) = stage
      pending.pushAll(stage.parents)
    }
  }

  private val representatives = mutable.HashMap.from(stages.keysIterator.map(id => id -> id))
  private def representative(id: Int): Int = {
    var root = id
    while (representatives(root) != root) root = representatives(root)
    var current = id
    while (current != root) {
      val next = representatives(current)
      representatives(current) = root
      current = next
    }
    root
  }

  stages.values.foreach { stage =>
    stage.parents.foreach {
      case parent: ShuffleMapStage if parent.isPipelined && stages.contains(parent.id) =>
        representatives(representative(stage.id)) = representative(parent.id)
      case _ =>
    }
  }

  private val members = stages.values.toSeq.groupBy(s => representative(s.id))
  private val groupByStage = members.iterator.flatMap { case (id, ss) =>
    ss.map(_.id -> id)
  }.toMap

  private def taskCount(stage: Stage): Long =
    if (stage == job.finalStage) job.numPartitions.toLong else stage.numPartitions.toLong

  val groups: Seq[Group] = members.toSeq.map { case (id, ss) =>
    val inputs = ss.flatMap { stage =>
      stage.parents.filter(p => stages.contains(p.id)).flatMap { parent =>
        val parentGroup = groupByStage(parent.id)
        if (parentGroup == id) {
          if (!parent.asInstanceOf[ShuffleMapStage].isPipelined) {
            throw new PipelinedShuffleUnsupportedException(
              "a materialized dependency inside a connected pipelined group")
          }
          None
        } else {
          Some(parentGroup)
        }
      }
    }.toSet
    val demand = ss.map(taskCount).sum
    Group(id, ss.toSet, inputs, demand, ss.exists {
      case s: ShuffleMapStage => s.isPipelined
      case _ => false
    })
  }.sortBy(_.stages.map(_.id).max)(Ordering.Int.reverse)

  private val byId = groups.map(g => g.id -> g).toMap
  private val admitted = mutable.HashSet.empty[Int]
  private val completedStages = mutable.HashSet.empty[Int]

  def contains(stage: Stage): Boolean = stages.contains(stage.id)

  def isAdmitted(stage: Stage): Boolean = groupByStage.get(stage.id).exists(admitted.contains)

  def isCompleted(stage: Stage): Boolean = completedStages.contains(stage.id)

  def complete(stage: Stage): Unit = {
    if (contains(stage)) completedStages += stage.id
  }

  private def completed(group: Group): Boolean =
    group.stages.forall(s => completedStages.contains(s.id))

  // A shared durable input can finish through another job before this plan admits it.
  def pendingGroups: Seq[Group] = groups.filterNot(g => admitted.contains(g.id) || completed(g))

  def started: Boolean = admitted.nonEmpty

  def reservedGroups: Seq[Group] =
    groups.filter(g => admitted.contains(g.id) && !completed(g))

  def reservedTaskCount: Long = reservedGroups.map { group =>
    val remaining = group.stages.iterator.filterNot(s => completedStages.contains(s.id))
      .map(taskCount).sum
    if (group.pipelined) remaining else group.reservation
  }.sum

  def hasUnsubmittedStages(running: collection.Set[Stage]): Boolean =
    reservedGroups.exists(_.stages.exists { s =>
      !completedStages.contains(s.id) && !running.contains(s)
    })

  /** New reservations required to start this group before its ordinary inputs finish. */
  def admissionClosure(group: Group): Seq[Group] = {
    val seen = mutable.HashSet.empty[Int]
    val pending = mutable.Stack(group.id)
    while (pending.nonEmpty) {
      val id = pending.pop()
      if (!completed(byId(id)) && seen.add(id)) pending.pushAll(byId(id).inputs)
    }
    groups.filter(g => seen.contains(g.id) && !admitted.contains(g.id))
  }

  def admit(groups: Seq[Group]): Unit = admitted ++= groups.map(_.id)
}

private[scheduler] object PipelinedExecutionPlan {
  case class Group(
      id: Int,
      stages: Set[Stage],
      inputs: Set[Int],
      taskCount: Long,
      pipelined: Boolean) {
    // An ordinary task never waits on a live input: that would put it in a pipelined group.
    // One free slot therefore guarantees progress through an ordinary stage's finite waves.
    // Its additional runnable tasks may use spare slots, but must not prevent admission of an
    // independent live producer. Barrier stages and live groups need all their slots together.
    def reservation: Long =
      if (pipelined || stages.exists(_.rdd.isBarrier())) taskCount else math.min(taskCount, 1L)
  }
}
