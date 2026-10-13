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
 * first. Reservations cover unfinished tasks, including those not yet started. Processed task
 * completions release their reservations; completed stages cannot retry within this execution.
 * All access is on the DAGScheduler event thread.
 */
private[scheduler] final class PipelinedExecutionPlan(
    job: ActiveJob,
    sourceCpuCapacity: Int = Int.MaxValue) {
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

  private def unfinishedTaskCount(stage: Stage): Long = stage match {
    case s: ShuffleMapStage => s.numPartitions.toLong - s.numAvailableOutputs
    case _: ResultStage => job.numPartitions.toLong - job.numFinished
  }

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
    val sources = ss.filter { stage =>
      stage.isInstanceOf[ShuffleMapStage] && !stage.rdd.isBarrier() &&
        !stage.parents.exists {
          case parent: ShuffleMapStage => parent.isPipelined
          case _ => false
        }
    }.toSet
    Group(id, ss.toSet, inputs, demand, sources, ss.exists {
      case s: ShuffleMapStage => s.isPipelined
      case _ => false
    })
  }.sortBy(_.stages.map(_.id).max)(Ordering.Int.reverse)

  private val byId = groups.map(g => g.id -> g).toMap
  private val admitted = mutable.HashSet.empty[Int]
  private val completedStages = mutable.HashSet.empty[Int]
  private val taskLimits = mutable.HashMap.empty[Int, Int]

  def taskLimit(stage: Stage): Int = taskLimits.getOrElse(stage.id, Int.MaxValue)

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
    if (group.pipelined) {
      // Processed successes no longer retain task state. Unsubmitted and deferred completions
      // still count, so this does not borrow capacity from readers waiting on live producers.
      group.stages.iterator.filterNot(s => completedStages.contains(s.id))
        .map(s => math.min(unfinishedTaskCount(s), taskLimit(s).toLong)).sum
    } else {
      group.reservation
    }
  }.sum

  def reservedSourceTaskCount: Long = reservedGroups.filter(_.pipelined)
    .flatMap(_.sources).filterNot(isCompleted)
    .map(s => math.min(unfinishedTaskCount(s), taskLimit(s).toLong)).sum

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

  def admit(
      groups: Seq[Group],
      availableSlots: Long,
      availableSources: Long = Long.MaxValue): Unit = {
    val sources = groups.filter(_.pipelined).flatMap(_.sources).sortBy(_.id)
    val minimumSources = groups.map(_.sourceReservation).sum
    require(availableSources >= minimumSources)
    sources.foreach(s => taskLimits(s.id) = math.min(s.numPartitions, 1))
    growSourceLimits(sources, math.min(availableSlots - groups.map(_.reservation).sum,
      availableSources - minimumSources))
    admitted ++= groups.map(_.id)
  }

  def growSourceLimits(
      availableSlots: Long,
      availableSources: Long = Long.MaxValue): Set[Stage] = {
    val sources = reservedGroups.filter(_.pipelined).flatMap(_.sources)
      .filterNot(isCompleted).sortBy(_.id)
    growSourceLimits(sources, math.min(availableSlots, availableSources))
  }

  private def growSourceLimits(sources: Seq[Stage], availableSlots: Long): Set[Stage] = {
    // Extra resident readers must not multiply simultaneously retained source operator state.
    // Readers can lend CPU locally; source waves need only enough runnable work to fill the CPUs.
    val sourceReservations = taskLimits.iterator.filterNot { case (id, _) =>
      completedStages.contains(id)
    }.map { case (id, limit) => math.min(limit.toLong, unfinishedTaskCount(stages(id))) }.sum
    var spare = math.min(availableSlots, math.max(0L, sourceCpuCapacity - sourceReservations))
    val changed = mutable.HashSet.empty[Stage]
    // Keep every reader resident and share remaining slots between non-reading producers.
    // Their launch limits apply even before any reader task set has been submitted.
    var growable = sources.filter(s => taskLimit(s) < unfinishedTaskCount(s))
    while (spare > 0 && growable.nonEmpty) {
      val increment = math.max(1L, spare / growable.size)
      growable.foreach { s =>
        val extra = math.min(spare, math.min(increment, unfinishedTaskCount(s) - taskLimit(s)))
        taskLimits(s.id) += extra.toInt
        spare -= extra
        if (extra > 0) changed += s
      }
      growable = growable.filter(s => taskLimit(s) < unfinishedTaskCount(s))
    }
    changed.toSet
  }
}

private[scheduler] object PipelinedExecutionPlan {
  case class Group(
      id: Int,
      stages: Set[Stage],
      inputs: Set[Int],
      taskCount: Long,
      sources: Set[Stage],
      pipelined: Boolean) {
    def sourceReservation: Long =
      if (pipelined) sources.toSeq.map(s => math.min(1L, s.numPartitions.toLong)).sum else 0L

    // Readers may wait on any live input and retain their full reservation. A source has
    // no live input: one running task guarantees progress, provided its launch limit leaves
    // room for every reader. Ordinary groups retain one slot; barriers require all slots.
    def reservation: Long = {
      if (pipelined) {
        taskCount - sources.toSeq.map(s => math.max(0, s.numPartitions - 1).toLong).sum
      } else if (stages.exists(_.rdd.isBarrier())) {
        taskCount
      } else {
        math.min(taskCount, 1L)
      }
    }
  }
}
