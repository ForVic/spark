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

package org.apache.spark.sql.execution.exchange

import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, RangePartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{BaseLimitExec, SparkPlan, TakeOrderedAndProjectExec, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.joins.ShuffledJoin
import org.apache.spark.sql.internal.SQLConf

/**
 * Forms finite batch groups using the configured pipelined transport. Internal exchanges stay
 * inline; regular inputs may be scheduled by DAGScheduler in the same execution. Selection uses
 * reader compatibility, sharing and estimated concurrent task demand, not query identity.
 * DAGScheduler owns group admission and readiness with or without adaptive planning.
 */
object PlanBatchPipelinedShuffle extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.batchPipelinedShuffleEnabled ||
        !PipelinedShuffleEligibility.enabled(plan, conf)) {
      return plan
    }

    val sc = plan.session.sparkContext
    val capacity = sc.maxNumConcurrentTasks(sc.resourceProfileManager.defaultResourceProfile)
    if (capacity <= 0) return plan

    lazy val counts = {
      val result = mutable.HashMap.empty[SparkPlan, Int]
      if (conf.exchangeReuseEnabled) {
        plan.collectWithSubqueries { case s: ShuffleExchangeExec => s }.foreach { s =>
          val key = s.canonicalized
          result.update(key, result.getOrElse(key, 0) + 1)
        }
      }
      result
    }
    val selected = mutable.HashSet.empty[Int]
    val deferred = mutable.HashSet.empty[Int]

    def blocksReader(p: SparkPlan): Boolean = p match {
      case _: BaseLimitExec => true
      case other => PipelinedShuffleEligibility.isUnsupportedConsumer(other)
    }

    def selectGroup(root: SparkPlan, allowPipelining: Boolean = true): Unit = {
      val width = if (allowPipelining) root.estimatedPartitionCount.filter(_ > 0) else None
      var demand = width.fold(Long.MaxValue)(_.toLong)
      val counted = mutable.HashSet.empty[Int]
      val materializedInputs = mutable.HashSet.empty[Int]

      def visit(p: SparkPlan, canRead: Boolean): Unit = p match {
        // Already executed stages and reused output are materialized inputs, not group members.
        case _: QueryStageExec | _: ReusedExchangeExec =>
        case b: BroadcastExchangeLike => selectGroup(b.child, allowPipelining = false)
        case top: TakeOrderedAndProjectExec =>
          // collect merges at the driver. doExecute instead has a SinglePartition shuffle;
          // budget its producer as well so the same plan is safe for either action.
          val width = top.child.estimatedPartitionCount.filter(_ > 0)
          val fits = canRead && width.exists(w => demand <= capacity.toLong - w)
          if (top.child.outputPartitioning == SinglePartition) {
            visit(top.child, canRead)
          } else if (fits) {
            demand += width.get
            visit(top.child, canRead = true)
          } else {
            visit(top.child, canRead = false)
          }
        case j: ShuffledJoin if needsJoinStatistics(j) =>
          j.children.foreach(visit(_, canRead = false))
        case other if hasPendingBroadcast(other) =>
          // AQE cannot launch a live group until every broadcast it needs is ready. Keep the
          // probe's input boundary so its producers can run while this build is materializing.
          // Groups inside those producers remain eligible. A completed build adds no wait.
          other.children.foreach(visit(_, canRead = false))
        case s: ShuffleExchangeExec =>
          lazy val producerWidth = s.child.estimatedPartitionCount.filter(_ > 0)
          val range = s.outputPartitioning.isInstanceOf[RangePartitioning]
          lazy val shared = conf.exchangeReuseEnabled && counts.getOrElse(s.canonicalized, 0) > 1
          lazy val fits = producerWidth.exists(w => demand <= capacity.toLong - w)
          if (s.pipelined || (canRead && !range && !shared &&
              !isGlobalAggregate(s.child) && fits)) {
            if (counted.add(s.id)) {
              demand = if (fits) demand + producerWidth.get else Long.MaxValue
              if (!s.pipelined) selected += s.id
            }
            visit(s.child, canRead = true)
          } else {
            // This edge remains durable, but its statistics are not needed for adaptive planning.
            // A sibling live producer need not wait for it before the execution is submitted.
            if (canRead && !range && !shared) materializedInputs += s.id
            // A range exchange samples its input in a separate job. Keep its immediate input
            // regular until sampling has an explicit execution contract of its own.
            selectGroup(s.child, allowPipelining = !range)
          }
        case other =>
          val readable = canRead && !blocksReader(other)
          other.children.foreach(visit(_, readable))
      }
      visit(root, allowPipelining && width.isDefined)
      if (counted.nonEmpty) deferred ++= materializedInputs
    }

    selectGroup(plan)
    if (selected.isEmpty && deferred.isEmpty) {
      // An adaptive pass may provide better input statistics for an already selected live edge.
      // Only inline, unexecuted exchanges are visited by the layout rule.
      return CoalescePipelinedShufflePartitions(plan)
    }

    def hasLiveInput(p: SparkPlan): Boolean = p match {
      case s: ShuffleExchangeExec => s.pipelined || selected.contains(s.id)
      case _: QueryStageExec | _: ReusedExchangeExec | _: BroadcastExchangeLike => false
      case other => other.children.exists(hasLiveInput)
    }
    val selectedPlan = plan.transformDown {
      case top: TakeOrderedAndProjectExec if hasLiveInput(top.child) =>
        top.copy(pipelined = true)
      case s: ShuffleExchangeExec if selected.contains(s.id) => s.copy(pipelined = true)
      case s: ShuffleExchangeExec if deferred.contains(s.id) =>
        s.copy(materializeForAdaptivePlanning = false)
    }
    CoalescePipelinedShufflePartitions(selectedPlan)
  }

  private def isGlobalAggregate(p: SparkPlan): Boolean = p match {
    // At most one row per producer, emitted only after its entire input has been consumed.
    // Reserving waiting reducers cannot overlap useful work and prevents other scans running.
    case a: BaseAggregateExec => a.groupingExpressions.isEmpty
    case u: UnaryExecNode => isGlobalAggregate(u.child)
    case _ => false
  }

  private def hasPendingBroadcast(p: SparkPlan): Boolean = {
    p.children.zip(p.requiredChildDistribution).exists {
      case (q: BroadcastQueryStageExec, _: BroadcastDistribution) => !q.isMaterialized
      case (_, _: BroadcastDistribution) => true
      case _ => false
    }
  }

  private def needsJoinStatistics(join: ShuffledJoin): Boolean = {
    val broadcastThreshold = conf.getConf(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD)
      .getOrElse(conf.autoBroadcastJoinThreshold)
    val strategyHint = join.logicalLink.exists {
      case j: Join =>
        j.hint.leftHint.exists(_.strategy.isDefined) ||
          j.hint.rightHint.exists(_.strategy.isDefined)
      case _ => false
    }
    // A live edge cannot supply the complete statistics used to change the join strategy.
    // Retain those boundaries unless the strategy is fixed or broadcasting is disabled.
    conf.adaptiveExecutionEnabled && broadcastThreshold >= 0 && !strategyHint
  }
}
