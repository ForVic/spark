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

import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, CoalescedHashPartitioning, RangePartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{BaseLimitExec, ExpandExec, SparkPlan, TakeOrderedAndProjectExec, UnaryExecNode, UnionExec}
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
object PlanBatchPipelinedShuffle extends Rule[SparkPlan] with JoinSelectionHelper {
  override def apply(plan: SparkPlan): SparkPlan = {
    // Proposal, admission and fallback revisit the same producers. Reuse their estimates within
    // this invocation, but never across adaptive rounds that can expose new runtime statistics.
    planWithEstimates(plan, mutable.HashMap.empty)
  }

  private def planWithEstimates(
      plan: SparkPlan,
      sizeCache: mutable.Map[Int, Option[BigInt]]): SparkPlan = {
    if (!conf.batchPipelinedShuffleEnabled ||
        !PipelinedShuffleEligibility.enabled(plan, conf)) {
      return plan
    }

    val sc = plan.session.sparkContext
    val capacity = Math.multiplyExact(
      sc.maxNumConcurrentTasks(sc.resourceProfileManager.defaultResourceProfile),
      sc.getConf.get(org.apache.spark.internal.config.EXECUTOR_TASK_RESIDENCY_MULTIPLIER))
    if (capacity <= 0) return plan

    if (conf.coalesceShufflePartitionsEnabled &&
        conf.getConf(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED)) {
      // Propose compatible live layouts before applying the slot limit. Otherwise the initial
      // shuffle partition count can reject a small exchange before its readers can be coalesced.
      val candidate = select(plan, Long.MaxValue, requireFit = false, sizeCache).get
      if (candidate.fastEquals(plan) && !candidate.exists {
          case s: ShuffleExchangeExec => s.pipelined
          case t: TakeOrderedAndProjectExec => t.pipelined
          case _ => false
        }) {
        // A tighter slot limit cannot select an edge that unrestricted selection rejected.
        return plan
      }
      val resized = candidate.exists {
        case s: ShuffleExchangeExec =>
          s.pipelined && s.outputPartitioning.isInstanceOf[CoalescedHashPartitioning]
        case _ => false
      }
      if (resized) {
        // Accept the proposal as a whole so aligned join inputs cannot keep different layouts.
        // If any live group still exceeds capacity, retry the original plan with bounded selection.
        select(candidate, capacity, requireFit = true, sizeCache) match {
          case Some(admitted) => return admitted
          case None =>
        }
        // A durable boundary separates its producer's layout from its consumers. A wide outer
        // stage must not prevent a small live group below that boundary from being considered.
        val withLiveInputs = planMaterializedInputs(plan, sizeCache)
        if (!withLiveInputs.fastEquals(plan)) {
          select(withLiveInputs, capacity, requireFit = true, sizeCache) match {
            case Some(admitted) => return admitted
            case None =>
          }
        }
      }
    }
    select(plan, capacity, requireFit = false, sizeCache).get
  }

  private def planMaterializedInputs(
      plan: SparkPlan,
      sizeCache: mutable.Map[Int, Option[BigInt]]): SparkPlan = plan match {
    case s: ShuffleExchangeExec if !s.pipelined =>
      s.withNewChildren(Seq(planWithEstimates(s.child, sizeCache)))
    case _: Exchange | _: QueryStageExec => plan
    case other => other.mapChildren(planMaterializedInputs(_, sizeCache))
  }

  private def select(
      plan: SparkPlan,
      capacity: Long,
      requireFit: Boolean,
      sizeCache: mutable.Map[Int, Option[BigInt]]): Option[SparkPlan] = {
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
    var groupsFit = true
    var hasLiveShuffle = false

    def blocksReader(p: SparkPlan): Boolean = p match {
      case _: BaseLimitExec => true
      case other => PipelinedShuffleEligibility.isUnsupportedConsumer(other)
    }

    def readsLiveInput(p: SparkPlan): Boolean = p match {
      case s: ShuffleExchangeExec => s.pipelined
      case _: Exchange | _: QueryStageExec => false
      case other => other.children.exists(readsLiveInput)
    }

    def hasUnreadyStage(p: SparkPlan): Boolean = p match {
      case stage: QueryStageExec => !stage.isMaterialized
      case _: BroadcastExchangeLike => true
      case other => other.children.exists(hasUnreadyStage)
    }

    def selectGroup(
        root: SparkPlan,
        allowPipelining: Boolean = true,
        retainAdaptiveInputs: Boolean = false): Unit = {
      lazy val width = if (allowPipelining) root.estimatedPartitionCount.filter(_ > 0) else None
      var producerDemand = 0L
      var validDemand = true
      val counted = mutable.HashSet.empty[Int]
      val materializedInputs = mutable.ArrayBuffer.empty[ShuffleExchangeExec]

      def fits(producers: Int): Boolean = {
        validDemand && width.exists(w => producerDemand <= capacity - w - producers)
      }

      def visit(p: SparkPlan, canRead: Boolean): Unit = p match {
        // Already executed stages and reused output are materialized inputs, not group members.
        case _: QueryStageExec | _: ReusedExchangeExec =>
        case b: BroadcastExchangeLike => selectGroup(b.child, allowPipelining = false)
        case top: TakeOrderedAndProjectExec =>
          // collect merges at the driver. doExecute instead has a SinglePartition shuffle;
          // budget its producer as well so the same plan is safe for either action.
          lazy val width = top.child.estimatedPartitionCount.filter(_ > 0)
          if (top.child.outputPartitioning == SinglePartition) {
            visit(top.child, canRead)
          } else if (canRead && width.exists(fits)) {
            producerDemand += width.get
            visit(top.child, canRead = true)
          } else {
            if (top.pipelined) groupsFit = false
            visit(top.child, canRead = false)
          }
        case j: ShuffledJoin if needsJoinStatistics(j) =>
          // AQE can infer a small join input from an earlier aggregate's observed input size.
          // Fusing that boundary forces a later shuffle to finish before the same decision,
          // potentially adding materialization that ordinary AQE would have removed.
          // Only a legal broadcast build benefits from those observations. Preserve streaming
          // below a semi/outer join's probe side and both sides of a full outer join.
          selectGroup(j.left, allowPipelining = false,
            retainAdaptiveInputs = canBuildBroadcastLeft(j.joinType))
          selectGroup(j.right, allowPipelining = false,
            retainAdaptiveInputs = canBuildBroadcastRight(j.joinType))
        case u: UnionExec if u.children.size > 1 && u.children.exists(hasUnreadyStage) =>
          // A single live group would wait for every branch's pending prerequisite. Preserve
          // branch output boundaries so ready branches execute while others materialize.
          u.children.foreach(visit(_, canRead = false))
        case other if hasPendingBroadcast(other) =>
          // AQE cannot launch a live group until every broadcast it needs is ready. Keep the
          // probe's input boundary so its producers can run while this build is materializing.
          // Groups inside those producers remain eligible. A completed build adds no wait.
          other.children.foreach(visit(_, canRead = false))
        case s: ShuffleExchangeExec =>
          hasLiveShuffle ||= s.pipelined
          lazy val producerWidth = s.child.estimatedPartitionCount.filter(_ > 0).map { width =>
            // The core scheduler caps source-task waves, preserving slots for every reader.
            // Only validate this reduced demand after the complete live layout is selected.
            if (requireFit && !readsLiveInput(s.child)) 1 else width
          }
          val range = s.outputPartitioning.isInstanceOf[RangePartitioning]
          lazy val shared = conf.exchangeReuseEnabled && counts.getOrElse(s.canonicalized, 0) > 1
          lazy val fitsProducer = producerWidth.exists(fits)
          lazy val smallEnough = {
            val target = s.advisoryPartitionSize.getOrElse(
              conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES))
            // Domain inference can make a large grouping-set expansion fit the reader slot
            // limit. Only promote small expansions; larger ones retain observed-output AQE.
            !s.child.exists(_.isInstanceOf[ExpandExec]) ||
              sizeCache.getOrElseUpdate(s.child.id,
                CoalescePipelinedShufflePartitions.estimatedSize(s.child)).forall(_ <= target)
          }
          if (s.pipelined || (canRead && !range && !isGlobalAggregate(s.child) &&
              fitsProducer && smallEnough && !shared)) {
            if (counted.add(s.id)) {
              if (fitsProducer) producerDemand += producerWidth.get else validDemand = false
              if (!s.pipelined) selected += s.id
            }
            visit(s.child, canRead = true)
          } else {
            // This edge remains durable, but its statistics are not needed for adaptive planning.
            // A sibling live producer need not wait for it before the execution is submitted.
            if (canRead && !range) materializedInputs += s
            // A range exchange samples its input in a separate job. Keep its immediate input
            // regular until sampling has an explicit execution contract of its own.
            selectGroup(s.child, allowPipelining = !range && !retainAdaptiveInputs,
              retainAdaptiveInputs = retainAdaptiveInputs)
          }
        case other =>
          val readable = canRead && !blocksReader(other)
          other.children.foreach(visit(_, readable))
      }
      // Do not plan file splits or canonicalize rejected exchanges unless this group actually
      // has a live candidate. Most adaptive passes only contain statistics-sensitive boundaries.
      visit(root, allowPipelining)
      if (counted.nonEmpty) {
        deferred ++= materializedInputs.iterator.filter(_.materializeForAdaptivePlanning)
          .filterNot { s =>
            conf.exchangeReuseEnabled && counts.getOrElse(s.canonicalized, 0) > 1
          }.map(_.id)
        if (!fits(0)) groupsFit = false
      }
    }

    selectGroup(plan)
    if (requireFit && !groupsFit) return None
    if (selected.isEmpty && deferred.isEmpty) {
      // An adaptive pass may provide better input statistics for an already selected live edge.
      // Only inline, unexecuted exchanges are visited by the layout rule.
      // Admission validates the layout just proposed by this invocation. Re-estimating the
      // same logical inputs here duplicates CBO work without any new runtime observations.
      return Some(if (hasLiveShuffle && !requireFit) {
        CoalescePipelinedShufflePartitions.coalesce(plan, sizeCache)
      } else {
        plan
      })
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
    Some(CoalescePipelinedShufflePartitions.coalesce(selectedPlan, sizeCache))
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
