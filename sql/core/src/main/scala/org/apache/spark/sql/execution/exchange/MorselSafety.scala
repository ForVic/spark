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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{Partial, PartialMerge}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.{Partial => PartialLimit, WindowExec, WindowGroupLimitExec}

/**
 * Decides whether the map side of a shuffle may run as morsels (see
 * [[org.apache.spark.scheduler.morsel.MorselScheduler]]): splitting a task's input into pieces
 * that are processed independently must produce the same multiset of rows for every reducer.
 *
 * Two kinds of stages qualify, depending on where their input comes from:
 *  - A stage that scans files is split into file ranges. Its pipeline may only hold deterministic
 *    row-wise operators (project, filter, expand, generate, the streamed side of broadcast
 *    joins), local sorts and partial aggregations.
 *  - A stage that reads several shuffle partitions per task (AQE coalescing) is split into those
 *    reducer partitions. Each one holds every row of its keys, so the pipeline may additionally
 *    hold key-grouped operators: final aggregations, windows, global sorts and sort-merge joins
 *    of two shuffles.
 *  - A stage that reads map outputs locally (AQE local shuffle reads) is split into map outputs,
 *    with the same operators as a file scan.
 * The output must be shuffled by a partitioning that places each row independently of the
 * others. Aggregates whose result depends on row order, like first(), may pick different rows;
 * their result already depends on the order in which the shuffle delivers rows.
 */
object MorselSafety {

  /** What the morsels of a stage are, which decides the operators the stage may hold. */
  private sealed trait Input
  /** File ranges: row-wise operators and partial aggregations only. */
  private case object Files extends Input
  /** Whole reducer partitions of coalesced shuffle reads: also key-grouped operators. */
  private case object ReducerPartitions extends Input
  /** Map outputs of local shuffle reads (after AQE turned a join into a broadcast join). */
  private case object MapOutputs extends Input

  def isMorselSafe(child: SparkPlan, partitioning: Partitioning): Boolean =
    unsafeReason(child, partitioning).isEmpty

  /** Why the map side of the shuffle may not run as morsels, if it may not. */
  def unsafeReason(child: SparkPlan, partitioning: Partitioning): Option[String] =
    partitioning match {
      case _: HashPartitioning | _: RangePartitioning | SinglePartition =>
        val input = if (scansFiles(child)) {
          Files
        } else if (child.exists {
            case read: AQEShuffleReadExec => isLocalRead(read)
            case _ => false
          }) {
          MapOutputs
        } else {
          ReducerPartitions
        }
        blocker(child, input).map(_.nodeName)
      case other => Some(other.getClass.getSimpleName)
    }

  /**
   * Whether the rows (not just their order) a morsel-safe stage outputs depend on how its input is
   * split: partial aggregations emit one row per key per participant, and partial window group
   * limits keep the top rows of each participant.
   */
  def outputVariesWithSplit(plan: SparkPlan): Boolean = plan match {
    case agg: BaseAggregateExec if isPartial(agg) => true
    case limit: WindowGroupLimitExec if limit.mode == PartialLimit => true
    case _: QueryStageExec | _: Exchange => false
    case other => other.children.exists(outputVariesWithSplit)
  }

  private def scansFiles(plan: SparkPlan): Boolean = plan match {
    case _: FileSourceScanExec => true
    case _: QueryStageExec | _: Exchange => false
    case other => other.children.exists(scansFiles)
  }

  private def isLocalRead(read: AQEShuffleReadExec): Boolean = read.partitionSpecs.forall {
    case _: PartialMapperPartitionSpec | _: CoalescedMapperPartitionSpec => true
    case _ => false
  }

  /** The first node of the stage's pipeline that is not morsel-safe, if any. */
  private def blocker(plan: SparkPlan, input: Input): Option[SparkPlan] = {
    def blockerOf(p: SparkPlan) = blocker(p, input)
    def keyGrouped(p: SparkPlan, keys: Seq[Expression]) =
      unless(input == ReducerPartitions && keys.nonEmpty && isDeterministic(p), p)
        .orElse(blockerOf(p.children.head))
    plan match {
      case _: FileSourceScanExec => unless(input == Files, plan)
      case read: AQEShuffleReadExec =>
        val specsMatch = input match {
          case ReducerPartitions =>
            read.partitionSpecs.forall(_.isInstanceOf[CoalescedPartitionSpec])
          case MapOutputs => isLocalRead(read)
          case Files => false
        }
        unless(specsMatch && read.child.isInstanceOf[ShuffleQueryStageExec], read)
      case _: ShuffleQueryStageExec => unless(input == ReducerPartitions, plan)
      case WholeStageCodegenExec(child) => blockerOf(child)
      case InputAdapter(child) => blockerOf(child)
      case ColumnarToRowExec(child) => blockerOf(child)
      case p @ (_: ProjectExec | _: FilterExec | _: ExpandExec | _: GenerateExec) =>
        unless(isDeterministic(p), p).orElse(blockerOf(p.children.head))
      // A partial aggregation can be applied to any split of its input; the final aggregation
      // after the shuffle merges the partial results. A sort aggregation only needs its input
      // sorted within each morsel, which a local sort below it provides. Other aggregations need
      // all rows of their keys, i.e. whole reducer partitions.
      case agg: BaseAggregateExec if agg.isInstanceOf[HashAggregateExec] ||
          agg.isInstanceOf[ObjectHashAggregateExec] || agg.isInstanceOf[SortAggregateExec] =>
        if (isPartial(agg)) {
          unless(isDeterministic(agg), agg).orElse(blockerOf(agg.child))
        } else {
          keyGrouped(agg, agg.groupingExpressions)
        }
      case sort: SortExec if !sort.global =>
        unless(isDeterministic(sort), sort).orElse(blockerOf(sort.child))
      case sort: SortExec => keyGrouped(sort, sort.sortOrder)
      case window: WindowExec => keyGrouped(window, window.partitionSpec)
      // A partial window group limit keeps each group's top rows within its input; the top rows
      // of a group are among the top rows of whichever morsel holds them.
      case limit: WindowGroupLimitExec if limit.mode == PartialLimit =>
        unless(isDeterministic(limit), limit).orElse(blockerOf(limit.child))
      case limit: WindowGroupLimitExec => keyGrouped(limit, limit.partitionSpec)
      // Each task of a union computes one partition of one child.
      case union: UnionExec if union.outputPartitioning.isInstanceOf[UnknownPartitioning] =>
        union.children.iterator.map(blockerOf).collectFirst { case Some(b) => b }
      // Joins of two shuffles: each participant reads the same reducer partitions on both sides
      // (see SharedMorselInput). The side read first must be drained before the other is read,
      // which the sorts under both sides of a sort-merge join guarantee. Shuffled hash joins are
      // not supported: their hash table cannot spill and is only freed when the task completes,
      // so a helper could wait forever for memory the task's thread holds while waiting for it.
      case join: SortMergeJoinExec =>
        shuffledJoinBlocker(join, join.isSkewJoin, input,
          join.children.forall(c => stripCodegen(c).isInstanceOf[SortExec]))
      case join: BroadcastHashJoinExec =>
        broadcastJoinBlocker(join, join.joinType, join.buildSide, input)
      case join: BroadcastNestedLoopJoinExec =>
        broadcastJoinBlocker(join, join.joinType, join.buildSide, input)
      case other => Some(other)
    }
  }

  private def shuffledJoinBlocker(
      join: SparkPlan,
      isSkewJoin: Boolean,
      input: Input,
      drainsFirst: Boolean): Option[SparkPlan] = {
    // Each side reads exactly one shuffle, so the order in which sides are read is simple.
    unless(input == ReducerPartitions && !isSkewJoin && drainsFirst && isDeterministic(join) &&
        join.children.forall(c => c.collect { case s: ShuffleQueryStageExec => s }.length == 1),
      join).orElse(join.children.iterator.map(blocker(_, input))
        .collectFirst { case Some(b) => b })
  }

  private def stripCodegen(plan: SparkPlan): SparkPlan = plan match {
    case WholeStageCodegenExec(child) => stripCodegen(child)
    case InputAdapter(child) => stripCodegen(child)
    case other => other
  }

  private def unless(safe: Boolean, plan: SparkPlan): Option[SparkPlan] =
    if (safe) None else Some(plan)

  private def broadcastJoinBlocker(
      join: BinaryExecNode,
      joinType: JoinType,
      buildSide: BuildSide,
      input: Input): Option[SparkPlan] = {
    val streamed = buildSide match {
      case BuildRight => join.left
      case BuildLeft => join.right
    }
    unless(streamsPreservedSide(joinType, buildSide) && isDeterministic(join), join)
      .orElse(blocker(streamed, input))
  }

  private def isDeterministic(plan: SparkPlan): Boolean = plan.expressions.forall(_.deterministic)

  private def isPartial(agg: BaseAggregateExec): Boolean =
    agg.requiredChildDistributionExpressions.isEmpty &&
      agg.aggregateExpressions.forall(e => e.mode == Partial || e.mode == PartialMerge)

  // Each streamed row's output depends only on that row and the broadcast side.
  private def streamsPreservedSide(joinType: JoinType, buildSide: BuildSide): Boolean =
    (joinType, buildSide) match {
      case (_: InnerLike, _) => true
      case (LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin, BuildRight) => true
      case (RightOuter, BuildLeft) => true
      case _ => false
    }
}
