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

import org.apache.spark.MapOutputStatistics
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.aggregate.{DeclarativeAggregate, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.{AggregateEstimation, EstimationUtils}
import org.apache.spark.sql.catalyst.plans.physical.{CoalescedBoundary, CoalescedHashPartitioning, HashPartitioning}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, ColumnarToRowExec, FilterExec, InputAdapter, ProjectExec, RowToColumnarExec, SortExec, SparkPlan, UnaryExecNode, UnionExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, LogicalQueryStage, QueryStageExec, ShufflePartitionsUtil}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Fix physical reader layouts before live writers start. Logical hash buckets keep their original
 * identity; the exchange routes bucket ranges directly into one stream per physical reader.
 * Estimates choose concurrency only and are never published as observed map-output statistics.
 * Missing estimates or a fixed input layout keep the entire compatible group unchanged.
 */
object CoalescePipelinedShufflePartitions extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.batchPipelinedShuffleEnabled || !conf.coalesceShufflePartitionsEnabled ||
        !conf.getConf(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED)) {
      return plan
    }

    val groups = mutable.ArrayBuffer.empty[Seq[ShuffleExchangeExec]]

    // Stop at exchanges: changing their input's width cannot change their declared output layout.
    def frontier(p: SparkPlan): Seq[SparkPlan] = p match {
      case _: Exchange | _: QueryStageExec => Seq(p)
      case leaf if leaf.children.isEmpty => Seq(leaf)
      case other => other.children.flatMap(frontier)
    }

    def independentChildren(p: SparkPlan): Boolean = p match {
      case u: UnionExec => u.isPlainUnion
      case _: BroadcastHashJoinExec | _: BroadcastNestedLoopJoinExec |
          _: CartesianProductExec => true
      case _ => false
    }

    def visit(p: SparkPlan): Unit = p match {
      case s: ShuffleExchangeExec =>
        groups += Seq(s)
        visit(s.child)
      case b: BroadcastExchangeLike => visit(b.child)
      case _: Exchange | _: QueryStageExec =>
      case u: UnaryExecNode => visit(u.child)
      case other if independentChildren(other) => other.children.foreach(visit)
      case other =>
        val inputs = frontier(other)
        val shuffles = inputs.collect { case s: ShuffleExchangeExec => s }
        // Broadcast inputs do not participate in the streamed input's partition alignment.
        val compatible = inputs.forall {
          case _: ShuffleExchangeExec | _: BroadcastExchangeLike |
              _: BroadcastQueryStageExec => true
          case _ => false
        }
        if (compatible && shuffles.nonEmpty) groups += shuffles
        inputs.foreach {
          case s: ShuffleExchangeExec => visit(s.child)
          case b: BroadcastExchangeLike => visit(b.child)
          case _ =>
        }
    }
    visit(plan)

    val minPartitions = conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM).getOrElse {
      val parallelismFirst =
        conf.getConf(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_PARALLELISM_FIRST)
          .getOrElse(conf.getConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST))
      if (parallelismFirst) {
        plan.session.sparkContext.defaultParallelism
      } else {
        1
      }
    }
    val layouts = mutable.HashMap.empty[Int, CoalescedHashPartitioning]
    groups.foreach { group =>
      val eligible = group.forall { s =>
        s.pipelined && s.shuffleOrigin == ENSURE_REQUIREMENTS &&
          s.outputPartitioning.isInstanceOf[HashPartitioning]
      }
      if (eligible && group.map(_.outputPartitioning.numPartitions).distinct.size == 1) {
        val n = group.head.outputPartitioning.numPartitions
        val sizes = group.map(s => estimatedSize(s.child).filter { size =>
          size >= 0 && size < conf.defaultSizeInBytes && size.isValidLong
        })
        val estimatedSizes = sizes.flatten.map(_.max(BigInt(n)))
        if (n > 1 && sizes.forall(_.isDefined) &&
            estimatedSizes.sum <= BigInt(Long.MaxValue)) {
          // Without completed writers only the total estimate is available. Use uniform bucket
          // estimates with the existing size/fan-in policy; do not pretend to know key skew.
          // Keep every bucket represented even for a zero/small estimate. Estimated emptiness
          // cannot justify dropping a route, and the AQE coalescer may omit zero-size ranges.
          val estimates = group.zip(estimatedSizes).map { case (s, size) =>
            val bytes = size.toLong
            Some(new MapOutputStatistics(s.id, Array.tabulate(n) { i =>
              bytes / n + (if (i < bytes % n) 1L else 0L)
            }))
          }
          val advisorySize = group.flatMap(_.advisoryPartitionSize).minOption
            .getOrElse(conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES))
          val specs = ShufflePartitionsUtil.coalescePartitions(
            estimates,
            group.map(_ => None),
            advisorySize,
            math.max(1, minPartitions),
            conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE),
            maxReducerPartitionsPerTask =
              conf.getConf(SQLConf.COALESCE_PARTITIONS_MAX_REDUCER_PARTITIONS_PER_TASK))
          if (specs.nonEmpty && specs.head.size < n) {
            val boundaries = specs.head.map {
              case CoalescedPartitionSpec(start, end, _) => CoalescedBoundary(start, end)
              case spec => throw new IllegalStateException(s"Unexpected estimated layout: $spec")
            }
            group.foreach { s =>
              layouts(s.id) = CoalescedHashPartitioning(
                s.outputPartitioning.asInstanceOf[HashPartitioning], boundaries)
            }
          }
        }
      }
    }

    plan.transformDown {
      case s: ShuffleExchangeExec if layouts.contains(s.id) =>
        s.copy(outputPartitioning = layouts(s.id))
    }
  }

  private[sql] def estimatedSize(plan: SparkPlan): Option[BigInt] = plan match {
    // Generated wrappers can inherit the final aggregate's logical link. Resolve the physical
    // operator first: a one-row final result does not describe its partial aggregate's output.
    case _: WholeStageCodegenExec | _: InputAdapter | _: ColumnarToRowExec |
        _: RowToColumnarExec | _: SortExec => estimatedSize(plan.children.head)
    case a: BaseAggregateExec if a.requiredChildDistributionExpressions.isEmpty ||
        a.aggregateExpressions.exists(e => e.mode == Partial || e.mode == PartialMerge) =>
      estimatedPartialAggregateSize(a)
    case p: ProjectExec if !p.containsTag(SparkPlan.LOGICAL_PLAN_TAG) =>
      estimatedSize(p.child).map(size => resizedRows(size, p.child.output, p.output))
    case f: FilterExec if !f.containsTag(SparkPlan.LOGICAL_PLAN_TAG) => estimatedSize(f.child)
    case _ => plan.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).map(_.stats.sizeInBytes)
      .filter(size => size >= 0 && size < conf.defaultSizeInBytes)
  }

  private def estimatedPartialAggregateSize(agg: BaseAggregateExec): Option[BigInt] = {
    // Opaque aggregate buffers can grow with the input; their declared row type is not a size.
    if (!agg.aggregateExpressions.forall(
        _.aggregateFunction.isInstanceOf[DeclarativeAggregate])) {
      return None
    }
    agg.logicalLink.collect { case a: Aggregate => a.child }.flatMap { adaptiveInput =>
      // Materialized stages report rows and bytes but usually no column statistics. A join
      // above them can consequently be estimated as a Cartesian product. For sizing only,
      // recover the original logical estimate where it still has column statistics. Do not
      // change AQE's runtime statistics or replace a measured empty input with an estimate.
      val input = adaptiveInput.transformDown {
        case stage: LogicalQueryStage if stage.stats.attributeStats.isEmpty &&
            !stage.stats.rowCount.contains(BigInt(0)) &&
            stage.logicalPlan.stats.attributeStats.nonEmpty => stage.logicalPlan
      }
      // Expansion-generated grouping keys need their own estimate. They are not columns of the
      // original logical input and can multiply its rows, so do not infer their cardinality here.
      if (!agg.groupingExpressions.forall(_.references.subsetOf(input.outputSet))) {
        None
      } else {
        val estimated = AggregateEstimation.estimate(
          Aggregate(agg.groupingExpressions, agg.resultExpressions, input))
        estimated match {
          case Some(stats) =>
            agg.child.estimatedPartitionCount.filter(_ > 0).map { producers =>
              // The same group may occur in every producer's partial output. Catalyst estimates
              // global groups; account for those duplicates before sizing physical shuffle rows.
              val rows = stats.rowCount.get * producers
              val bounded = if (agg.groupingExpressions.nonEmpty) {
                rows.min(input.stats.rowCount.get)
              } else {
                rows
              }
              EstimationUtils.getOutputSize(agg.output, bounded, stats.attributeStats)
            }
          case None =>
            // No group cardinality is known. Retain an input-sized estimate with the buffer-row
            // width, instead of treating a scalar final aggregate as a tiny intermediate.
            Option.when(input.stats.sizeInBytes < conf.defaultSizeInBytes) {
              resizedRows(input.stats.sizeInBytes, input.output, agg.output)
            }
        }
      }
    }
  }

  private def resizedRows(
      bytes: BigInt,
      from: Seq[Attribute],
      to: Seq[Attribute]): BigInt = {
    val fromWidth = EstimationUtils.getSizePerRow(from)
    (bytes * EstimationUtils.getSizePerRow(to) + fromWidth - 1) / fromWidth
  }
}
