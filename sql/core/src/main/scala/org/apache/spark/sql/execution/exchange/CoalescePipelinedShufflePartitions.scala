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

import org.apache.spark.{MapOutputStatistics, SparkEnv}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeSet}
import org.apache.spark.sql.catalyst.expressions.aggregate.{DeclarativeAggregate, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, Filter, Join, LeafNode, LogicalPlan, Project, Statistics, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.{AggregateEstimation, EstimationUtils}
import org.apache.spark.sql.catalyst.plans.physical.{CoalescedBoundary, CoalescedHashPartitioning, HashPartitioning}
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, ColumnarToRowExec, FilterExec, InputAdapter, ProjectExec, RowToColumnarExec, SortExec, SparkPlan, UnaryExecNode, UnionExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadRule, BroadcastQueryStageExec, LogicalQueryStage, QueryStageExec, ShufflePartitionsUtil}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, CartesianProductExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Fix physical reader layouts before live writers start. Logical hash buckets keep their original
 * identity; the exchange routes bucket ranges directly into one stream per physical reader.
 * Estimates choose concurrency only and are never published as observed map-output statistics.
 * Missing estimates or a fixed input layout keep the entire compatible group unchanged.
 */
object CoalescePipelinedShufflePartitions extends AQEShuffleReadRule {
  override protected def supportedShuffleOrigins: Seq[ShuffleOrigin] = Seq(ENSURE_REQUIREMENTS)

  override def apply(plan: SparkPlan): SparkPlan = {
    coalesce(plan, mutable.HashMap.empty)
  }

  private[exchange] def coalesce(
      plan: SparkPlan,
      sizeCache: mutable.Map[Int, Option[BigInt]]): SparkPlan = {
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
    lazy val capacity = plan.session.sparkContext.maxNumConcurrentTasks(
      plan.session.sparkContext.resourceProfileManager.defaultResourceProfile)
    val localCpuScheduling = plan.session.sparkContext.getConf.get(
      org.apache.spark.internal.config.EXECUTOR_TASK_RESIDENCY_MULTIPLIER) > 1
    def originalHash(s: ShuffleExchangeExec): Option[HashPartitioning] =
      s.outputPartitioning match {
        case h: HashPartitioning => Some(h)
        case c: CoalescedHashPartitioning => Some(c.from)
        case _ => None
      }
    def groupSizes(group: Seq[ShuffleExchangeExec]): Seq[Option[BigInt]] = group.map { s =>
      sizeCache.getOrElseUpdate(s.child.id, estimatedSize(s.child)).filter { size =>
        size >= 0 && size < conf.defaultSizeInBytes && size.isValidLong
      }
    }
    if (!groups.exists(_.exists(_.pipelined))) return plan
    val shuffleIds = groups.flatten.map(_.id).toSet
    val liveInputs = groups.flatten.filter(_.pipelined)
    val liveIds = liveInputs.map(_.id).toSet
    val livePrerequisites = liveInputs.flatMap(_.child.collect {
      case s: ShuffleExchangeExec => s.id
    }).toSet
    // Legacy scheduling must reserve concurrent slots for independent groups. With local CPU
    // admission, ready tasks share cores at execution time. Permanently dividing their reader
    // counts would strand capacity when a sibling finishes or waits for a prerequisite.
    // Durable ancestors must not be counted again as independent ordinary siblings.
    // Only live layouts change below; ordinary and fixed/unknown layouts remain with AQE.
    lazy val independent = groups.filter { group =>
      group.forall { s =>
        isSupported(s) && originalHash(s).isDefined &&
          !s.child.exists(p => liveIds.contains(p.id)) &&
          (s.pipelined || (!livePrerequisites.contains(s.id) &&
            !s.child.exists(p => shuffleIds.contains(p.id))))
      } && group.map(s => originalHash(s).get.numPartitions).distinct.size == 1 &&
        groupSizes(group).forall(_.isDefined)
    }
    val sharedMinimum = if (!localCpuScheduling &&
        independent.size > 1 && minPartitions > 1 && capacity > 0) {
      val reservedProducers = independent.map(_.size).sum
      val budget = math.min(minPartitions, math.max(1, capacity - reservedProducers))
      val extra = math.max(0, budget - independent.size)
      val sizes = independent.map(group => groupSizes(group).flatten.sum.max(BigInt(1)))
      val totalSize = sizes.sum
      independent.zip(sizes).map { case (group, size) =>
        group.head.id -> (1 + (BigInt(extra) * size / totalSize).toInt)
      }.toMap
    } else {
      Map.empty[Int, Int]
    }
    groups.foreach { group =>
      val minimum = sharedMinimum.getOrElse(group.head.id, minPartitions)
      val eligible = group.forall { s =>
        s.pipelined && isSupported(s) &&
          originalHash(s).isDefined
      }
      if (eligible && group.map(s => originalHash(s).get.numPartitions).distinct.size == 1) {
        val n = originalHash(group.head).get.numPartitions
        val sizes = groupSizes(group)
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
          // With local CPU scheduling, readers can use every core after producers finish.
          // Do not permanently divide their parallelism by the number of producer stages.
          // Legacy scheduling still needs a fixed share for simultaneous producers. Both paths
          // retain the complete group's residency admission check.
          val readerParallelism = if (capacity > 0 && minPartitions > 1) {
            val stageParallelism = if (localCpuScheduling) capacity else {
              math.max(1, capacity / (group.size + 1))
            }
            val cpuBudget = math.min(minimum, stageParallelism)
            val streamSize = SparkEnv.get.pipelinedShuffleManager.advisoryBytesPerStream
            val widths = group.map(_.child.estimatedPartitionCount.filter(_ > 0))
            val readerBudget = if (streamSize > 0 && widths.forall(_.isDefined)) {
              // Each physical reader creates a stream from every producer. Avoid subdividing a
              // small output into mostly partial frames and short-lived transport connections.
              // Unknown producer widths keep the CPU budget; explicit bucket fan-in still wins.
              val producers = widths.flatten.map(BigInt(_)).sum
              val usefulStreams = (estimatedSizes.sum / (producers * streamSize)).max(BigInt(1))
              usefulStreams.min(BigInt(cpuBudget)).toInt
            } else {
              cpuBudget
            }
            val bucketsPerReader = (n.toLong + readerBudget - 1) / readerBudget
            val largestBucket = estimatedSizes.map(size => (size + n - 1) / n).sum
            // AQE packs whole buckets. Round their target up first, so a fractional bucket
            // does not create an extra reader that consumes the producers' reserved capacity.
            val targetSize = largestBucket * bucketsPerReader
            (estimatedSizes.sum / targetSize).min(BigInt(minimum)).max(BigInt(1)).toInt
          } else {
            minimum
          }
          val specs = ShufflePartitionsUtil.coalescePartitions(
            estimates,
            group.map(_ => None),
            advisorySize,
            math.max(1, readerParallelism),
            conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE),
            maxReducerPartitionsPerTask =
              conf.getConf(SQLConf.COALESCE_PARTITIONS_MAX_REDUCER_PARTITIONS_PER_TASK))
          // Only tighten unstarted layouts. Completed query stages were excluded by visit().
          if (specs.nonEmpty &&
              specs.head.size < group.map(_.outputPartitioning.numPartitions).min) {
            val boundaries = specs.head.map {
              case CoalescedPartitionSpec(start, end, _) => CoalescedBoundary(start, end)
              case spec => throw new IllegalStateException(s"Unexpected estimated layout: $spec")
            }
            group.foreach { s =>
              val layout = CoalescedHashPartitioning(originalHash(s).get, boundaries)
              if (layout != s.outputPartitioning) layouts(s.id) = layout
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
      estimatedAggregateSize(a, partial = true)
    case a: BaseAggregateExec =>
      estimatedAggregateSize(a, partial = false).orElse(logicalSize(a))
    case p: ProjectExec if !p.containsTag(SparkPlan.LOGICAL_PLAN_TAG) =>
      estimatedSize(p.child).map(size => resizedRows(size, p.child.output, p.output))
    case f: FilterExec if !f.containsTag(SparkPlan.LOGICAL_PLAN_TAG) => estimatedSize(f.child)
    case _ => logicalSize(plan)
  }

  private def logicalSize(plan: SparkPlan): Option[BigInt] = {
    plan.getTagValue(SparkPlan.LOGICAL_PLAN_TAG).map(_.stats.sizeInBytes)
      .filter(size => size >= 0 && size < conf.defaultSizeInBytes)
  }

  private def estimatedAggregateSize(
      agg: BaseAggregateExec,
      partial: Boolean): Option[BigInt] = {
    // Opaque aggregate buffers can grow with the input; their declared row type is not a size.
    if (!agg.aggregateExpressions.forall(
        _.aggregateFunction.isInstanceOf[DeclarativeAggregate])) {
      return None
    }
    agg.logicalLink.collect { case a: Aggregate => a.child }.flatMap { adaptiveInput =>
      // A retained physical aggregate can still link to the original logical plan. Match its
      // completed physical inputs before estimating, rather than discarding their measured rows.
      val completed = agg.child.collect {
        case stage: QueryStageExec if stage.isMaterialized =>
          stage.plan.logicalLink.filter(_.output == stage.output).flatMap { logical =>
            stage.computeStats().map(stats => logical -> estimatedStage(logical, stats))
          }
      }.flatten.toMap
      val input = adaptiveInput.transformDown {
        case stage: LogicalQueryStage if stage.stats.isRuntime =>
          estimatedStage(stage.logicalPlan, stage.stats)
        case logical if completed.contains(logical) => completed(logical)
      }
      // Expansion-generated grouping keys need their own estimate. They are not columns of the
      // original logical input and can multiply its rows, so do not infer their cardinality here.
      if (!agg.groupingExpressions.forall(_.references.subsetOf(input.outputSet))) {
        None
      } else {
        val estimated = AggregateEstimation.estimate(
          Aggregate(agg.groupingExpressions, agg.resultExpressions, input))
        val tupleEstimate = Option.when(agg.groupingExpressions.forall(_.deterministic)) {
          groupingCardinality(input, AttributeSet(agg.groupingExpressions.flatMap(_.references)))
        }.flatten
        val groups = Seq(estimated.flatMap(_.rowCount), tupleEstimate).flatten.minOption
        groups match {
          case Some(groupRows) =>
            val copies = if (partial) {
              agg.child.estimatedPartitionCount.filter(_ > 0)
            } else {
              // The required distribution has brought each group to one final aggregate.
              // Reuse its key-domain estimate without multiplying by the input task count.
              Some(1)
            }
            copies.map { count =>
              // A partial group can occur in every producer. Apply the tuple estimate before
              // this multiplication, and retain the input-row cap when it is available.
              val rows = groupRows * count
              val bounded = if (agg.groupingExpressions.nonEmpty) {
                input.stats.rowCount.fold(rows)(rows.min)
              } else {
                rows
              }
              val columns = estimated.map(_.attributeStats).getOrElse(input.stats.attributeStats)
              val size = if (bounded == 0) BigInt(0) else {
                EstimationUtils.getOutputSize(agg.output, bounded, columns)
              }
              // Without CBO a projection may retain bytes but omit rowCount. Grouped output
              // cannot contain more rows than its input; retain that existing byte estimate
              // as a cap instead of multiplying all input rows by the producer count.
              if (agg.groupingExpressions.nonEmpty &&
                  input.stats.sizeInBytes < conf.defaultSizeInBytes) {
                size.min(resizedRows(input.stats.sizeInBytes, input.output, agg.output))
              } else {
                size
              }
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

  // This leaf exists only in a temporary estimation tree. It never replaces AQE's runtime stats.
  private case class EstimatedStage(original: LogicalPlan, estimate: Statistics) extends LeafNode {
    override def output: Seq[Attribute] = original.output
    override def computeStats(): Statistics = estimate
  }

  private def estimatedStage(original: LogicalPlan, observed: Statistics): EstimatedStage = {
    val catalog = original.transformDown {
      case stage: LogicalQueryStage => stage.logicalPlan
    }.stats
    val columns = (catalog.rowCount, observed.rowCount) match {
      case (Some(oldRows), Some(newRows)) if oldRows > 0 =>
        AttributeMap(catalog.attributeStats.toSeq.map { case (attr, column) =>
          // A smaller measured row count does not imply a proportional loss of distinct keys.
          // Keep the catalog domain, capped by the observed rows, for subsequent join estimates.
          attr -> column.updateCountStats(oldRows, newRows).copy(
            distinctCount = column.distinctCount.map(_.min(newRows)), histogram = None)
        })
      case _ => observed.attributeStats
    }
    EstimatedStage(original, observed.copy(attributeStats = columns, isRuntime = false))
  }

  // Size grouped tuples using their lineage as well as individual column counts. Join fan-out
  // repeats source tuples; it cannot create new combinations of columns from the same source.
  // These are estimates for reader sizing only, never correctness bounds or runtime statistics.
  private[sql] def groupingCardinality(
      plan: LogicalPlan,
      attributes: AttributeSet): Option[BigInt] = {
    if (attributes.isEmpty) return Some(BigInt(1))
    if (!attributes.subsetOf(plan.outputSet)) return None

    val columns = attributes.toSeq.map(plan.stats.attributeStats.get)
    val columnBound = if (columns.forall(_.exists(_.distinctCount.isDefined))) {
      Some(columns.flatten.map { stat =>
        stat.distinctCount.get + (if (stat.nullCount.contains(BigInt(0))) 0 else 1)
      }.product)
    } else {
      None
    }
    val sourceBound = plan match {
      case e: Expand =>
        val positions = attributes.toSeq.map(attr => e.output.indexWhere(_.exprId == attr.exprId))
        val counts = e.projections.map { projection =>
          val expressions = positions.map(projection)
          Option.when(expressions.forall(_.deterministic)) {
            groupingCardinality(e.child, AttributeSet(expressions.flatMap(_.references)))
          }.flatten
        }
        // Grouping sets repeat source tuples with some keys replaced by constants. Each set
        // contributes at most its source domain; different sets can overlap, so sum their caps.
        Option.when(counts.forall(_.isDefined))(counts.flatten.sum)
      case p: Project =>
        val expressions = p.projectList.filter(e => attributes.contains(e.toAttribute))
        if (expressions.forall(_.deterministic)) {
          groupingCardinality(p.child, AttributeSet(expressions.flatMap(_.references)))
        } else {
          None
        }
      case f: Filter => groupingCardinality(f.child, attributes)
      case s: SubqueryAlias => groupingCardinality(s.child, attributes)
      case s: LogicalQueryStage => groupingCardinality(s.logicalPlan, attributes)
      case s: EstimatedStage => groupingCardinality(s.original, attributes)
      case j: Join if j.joinType.isInstanceOf[InnerLike] =>
        for {
          left <- groupingCardinality(j.left, attributes.intersect(j.left.outputSet))
          right <- groupingCardinality(j.right, attributes.intersect(j.right.outputSet))
        } yield left * right
      case _ => None
    }
    Seq(plan.stats.rowCount, columnBound, sourceBound).flatten.minOption
  }

  private def resizedRows(
      bytes: BigInt,
      from: Seq[Attribute],
      to: Seq[Attribute]): BigInt = {
    val fromWidth = EstimationUtils.getSizePerRow(from)
    (bytes * EstimationUtils.getSizePerRow(to) + fromWidth - 1) / fromWidth
  }
}
