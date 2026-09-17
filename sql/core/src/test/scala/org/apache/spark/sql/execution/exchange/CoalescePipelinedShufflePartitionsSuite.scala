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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, ColumnStat}
import org.apache.spark.sql.catalyst.plans.physical.{CoalescedBoundary, CoalescedHashPartitioning, HashPartitioning, HashPartitioningLike, IdentityBroadcastMode}
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.execution.{LocalTableScanExec, ProjectExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.adaptive.{LogicalQueryStage, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions.{collect_list, countDistinct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

class CoalescePipelinedShufflePartitionsSuite extends QueryTest with SharedSparkSession {
  private def exchange(size: Option[BigInt] = Some(BigInt(0))): ShuffleExchangeExec = {
    val attr = AttributeReference("key", LongType)()
    val child = LocalTableScanExec(Seq(attr), Nil, None)
    size.foreach { bytes =>
      child.setLogicalLink(StatsTestPlan(Seq(attr), 0, AttributeMap(Nil), Some(bytes)))
    }
    ShuffleExchangeExec(HashPartitioning(Seq(attr), 8), child, pipelined = true)
  }

  private def join(left: SparkPlan, right: SparkPlan): SparkPlan =
    SortMergeJoinExec(Seq(left.output.head), Seq(right.output.head), Inner, None, left, right)

  private def coalesce(plan: SparkPlan): SparkPlan = withSQLConf(
      SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key -> "true",
      SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key -> "true",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true") {
    CoalescePipelinedShufflePartitions(plan)
  }

  test("estimated empty inputs keep every hash route and respect the fan-in limit") {
    withSQLConf(SQLConf.COALESCE_PARTITIONS_MAX_REDUCER_PARTITIONS_PER_TASK.key -> "3") {
      val result = coalesce(exchange()).asInstanceOf[ShuffleExchangeExec]
      val expected = Seq(CoalescedBoundary(0, 3), CoalescedBoundary(3, 6), CoalescedBoundary(6, 8))
      assert(result.outputPartitioning.asInstanceOf[CoalescedHashPartitioning].partitions ===
        expected)
      assert(coalesce(result).fastEquals(result))
    }
  }

  test("a fixed or unknown sibling keeps the complete join layout unchanged") {
    val fixedInputs = Seq(
      exchange(None),
      exchange(Some(BigInt(Long.MaxValue))),
      exchange().copy(pipelined = false),
      exchange().copy(shuffleOrigin = REPARTITION_BY_NUM),
      exchange().child)
    fixedInputs.foreach { fixed =>
      val plan = join(exchange(), fixed)
      assert(coalesce(plan).fastEquals(plan), s"Changed an incompatible layout: $fixed")
    }
  }

  test("live reader sizing can retain the ordinary AQE parallelism policy") {
    withSQLConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "true",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1") {
      val plan = exchange(Some(BigInt(8 * 1024 * 1024)))
      assert(coalesce(plan).outputPartitioning.numPartitions > 1)
      withSQLConf(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_PARALLELISM_FIRST.key -> "false") {
        assert(coalesce(plan).outputPartitioning.numPartitions == 1)
        assert(spark.sessionState.conf.getConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST))
      }
    }
  }

  test("a generated projection cannot turn an unknown input size into a known estimate") {
    withSQLConf(SQLConf.DEFAULT_SIZE_IN_BYTES.key -> "1m") {
      val key = AttributeReference("key", LongType)()
      val value = AttributeReference("value", LongType)()
      val input = LocalTableScanExec(Seq(key, value), Nil, None)
      input.setLogicalLink(StatsTestPlan(input.output, 0, AttributeMap(Nil),
        Some(BigInt(spark.sessionState.conf.defaultSizeInBytes))))
      val project = ProjectExec(Seq(key), input)
      val shuffle = ShuffleExchangeExec(HashPartitioning(Seq(key), 8), project, pipelined = true)
      assert(CoalescePipelinedShufflePartitions.estimatedSize(project).isEmpty)
      assert(coalesce(shuffle).fastEquals(shuffle))
    }
  }

  test("nested joins share one layout while plain union branches are independent") {
    val plan = join(join(exchange(), exchange()), exchange())
    val result = coalesce(plan)
    val partitions = result.collect { case s: ShuffleExchangeExec => s.outputPartitioning }
    assert(partitions.size == 3)
    assert(partitions.forall(_.isInstanceOf[CoalescedHashPartitioning]))
    assert(partitions.map(_.numPartitions).distinct === Seq(1))

    val fixed = exchange(None)
    val alignedUnion = UnionExec(Seq(exchange(), fixed))
    assert(coalesce(alignedUnion).fastEquals(alignedUnion))
    withSQLConf(SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false") {
      val union = coalesce(UnionExec(Seq(exchange(), fixed)))
      assert(union.children.head.outputPartitioning.numPartitions == 1)
      assert(union.children(1).fastEquals(fixed))
    }
  }

  test("a materialized boundary does not prevent coalescing its independent live input") {
    val inner = exchange()
    val outer = inner.copy(child = inner, pipelined = false)
    val result = coalesce(outer).asInstanceOf[ShuffleExchangeExec]
    assert(result.outputPartitioning == outer.outputPartitioning)
    assert(result.child.outputPartitioning.numPartitions == 1)
  }

  test("broadcast preparation has its own live input layout") {
    val broadcast = BroadcastExchangeExec(IdentityBroadcastMode, exchange())
    val result = coalesce(broadcast).asInstanceOf[BroadcastExchangeExec]
    assert(result.child.outputPartitioning.numPartitions == 1)
    assert(result.outputPartitioning == broadcast.outputPartitioning)
    val nested = BroadcastNestedLoopJoinExec(exchange(), broadcast, BuildRight, Inner, None)
    val outer = coalesce(join(nested, exchange()))
    val widths = outer.collect { case s: ShuffleExchangeExec => s.numPartitions }
    assert(widths === Seq(1, 1, 1))
  }

  test("writer layouts reject incomplete or overlapping hash ranges") {
    val input = exchange()
    val invalid = Seq(
      Seq.empty,
      Seq(CoalescedBoundary(1, 8)),
      Seq(CoalescedBoundary(0, 7)),
      Seq(CoalescedBoundary(0, 3), CoalescedBoundary(4, 8)),
      Seq(CoalescedBoundary(0, 4), CoalescedBoundary(3, 8)))
    invalid.foreach { boundaries =>
      val exchange = input.copy(outputPartitioning = CoalescedHashPartitioning(
        input.outputPartitioning.asInstanceOf[HashPartitioning], boundaries))
      val error = intercept[IllegalArgumentException](exchange.shuffleDependency)
      assert(error.getMessage.contains("cover every hash bucket exactly once"))
    }
  }

  test("a scalar final result does not size its high-cardinality partial aggregate") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "8") {
      val plan = spark.range(0, 1000000, 1, 8).selectExpr("hash(id) AS k").agg(countDistinct("k"))
        .queryExecution.executedPlan.transformDown {
          case s: ShuffleExchangeExec => s.copy(pipelined = true)
        }
      val result = coalesce(plan)
      val shuffles = result.collect {
        case s: ShuffleExchangeExec if s.outputPartitioning.isInstanceOf[HashPartitioningLike] => s
      }
      assert(shuffles.nonEmpty, s"$result")
      assert(shuffles.forall(_.numPartitions > 1))
    }
  }

  test("opaque partial buffers retain their reader count") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "8") {
      val input = spark.range(10000).selectExpr("id % 10 AS k", "id")
      val plan = input.groupBy("k").agg(collect_list("id"))
        .queryExecution.executedPlan.transformDown {
          case s: ShuffleExchangeExec => s.copy(pipelined = true)
        }
      assert(coalesce(plan).fastEquals(plan))
    }
  }

  test("partial group estimates include duplicates across producers") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "false",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "256k",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1") {
      Seq(1, 8).foreach { producers =>
        val child = spark.range(0, 1000000, 1, producers).queryExecution.executedPlan
        val attr = child.output.head
        val input = StatsTestPlan(Seq(attr), 1000000,
          AttributeMap(Seq(attr -> ColumnStat(distinctCount = Some(BigInt(10000)),
            nullCount = Some(BigInt(0))))))
        val partial = HashAggregateExec(None, false, None, Seq(attr), Nil, Nil, 0, Seq(attr), child)
        partial.setLogicalLink(Aggregate(Seq(attr), Seq(attr), input))
        val shuffle = ShuffleExchangeExec(HashPartitioning(Seq(attr), 8), partial, pipelined = true)
        val result = coalesce(shuffle).asInstanceOf[ShuffleExchangeExec]
        assert(result.numPartitions == producers)
      }
    }
  }

  test("partial estimates recover catalog columns lost at an adaptive stage boundary") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "false",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "256k",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1") {
      val child = spark.range(0, 1000000, 1, 1).queryExecution.executedPlan
      val attr = child.output.head
      val inputs = Seq((true, 1000000L), (false, 1000000L), (true, 0L))
      inputs.foreach { case (hasCatalogColumns, rows) =>
        val columns = if (hasCatalogColumns) {
          AttributeMap(Seq(attr -> ColumnStat(distinctCount = Some(BigInt(10000)),
            nullCount = Some(BigInt(0)))))
        } else {
          AttributeMap[ColumnStat](Nil)
        }
        val logical = StatsTestPlan(Seq(attr), 1000000, columns)
        val materialized = ShuffleExchangeExec(HashPartitioning(Seq(attr), 1), child)
        materialized.metrics("dataSize").set(rows * 16)
        materialized.metrics("shuffleRecordsWritten").set(rows)
        val stage = new ShuffleQueryStageExec(0, materialized, materialized.canonicalized) {
          _resultOption.set(Some(()))
        }
        val input = LogicalQueryStage(logical, stage)
        val observed = input.stats
        assert(observed.isRuntime && observed.attributeStats.isEmpty)
        val partial = HashAggregateExec(None, false, None, Seq(attr), Nil, Nil, 0, Seq(attr), child)
        partial.setLogicalLink(Aggregate(Seq(attr), Seq(attr), input))
        val shuffle = ShuffleExchangeExec(HashPartitioning(Seq(attr), 8), partial, pipelined = true)
        val result = coalesce(shuffle).asInstanceOf[ShuffleExchangeExec]
        assert(result.numPartitions == (if (hasCatalogColumns) 1 else 8))
        assert(input.stats == observed)
        if (rows == 0) {
          assert(CoalescePipelinedShufflePartitions.estimatedSize(partial).contains(BigInt(0)))
        }
      }
    }
  }
}
