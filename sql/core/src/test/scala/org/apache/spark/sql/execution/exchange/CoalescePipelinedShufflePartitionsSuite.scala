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
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, EqualTo, Literal}
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, ColumnStat, Expand, Join, JoinHint, Project}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.plans.physical.{CoalescedBoundary, CoalescedHashPartitioning, HashPartitioning, HashPartitioningLike, IdentityBroadcastMode}
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.{CoalescedPartitionSpec, LocalTableScanExec, ProjectExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, LogicalQueryStage, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions.{collect_list, countDistinct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.types.LongType

class CoalescePipelinedShufflePartitionsSuite extends QueryTest with SharedSparkSession {
  override protected def createSparkSession: SparkSession =
    new TestSparkSession(sparkConf, numCores = 8)

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
      val sc = spark.sparkContext
      val capacity = sc.maxNumConcurrentTasks(sc.resourceProfileManager.defaultResourceProfile)
      val readers = coalesce(plan).outputPartitioning.numPartitions
      assert(readers >= 1 && readers < capacity)
      if (capacity > 2) assert(readers > 1)
      withSQLConf(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_PARALLELISM_FIRST.key -> "false") {
        assert(coalesce(plan).outputPartitioning.numPartitions == 1)
        assert(spark.sessionState.conf.getConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST))
      }
    }
  }

  test("sparse framed streams use fewer readers while dense outputs retain parallelism") {
    withSQLConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "true",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1") {
      def framedExchange(bytes: Long): ShuffleExchangeExec = {
        val child = spark.range(0, 128000, 1, 128).queryExecution.sparkPlan
        child.setLogicalLink(StatsTestPlan(child.output, 0, AttributeMap(Nil), Some(BigInt(bytes))))
        ShuffleExchangeExec(HashPartitioning(child.output, 200), child, pipelined = true)
      }
      // Exact bucket sizes keep whole-bucket rounding independent of the framing constraint.
      val sparse = framedExchange(200 * 48 * 1024L)
      assert(sparse.child.estimatedPartitionCount.contains(128))
      assert(CoalescePipelinedShufflePartitions.estimatedSize(sparse.child)
        .contains(BigInt(200 * 48 * 1024L)))
      assert(coalesce(sparse).outputPartitioning.numPartitions == 2)
      assert(coalesce(framedExchange(200 * 384 * 1024L))
        .outputPartitioning.numPartitions == 4)
      val ordinary = sparse.copy(pipelined = false)
      assert(coalesce(ordinary).fastEquals(ordinary))
    }
  }

  test("live parallelism shares capacity with its simultaneously running producers") {
    withSQLConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "true",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1") {
      def wideExchange(): ShuffleExchangeExec = {
        val input = exchange(Some(BigInt(32 * 1024 * 1024)))
        input.copy(outputPartitioning = HashPartitioning(input.output, 200))
      }
      val capacity = spark.sparkContext.maxNumConcurrentTasks(
        spark.sparkContext.resourceProfileManager.defaultResourceProfile)
      val one = wideExchange()
      val readers = coalesce(one).outputPartitioning.numPartitions
      assert(readers > 1 && readers * 2 <= capacity,
        "a standalone live edge must leave comparable producer concurrency")
      val joined = coalesce(join(wideExchange(), wideExchange()))
      val joinedReaders = joined.collect { case s: ShuffleExchangeExec => s.numPartitions }
      assert(joinedReaders.distinct.size == 1)
      assert(joinedReaders.head * 3 <= capacity,
        "aligned live inputs share consumer tasks but each has its own producers")
      val ordinary = one.copy(pipelined = false)
      assert(coalesce(ordinary).fastEquals(ordinary))
    }
  }

  test("independent live branches share reader parallelism and reserve their producers") {
    withSQLConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "true",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1",
        SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false") {
      val branches = Seq.fill(2)(exchange(Some(BigInt(8 * 1024 * 1024))))
      val alone = coalesce(branches.head).outputPartitioning.numPartitions
      val result = coalesce(UnionExec(branches))
      val readers = result.children.map(_.outputPartitioning.numPartitions)
      assert(readers.forall(n => n > 1 && n < alone))
      assert(readers.sum + branches.size <= 8,
        "independent readers must leave capacity for both live producers")
      assert(coalesce(result).fastEquals(result))

      // A dependent live exchange is not another independent branch competing for this budget.
      val inner = branches(1)
      inner.setLogicalLink(inner.child.logicalLink.get)
      val nested = inner.copy(child = inner)
      val sequential = coalesce(nested).collect { case s: ShuffleExchangeExec => s.numPartitions }
      assert(sequential === Seq(alone, alone))
    }
  }

  test("local CPU admission preserves reader parallelism after independent branches finish") {
    withSparkContextConf("spark.executor.taskScheduler.residencyMultiplier" -> "8") {
      withSQLConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "true",
          SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1",
          SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false") {
        val live = exchange(Some(BigInt(8 * 1024 * 1024)))
        val alone = coalesce(live).outputPartitioning.numPartitions
        assert(alone > 1)
        Seq(false, true).foreach { pipelined =>
          val sibling = exchange(Some(BigInt(8 * 1024 * 1024))).copy(pipelined = pipelined)
          val result = coalesce(UnionExec(Seq(live, sibling)))
          assert(result.children.head.outputPartitioning.numPartitions == alone)
          if (pipelined) {
            assert(result.children(1).outputPartitioning.numPartitions == alone)
          } else {
            assert(result.children(1).fastEquals(sibling))
          }
        }
      }
    }
  }

  test("ordinary siblings share the live reader budget without changing their own layout") {
    withSQLConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "true",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1",
        SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false") {
      val live = exchange(Some(BigInt(8 * 1024 * 1024)))
      val ordinary = exchange(Some(BigInt(8 * 1024 * 1024))).copy(pipelined = false)
      val alone = coalesce(live).outputPartitioning.numPartitions
      val mixed = coalesce(UnionExec(Seq(live, ordinary)))
      val pairedLive = coalesce(UnionExec(Seq(live, ordinary.copy(pipelined = true))))
      val readers = mixed.children.head.outputPartitioning.numPartitions
      assert(readers > 1 && readers < alone)
      assert(readers == pairedLive.children.head.outputPartitioning.numPartitions)
      assert(mixed.children(1).fastEquals(ordinary))

      // A durable ancestor is a prerequisite, not another concurrent branch of the same input.
      ordinary.setLogicalLink(ordinary.child.logicalLink.get)
      val dependent = live.copy(child = ordinary)
      val result = coalesce(dependent).asInstanceOf[ShuffleExchangeExec]
      assert(result.numPartitions == alone)
      assert(result.child.fastEquals(ordinary))
    }
  }

  test("independent live branches retain shared budgets across durable prerequisites") {
    withSQLConf(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "true",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1",
        SQLConf.UNION_OUTPUT_PARTITIONING.key -> "false") {
      val branches = Seq.fill(2) {
        val upstream = exchange(Some(BigInt(8 * 1024 * 1024))).copy(pipelined = false)
        upstream.setLogicalLink(upstream.child.logicalLink.get)
        upstream.copy(child = upstream, pipelined = true)
      }
      val alone = coalesce(branches.head).outputPartitioning.numPartitions
      val result = coalesce(UnionExec(branches))
      val readers = result.children.map(_.outputPartitioning.numPartitions)
      assert(readers.forall(n => n > 1 && n < alone))
      assert(readers.sum + branches.size <= 8)
      result.children.zip(branches).foreach { case (resized, original) =>
        assert(resized.children.head.fastEquals(original.child))
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
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "1m",
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

  test("final group estimates do not multiply key domains by producer count") {
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      Seq(1, 8).foreach { producers =>
        val child = spark.range(0, 1000000, 1, producers).selectExpr("id AS k", "id AS label")
          .queryExecution.executedPlan
        val attrs = child.output
        val columns = AttributeMap(attrs.map(_ -> ColumnStat(
          distinctCount = Some(BigInt(500)), nullCount = Some(BigInt(0)))))
        val dimension = StatsTestPlan(attrs, 500, columns)
        val factKey = AttributeReference("fact_key", LongType)()
        val fact = StatsTestPlan(Seq(factKey), 1000000, AttributeMap(Nil))
        val input = Join(dimension, fact, Inner, None, JoinHint.NONE)
        val logical = Aggregate(attrs, attrs, input)
        val aggregate = HashAggregateExec(Some(attrs), false, None, attrs,
          Nil, Nil, 0, attrs, child)
        aggregate.setLogicalLink(logical)
        val bytes = EstimationUtils.getOutputSize(attrs, BigInt(500), columns)
        assert(logical.stats.sizeInBytes > bytes)
        assert(CoalescePipelinedShufflePartitions.estimatedSize(aggregate).contains(bytes))
      }
    }
  }

  test("final aggregates retain observed size when their original aggregate link is gone") {
    val child = spark.range(0, 100, 1, 8).queryExecution.sparkPlan
    val attrs = child.output
    val observed = StatsTestPlan(attrs, 4, AttributeMap(Nil), Some(BigInt(64)))
    val aggregate = HashAggregateExec(Some(attrs), false, None, attrs, Nil, Nil, 0, attrs, child)
    aggregate.setLogicalLink(observed)
    assert(CoalescePipelinedShufflePartitions.estimatedSize(aggregate).contains(BigInt(64)))
    val partial = aggregate.copy(requiredChildDistributionExpressions = None)
    partial.setLogicalLink(observed)
    assert(CoalescePipelinedShufflePartitions.estimatedSize(partial).isEmpty)
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
  test("grouped dimension columns are bounded together before sizing partial output") {
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "false",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "256k",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1") {
      val child = spark.range(0, 1000000, 1, 8).selectExpr("id AS k", "id AS label")
        .queryExecution.executedPlan
      val attrs = child.output
      val columns = AttributeMap(attrs.map(_ -> ColumnStat(
        distinctCount = Some(BigInt(500)), nullCount = Some(BigInt(0)))))
      val dimension = StatsTestPlan(attrs, 500, columns)
      val factKey = AttributeReference("fact_key", LongType)()
      val fact = StatsTestPlan(Seq(factKey), 1000000,
        AttributeMap(Seq(factKey -> ColumnStat(distinctCount = Some(BigInt(1000000)),
          nullCount = Some(BigInt(0))))))
      val input = Join(dimension, fact, Inner, None, JoinHint.NONE)
      val partial = HashAggregateExec(None, false, None, attrs, Nil, Nil, 0, attrs, child)
      partial.setLogicalLink(Aggregate(attrs, attrs, input))
      val shuffle = ShuffleExchangeExec(HashPartitioning(attrs, 8), partial, pipelined = true)
      assert(coalesce(shuffle).outputPartitioning.numPartitions == 1)
      val expectedRows = BigInt(500 * 8)
      assert(CoalescePipelinedShufflePartitions.estimatedSize(partial).contains(
        EstimationUtils.getOutputSize(attrs, expectedRows, columns)))
    }
  }

  test("tuple estimates follow deterministic projections and combine independent sources") {
    val key = AttributeReference("key", LongType)()
    val label = AttributeReference("label", LongType)()
    val other = AttributeReference("other", LongType)()
    def input(attrs: Seq[Attribute], rows: Long): StatsTestPlan =
      StatsTestPlan(attrs, rows, AttributeMap(attrs.map(_ -> ColumnStat(
        distinctCount = Some(BigInt(rows)), nullCount = Some(BigInt(0))))))
    val left = input(Seq(key, label), 500)
    val right = input(Seq(other), 100)
    val projected = Project(Seq(Alias(Add(key, Literal(1L)), "mapped")(), label), left)
    val joined = Join(projected, right, Inner, None, JoinHint.NONE)
    assert(CoalescePipelinedShufflePartitions.groupingCardinality(
      joined, AttributeSet(projected.output)).contains(BigInt(500)))
    assert(CoalescePipelinedShufflePartitions.groupingCardinality(
      joined, joined.outputSet).contains(BigInt(50000)))
  }

  test("grouping sets sum source tuple domains instead of expanded input rows") {
    val key = AttributeReference("key", LongType)()
    val label = AttributeReference("label", LongType)()
    val input = StatsTestPlan(Seq(key, label), 500,
      AttributeMap(Seq(key, label).map(_ -> ColumnStat(
        distinctCount = Some(BigInt(500)), nullCount = Some(BigInt(0))))))
    val output = Seq("key", "label", "grouping_id").map(AttributeReference(_, LongType)())
    val nil = Literal.create(null, LongType)
    val expanded = Expand(Seq(
      Seq(key, label, Literal(0L)),
      Seq(key, nil, Literal(1L)),
      Seq(nil, nil, Literal(3L))), output, input)
    assert(CoalescePipelinedShufflePartitions.groupingCardinality(
      expanded, expanded.outputSet).contains(BigInt(1001)))
    // Physical grouping keys can change nullability without changing attribute identity.
    assert(CoalescePipelinedShufflePartitions.groupingCardinality(
      expanded, AttributeSet(output.map(_.withNullability(false)))).contains(BigInt(1001)))
  }

  test("runtime row counts preserve key domains when estimating a downstream join") {
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true") {
      val key = AttributeReference("key", LongType)()
      val value = AttributeReference("value", LongType)()
      val other = AttributeReference("other", LongType)()
      def input(attrs: Seq[Attribute]): StatsTestPlan = {
        val columns = attrs.map { attr =>
          val distinct = if (attr == value) 1000000 else 10000
          attr -> ColumnStat(distinctCount = Some(BigInt(distinct)),
            min = Some(0L), max = Some(distinct.toLong - 1), nullCount = Some(BigInt(0)))
        }
        StatsTestPlan(attrs, 1000000, AttributeMap(columns))
      }
      def completed(logical: StatsTestPlan, id: Int): ShuffleQueryStageExec = {
        val scan = LocalTableScanExec(logical.output, Nil, None)
        val exchange = ShuffleExchangeExec(HashPartitioning(Seq(logical.output.head), 8), scan)
        exchange.setLogicalLink(logical)
        exchange.metrics("dataSize").set(240000)
        exchange.metrics("shuffleRecordsWritten").set(10000)
        new ShuffleQueryStageExec(id, exchange, exchange.canonicalized) {
          _resultOption.set(Some(()))
        }
      }
      val left = input(Seq(key, value))
      val right = input(Seq(other))
      val logical = Join(left, right, Inner, Some(EqualTo(key, other)), JoinHint.NONE)
      val physical = join(completed(left, 0), completed(right, 1))
      val partial = HashAggregateExec(None, false, None, Seq(value), Nil, Nil, 0,
        Seq(value), physical)
      partial.setLogicalLink(Aggregate(Seq(value), Seq(value), logical))
      // Both measured inputs retain 10,000 possible join keys. Scaling their domains down
      // with the row ratio would inflate this join to a million rows and reserve extra readers.
      assert(CoalescePipelinedShufflePartitions.estimatedSize(partial).contains(BigInt(160000)))
      assert(left.stats.rowCount.contains(BigInt(1000000)))
      assert(right.stats.rowCount.contains(BigInt(1000000)))
    }
  }

  test("live layouts tighten after AQE settles the producer partition count") {
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "false",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "8k",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key -> "1") {
      val key = AttributeReference("key", LongType)()
      val input = LocalTableScanExec(Seq(key), Nil, None)
      val logical = StatsTestPlan(Seq(key), 100000, AttributeMap(Seq(key -> ColumnStat(
        distinctCount = Some(BigInt(10)), nullCount = Some(BigInt(0))))))
      val materialized = ShuffleExchangeExec(HashPartitioning(Seq(key), 200), input)
      materialized.setLogicalLink(logical)
      materialized.metrics("dataSize").set(1600000)
      materialized.metrics("shuffleRecordsWritten").set(100000)
      val stage = new ShuffleQueryStageExec(0, materialized, materialized.canonicalized) {
        _resultOption.set(Some(()))
      }
      val partial = HashAggregateExec(None, false, None, Seq(key), Nil, Nil, 0, Seq(key), stage)
      partial.setLogicalLink(Aggregate(Seq(key), Seq(key), logical))
      val exchange = ShuffleExchangeExec(HashPartitioning(Seq(key), 200), partial, pipelined = true)
      val selected = coalesce(exchange).asInstanceOf[ShuffleExchangeExec]
      assert(selected.numPartitions == 4)
      val specs = (0 until 200 by 25).map(start => CoalescedPartitionSpec(start, start + 25))
      val read = AQEShuffleReadExec(stage, specs)
      val ready = selected.withNewChildren(Seq(partial.withNewChildren(Seq(read))))
      assert(coalesce(ready).outputPartitioning.numPartitions == 1)
      assert(stage.outputPartitioning.numPartitions == 200)
    }
  }

  test("completed physical inputs refine an aggregate retaining its original logical link") {
    withSQLConf(SQLConf.CBO_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "false",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "256k") {
      val child = spark.range(0, 1000000, 1, 8).queryExecution.executedPlan
      val attr = child.output.head
      val original = StatsTestPlan(Seq(attr), 1000000,
        AttributeMap(Seq(attr -> ColumnStat(distinctCount = Some(BigInt(10000)),
          nullCount = Some(BigInt(0))))))
      val materialized = ShuffleExchangeExec(HashPartitioning(Seq(attr), 8), child)
      materialized.setLogicalLink(original)
      materialized.metrics("dataSize").set(1600)
      materialized.metrics("shuffleRecordsWritten").set(100)
      val stage = new ShuffleQueryStageExec(0, materialized, materialized.canonicalized) {
        _resultOption.set(Some(()))
      }
      val observed = stage.computeStats()
      val partial = HashAggregateExec(None, false, None, Seq(attr), Nil, Nil, 0, Seq(attr), stage)
      partial.setLogicalLink(Aggregate(Seq(attr), Seq(attr), original))
      val hash = HashPartitioning(Seq(attr), 8)
      val layout = CoalescedHashPartitioning(hash,
        (0 until 8 by 2).map(start => CoalescedBoundary(start, start + 2)))
      val shuffle = ShuffleExchangeExec(layout, partial, pipelined = true)
      assert(coalesce(shuffle).outputPartitioning.numPartitions == 1)
      assert(stage.computeStats() == observed)
      assert(original.stats.rowCount.contains(BigInt(1000000)))
    }
  }

}
