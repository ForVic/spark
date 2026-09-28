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

package org.apache.spark.sql.execution.adaptive

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RoundRobinPartitioning}
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.execution.{LeafExecNode, LocalTableScanExec}
import org.apache.spark.sql.execution.exchange.{CoalescePipelinedShufflePartitions, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

class BatchMemoryAdaptiveShuffleSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  override protected def sparkConf: SparkConf = super.sparkConf
    .setMaster("local[2]")
    .set("spark.shuffle.memory.enabled", "true")
    .set("spark.shuffle.manager.incremental", "batch-memory")
    .set("spark.executor.taskScheduler.residencyMultiplier", "8")
    .set("spark.shuffle.streaming.networkBufferSize", "4096")
    .set(SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key, "true")
    .set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, "false")
    .set(SQLConf.SHUFFLE_PARTITIONS.key, "4")

  test("AQE and ordinary execution consume live memory output with more readers than cores") {
    for (adaptive <- Seq(false, true)) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        val query = spark.sql("""
          SELECT id % 5000 AS k, count(*) AS n
          FROM range(0, 100000, 1, 8) GROUP BY id % 5000
          """)
        checkAnswer(query, (0L until 5000L).map(k => Row(k, 20L)))
        assert(collectWithSubqueries(query.queryExecution.executedPlan) {
          case s: ShuffleExchangeExec if s.pipelined => s
        }.nonEmpty, query.queryExecution.executedPlan.toString)
      }
    }
  }

  test("joins and partial actions preserve results across repeated live execution") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val text = """
        SELECT a.k, a.n, b.n FROM
          (SELECT id % 101 AS k, count(*) AS n FROM range(0, 10000, 1, 8)
            GROUP BY id % 101) a
        JOIN
          (SELECT id % 101 AS k, count(*) AS n FROM range(0, 10000, 1, 8)
            GROUP BY id % 101) b ON a.k = b.k
        """
      val expected = withSQLConf(SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key -> "false") {
        spark.sql(text).collect().toSeq
      }
      for (_ <- 0 until 2) checkAnswer(spark.sql(text), expected)
      assert(spark.sql(text).take(3).length == 3)
    }
  }

  test("live inputs prepare concurrently with captured properties before job submission") {
    val started = new CountDownLatch(2)
    val release = new CountDownLatch(1)
    spark.sparkContext.setLocalProperty("batch.input.preparation", "captured")
    val exchanges = (0 until 2).map { _ =>
      ShuffleExchangeExec(RoundRobinPartitioning(2),
        BatchPreparingInput(started, release), pipelined = true)
    }
    try {
      exchanges.foreach(_.prepare())
      assert(started.await(10, TimeUnit.SECONDS), "independent input preparation was serialized")
    } finally {
      release.countDown()
      spark.sparkContext.setLocalProperty("batch.input.preparation", null)
    }
    exchanges.foreach { exchange =>
      assert(exchange.inputRDD.getNumPartitions == 0)
      assert(exchange.inputRDD eq exchange.inputRDD)
      assert(exchange.futureAction.get().isEmpty)
    }
  }

  test("live input preparation failures reach the caller") {
    spark.sparkContext.setLocalProperty("batch.input.preparation", "captured")
    try {
      val exchange = ShuffleExchangeExec(RoundRobinPartitioning(2),
        BatchPreparingInput(new CountDownLatch(0), new CountDownLatch(0), fail = true),
        pipelined = true)
      exchange.prepare()
      val error = intercept[SparkException] { exchange.inputRDD }
      assert(error.getCause.getMessage.contains("input preparation failed"))
    } finally {
      spark.sparkContext.setLocalProperty("batch.input.preparation", null)
    }
  }

  test("local CPU scheduling preserves reader parallelism after producers finish") {
    withSQLConf(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
        SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key -> "true") {
      val key = AttributeReference("key", LongType)()
      val child = LocalTableScanExec(Seq(key), Nil, None)
      child.setLogicalLink(StatsTestPlan(Seq(key), 0, AttributeMap(Nil), Some(BigInt(32 << 20))))
      val exchange = ShuffleExchangeExec(HashPartitioning(Seq(key), 8), child, pipelined = true)
      val result = CoalescePipelinedShufflePartitions(exchange)
      assert(result.outputPartitioning.numPartitions == 2)
    }
  }
}

private case class BatchPreparingInput(
    started: CountDownLatch,
    release: CountDownLatch,
    fail: Boolean = false) extends LeafExecNode {
  override def output: Seq[Attribute] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    assert(sparkContext.getLocalProperty("batch.input.preparation") == "captured")
    started.countDown()
    assert(release.await(10, TimeUnit.SECONDS))
    if (fail) throw new IllegalStateException("input preparation failed")
    sparkContext.emptyRDD[InternalRow]
  }
}
