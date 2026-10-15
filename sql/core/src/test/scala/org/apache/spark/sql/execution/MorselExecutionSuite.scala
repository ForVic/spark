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

package org.apache.spark.sql.execution

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.config
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.scheduler.morsel.MorselScheduler
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.util.ThreadUtils

class MorselExecutionSuite extends QueryTest with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(config.MORSEL_ENABLED, true)
    .set(config.MORSEL_HELPER_DELAY, 0L)
    // Make every file its own morsel.
    .set(config.MORSEL_TARGET_SIZE, 1024L)

  override protected def createSparkSession: TestSparkSession =
    new TestSparkSession(sparkConf, maxLocalTaskFailures = 1, numCores = 4)

  import testImplicits._

  private def scheduler: MorselScheduler = MorselScheduler.get.get

  // A single scan task over many small files, so that idle slots steal its morsels.
  private def withData(f: String => Unit): Unit = withTempPath { dir =>
    val path = dir.getCanonicalPath
    spark.range(0, 200000, 1, 40)
      .selectExpr("id", "id % 97 AS k", "cast(id * 7 % 1000 AS double) AS v",
        "concat('s', cast(id % 13 AS string)) AS s")
      .write.parquet(path)
    withSQLConf(
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1g",
        SQLConf.FILES_MIN_PARTITION_NUM.key -> "1",
        SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1") {
      f(path)
    }
  }

  private def morselStages(df: DataFrame): Seq[Boolean] = {
    df.collect()
    collect(df.queryExecution.executedPlan) {
      case s: ShuffleQueryStageExec => s.shuffle
    }.collect { case e: ShuffleExchangeExec => e.shuffleDependency.morselSafe }
  }

  private def checkSameResults(query: String => DataFrame): Unit = withData { path =>
    val expected = withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "false") {
      query(path).collect().toSeq
    }
    val stolenBefore = scheduler.morselsStolen.get
    val jobsBefore = scheduler.jobsRun.get
    withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
      checkAnswer(query(path), expected)
    }
    assert(scheduler.jobsRun.get > jobsBefore, "no task ran as morsels")
    assert(scheduler.morselsStolen.get > stolenBefore, "no morsel was stolen")
  }

  test("partial aggregation") {
    checkSameResults { path =>
      spark.read.parquet(path).groupBy("k")
        .agg(count("*"), sum("v"), max("s"), countDistinct("s"), avg("id"))
    }
  }

  test("filter and project followed by a hash shuffle") {
    checkSameResults { path =>
      spark.read.parquet(path).filter("v > 100").selectExpr("id", "k * 2 AS k2", "upper(s) AS s")
        .repartition(7, col("k2"))
    }
  }

  test("more reduce partitions than the bypass merge threshold") {
    // Participants sort their output into spill files instead of writing a file per partition.
    checkSameResults { path =>
      spark.read.parquet(path).selectExpr("id", "k", "v").repartition(300, col("id"))
    }
  }

  test("range partitioning") {
    checkSameResults { path =>
      spark.read.parquet(path).filter("k < 50").orderBy("v", "id")
    }
  }

  test("broadcast joins") {
    val dim = (0 until 97 by 2).map(i => (i.toLong, s"name$i")).toDF("dk", "name")
    for (joinType <- Seq("inner", "left_outer", "left_semi", "left_anti")) {
      checkSameResults { path =>
        spark.read.parquet(path).join(broadcast(dim), col("k") === col("dk"), joinType)
          .groupBy("k").agg(count("*"), sum("v"))
      }
    }
    checkSameResults { path =>
      spark.read.parquet(path).join(broadcast(dim), col("k") < col("dk") && col("dk") < 4)
        .groupBy("dk").agg(count("*"))
    }
  }

  test("expand and generate") {
    checkSameResults { path =>
      spark.read.parquet(path).rollup("k", "s").agg(sum("v"), count("*"))
    }
    checkSameResults { path =>
      spark.read.parquet(path).select(col("k"), explode(array(col("v"), col("v") + 1)).as("e"))
        .groupBy("k").agg(sum("e"))
    }
  }

  test("union") {
    checkSameResults { path =>
      val df = spark.read.parquet(path)
      df.filter("k < 10").union(df.filter("k > 90").selectExpr("id", "k", "v * 2", "s"))
        .groupBy("k").agg(sum("v"), count("*"))
    }
  }

  test("key-grouped operators over coalesced shuffle partitions") {
    // AQE coalesces the 50 shuffle partitions so that each reduce task reads several of them;
    // the reduce stage then runs one reducer partition per morsel.
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "50") {
      val finalAgg = (path: String) => spark.read.parquet(path).groupBy("k")
        .agg(sum("v").as("sv"), count("*").as("c")).groupBy(col("c") % 7).agg(sum("sv"))
      val window = (path: String) => spark.read.parquet(path)
        .withColumn("r", row_number().over(Window.partitionBy("k").orderBy("id")))
        .groupBy(col("r") % 11).agg(sum("v"), count("*"))
      // The partial window group limit runs before the shuffle, in the scan stage.
      val topRanks = (path: String) => spark.read.parquet(path)
        .withColumn("r", rank().over(Window.partitionBy("k").orderBy("v")))
        .filter("r <= 2").groupBy(col("r")).agg(sum("v"), count("*"))
      for (query <- Seq(finalAgg, window, topRanks)) {
        checkSameResults(query)
        withData { path =>
          withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
            assert(morselStages(query(path)) == Seq(true, true))
          }
        }
      }
    }
  }

  test("joins of two shuffles") {
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "50",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      for (joinType <- Seq("inner", "left_outer", "full_outer", "left_anti");
           hint <- Seq("merge", "shuffle_hash")) {
        val query = (path: String) => {
          val df = spark.read.parquet(path)
          df.filter("id % 3 != 0").select(col("id"), col("k"), col("v"))
            .join(df.filter("id % 5 != 0").select(col("id"), col("v").as("w")).hint(hint),
              Seq("id"), joinType)
            .groupBy(col("k")).agg(count("*"), sum("v"))
        }
        withData { path =>
          withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
            // Shuffled hash joins are not split: their hash table cannot spill.
            assert(morselStages(query(path)).head == (hint == "merge"))
          }
        }
        checkSameResults(query)
      }
    }
  }

  test("only split-invariant output carries a row-based checksum") {
    withData { path =>
      withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
        def variesWithSplit(df: DataFrame): Seq[Boolean] = {
          df.collect()
          collect(df.queryExecution.executedPlan) {
            case s: ShuffleQueryStageExec => s.shuffle
          }.collect { case e: ShuffleExchangeExec =>
            e.shuffleDependency.morselOutputVariesWithSplit
          }
        }
        val df = spark.read.parquet(path)
        // Each participant emits its own partial aggregate rows.
        assert(variesWithSplit(df.groupBy("k").count()) == Seq(true))
        // Row-wise pipelines emit the same rows however their input is split.
        assert(variesWithSplit(df.filter("v > 10").repartition(7, col("k"))) == Seq(false))
      }
    }
  }

  test("large Parquet row groups are split into page ranges") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      // One file with one row group of 10 pages per column.
      spark.range(0, 200000, 1, 1)
        .selectExpr("id", "id % 97 AS k", "cast(id * 7 % 1000 AS double) AS v")
        .write.option("parquet.block.size", 512L * 1024 * 1024)
        .option("parquet.page.row.count.limit", 20000).parquet(path)
      val queries = Seq[DataFrame => DataFrame](
        _.groupBy("k").agg(sum("v"), count("*"), min("id"), max("id")),
        _.select(col("k"), col("_metadata.row_index").as("r")).groupBy("k").agg(sum("r")))
      for (query <- queries; vectorized <- Seq("true", "false")) {
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> vectorized) {
          val expected = withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "false") {
            query(spark.read.parquet(path)).collect().toSeq
          }
          val stolenBefore = scheduler.morselsStolen.get
          withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
            checkAnswer(query(spark.read.parquet(path)), expected)
          }
          // Only the vectorized reader without row indexes splits the row group.
          if (vectorized == "true" && query == queries.head) {
            assert(scheduler.morselsStolen.get > stolenBefore)
          }
        }
      }
    }
  }

  test("row groups of one file are morsels that the task's thread may merge") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      // One file with many small row groups.
      spark.range(0, 400000, 1, 1)
        .selectExpr("id", "id % 97 AS k", "cast(id * 7 % 1000 AS double) AS v")
        .write.option("parquet.block.size", 64 * 1024).parquet(path)
      val query = (df: DataFrame) => df.groupBy("k").agg(sum("v"), count("*"), max("id"))
      val expected = withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "false") {
        query(spark.read.parquet(path)).collect().toSeq
      }
      val stolenBefore = scheduler.morselsStolen.get
      withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
        checkAnswer(query(spark.read.parquet(path)), expected)
      }
      assert(scheduler.morselsStolen.get > stolenBefore)
    }
  }

  test("local shuffle reads after AQE turns a join into a broadcast join") {
    // Planned as a sort-merge join; at runtime the filtered side is tiny, so AQE broadcasts it
    // and reads the other side's map outputs locally.
    withSQLConf(
        SQLConf.SHUFFLE_PARTITIONS.key -> "50",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1000",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      val query = (path: String) => {
        val df = spark.read.parquet(path)
        df.join(df.filter("id < 100").select(col("id").as("id2")), col("id") === col("id2"))
          .groupBy(col("k")).agg(count("*"), sum("v"))
      }
      checkSameResults(query)
      withData { path =>
        withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
          val df = query(path)
          val stages = morselStages(df)
          assert(collect(df.queryExecution.executedPlan) {
            case read: AQEShuffleReadExec if read.isLocalRead => read
          }.nonEmpty)
          assert(stages.head)
        }
      }
    }
  }

  test("stages that are not morsel-safe") {
    withData { path =>
      withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
        val df = spark.read.parquet(path)
        assert(morselStages(df.groupBy("k").count()) == Seq(true))
        // Round-robin partitioning depends on the order of rows within a task.
        assert(morselStages(df.repartition(5)) == Seq(false))
        assert(morselStages(df.filter(rand(1) > 0.5).groupBy("k").count()) == Seq(false))
        assert(morselStages(df.select(monotonically_increasing_id().as("i"), col("k"))
          .groupBy("k").agg(max("i"))) == Seq(false))
        // A final global aggregation and joins of two shuffles (stages are listed top-down).
        assert(morselStages(df.groupBy().agg(sum("v").as("s")).repartition(3, col("s"))) ==
          Seq(false, true))
        // A join whose sides are not sorted where it reads them (the aggregates' output is
        // already ordered by the join key).
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val left = df.groupBy("k").agg(sum("v").as("a"))
          val right = df.groupBy("k").agg(max("v").as("b"))
          assert(morselStages(left.join(right, "k").join(df.select("k", "s"), "k")
            .groupBy("s").count()).head == false)
        }
      }
      withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "false") {
        assert(morselStages(spark.read.parquet(path).groupBy("k").count()) == Seq(false))
      }
    }
  }

  test("metrics include the work of helpers") {
    withData { path =>
      withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
        var recordsRead = 0L
        val listener = new SparkListener {
          override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
            recordsRead += taskEnd.taskMetrics.inputMetrics.recordsRead
          }
        }
        spark.sparkContext.addSparkListener(listener)
        try {
          val stolenBefore = scheduler.morselsStolen.get
          val df = spark.read.parquet(path).groupBy("k").count()
          df.collect()
          spark.sparkContext.listenerBus.waitUntilEmpty()
          assert(scheduler.morselsStolen.get > stolenBefore)
          assert(listener.synchronized(recordsRead) == 200000)
          val scan = collect(df.queryExecution.executedPlan) { case s: FileSourceScanExec => s }
          assert(scan.head.metrics("numOutputRows").value == 200000)
        } finally {
          spark.sparkContext.removeSparkListener(listener)
        }
      }
    }
  }

  test("cancelling a job stops all participants") {
    withData { path =>
      withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
        val slow = udf((id: Long) => { Thread.sleep(1); id })
        val result = Future {
          spark.sparkContext.setJobGroup("morsel-cancel", "cancel", interruptOnCancel = true)
          spark.read.parquet(path).select(slow(col("id")).as("x"), col("k"))
            .groupBy("k").agg(sum("x")).collect()
        }(ExecutionContext.global)
        // Wait until helpers are working on the task, then cancel it.
        eventually(timeout(30.seconds)) {
          assert(scheduler.numActiveHelpers > 0)
        }
        spark.sparkContext.cancelJobGroup("morsel-cancel")
        intercept[SparkException](ThreadUtils.awaitResult(result, 60.seconds))
        checkAnswer(spark.read.parquet(path).groupBy().count(), Row(200000))
      }
    }
  }

  test("an error in any participant fails the task") {
    withData { path =>
      withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
        // Every morsel fails, so both the task's thread and helpers fail.
        val fails = udf((id: Long) => if (id % 5000 == 4999) throw new IllegalStateException(
          "bad row") else id)
        val e = intercept[SparkException] {
          spark.read.parquet(path).select(fails(col("id")).as("x"), col("k"))
            .groupBy("k").agg(sum("x")).collect()
        }
        assert(e.getMessage.contains("bad row"))
        // The scheduler and the executor still work.
        checkAnswer(spark.read.parquet(path).groupBy().count(), Row(200000))
      }
    }
  }
}
