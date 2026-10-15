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

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.config
import org.apache.spark.memory.SparkOutOfMemoryError
import org.apache.spark.paths.SparkPath
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.scheduler.morsel.MorselScheduler
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.{SimpleBatchTable, SimpleScanBuilder, TestingV2Source}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.{HasPartitionStatistics, InputPartition, PartitionReader, PartitionReaderFactory, ScanBuilder, SplittableInputPartition}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetMorsels
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.{MorselSafety, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, QueryExecutionListener}
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
            val df = query(path)
            assert(morselStages(df).head)
            assert(collect(df.queryExecution.executedPlan) {
              case j: ShuffledHashJoinExec => j
              case j: SortMergeJoinExec => j
            }.head.isInstanceOf[ShuffledHashJoinExec] == (hint == "shuffle_hash"))
          }
        }
        checkSameResults(query)
      }
    }
  }

  // A join of a fact table whose key 0 holds 40% of the rows with a small dimension table. The fact
  // side's shuffle has 20 map outputs, so its reducer partitions split into map output ranges.
  private def skewJoin(joinType: String, hint: String, aggregateByKey: Boolean = false) = {
    (_: String) => {
      val fact = spark.range(0, 400000, 1, 20)
        .selectExpr("CASE WHEN id % 10 < 4 THEN 0 ELSE id % 5000 END AS k", "id AS v")
      val dim = spark.range(0, 5000, 1, 4).selectExpr("id AS k", "id * 2 AS w")
      val joined = fact.join(dim.hint(hint), Seq("k"), joinType)
      if (aggregateByKey) {
        joined.groupBy("k").agg(count("*").as("c"), sum("v").as("s"))
          .agg(count("*"), sum("c"), sum("s"))
      } else {
        // Semi and anti joins output the fact side only.
        val w = if (joined.columns.contains("w")) coalesce(col("w"), lit(1L)) else lit(1L)
        joined.agg(count("*"), sum("v"), sum(w))
      }
    }
  }

  private val skewJoinConfs = Seq(
    SQLConf.SHUFFLE_PARTITIONS.key -> "4",
    SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "false",
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1")

  test("skewed joins that AQE splits") {
    withSQLConf(skewJoinConfs ++ Seq(
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD.key -> "1k",
        SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR.key -> "2",
        SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "400k"): _*) {
      for (joinType <- Seq("inner", "left_outer"); hint <- Seq("merge", "shuffle_hash")) {
        val query = skewJoin(joinType, hint)
        withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
          val df = query("")
          assert(morselStages(df).head)
          assert(collect(df.queryExecution.executedPlan) {
            case j: SortMergeJoinExec => j.isSkewJoin
            case j: ShuffledHashJoinExec => j.isSkewJoin
          } == Seq(true))
        }
        checkSameResults(query)
      }
    }
  }

  test("a skewed partition that AQE does not split") {
    withSQLConf(skewJoinConfs :+ (SQLConf.SKEW_JOIN_ENABLED.key -> "false"): _*) {
      // Every join task reads one reducer partition, so only one-sided morsels can be stolen.
      for (joinType <- Seq("inner", "left_outer", "left_semi", "left_anti");
           hint <- Seq("merge", "shuffle_hash")) {
        checkSameResults(skewJoin(joinType, hint))
      }
    }
  }

  test("joins whose skewed side cannot be split") {
    withSQLConf(skewJoinConfs :+ (SQLConf.SKEW_JOIN_ENABLED.key -> "false"): _*) {
      // The skewed side is preserved by the join, or all rows of a key are needed after the join.
      val queries = Seq(skewJoin("right_outer", "merge"), skewJoin("full_outer", "merge"),
        skewJoin("inner", "merge", aggregateByKey = true))
      for (query <- queries) {
        val expected = withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "false") {
          query("").collect().toSeq
        }
        val stolenBefore = scheduler.morselsStolen.get
        withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
          checkAnswer(query(""), expected)
        }
        assert(scheduler.morselsStolen.get == stolenBefore)
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

  test("morsels are sized by the columns that are read") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      // One row group of ten wide columns and a narrow one.
      spark.range(0, 200000, 1, 1)
        .selectExpr("id" +: (0 until 10).map(i => s"repeat(cast(id AS string), 20) AS c$i"): _*)
        .write.option("parquet.block.size", 512L * 1024 * 1024)
        .option("parquet.page.row.count.limit", 20000).parquet(path)
      val file = new File(path).listFiles().filter(_.getName.endsWith(".parquet")).head
      val partitioned = PartitionedFile(InternalRow.empty,
        SparkPath.fromPath(new Path(file.toURI)), 0, file.length())
      val conf = spark.sessionState.newHadoopConf()
      def morsels(columns: Option[Set[String]]): Int = ParquetMorsels.split(partitioned, conf,
        targetSize = 1024 * 1024, splitRowGroups = true, readColumns = columns).length
      assert(morsels(None) > 4)
      assert(morsels(Some(Set("C0"))) > 1)
      // The narrow column is too little work to split, as is reading no column at all.
      assert(morsels(Some(Set("id"))) == 1)
      assert(morsels(Some(Set.empty)) == 1)
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

  test("helpers that run out of memory give their morsels back") {
    val abortsBefore = scheduler.helperAborts.get
    MorselExecutionSuite.helperMemoryFailures.set(3)
    try {
      checkSameResults { path =>
        val oom = udf((id: Long) => {
          if (id % 1000 == 999) MorselExecutionSuite.maybeRunOutOfMemory()
          id
        })
        spark.read.parquet(path).select(oom(col("id")).as("x"), col("k"))
          .groupBy("k").agg(sum("x"), count("*"))
      }
    } finally {
      MorselExecutionSuite.helperMemoryFailures.set(0)
    }
    assert(scheduler.helperAborts.get > abortsBefore)
  }

  private def dataFiles(dir: File): Seq[File] =
    if (dir.isDirectory) dir.listFiles().toSeq.flatMap(dataFiles)
    else if (dir.getName.startsWith("part-")) Seq(dir)
    else Nil

  /** Runs `write` and returns the number of rows the write command reports. */
  private def writtenRows(write: => Unit): Long = {
    var rows = 0L
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        qe.executedPlan.foreach {
          case w: DataWritingCommandExec => rows += w.cmd.metrics("numOutputRows").value
          case _ =>
        }
      override def onFailure(funcName: String, qe: QueryExecution, e: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)
    try {
      write
      spark.sparkContext.listenerBus.waitUntilEmpty()
    } finally {
      spark.listenerManager.unregister(listener)
    }
    rows
  }

  test("file writes") {
    withData { path =>
      val df = spark.read.parquet(path).filter("v > 10").selectExpr("id", "v", "s", "k % 5 AS p")
      val expected = df.collect().toSeq
      for (partitioned <- Seq(false, true); format <- Seq("parquet", "orc", "json")) {
        withTempPath { out =>
          val stolenBefore = scheduler.morselsStolen.get
          val rows = withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
            writtenRows {
              val writer = df.write.format(format)
              (if (partitioned) writer.partitionBy("p") else writer).save(out.getCanonicalPath)
            }
          }
          assert(scheduler.morselsStolen.get > stolenBefore)
          assert(rows == expected.length)
          checkAnswer(spark.read.schema(df.schema).format(format).load(out.getCanonicalPath)
            .select("id", "v", "s", "p"), expected)
          // Helpers write files of their own, alongside the task's.
          val files = dataFiles(out)
          assert(files.exists(_.getName.contains("-m")))
          assert(files.exists(!_.getName.contains("-m")))
        }
      }
    }
  }

  test("writes into tables") {
    withData { path =>
      withTable("t") {
        sql("CREATE TABLE t (id BIGINT, v DOUBLE, p BIGINT) USING parquet PARTITIONED BY (p)")
        withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
          val stolenBefore = scheduler.morselsStolen.get
          sql(s"INSERT INTO t SELECT id, v, k % 3 FROM parquet.`$path`")
          sql(s"INSERT OVERWRITE t PARTITION (p = 1) SELECT id, v FROM parquet.`$path` " +
            "WHERE k % 3 = 1 AND id % 2 = 0")
          assert(scheduler.morselsStolen.get > stolenBefore)
        }
        val source = spark.read.parquet(path)
        checkAnswer(spark.table("t"),
          source.filter("k % 3 != 1 OR id % 2 = 0").selectExpr("id", "v", "k % 3"))
      }
    }
  }

  test("writes after an aggregation over coalesced shuffle partitions") {
    withData { path =>
      withTempPath { out =>
        withSQLConf(
            SQLConf.SHUFFLE_PARTITIONS.key -> "50",
            SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
          val stolenBefore = scheduler.morselsStolen.get
          spark.read.parquet(path).groupBy("k").agg(sum("v").as("sv"), count("*").as("c"))
            .write.parquet(out.getCanonicalPath)
          assert(scheduler.morselsStolen.get > stolenBefore)
        }
        checkAnswer(spark.read.parquet(out.getCanonicalPath),
          spark.read.parquet(path).groupBy("k").agg(sum("v").as("sv"), count("*").as("c")))
        assert(dataFiles(out).exists(_.getName.contains("-m")))
      }
    }
  }

  test("helpers that run out of memory while writing delete their files") {
    withData { path =>
      withTempPath { out =>
        val abortsBefore = scheduler.helperAborts.get
        MorselExecutionSuite.helperMemoryFailures.set(3)
        try {
          val oom = udf((id: Long) => {
            if (id % 1000 == 999) MorselExecutionSuite.maybeRunOutOfMemory()
            id
          })
          withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
            spark.read.parquet(path).select(oom(col("id")).as("id"), col("k"))
              .write.partitionBy("k").parquet(out.getCanonicalPath)
          }
        } finally {
          MorselExecutionSuite.helperMemoryFailures.set(0)
        }
        assert(scheduler.helperAborts.get > abortsBefore)
        checkAnswer(spark.read.parquet(out.getCanonicalPath).groupBy().agg(count("*"),
          countDistinct("id"), sum("id")), Row(200000, 200000, 200000L * 199999 / 2))
      }
    }
  }

  test("a failed write leaves no files") {
    withData { path =>
      withTempPath { out =>
        val fails = udf((id: Long) => {
          if (id % 5000 == 4999 && Thread.currentThread().getName.startsWith("morsel-helper")) {
            throw new IllegalStateException("bad row")
          }
          id
        })
        withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
          val e = intercept[SparkException] {
            spark.read.parquet(path).select(fails(col("id")).as("id"), col("k"))
              .write.parquet(out.getCanonicalPath)
          }
          assert(e.getMessage.contains("bad row"))
        }
        assert(dataFiles(out).isEmpty)
      }
    }
  }

  test("writes that rely on the order of their input are not split") {
    withData { path =>
      // The rows of each output partition are only adjacent if the stage sorts them.
      val plan = spark.read.parquet(path).queryExecution.executedPlan
      assert(MorselSafety.writeUnsafeReason(plan, requiresGroupedInput = true).nonEmpty)
      assert(MorselSafety.writeUnsafeReason(plan, requiresGroupedInput = false).isEmpty)
      val sorted = spark.read.parquet(path).sortWithinPartitions("k").queryExecution.executedPlan
      assert(MorselSafety.writeUnsafeReason(sorted, requiresGroupedInput = true).isEmpty)
    }
  }

  test("data source V2 scans") {
    // A source whose single input partition splits into pieces.
    checkSameResults { _ =>
      spark.read.format(classOf[SplittableRangeSource].getName).load()
        .groupBy(col("i") % 7).agg(sum("j"), count("*"))
    }
    // The built-in file sources through V2: files and row groups are morsels.
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      checkSameResults { path =>
        val df = spark.read.parquet(path)
        assert(df.queryExecution.executedPlan.exists(_.isInstanceOf[BatchScanExec]))
        df.groupBy("k").agg(count("*"), sum("v"), max("s"))
      }
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.range(0, 400000, 1, 1)
          .selectExpr("id", "id % 97 AS k", "cast(id * 7 % 1000 AS double) AS v")
          .write.option("parquet.block.size", 64 * 1024).parquet(path)
        val query = () => spark.read.parquet(path).groupBy("k").agg(sum("v"), count("*"))
        val expected = withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "false") {
          query().collect().toSeq
        }
        val stolenBefore = scheduler.morselsStolen.get
        withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
          checkAnswer(query(), expected)
        }
        assert(scheduler.morselsStolen.get > stolenBefore)
      }
    }
  }
}

object MorselExecutionSuite {
  /** How many more times a helper should run out of memory, see [[maybeRunOutOfMemory]]. */
  val helperMemoryFailures = new AtomicInteger(0)

  def maybeRunOutOfMemory(): Unit = {
    if (Thread.currentThread().getName.startsWith("morsel-helper") &&
        helperMemoryFailures.getAndDecrement() > 0) {
      // scalastyle:off throwerror
      throw new SparkOutOfMemoryError("UNABLE_TO_ACQUIRE_MEMORY",
        Map("requestedBytes" -> "1", "receivedBytes" -> "0").asJava)
      // scalastyle:on throwerror
    }
  }
}

/** A data source V2 with one input partition, which it can split into ranges of rows. */
class SplittableRangeSource extends TestingV2Source {
  override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
      new SimpleScanBuilder {
        override def planInputPartitions(): Array[InputPartition] =
          Array(SplittableRange(0, 200000))
        override def createReaderFactory(): PartitionReaderFactory = SplittableRangeReaderFactory
      }
  }
}

case class SplittableRange(start: Int, end: Int)
  extends SplittableInputPartition with HasPartitionStatistics {

  override def split(targetSizeInBytes: Long): Array[InputPartition] =
    (start until end by 5000).map(s => SplittableRange(s, math.min(s + 5000, end))).toArray

  override def sizeInBytes(): java.util.OptionalLong = java.util.OptionalLong.of(8L * (end - start))
  override def numRows(): java.util.OptionalLong = java.util.OptionalLong.of(end - start)
  override def filesCount(): java.util.OptionalLong = java.util.OptionalLong.empty()
}

object SplittableRangeReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val SplittableRange(start, end) = partition
    new PartitionReader[InternalRow] {
      private var current = start - 1
      override def next(): Boolean = {
        current += 1
        current < end
      }
      override def get(): InternalRow = InternalRow(current, -current)
      override def close(): Unit = {}
    }
  }
}
