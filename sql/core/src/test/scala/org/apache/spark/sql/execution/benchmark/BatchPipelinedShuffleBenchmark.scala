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

package org.apache.spark.sql.execution.benchmark

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import jdk.jfr.{Configuration, Recording}

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.physical.CoalescedHashPartitioning
import org.apache.spark.sql.catalyst.util.{resourceToString, CharVarcharUtils}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.exchange.{CoalescePipelinedShufflePartitions, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.memory.MemoryAllocator

/**
 * Compare ordinary execution with the batch policy using identical inputs and partition settings.
 * With no arguments, run synthetic aggregation and join cases. With --tpcds <parquet directory>
 * [comma-separated queries], run the repository's TPC-DS v1.4 queries.
 * --prepare-tpcds <raw> <parquet> converts dsdgen's pipe-delimited files. These are research runs,
 * not audited TPC-DS results. Add --explain to print the executed plans before timing.
 * --diagnose <directory> writes alternating paired samples, task metrics and final plans;
 * --profile additionally records CPU/allocation/lock samples with JFR, after warmup.
 * spark.shuffle.benchmark.iterations controls the number of pairs (default 12).
 * spark.shuffle.benchmark.coalescingAblation adds a third mode with live reader coalescing,
 * rotating measurement order within the same JVM. Every mode is checked against ordinary output.
 * spark.shuffle.benchmark.catalogStatistics registers external tables and analyzes all columns
 * before measuring. It uses Spark's existing CBO for every mode; setup time is reported separately.
 *
 * Configure spark.master, spark.sql.shuffle.partitions and spark.shuffle.manager.incremental using
 * the usual Spark properties. Local execution defaults to the channel transport; a cluster uses
 * the RPC transport. The optimizer contains no knowledge of the queries in this harness.
 */
object BatchPipelinedShuffleBenchmark extends BenchmarkBase with AdaptiveSparkPlanHelper {
  override def runBenchmarkSuite(args: Array[String]): Unit = {
    // BenchmarkBase sets this for its microbenchmarks. Measure normal scheduling here.
    System.clearProperty("spark.testing")
    require(!org.apache.spark.util.Utils.isTesting,
      "Unset SPARK_TESTING for end-to-end benchmarks, including SBT Test / envVars.")
    require(!MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED,
      "Disable spark.memory.debugFill for benchmarks; SBT Test enables it by default.")
    // scalastyle:off println
    println(s"Benchmark runtime: memoryDebugFill=${MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED}, " +
      s"testing=${org.apache.spark.util.Utils.isTesting}, " +
      s"assertions=${classOf[MemoryAllocator].desiredAssertionStatus()}")
    // scalastyle:on println
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[8]")
      .setAppName("batch-pipelined-shuffle-benchmark")
      .setIfMissing(SQLConf.SHUFFLE_PARTITIONS.key, "2")
      .setIfMissing(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set(SQLConf.LOCAL_PIPELINED_SHUFFLE_ENABLED.key, "false")
      .set("spark.speculation", "false")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.ui.enabled", "false")
      .setIfMissing("spark.sql.ui.retainedExecutions", "10")
      .setIfMissing("spark.ui.retainedJobs", "20")
      .setIfMissing("spark.ui.retainedStages", "40")
    if (org.apache.spark.util.Utils.isLocalMaster(conf)) {
      conf.setIfMissing("spark.shuffle.manager.incremental",
        "org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager")
    }
    val spark = SparkSession.builder().config(conf).getOrCreate()
    try {
      val explainPlans = args.contains("--explain")
      val diagnostics = args.indexOf("--diagnose") match {
        case -1 => None
        case i =>
          require(i + 1 < args.length, "--diagnose needs an output directory")
          Some(Files.createDirectories(Paths.get(args(i + 1))))
      }
      val filtered = args.toList.filterNot(a => a == "--explain" || a == "--profile")
      val positional = filtered.indexOf("--diagnose") match {
        case -1 => filtered
        case i => filtered.take(i) ++ filtered.drop(i + 2)
      }
      positional match {
        case "--prepare-tpcds" :: raw :: parquet :: Nil =>
          spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key, false)
          TPCDSSchemaHelper.getTableColumns.toSeq.sortBy(_._1).foreach { case (table, schema) =>
            spark.read.schema(CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema))
              .option("sep", "|").option("quote", "")
              .option("nullValue", "").csv(s"$raw/$table.dat")
              .write.mode("errorifexists").parquet(s"$parquet/$table")
          }
        case "--tpcds" :: path :: rest if rest.size <= 1 =>
          val catalogStatistics = sys.props
            .getOrElse("spark.shuffle.benchmark.catalogStatistics", "false").toBoolean
          val setupStart = System.nanoTime()
          if (catalogStatistics) {
            val database = "pipeline_benchmark_" + java.util.UUID.randomUUID().toString
              .replace("-", "")
            spark.sql(s"CREATE DATABASE $database")
            spark.catalog.setCurrentDatabase(database)
            spark.conf.set(SQLConf.CBO_ENABLED.key, true)
            spark.conf.set(SQLConf.PLAN_STATS_ENABLED.key, true)
          }
          TPCDSSchemaHelper.getTableColumns.toSeq.sortBy(_._1).foreach { case (table, schema) =>
            val rowSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema)
            if (catalogStatistics) {
              spark.catalog.createTable(table, "parquet", rowSchema, Map("path" -> s"$path/$table"))
              spark.sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR ALL COLUMNS")
            } else {
              spark.read.schema(rowSchema).parquet(s"$path/$table").createOrReplaceTempView(table)
            }
          }
          // scalastyle:off println
          println(s"TPC-DS catalogStatistics=$catalogStatistics, " +
            s"setupSeconds=${(System.nanoTime() - setupStart) / 1e9}")
          // scalastyle:on println
          val loader = Thread.currentThread().getContextClassLoader
          val all = (1 to 99).flatMap(i => Seq(s"q$i", s"q${i}a", s"q${i}b"))
            .filter(q => loader.getResource(s"tpcds/$q.sql") != null)
          val selected = rest.headOption.map(_.split(",").toSet)
          val queries = all.filter(q => selected.forall(_.contains(q)))
          require(queries.nonEmpty, "No matching TPC-DS queries")
          queries.foreach { name =>
            compare(spark, name, resourceToString(s"tpcds/$name.sql", classLoader = loader),
              explainPlans = explainPlans, diagnostics = diagnostics,
              profile = args.contains("--profile"))
          }
        case Nil =>
          compare(spark, "aggregation and Top-N", """
            SELECT id % 1000 AS k, sum(id) AS total
            FROM (SELECT /*+ REPARTITION(2, id) */ id FROM range(0, 1000000, 1, 2))
            GROUP BY id % 1000 ORDER BY total DESC LIMIT 100
            """, requirePipeline = true, explainPlans = explainPlans,
            diagnostics = diagnostics, profile = args.contains("--profile"))
          compare(spark, "shuffled join and aggregate", """
            SELECT /*+ MERGE(l, r) */ l.id % 100 AS k, sum(l.id + r.id) AS total
            FROM range(0, 200000, 1, 2) l JOIN range(0, 300000, 1, 2) r ON l.id = r.id
            GROUP BY l.id % 100
            """, requirePipeline = true, explainPlans = explainPlans,
            diagnostics = diagnostics, profile = args.contains("--profile"))
        case _ =>
          throw new IllegalArgumentException("Expected --tpcds <parquet> [queries], " +
            "--prepare-tpcds <raw> <parquet>, or no arguments")
      }
    } finally {
      spark.stop()
    }
  }

  private def compare(
      spark: SparkSession,
      name: String,
      query: String,
      requirePipeline: Boolean = false,
      explainPlans: Boolean = false,
      diagnostics: Option[Path] = None,
      profile: Boolean = false): Unit = {
    val key = SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key
    spark.conf.set(key, false)
    val regular = spark.sql(query)
    val expected = regular.collect().toSeq
    spark.conf.set(key, true)
    val df = spark.sql(query)
    val actual = df.collect().toSeq
    val difference = QueryTest.sameRows(expected, actual)
    require(difference.isEmpty, s"$name returned different rows: ${difference.getOrElse("")}")
    val exchanges = collectWithSubqueries(df.queryExecution.executedPlan) {
      case s: ShuffleExchangeExec => s
    }
    val pipelined = exchanges.count(_.pipelined)
    require(!requirePipeline || pipelined > 0, s"$name did not exercise pipelining")
    // Keep plan inspection and correctness checking outside the timed region. A zero count is an
    // explicit fallback, never reported as a successful pipelining experiment.
    // scalastyle:off println
    println(s"$name: rows=${actual.size}, pipelined=$pipelined, " +
      s"materialized=${exchanges.size - pipelined}")
    if (explainPlans) {
      println(s"$name materialized AQE plan:")
      regular.explain()
      println(s"$name batch pipelining plan:")
      df.explain()
    }
    // scalastyle:on println
    if (diagnostics.isDefined) {
      diagnose(spark, name, query, expected, diagnostics.get, profile)
      return
    }
    val benchmark = new Benchmark(name, 1, minNumIters = 3,
      warmupTime = 1.second, minTime = 3.seconds, output = output)
    Seq(false, true).foreach { enabled =>
      benchmark.addCase(if (enabled) "batch pipelining" else "materialized AQE") { _ =>
        spark.conf.set(key, enabled)
        spark.sql(query).collect()
      }
    }
    benchmark.run()
  }

  /** Separate planning from execution, alternate order, and retain task-level evidence. */
  private def diagnose(
      spark: SparkSession,
      name: String,
      query: String,
      expected: Seq[Row],
      directory: Path,
      profile: Boolean): Unit = {
    val sc = spark.sparkContext
    val key = SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key
    val coalescingKey = SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key
    val ablation = sys.props.getOrElse("spark.shuffle.benchmark.coalescingAblation", "false")
      .toBoolean
    val initialCoalescing = spark.conf.get(coalescingKey).toBoolean
    val modes = if (ablation) Seq((false, false), (true, false), (true, true))
      else Seq((false, initialCoalescing), (true, initialCoalescing))
    def configure(mode: (Boolean, Boolean)): Unit = {
      spark.conf.set(key, mode._1)
      spark.conf.set(coalescingKey, mode._2)
    }
    val iterations = sys.props.getOrElse("spark.shuffle.benchmark.iterations", "12").toInt
    val warmups = sys.props.getOrElse("spark.shuffle.benchmark.warmups", "3").toInt
    require(iterations > 0)
    require(warmups > 0)
    val samples = ArrayBuffer("iteration,pipelined,start_ms,end_ms,planning_ms,execution_ms," +
      "stages,tasks,task_ms,cpu_ms,deserialize_ms,gc_ms,fetch_wait_ms,write_ms," +
      "shuffle_read_bytes,shuffle_write_bytes,shuffle_read_records,shuffle_write_records," +
      "pipelined_exchanges,materialized_exchanges,coalesced_live_exchanges," +
      "live_logical_buckets,live_physical_readers,coalescing")
    val tasks = ArrayBuffer("iteration,pipelined,stage,task,launch_ms,finish_ms,run_ms,cpu_ms," +
      "deserialize_ms,gc_ms,fetch_wait_ms,write_ms,host,executor_id,coalescing")
    class DiagnosticListener extends SparkListener {
      val completed = ArrayBuffer.empty[SparkListenerTaskEnd]
      var stages = 0
      override def onTaskEnd(e: SparkListenerTaskEnd): Unit = completed += e
      override def onStageCompleted(e: SparkListenerStageCompleted): Unit = stages += 1
      def reset(): Unit = { completed.clear(); stages = 0 }
    }
    val listener = new DiagnosticListener
    sc.addSparkListener(listener)
    val recording = if (profile) Some(new Recording(Configuration.getConfiguration("profile")))
      else None
    try {
      // Warm every mode before profiling or measuring. Every action still has a fresh plan.
      for (_ <- 0 until warmups; mode <- modes) {
        configure(mode)
        val difference = QueryTest.sameRows(expected, spark.sql(query).collect().toSeq)
        require(difference.isEmpty, s"$name mode=$mode: ${difference.getOrElse("")}")
      }
      recording.foreach { r =>
        r.enable("jdk.ThreadPark").withThreshold(java.time.Duration.ofMillis(1))
        r.enable("jdk.JavaMonitorEnter").withThreshold(java.time.Duration.ofMillis(1))
        r.start()
      }
      for (i <- 0 until iterations;
          mode <- {
            // Follow each three-mode ordering with its reverse. Even a short, four-round run
            // then balances every pair's relative order; six rounds cover all permutations.
            val offset = (if (modes.size == 3) i / 2 else i) % modes.size
            val rotated = modes.drop(offset) ++ modes.take(offset)
            if (modes.size == 3 && i % 2 != 0) rotated.reverse else rotated
          }) {
        val enabled = mode._1
        sc.listenerBus.waitUntilEmpty(10000)
        listener.reset()
        configure(mode)
        val startMs = System.currentTimeMillis()
        val start = System.nanoTime()
        val df = spark.sql(query)
        df.queryExecution.executedPlan
        val planned = System.nanoTime()
        df.collect()
        val finished = System.nanoTime()
        val endMs = System.currentTimeMillis()
        sc.listenerBus.waitUntilEmpty(10000)
        val metrics = listener.completed.map(_.taskMetrics).filter(_ != null)
        // AQE choices can vary between executions. Classify every measured plan outside timing,
        // rather than attributing all repetitions using the separate correctness run's plan.
        val exchanges = collectWithSubqueries(df.queryExecution.executedPlan) {
          case s: ShuffleExchangeExec => s
        }
        val values = Seq(i, enabled, startMs, endMs, (planned - start) / 1e6,
          (finished - planned) / 1e6, listener.stages, metrics.size,
          metrics.map(_.executorRunTime).sum, metrics.map(_.executorCpuTime).sum / 1e6,
          metrics.map(_.executorDeserializeTime).sum, metrics.map(_.jvmGCTime).sum,
          metrics.map(_.shuffleReadMetrics.fetchWaitTime).sum,
          metrics.map(_.shuffleWriteMetrics.writeTime).sum / 1e6,
          metrics.map(_.shuffleReadMetrics.totalBytesRead).sum,
          metrics.map(_.shuffleWriteMetrics.bytesWritten).sum,
          metrics.map(_.shuffleReadMetrics.recordsRead).sum,
          metrics.map(_.shuffleWriteMetrics.recordsWritten).sum,
          exchanges.count(_.pipelined), exchanges.count(!_.pipelined),
          exchanges.count(s => s.pipelined &&
            s.outputPartitioning.isInstanceOf[CoalescedHashPartitioning]),
          exchanges.filter(_.pipelined).map(_.outputPartitioning match {
            case p: CoalescedHashPartitioning => p.from.numPartitions
            case p => p.numPartitions
          }).sum,
          exchanges.filter(_.pipelined).map(_.numPartitions).sum, mode._2)
        samples += values.mkString(",")
        listener.completed.foreach { e =>
          val m = e.taskMetrics
          if (m != null) {
            tasks += Seq(i, enabled, e.stageId, e.taskInfo.taskId, e.taskInfo.launchTime,
              e.taskInfo.finishTime, m.executorRunTime, m.executorCpuTime / 1e6,
              m.executorDeserializeTime, m.jvmGCTime, m.shuffleReadMetrics.fetchWaitTime,
              m.shuffleWriteMetrics.writeTime / 1e6, e.taskInfo.host,
              e.taskInfo.executorId, mode._2).mkString(",")
          }
        }
        if (i == 0) {
          val suffix = if (ablation) s"-coalescing-${mode._2}" else ""
          Files.writeString(directory.resolve(s"$name-$enabled$suffix-plan.txt"),
            df.queryExecution.executedPlan.toString, UTF_8)
          val layouts = exchanges.map { s =>
            val logical = s.child.collectFirst {
              case p if p.logicalLink.isDefined => p.logicalLink.get
            }
            Seq(s.id, s.pipelined, s.shuffleOrigin, s.numPartitions,
              logical.map(_.nodeName).getOrElse("unknown"),
              logical.map(_.stats.sizeInBytes.toString).getOrElse("unknown"),
              CoalescePipelinedShufflePartitions.estimatedSize(s.child)
                .map(_.toString).getOrElse("unknown"),
              s.outputPartitioning).mkString("\t")
          }
          Files.writeString(directory.resolve(s"$name-$enabled$suffix-exchanges.tsv"),
            "id\tpipelined\torigin\treaders\tlogical_node\tlogical_link_bytes\t" +
              "estimated_input_bytes\tpartitioning\n" +
              layouts.mkString("\n"), UTF_8)
        }
      }
      recording.foreach { r =>
        r.stop()
        r.dump(directory.resolve(s"$name.jfr"))
      }
      Files.writeString(directory.resolve(s"$name-samples.csv"), samples.mkString("\n"), UTF_8)
      Files.writeString(directory.resolve(s"$name-tasks.csv"), tasks.mkString("\n"), UTF_8)
      // scalastyle:off println
      println(s"$name: wrote $iterations paired diagnostic samples to $directory")
      // scalastyle:on println
    } finally {
      recording.foreach(_.close())
      sc.removeSparkListener(listener)
      spark.conf.set(coalescingKey, initialCoalescing)
    }
  }
}
