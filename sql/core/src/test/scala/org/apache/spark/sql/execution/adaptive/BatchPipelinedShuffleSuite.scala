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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.mockito.Mockito.{doThrow, spy}

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.physical.{CoalescedBoundary, CoalescedHashPartitioning, HashPartitioning, SinglePartition}
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.{ExpandExec, FileSourceScanExec, LocalTableScanExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, PlanBatchPipelinedShuffle, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType
import org.apache.spark.util.{ThreadUtils, Utils}

/** Exercises the same batch policy through both transports, with deliberately bounded buffers. */
class BatchPipelinedShuffleSuite extends QueryTest with AdaptiveSparkPlanHelper {
  protected var spark: SparkSession = _
  protected def master: String = "local[8]"
  protected def managers: Seq[String] = Seq(
    "org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager",
    "org.apache.spark.shuffle.streaming.StreamingShuffleManager")

  private def withSession(
      manager: String,
      writerMemory: Long = 1048576L,
      readerMemory: Long = 1048576L)(body: SparkSession => Unit): Unit = {
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = SparkSession.builder().master(master).appName("batch-pipelined-shuffle")
      .config("spark.shuffle.manager.incremental", manager)
      .config("spark.shuffle.channel.batchSize", 16)
      .config("spark.shuffle.channel.queueCapacity", 2)
      .config("spark.shuffle.streaming.networkBufferSize", 4096)
      .config("spark.shuffle.streaming.writerMaxMemory", writerMemory)
      .config("spark.shuffle.streaming.readerMaxMemory", readerMemory)
      .config("spark.shuffle.streaming.writerConnectionTimeout", "15s")
      .config("spark.speculation", false)
      .config("spark.dynamicAllocation.enabled", false)
      .config("spark.executor.cores", 4)
      .config("spark.cores.max", 8)
      .config("spark.scheduler.minRegisteredResourcesRatio", 1.0)
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, true)
      .config(SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key, true)
      .config(SQLConf.SHUFFLE_PARTITIONS.key, 2)
      .getOrCreate()
    try body(spark) finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def pipelined(plan: SparkPlan): Seq[ShuffleExchangeExec] =
    collect(plan) { case s: ShuffleExchangeExec if s.pipelined => s }

  for (manager <- managers) {
    val transport = manager.split('.').last

    if (manager.endsWith("StreamingShuffleManager")) {
      test(s"$transport: queued ordinary work cannot take a live group's reserved capacity") {
        withSession(manager) { spark =>
          import spark.implicits._
          spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)
          spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
          spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 200)
          withTempDir { directory =>
            val path = directory.getAbsolutePath
            val jobStarts = new AtomicInteger()
            spark.sparkContext.addSparkListener(new SparkListener {
              override def onJobStart(event: SparkListenerJobStart): Unit = {
                jobStarts.incrementAndGet()
              }
            })
            def awaitCondition(condition: => Boolean): Unit = {
              val deadline = System.nanoTime() + 20.seconds.toNanos
              while (!condition && System.nanoTime() < deadline) Thread.sleep(10)
              assert(condition)
            }
            val ordinary = spark.sparkContext.parallelize(0 until 16, 16)
              .mapPartitionsWithIndex { (partition, _) =>
                require(new java.io.File(path, s"started-$partition").createNewFile())
                val releaseAll = new java.io.File(path, "release-all")
                val releaseOne = new java.io.File(path, "release-one")
                val deadline = System.nanoTime() + 60.seconds.toNanos
                while (!releaseAll.exists() && !(partition == 0 && releaseOne.exists()) &&
                    System.nanoTime() < deadline) {
                  Thread.sleep(10)
                }
                require(releaseAll.exists() || (partition == 0 && releaseOne.exists()))
                Iterator.single(partition)
              }.countAsync()
            val executor = ThreadUtils.newDaemonSingleThreadExecutor("live-reservation-test")
            implicit val executionContext: ExecutionContext =
              ExecutionContext.fromExecutorService(executor)
            try {
              awaitCondition(directory.list().count(_.startsWith("started-")) == 8)
              val result = spark.range(0, 1600, 1, 8).groupBy(($"id" % 8).as("k")).count()
              assert(pipelined(result.queryExecution.executedPlan).nonEmpty)
              val live = Future(result.collect())
              awaitCondition(jobStarts.get() >= 2)
              require(new java.io.File(path, "release-one").createNewFile())
              assert(ThreadUtils.awaitResult(live, 15.seconds).map(_.getLong(1)).sum == 1600)
              assert(!ordinary.isCompleted, "ordinary tasks remain blocked with a queued backlog")
            } finally {
              new java.io.File(path, "release-all").createNewFile()
              ThreadUtils.awaitResult(ordinary, 20.seconds)
              executor.shutdownNow()
            }
          }
        }
      }

      test(s"$transport: a waiting reader lends its CPU to the last producer") {
        withSession(manager) { spark =>
          import spark.implicits._
          spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)
          spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
          spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 200)
          withTempDir { directory =>
            val path = directory.getAbsolutePath
            val waitForProducers = udf { value: Long =>
              val partition = org.apache.spark.TaskContext.getPartitionId()
              val ready = new java.io.File(path, partition.toString)
              require(ready.createNewFile())
              val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20)
              while (new java.io.File(path).list().length < 8 && System.nanoTime() < deadline) {
                Thread.sleep(10)
              }
              require(new java.io.File(path).list().length == 8,
                "all eight producers must start while the reader is waiting")
              value
            }.asNondeterministic()
            val input = spark.range(0, 8, 1, 8).select(waitForProducers($"id").as("value"))
            val result = input.groupBy(($"value" % 2).as("k")).count()
            val exchanges = pipelined(result.queryExecution.executedPlan)
            assert(exchanges.size == 1, result.queryExecution.executedPlan.toString)
            assert(exchanges.head.numPartitions == 1)
            assert(result.collect().map(_.getLong(1)).toSeq.sorted == Seq(4L, 4L))
          }
        }
      }

      test(s"$transport: bounded writer CPU waits let the reader drain its buffers") {
        withSession(manager, writerMemory = 16384L, readerMemory = 65536L) { spark =>
          import spark.implicits._
          spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)
          spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
          spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 200)
          val tasks = mutable.ArrayBuffer.empty[SparkListenerTaskEnd]
          spark.sparkContext.addSparkListener(new SparkListener {
            override def onTaskEnd(e: SparkListenerTaskEnd): Unit = tasks += e
          })
          val result = spark.range(0, 64000, 1, 8).groupBy(($"id" % 32000).as("k"))
            .count()
          assert(pipelined(result.queryExecution.executedPlan).nonEmpty,
            result.queryExecution.executedPlan.toString)
          assert(result.collect().map(_.getLong(1)).sum == 64000L)
          spark.sparkContext.listenerBus.waitUntilEmpty(10000)
          val backpressure = tasks.flatMap(_.taskInfo.accumulables).filter(
            _.name.exists(_.endsWith(".writerBackpressureNs")))
            .flatMap(_.update).map(_.asInstanceOf[Long]).sum
          assert(backpressure > 0, "the bounded writer buffers must exercise the blocking path")
        }
      }

      test(s"$transport: report payload and wait metrics from executor tasks") {
        withSession(manager) { spark =>
          import spark.implicits._
          spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)
          val tasks = mutable.ArrayBuffer.empty[SparkListenerTaskEnd]
          spark.sparkContext.addSparkListener(new SparkListener {
            override def onTaskEnd(e: SparkListenerTaskEnd): Unit = tasks += e
          })
          val ds = spark.range(0, 10000, 1, 2).groupBy(($"id" % 10).as("k")).count()
          assert(ds.collect().forall(_.getLong(1) == 1000L))
          spark.sparkContext.listenerBus.waitUntilEmpty(10000)
          assert(pipelined(ds.queryExecution.executedPlan).nonEmpty)
          val metrics = tasks.map(_.taskMetrics)
          assert(metrics.map(_.shuffleWriteMetrics.recordsWritten).sum == 20L)
          assert(metrics.map(_.shuffleReadMetrics.recordsRead).sum == 20L)
          val written = metrics.map(_.shuffleWriteMetrics.bytesWritten).sum
          assert(written > 0L)
          assert(metrics.map(_.shuffleReadMetrics.totalBytesRead).sum == written)
          val diagnostics = tasks.flatMap(_.taskInfo.accumulables).filter(
            _.name.exists(_.startsWith("streamingShuffle.")))
          def total(name: String): Long = diagnostics.filter(
            _.name.exists(_.endsWith(s".$name"))).flatMap(_.update).map(
            _.asInstanceOf[Long]).sum
          assert(total("writerBytes") == written)
          assert(total("readerBytes") == written)
          assert(total("readerMessages") == total("writerMessages"))
          assert(total("readerWaitNs") > 0L)
          assert(total("writerSetupNs") > 0L)
          assert(total("readerConnectNs") > 0L)
          // Live transport durations must not be reported as disk/buffer-cache write time.
          assert(metrics.map(_.shuffleWriteMetrics.writeTime).sum == 0L)
        }
      }
    }

    Seq(false, true).foreach { adaptive =>
      test(s"$transport: coalescing and producer waves with adaptive planning=$adaptive") {
        withSession(manager) { spark =>
          import spark.implicits._
          spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, adaptive)
          spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
          spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 200)
          val ds = spark.range(0, 20000, 1, 16).groupBy(($"id" % 101).as("k"))
            .agg(sum("id").as("total"))
          val expected = (0L until 20000L).groupBy(_ % 101).map { case (k, ids) =>
            k -> ids.sum
          }
          assert(ds.collect().map(r => r.getLong(0) -> r.getLong(1)).toMap === expected)
          val exchanges = pipelined(ds.queryExecution.executedPlan)
          val coalesced = exchanges.filter(
            _.outputPartitioning.isInstanceOf[CoalescedHashPartitioning])
          assert(coalesced.nonEmpty, s"${ds.queryExecution.executedPlan}")
          assert(coalesced.forall(_.numPartitions == 1))
          assert(ds.collect().map(r => r.getLong(0) -> r.getLong(1)).toMap === expected)
        }
      }
    }

    test(s"$transport: large grouping sets retain observed-output AQE") {
      withSession(manager) { spark =>
        spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)
        spark.conf.set(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key, "1m")
        def exchange(rows: Long): ShuffleExchangeExec = {
          val plan = withSQLConf(SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key -> "false") {
            spark.range(0, rows, 1, 2).rollup("id").count().queryExecution.executedPlan
          }
          val original = collect(plan) {
            case s: ShuffleExchangeExec if s.child.exists(_.isInstanceOf[ExpandExec]) => s
          }.head
          // One reader and two producers fit; the size policy must make the decision.
          val keys = original.outputPartitioning.asInstanceOf[HashPartitioning].expressions
          original.copy(outputPartitioning = HashPartitioning(keys, 1))
        }
        val large = exchange(1000000)
        assert(pipelined(PlanBatchPipelinedShuffle(large)).isEmpty)
        assert(pipelined(PlanBatchPipelinedShuffle(exchange(10))).size == 1)
        val largerTarget = large.copy(advisoryPartitionSize = Some(128L * 1024 * 1024))
        assert(pipelined(PlanBatchPipelinedShuffle(largerTarget)).size == 1)
        val unexpanded = spark.range(0, 1000000, 1, 2).queryExecution.executedPlan
        val regularGroup = ShuffleExchangeExec(
          HashPartitioning(Seq(unexpanded.output.head), 1), unexpanded)
        assert(pipelined(PlanBatchPipelinedShuffle(regularGroup)).size == 1)
      }
    }

    test(s"$transport: coalescing revisits an already selected live edge") {
      withSession(manager) { spark =>
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
        val child = spark.range(0, 1000, 1, 2).queryExecution.executedPlan
        val selected = ShuffleExchangeExec(HashPartitioning(Seq(child.output.head), 2),
          child, pipelined = true)
        val planned = PlanBatchPipelinedShuffle(selected).asInstanceOf[ShuffleExchangeExec]
        assert(planned.numPartitions == 1)
        assert(planned.executeCollect().map(_.getLong(0)).sorted.toSeq === (0L until 1000L))
      }
    }

    test(s"$transport: coalesced live join inputs share the physical layout") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
        spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 200)
        spark.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, -1)
        spark.conf.set(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key, -1)
        val left = spark.range(0, 1000, 1, 2).select($"id".as("k"), ($"id" + 1).as("v"))
        val right = spark.range(0, 100, 1, 2).select($"id".as("k"))
        val ds = left.join(right.hint("shuffle_hash"), Seq("k"))
        assert(ds.collect().map(r => r.getLong(0) -> r.getLong(1)).toMap ===
          (0L until 100L).map(i => i -> (i + 1)).toMap)
        val layouts = pipelined(ds.queryExecution.executedPlan).map(_.outputPartitioning)
          .collect { case p: CoalescedHashPartitioning => p.partitions }
        assert(layouts.size == 2, s"${ds.queryExecution.executedPlan}")
        assert(layouts.distinct == Seq(Seq(CoalescedBoundary(0, 200))))
      }
    }

    test(s"$transport: an oversized proposal restores the original reader layout") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
        spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 200)
        // The hard fan-in limit needs eight readers, leaving no slot for even one producer.
        spark.conf.set(SQLConf.COALESCE_PARTITIONS_MAX_REDUCER_PARTITIONS_PER_TASK.key, 25)
        spark.conf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key, 1)
        val ds = spark.range(0, 20000, 1, 2).groupBy(($"id" % 101).as("k"))
          .agg(sum("id").as("total"))
        val plan = ds.queryExecution.executedPlan
        val exchanges = collect(plan) { case s: ShuffleExchangeExec => s }
        assert(exchanges.nonEmpty)
        assert(exchanges.forall(s => !s.pipelined &&
          s.outputPartitioning.isInstanceOf[HashPartitioning] && s.numPartitions == 200))
        assert(PlanBatchPipelinedShuffle(plan).fastEquals(plan))
      }
    }

    test(s"$transport: reader parallelism leaves capacity for live producers") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
        spark.conf.set(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key, true)
        spark.conf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key, "1")
        spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 200)
        val result = spark.range(0, 20000, 1, 8).groupBy(($"id" % 997).as("k")).count()
        val expected = (0L until 20000L).groupBy(_ % 997).view.mapValues(_.size.toLong).toMap
        assert(result.collect().map(r => r.getLong(0) -> r.getLong(1)).toMap === expected)
        val plan = result.queryExecution.executedPlan
        val live = pipelined(plan)
        assert(live.size == 1 && live.head.numPartitions > 1 && live.head.numPartitions < 8,
          s"$plan")
        assert(PlanBatchPipelinedShuffle(plan).fastEquals(plan))
      }
    }

    test(s"$transport: aligned readers leave capacity for both live join inputs") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
        spark.conf.set(SQLConf.COALESCE_PARTITIONS_PARALLELISM_FIRST.key, true)
        spark.conf.set(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE.key, "1")
        spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 200)
        // Keep useful payload in each stream while exercising two aligned reader ranges.
        val left = spark.range(0, 20000, 1, 8).select($"id".as("k"))
        val right = spark.range(20, 20020, 1, 8).select($"id".as("k"))
        val result = left.hint("merge").join(right, "k")
        val expected = 20L until 20000L
        assert(result.collect().map(_.getLong(0)).toSeq.sorted === expected)
        val plan = result.queryExecution.executedPlan
        val live = pipelined(plan)
        assert(live.size == 2 && live.map(_.numPartitions).distinct.size == 1, s"$plan")
        assert(live.head.numPartitions > 1 && live.head.numPartitions <= 6, s"$plan")
      }
    }

    test(s"$transport: a durable boundary preserves a smaller live input group") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
        spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 200)
        val ds = spark.range(0, 20000, 1, 2).groupBy(($"id" % 101).as("k"))
          .agg(sum("id").as("total")).repartition(200, $"total")
        val expected = (0L until 20000L).groupBy(_ % 101).map { case (k, ids) => k -> ids.sum }
        assert(ds.collect().map(r => r.getLong(0) -> r.getLong(1)).toMap === expected)
        val plan = ds.queryExecution.executedPlan
        val exchanges = collect(plan) { case s: ShuffleExchangeExec => s }
        assert(exchanges.exists(s => !s.pipelined && s.numPartitions == 200))
        val live = exchanges.filter(_.pipelined)
        assert(live.size == 1 && live.head.numPartitions == 1, s"$plan")
        assert(PlanBatchPipelinedShuffle(plan).fastEquals(plan))
      }
    }

    test(s"$transport: materialized inputs below aligned live outputs retain AQE coalescing") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
        spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 200)
        val input = spark.range(0, 100, 1, 8).select($"id", ($"id" % 11).as("k"))
        val joined = input.hint("merge").join(input, Seq("id", "k"))
        val left = joined.groupBy($"k").agg(sum($"id").as("total"))
        val right = joined.groupBy($"k").agg(sum($"id" + 1).as("total"))
        val query = left.union(right)
        val expected = (0L until 100L).groupBy(_ % 11).toSeq.flatMap { case (k, ids) =>
          Seq((k, ids.sum), (k, ids.sum + ids.size))
        }
        val actual = query.collect().map(r => (r.getLong(0), r.getLong(1))).toSeq.sorted
        assert(actual === expected.sorted)
        val plan = query.queryExecution.executedPlan
        assert(pipelined(plan).size == 2, s"$plan")
        val joins = collect(plan) { case j: SortMergeJoinExec => j }
        assert(joins.nonEmpty, s"$plan")
        joins.foreach { join =>
          val reads = collect(join) { case r: AQEShuffleReadExec => r }
          assert(reads.size == 2 && reads.forall(_.partitionSpecs.size < 200), s"$join")
        }
      }
    }

    test(s"$transport: early union result drains prepared but unvisited inputs") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
        spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 200)
        def branch(offset: Long): DataFrame = spark.range(offset, offset + 20000L, 1, 2)
          .groupBy(($"id" % 10001L).as("k")).count()
        val query = branch(0).union(branch(20000))
        val plan = query.queryExecution.executedPlan
        assert(pipelined(plan).size == 2, s"$plan")
        assert(plan.executeTake(1).length == 1)
        // A fresh action must not reuse the prepared iterator from the earlier task.
        assert(plan.executeCollect().map(_.getLong(1)).sum == 40000L)
      }
    }

    test(s"$transport: coalesced live readers inside broadcast preparation") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
        val build = spark.range(0, 20000, 1, 2).groupBy(($"id" % 101).as("k"))
          .agg(sum("id").as("total"))
          .groupBy(($"k" % 7).as("g")).agg(sum("total").as("total"))
        val ds = spark.range(7).select($"id".as("g")).join(broadcast(build), Seq("g"))
        val expected = (0L until 20000L).groupBy(id => (id % 101) % 7)
          .map { case (k, ids) => k -> ids.sum }
        assert(ds.collect().map(r => r.getLong(0) -> r.getLong(1)).toMap === expected)
        val coalesced = pipelined(ds.queryExecution.executedPlan).filter(
          _.outputPartitioning.isInstanceOf[CoalescedHashPartitioning])
        assert(coalesced.nonEmpty, s"${ds.queryExecution.executedPlan}")
        assert(coalesced.forall(_.numPartitions == 1))
      }
    }

    test(s"$transport: coalesced routing keeps original buckets and supports partial actions") {
      withSession(manager) { spark =>
        import spark.implicits._
        val input = spark.range(0, 20000, 1, 2)
        val child = input.queryExecution.executedPlan
        val boundaries = Seq(CoalescedBoundary(0, 3), CoalescedBoundary(3, 8))
        val expected = input.select($"id", (pmod(hash($"id"), lit(8)) >= 3).as("right"))
          .collect().map(r => r.getLong(0) -> (if (r.getBoolean(1)) 1 else 0)).toMap
        Seq(false, true).foreach { live =>
          val exchange = ShuffleExchangeExec(
            CoalescedHashPartitioning(HashPartitioning(Seq(child.output.head), 8), boundaries),
            child, pipelined = live)
          def routed: RDD[(Long, Int)] =
            exchange.execute().mapPartitionsWithIndex { (index, rows) =>
              rows.map(row => row.getLong(0) -> index)
            }
          assert(routed.collect().toMap === expected)
          assert(routed.take(3).forall { case (id, partition) => expected(id) == partition })
          assert(routed.collect().toMap === expected)
          assert(exchange.shuffleDependency.partitioner.numPartitions == 2)
        }
      }
    }

    test(s"$transport: estimated coalescing preserves an explicit repartition count") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
        val ds = spark.range(0, 1000, 1, 2).repartition(2, $"id")
        assert(ds.collect().length == 1000)
        val exchanges = pipelined(ds.queryExecution.executedPlan)
        assert(exchanges.nonEmpty)
        assert(exchanges.forall(_.outputPartitioning.isInstanceOf[HashPartitioning]))
        assert(exchanges.forall(_.numPartitions == 2))
      }
    }

    Seq(false, true).foreach { adaptive =>
      test(s"$transport: mixed finite execution with adaptive planning=$adaptive") {
        withSession(manager) { spark =>
          import spark.implicits._
          spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, adaptive)
          val live = spark.range(0, 20000, 1, 2).repartition(2, $"id").as[Long]
          val durable = spark.range(0, 10000, 1, 2).agg(sum($"id").as("id")).as[Long]
          val query = live.union(durable)
          val rows = query.collect().sorted
          assert(rows.toSeq === ((0L until 20000L) :+ 49995000L))
          assert(pipelined(query.queryExecution.executedPlan).nonEmpty,
            s"${query.queryExecution.executedPlan}")
          if (adaptive) {
            assert(collect(query.queryExecution.executedPlan) {
              case s: ShuffleExchangeExec if !s.pipelined &&
                  !s.materializeForAdaptivePlanning => s
            }.nonEmpty)
          }
          // Re-execution gets fresh live streams while durable outputs can be reused.
          assert(query.collect().sorted.toSeq === rows.toSeq)
        }
      }
    }

    test(s"$transport: pipeline into an AQE materialized output and reuse that output") {
      withSession(manager) { spark =>
        import spark.implicits._
        // Retain this boundary between actions, as in normal classic batch execution. Explicit
        // shuffle-file cleanup trades this reuse away and may require a whole-query retry.
        spark.conf.set(SQLConf.CLASSIC_SHUFFLE_DEPENDENCY_FILE_CLEANUP_ENABLED.key, false)
        val active = mutable.HashMap.empty[Int, Int]
        @volatile var overlapped = false
        spark.sparkContext.addSparkListener(new SparkListener {
          override def onTaskStart(e: SparkListenerTaskStart): Unit = {
            active.update(e.stageId, active.getOrElse(e.stageId, 0) + 1)
            if (active.size >= 2) overlapped = true
          }
          override def onTaskEnd(e: SparkListenerTaskEnd): Unit = {
            val remaining = active(e.stageId) - 1
            if (remaining == 0) active.remove(e.stageId) else active.update(e.stageId, remaining)
          }
        })
        val ds = spark.range(0, 20000, 1, 2).repartition(2, $"id")
          .groupBy(($"id" % 13).as("k")).agg(sum("id").as("total")).coalesce(1)
        val expected = (0L until 20000L).groupBy(_ % 13).map { case (k, vs) => k -> vs.sum }
        def result: Map[Long, Long] = ds.collect().map(r => r.getLong(0) -> r.getLong(1)).toMap
        assert(result === expected)
        val plan = ds.queryExecution.executedPlan
        val stages = collect(plan) { case q: ShuffleQueryStageExec => q }
        assert(stages.exists(q => pipelined(q.plan).nonEmpty), s"$plan")
        spark.sparkContext.listenerBus.waitUntilEmpty()
        assert(overlapped, "producer and materializing consumer must actually run together")
        assert(result === expected)
      }
    }

    test(s"$transport: file scan width and mixed materialized/pipelined groups") {
      withSession(manager) { spark =>
        import spark.implicits._
        val dir = Utils.createTempDir()
        try {
          val path = new java.io.File(dir, "data").getCanonicalPath
          spark.range(0, 10000, 1, 2).write.parquet(path)
          val ds = spark.read.parquet(path).repartition(2, $"id")
            .groupBy(($"id" % 11).as("k")).count()
          assert(ds.collect().map(_.getLong(1)).sum === 10000L)
          val plan = ds.queryExecution.executedPlan
          assert(pipelined(plan).nonEmpty, s"$plan")
          val scans = collect(plan) { case s: FileSourceScanExec => s }
          assert(scans.nonEmpty)
          assert(scans.forall(_.estimatedPartitionCount.exists(_ > 0)))
        } finally {
          Utils.deleteRecursively(dir)
        }
      }
    }

    test(s"$transport: Top-N without an existing shuffle keeps its driver merge") {
      withSession(manager) { spark =>
        import spark.implicits._
        val ds = spark.range(0, 1000, 1, 2).orderBy($"id".desc).limit(3)
        assert(ds.collect().toSeq === Seq(999L, 998L, 997L))
        assert(pipelined(ds.queryExecution.executedPlan).isEmpty)
        assert(collect(ds.queryExecution.executedPlan) {
          case t: TakeOrderedAndProjectExec if t.pipelined => t
        }.isEmpty)
      }
    }

    test(s"$transport: Top-N pipelines its executor merge and preserves its driver merge") {
      withSession(manager) { spark =>
        import spark.implicits._
        val tasks = new AtomicInteger
        spark.sparkContext.addSparkListener(new SparkListener {
          override def onTaskEnd(e: SparkListenerTaskEnd): Unit = tasks.incrementAndGet()
        })
        val ds = spark.range(0, 20000, 1, 2).repartition(2, $"id")
          .orderBy($"id".desc).offset(5).limit(7).select(($"id" + 1).as("value"))
        assert(ds.collect().map(_.getLong(0)).toSeq === (19989L to 19995L).reverse)
        spark.sparkContext.listenerBus.waitUntilEmpty()
        assert(tasks.get() == 4, "collect needs two producers and two driver-merge inputs")
        val plan = ds.queryExecution.executedPlan
        assert(pipelined(plan).nonEmpty, s"$plan")
        assert(collect(plan) {
          case t: TakeOrderedAndProjectExec if t.pipelined => t
        }.nonEmpty, s"$plan")
        // Exercise doExecute, as used by benchmark sinks, as well as executeCollect.
        ds.write.format("noop").mode("overwrite").save()
      }
    }

    test(s"$transport: empty hash-join build drains the streaming input") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", -1)
        val left = spark.range(0, 20000, 1, 2).select($"id", ($"id" % 2).as("k"))
        val reject = udf((_: Long) => false)
        val right = spark.range(0, 20, 1, 2).filter(reject($"id")).select($"id".as("k"))
        val ds = left.join(right.hint("shuffle_hash"), Seq("k"))
        assert(ds.collect().isEmpty)
        assert(pipelined(ds.queryExecution.executedPlan).nonEmpty,
          s"${ds.queryExecution.executedPlan}")
      }
    }

    test(s"$transport: overly wide groups retain regular AQE and early limits finish") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 200)
        val ds = spark.range(0, 1000, 1, 2).groupBy(($"id" % 7).as("k")).count()
        assert(ds.collect().map(_.getLong(1)).sum === 1000L)
        assert(pipelined(ds.queryExecution.executedPlan).isEmpty)
        val top = spark.range(0, 1000, 1, 9).orderBy($"id".desc).limit(3)
        assert(top.collect().toSeq === Seq(999L, 998L, 997L))
        assert(pipelined(top.queryExecution.executedPlan).isEmpty)
        assert(collect(top.queryExecution.executedPlan) {
          case t: TakeOrderedAndProjectExec if t.pipelined => t
        }.isEmpty)
        spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, 2)
        val limited = spark.range(0, 20000, 1, 2).repartition(2, $"id").limit(1)
        assert(limited.collect().length === 1)
        assert(pipelined(limited.queryExecution.executedPlan).isEmpty)
      }
    }

    test(s"$transport: partial result jobs read fresh live reducer subsets") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set("spark.sql.limit.initialNumPartitions", 1)
        val grouped = spark.range(0, 2000, 1, 2).groupBy(($"id" % 7).as("k")).count()
        val plan = grouped.queryExecution.executedPlan
        // executeTake first launches one reader, then another job for the remaining partition.
        // Each producer must wait only for this job's readers, even when a reducer is empty.
        val rows = plan.executeTake(10).map(r => r.getLong(0) -> r.getLong(1)).toMap
        val expected = (0L until 2000L).groupBy(_ % 7).map { case (k, vs) => k -> vs.size.toLong }
        assert(rows === expected)
        assert(pipelined(plan).nonEmpty)
        assert(plan.executeTail(10).map(r => r.getLong(0) -> r.getLong(1)).toMap === expected)

        // A union offsets task indexes on its second branch. RPC must identify readers by
        // their requested reduce partition, not by the enclosing result task's partition ID.
        val right = spark.range(2000, 5000, 1, 2).groupBy(($"id" % 11).as("k")).count()
        val union = grouped.union(right).queryExecution.executedPlan
        val rightExpected = (2000L until 5000L).groupBy(_ % 11)
          .map { case (k, vs) => k -> vs.size.toLong }
        val unionRows = union.executeTail(30).map(r => r.getLong(0) -> r.getLong(1)).sorted
        assert(unionRows.toSeq === (expected.toSeq ++ rightExpected.toSeq).sorted)
        assert(pipelined(union).size === 2)

        val scalar = spark.sql("""
          SELECT (SELECT k FROM (
            SELECT id % 1 AS k FROM range(0, 100, 1, 2) GROUP BY id % 1)) AS k
          """)
        assert(scalar.collect().head.getLong(0) === 0L)
      }
    }

    test(s"$transport: rejected global aggregates do not plan input partitions") {
      withSession(manager) { _ =>
        val attr = AttributeReference("k", LongType)()
        val input = spy(LocalTableScanExec(Seq(attr), Nil, None))
        doThrow(new IllegalStateException("Input partition planning was not needed"))
          .when(input).outputPartitioning
        val partial = HashAggregateExec(None, false, None, Nil, Nil, Nil, 0, Nil, input)
        val shuffle = ShuffleExchangeExec(SinglePartition, partial)
        assert(PlanBatchPipelinedShuffle(shuffle) eq shuffle)
      }
    }

    test(s"$transport: preserve AQE join selection and tiny global aggregates") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", 10485760)
        val keep = udf((id: Long) => id < 10)
        val left = spark.range(0, 10000, 1, 2).filter(keep($"id"))
        val right = spark.range(0, 1000, 1, 2)
        val joined = left.join(right, "id")
        assert(joined.collect().length == 10)
        assert(collect(joined.queryExecution.executedPlan) {
          case j: org.apache.spark.sql.execution.joins.BroadcastHashJoinExec => j
        }.nonEmpty, "pipelining must not prevent an adaptive broadcast join")
        val total = spark.range(0, 10000, 1, 2).agg(sum("id"))
        assert(total.collect().head.getLong(0) == 49995000L)
        assert(pipelined(total.queryExecution.executedPlan).isEmpty,
          "one row emitted at producer completion offers no useful execution overlap")
      }
    }

    test(s"$transport: preserve nested adaptive statistics before a join decision") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, -1)
        spark.conf.set(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key, 2048)
        spark.conf.set(SQLConf.CBO_ENABLED.key, true)
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
        val keep = udf((id: Long) => id < 40)
        def execute(enabled: Boolean): SparkPlan = {
          spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key, enabled)
          val grouped = spark.range(0, 2000000, 1, 4).filter(keep($"id"))
            .select(($"id" % 100000).as("g")).groupBy("g").count()
            .select(($"g" % 2).as("k"), $"g".as("j"), $"count")
          val dimension = spark.range(0, 10000, 1, 2).select($"id".as("k"))
          val result = grouped.join(dimension, "k")
          assert(result.collect().toSet == (0L until 40).map(i => Row(i % 2, i, 1L)).toSet)
          result.queryExecution.executedPlan
        }
        val ordinary = execute(false)
        val streaming = execute(true)
        Seq(ordinary, streaming).foreach { plan =>
          assert(collect(plan) {
            case j: org.apache.spark.sql.execution.joins.BroadcastHashJoinExec => j
          }.nonEmpty, plan.toString)
        }
        val ordinaryShuffles = collect(ordinary) { case s: ShuffleExchangeExec => s }
        val streamingShuffles = collect(streaming) { case s: ShuffleExchangeExec => s }
        assert(ordinaryShuffles.size == 2, ordinary.toString)
        assert(streamingShuffles.size <= ordinaryShuffles.size,
          s"Live execution added a shuffle before the adaptive broadcast:\n$streaming")
      }
    }

    test(s"$transport: adaptive statistics do not block a non-build input's live aggregate") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, -1)
        spark.conf.set(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key, 2048)
        spark.conf.set(SQLConf.CBO_ENABLED.key, true)
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, true)
        val keep = udf((id: Long) => id < 40)
        for (joinType <- Seq("full_outer", "left_semi")) {
          def query: DataFrame = {
            val grouped = spark.range(0, 2000000, 1, 4).filter(keep($"id"))
              .select(($"id" % 100000).as("g")).groupBy("g").count()
              .select(($"g" % 2).as("k"), $"g".as("j"), $"count")
            val dimension = spark.range(0, 10000, 1, 2).select($"id".as("k"))
            grouped.join(dimension, Seq("k"), joinType)
          }
          spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key, false)
          val expected = query.collect().toSeq
          spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key, true)
          val result = query
          checkAnswer(result, expected)
          assert(pipelined(result.queryExecution.executedPlan).nonEmpty,
            s"$joinType cannot broadcast its left input:\n${result.queryExecution.executedPlan}")
        }
      }
    }

    test(s"$transport: a pending broadcast does not delay independent shuffle producers") {
      withSession(manager) { spark =>
        import spark.implicits._
        val input = spark.range(0, 10000, 1, 2).select(($"id" % 100).as("k"), $"id")
        val grouped = input.groupBy("k").agg(sum("id").as("total"))
        val build = spark.range(0, 100, 1, 2).select($"id".as("k")).hint("broadcast")
        val joined = grouped.join(build, "k")
        val expected = grouped.collect().toSeq
        checkAnswer(joined, expected)
        assert(pipelined(joined.queryExecution.executedPlan).isEmpty,
          "the probe shuffle must be schedulable before the broadcast build completes")

        // A broadcast inside the producer does not add a dependency to its consumer. Its
        // downstream aggregation should still pipeline after the build becomes available.
        val aggregate = input.join(build, "k").groupBy("k").agg(sum("id").as("total"))
        checkAnswer(aggregate, expected)
        assert(pipelined(aggregate.queryExecution.executedPlan).nonEmpty)
      }
    }

    test(s"$transport: union branches retain independent prerequisite readiness") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, false)
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key, false)
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_COALESCE_ENABLED.key, false)
        spark.conf.set(SQLConf.EXCHANGE_REUSE_ENABLED.key, false)
        def branch(offset: Long): DataFrame = {
          val input = spark.range(offset, offset + 100, 1, 2).select(($"id" % 10).as("k"))
          val build = spark.range(10).select($"id".as("k"))
          input.join(broadcast(build), "k").groupBy("k").count()
        }
        val plan = branch(0).union(branch(100)).queryExecution.executedPlan
        assert(collect(plan) { case s: ShuffleExchangeExec => s }.size == 2)
        spark.conf.set(SQLConf.BATCH_PIPELINED_SHUFFLE_ENABLED.key, true)
        assert(pipelined(PlanBatchPipelinedShuffle(plan)).isEmpty)
        val ready = plan.transformDown {
          case b: BroadcastExchangeExec =>
            new BroadcastQueryStageExec(b.id, b, b.canonicalized) {
              _resultOption.set(Some(null))
            }
        }
        assert(pipelined(PlanBatchPipelinedShuffle(ready)).size == 2)
        val shuffle = collect(ready) { case s: ShuffleExchangeExec => s }.head
        val pending = new ShuffleQueryStageExec(shuffle.id, shuffle, shuffle.canonicalized)
        val waiting = ready.transformDown {
          case s: ShuffleExchangeExec if s.id == shuffle.id => pending
        }
        assert(pipelined(PlanBatchPipelinedShuffle(waiting)).isEmpty)
        pending.resultOption.set(Some(null))
        assert(pipelined(PlanBatchPipelinedShuffle(waiting)).size == 1)
      }
    }

    test(s"$transport: independent AQE groups wait for slots and then join") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", -1)
        val left = spark.range(0, 20000, 1, 3).repartition(2, $"id")
          .groupBy(($"id" % 7).as("k")).agg(sum("id").as("a")).coalesce(1)
        val right = spark.range(0, 10000, 1, 3).repartition(2, $"id")
          .groupBy(($"id" % 5).as("k")).agg(sum("id").as("b")).coalesce(1)
        val ds = left.join(right, "k")
        val rows = ds.collect().map(r => r.getLong(0) -> (r.getLong(1), r.getLong(2))).toMap
        val l = (0L until 20000L).groupBy(_ % 7).map { case (k, vs) => k -> vs.sum }
        val r = (0L until 10000L).groupBy(_ % 5).map { case (k, vs) => k -> vs.sum }
        assert(rows === r.map { case (k, v) => k -> (l(k), v) })
        assert(pipelined(ds.queryExecution.executedPlan).size >= 2)
      }
    }

    test(s"$transport: asymmetric joins preserve partition compatibility under AQE") {
      withSession(manager) { spark =>
        import spark.implicits._
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", -1)
        val left = spark.range(0, 10000, 1, 7).groupBy(($"id" % 101).as("g")).count()
          .select(($"g" % 7).as("k"), $"count".as("n"))
        val right = spark.range(0, 1000, 1, 7).select($"id".as("k"))
        val ds = left.join(right, "k")
        val rows = ds.collect()
        assert(rows.length === 101)
        assert(rows.map(_.getLong(1)).sum === 10000L)
        assert(pipelined(ds.queryExecution.executedPlan).nonEmpty,
          s"${ds.queryExecution.executedPlan}")
      }
    }

    test(s"$transport: repeated pipelined actions use fresh transport state") {
      withSession(manager) { spark =>
        import spark.implicits._
        val ds = spark.range(0, 20000, 1, 2).groupBy(($"id" % 7).as("k")).count()
        val first = ds.collect().toSet
        assert(pipelined(ds.queryExecution.executedPlan).nonEmpty,
          s"${ds.queryExecution.executedPlan}")
        assert(ds.collect().toSet === first)
        assert(ds.collect().toSet === first)
      }
    }
  }
}

/** The RPC implementation must also work with producers and consumers in separate JVMs. */
class BatchPipelinedShuffleClusterSuite extends BatchPipelinedShuffleSuite {
  override protected def master: String = "local-cluster[2,4,1024]"
  override protected def managers: Seq[String] =
    Seq("org.apache.spark.shuffle.streaming.StreamingShuffleManager")
}
