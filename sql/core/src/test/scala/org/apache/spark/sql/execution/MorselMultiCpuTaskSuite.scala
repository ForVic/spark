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

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.scheduler.morsel.MorselScheduler
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}

/**
 * Morsel execution with fewer, larger tasks: every task gets two CPUs and uses the second for
 * a helper of its own.
 */
class MorselMultiCpuTaskSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(config.MORSEL_ENABLED, true)
    .set(config.MORSEL_TARGET_SIZE, 1024L)
    .set(config.CPUS_PER_TASK, 2)
    // Idle slots would also get helpers; only use the tasks' own spare CPUs.
    .set(config.MORSEL_HELPER_DELAY, 1000000L)

  override protected def createSparkSession: TestSparkSession =
    new TestSparkSession(sparkConf, maxLocalTaskFailures = 1, numCores = 4)

  private def scheduler: MorselScheduler = MorselScheduler.get.get

  test("tasks use their spare CPUs for helpers") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(0, 200000, 1, 40)
        .selectExpr("id", "id % 97 AS k", "cast(id * 7 % 1000 AS double) AS v")
        .write.parquet(path)
      // Two scan tasks of two CPUs each fill the executor's four cores.
      withSQLConf(
          SQLConf.FILES_MAX_PARTITION_BYTES.key -> "1g",
          SQLConf.FILES_MIN_PARTITION_NUM.key -> "2",
          SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1",
          SQLConf.SHUFFLE_PARTITIONS.key -> "20",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        val queries = Seq[String => DataFrame](
          path => spark.read.parquet(path).groupBy("k").agg(sum("v"), count("*")),
          path => {
            val df = spark.read.parquet(path)
            df.join(df.select(col("id"), col("v").as("w")).hint("shuffle_hash"), "id")
              .groupBy("k").agg(sum("w"))
          })
        for (query <- queries) {
          val expected = withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "false") {
            query(path).collect().toSeq
          }
          val stolenBefore = scheduler.morselsStolen.get
          withSQLConf(SQLConf.MORSEL_EXECUTION_ENABLED.key -> "true") {
            checkAnswer(query(path), expected)
          }
          assert(scheduler.morselsStolen.get > stolenBefore)
        }
      }
    }
  }
}
