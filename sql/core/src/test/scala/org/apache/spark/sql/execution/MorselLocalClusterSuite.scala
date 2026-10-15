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

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Morsel execution on executors in their own JVMs, which fetch each other's shuffle output.
 */
class MorselLocalClusterSuite extends SparkFunSuite {

  private var spark: SparkSession = _
  private var dataDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local-cluster[2,2,1024]")
      .appName("morsel-local-cluster")
      .config(config.MORSEL_ENABLED.key, "true")
      .config(config.MORSEL_HELPER_DELAY.key, "0")
      .config(config.MORSEL_TARGET_SIZE.key, "1024")
      .config(config.TASK_MAX_FAILURES.key, "4")
      .config(SQLConf.SHUFFLE_PARTITIONS.key, "40")
      .config(SQLConf.FILES_MAX_PARTITION_BYTES.key, "256k")
      .config(SQLConf.FILES_OPEN_COST_IN_BYTES.key, "1")
      .getOrCreate()
    dataDir = Utils.createTempDir()
    spark.range(0, 400000, 1, 80)
      .selectExpr("id", "id % 97 AS k", "cast(id * 7 % 1000 AS double) AS v",
        "concat('s', cast(id % 13 AS string)) AS s")
      .write.mode("overwrite").parquet(dataDir.getCanonicalPath)
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      Utils.deleteRecursively(dataDir)
    } finally {
      super.afterAll()
    }
  }

  private def data: DataFrame = spark.read.parquet(dataDir.getCanonicalPath)

  private def withMorsels[T](enabled: Boolean)(f: => T): T = {
    val key = SQLConf.MORSEL_EXECUTION_ENABLED.key
    spark.conf.set(key, enabled)
    try f finally spark.conf.unset(key)
  }

  private def checkSameResults(query: => DataFrame): Unit = {
    val expected = withMorsels(enabled = false)(query.collect().map(_.toString).sorted.toSeq)
    val actual = withMorsels(enabled = true)(query.collect().map(_.toString).sorted.toSeq)
    assert(actual == expected)
  }

  test("aggregations, windows and joins of shuffles") {
    checkSameResults(data.groupBy("k").agg(count("*"), sum("v"), countDistinct("s")))
    checkSameResults(data.groupBy("k").agg(sum("v").as("sv")).groupBy(col("sv") % 3).count())
    checkSameResults(data.selectExpr("k", "v", "rank() OVER (PARTITION BY k ORDER BY v, id) AS r")
      .filter("r <= 3").groupBy("r").agg(sum("v")))
    for (hint <- Seq("merge", "shuffle_hash")) {
      checkSameResults(data.filter("id % 3 != 0")
        .join(data.select(col("id"), col("v").as("w")).hint(hint), Seq("id"), "left_outer")
        .groupBy("k").agg(count("*"), sum("w")))
    }
  }

  test("file writes") {
    val out = Utils.createTempDir()
    try {
      withMorsels(enabled = true) {
        data.filter("v > 10").write.mode("overwrite").partitionBy("k")
          .parquet(out.getCanonicalPath)
      }
      val written = spark.read.parquet(out.getCanonicalPath)
      assert(written.count() == data.filter("v > 10").count())
      assert(written.except(data.filter("v > 10").select(written.columns.toSeq.map(col): _*))
        .isEmpty)
    } finally {
      Utils.deleteRecursively(out)
    }
  }

  test("an executor that dies while helpers run its tasks") {
    // The first executor to run the reduce stage dies once, losing its map output: the reduce
    // tasks fail to fetch it, and its map tasks run again, as morsels.
    val marker = new File(Utils.createTempDir(), "died")
    val dieOnce = udf((k: Long) => {
      if (k == 13 && marker.createNewFile()) {
        Runtime.getRuntime.halt(1)
      }
      k
    })
    val query = () => data.groupBy("k").agg(sum("v").as("sv"), count("*").as("c"))
      .select(dieOnce(col("k")).as("k"), col("sv"), col("c"))
      .groupBy(col("k") % 5).agg(sum("sv"), sum("c"))
    val expected = withMorsels(enabled = false) {
      marker.createNewFile()
      try query().collect().map(_.toString).sorted.toSeq finally marker.delete()
    }
    val actual = withMorsels(enabled = true)(query().collect().map(_.toString).sorted.toSeq)
    assert(marker.exists())
    assert(actual == expected)
  }
}
