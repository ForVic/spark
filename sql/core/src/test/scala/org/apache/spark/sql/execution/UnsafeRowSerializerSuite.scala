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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException}
import java.util.{HashMap, Properties}

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Tests.TEST_MEMORY
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents
import org.apache.spark.sql.{LocalSparkSession, Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.metric.SQLShuffleReadMetricsReporter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.collection.ExternalSorter

/**
 * used to test close InputStream in UnsafeRowSerializer
 */
class ClosableByteArrayInputStream(buf: Array[Byte]) extends ByteArrayInputStream(buf) {
  var closed: Boolean = false
  override def close(): Unit = {
    closed = true
    super.close()
  }
}

class UnsafeRowSerializerSuite extends SparkFunSuite with LocalSparkSession {

  private def toUnsafeRow(row: Row, schema: Array[DataType]): UnsafeRow = {
    val converter = unsafeRowConverter(schema)
    converter(row)
  }

  private def unsafeRowConverter(schema: Array[DataType]): Row => UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    (row: Row) => {
      converter(CatalystTypeConverters.convertToCatalyst(row).asInstanceOf[InternalRow])
    }
  }

  test("toUnsafeRow() test helper method") {
    // This currently doesn't work because the generic getter throws an exception.
    val row = Row("Hello", 123)
    val unsafeRow = toUnsafeRow(row, Array(StringType, IntegerType))
    assert(row.getString(0) === unsafeRow.getUTF8String(0).toString)
    assert(row.getInt(1) === unsafeRow.getInt(1))
  }

  test("basic row serialization") {
    val rows = Seq(Row("Hello", 1), Row("World", 2))
    val unsafeRows = rows.map(row => toUnsafeRow(row, Array(StringType, IntegerType)))
    val serializer = new UnsafeRowSerializer(numFields = 2).newInstance()
    val baos = new ByteArrayOutputStream()
    val serializerStream = serializer.serializeStream(baos)
    for (unsafeRow <- unsafeRows) {
      serializerStream.writeKey(0)
      serializerStream.writeValue(unsafeRow)
    }
    serializerStream.close()
    val input = new ClosableByteArrayInputStream(baos.toByteArray)
    val deserializerIter = serializer.deserializeStream(input).asKeyValueIterator
    for (expectedRow <- unsafeRows) {
      val actualRow = deserializerIter.next().asInstanceOf[(Integer, UnsafeRow)]._2
      assert(expectedRow.getSizeInBytes === actualRow.getSizeInBytes)
      assert(expectedRow.getString(0) === actualRow.getString(0))
      assert(expectedRow.getInt(1) === actualRow.getInt(1))
    }
    assert(!deserializerIter.hasNext)
    assert(input.closed)
  }

  test("already buffered destinations preserve row framing without a second buffer") {
    val rows = Seq(Row("small", 1), Row("x" * 12000, 2))
      .map(row => toUnsafeRow(row, Array(StringType, IntegerType)))
    val serializer = new UnsafeRowSerializer(numFields = 2).newInstance()
    def write(useBuffering: Boolean): Array[Byte] = {
      val output = new ByteArrayOutputStream()
      val stream = serializer.serializeStream(output, useBuffering)
      rows.foreach { row =>
        val before = output.size()
        stream.writeValue(row)
        if (!useBuffering) {
          assert(output.size() - before === 4 + row.getSizeInBytes)
        }
      }
      stream.close()
      output.toByteArray
    }
    val bytes = write(useBuffering = false)
    assert(bytes.toSeq === write(useBuffering = true).toSeq)
    val input = new ClosableByteArrayInputStream(bytes)
    val decoded = serializer.deserializeStream(input).asKeyValueIterator
      .map(_._2.asInstanceOf[UnsafeRow].copy()).toArray
    assert(decoded.toSeq === rows)
    assert(input.closed)
  }

  test("a stream can switch from heap rows to off-heap rows without changing framing") {
    val row = toUnsafeRow(Row("x" * 12000, 7), Array(StringType, IntegerType))
    val address = Platform.allocateMemory(row.getSizeInBytes)
    try {
      Platform.copyMemory(row.getBaseObject, row.getBaseOffset,
        null, address, row.getSizeInBytes)
      val offHeap = new UnsafeRow(2)
      offHeap.pointTo(null, address, row.getSizeInBytes)
      for (buffered <- Seq(false, true)) {
        val serializer = new UnsafeRowSerializer(2).newInstance()
        val output = new ByteArrayOutputStream()
        val stream = serializer.serializeStream(output, buffered)
        Seq(row, offHeap, row).foreach(stream.writeValue(_))
        stream.close()
        val decoded = serializer.deserializeStream(new ByteArrayInputStream(output.toByteArray))
          .asKeyValueIterator.map(_._2.asInstanceOf[UnsafeRow].copy()).toSeq
        assert(decoded == Seq.fill(3)(row))
      }
    } finally {
      Platform.freeMemory(address)
    }
  }

  test("close empty input stream") {
    val input = new ClosableByteArrayInputStream(Array.empty)
    val serializer = new UnsafeRowSerializer(numFields = 2).newInstance()
    val deserializerIter = serializer.deserializeStream(input).asKeyValueIterator
    assert(!deserializerIter.hasNext)
    assert(input.closed)
  }

  gridTest("row decoding preserves framing across short reads")(Seq(1, 7, 8191, 8192)) {
    chunkSize =>
      val rows = Seq(0, 1, 1013, 8160, 8176, 16384).zipWithIndex.map { case (size, index) =>
        toUnsafeRow(Row("x" * size, index), Array(StringType, IntegerType))
      }
      val serializer = new UnsafeRowSerializer(2).newInstance()
      val output = new ByteArrayOutputStream()
      val writer = serializer.serializeStream(output)
      rows.foreach(writer.writeValue(_))
      writer.close()
      for (useIterator <- Seq(false, true)) {
        val input = new ClosableByteArrayInputStream(output.toByteArray) {
          override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
            super.read(bytes, offset, math.min(length, chunkSize))
          }
        }
        val reader = serializer.deserializeStream(input)
        if (useIterator) {
          assert(reader.asKeyValueIterator.map(_._2.asInstanceOf[UnsafeRow].copy()).toSeq ===
            rows)
          assert(input.closed)
        } else {
          rows.foreach(row => assert(reader.readValue[UnsafeRow]().copy() === row))
          intercept[EOFException](reader.readValue[UnsafeRow]())
        }
        reader.close()
        assert(input.closed)
      }
  }

  test("row decoding reports truncated payloads") {
    val row = toUnsafeRow(Row("x" * 16384, 1), Array(StringType, IntegerType))
    val serializer = new UnsafeRowSerializer(2).newInstance()
    val output = new ByteArrayOutputStream()
    val writer = serializer.serializeStream(output)
    writer.writeValue(row)
    writer.close()
    for (useIterator <- Seq(false, true)) {
      val input = new ClosableByteArrayInputStream(output.toByteArray.dropRight(1))
      val reader = serializer.deserializeStream(input)
      try {
        intercept[EOFException] {
          if (useIterator) reader.asKeyValueIterator.next() else reader.readValue[UnsafeRow]()
        }
      } finally {
        reader.close()
      }
      assert(input.closed)
    }
  }

  test("SPARK-10466: external sorter spilling with unsafe row serializer") {
    val conf = new SparkConf()
      .set(SHUFFLE_SPILL_INITIAL_MEM_THRESHOLD, 1L)
      .set(SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD, 0)
      .set(TEST_MEMORY, 80000L)

    spark = SparkSession.builder().master("local").appName("test").config(conf).getOrCreate()
    // prepare data
    val converter = unsafeRowConverter(Array(IntegerType))
    val data = (1 to 10000).iterator.map { i =>
      (i, converter(Row(i)))
    }

    val shuffleExecutorComponents = new LocalDiskShuffleExecutorComponents(conf)
    shuffleExecutorComponents.initializeExecutor(
      spark.sparkContext.applicationId, "0", new HashMap[String, String])
    val mapOutputWriter = shuffleExecutorComponents.createMapOutputWriter(
      0, 0, 10)

    val taskMemoryManager = new TaskMemoryManager(spark.sparkContext.env.memoryManager, 0)
    val taskContext = new TaskContextImpl(0, 0, 0, 0, 0, 1, taskMemoryManager, new Properties, null)

    val sorter = new ExternalSorter[Int, UnsafeRow, UnsafeRow](
      taskContext,
      partitioner = Some(new HashPartitioner(10)),
      serializer = new UnsafeRowSerializer(numFields = 1))

    // Ensure we spilled something and have to merge them later
    assert(sorter.numSpills === 0)
    sorter.insertAll(data)
    assert(sorter.numSpills > 0)

    // Merging spilled files should not throw assertion error
    sorter.writePartitionedMapOutput(0, 0, mapOutputWriter,
      taskContext.taskMetrics.shuffleWriteMetrics)
  }

  test("SPARK-10403: unsafe row serializer with SortShuffleManager") {
    val conf = new SparkConf().set(SHUFFLE_MANAGER, "sort")
    spark = SparkSession.builder().master("local").appName("test").config(conf).getOrCreate()
    val row = Row("Hello", 123)
    val unsafeRow = toUnsafeRow(row, Array(StringType, IntegerType))
    val rowsRDD = spark.sparkContext.parallelize(
      Seq((0, unsafeRow), (1, unsafeRow), (0, unsafeRow))
    ).asInstanceOf[RDD[Product2[Int, InternalRow]]]
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rowsRDD,
        new PartitionIdPassthrough(2),
        new UnsafeRowSerializer(2))
    val shuffled = new ShuffledRowRDD(
      dependency,
      SQLShuffleReadMetricsReporter.createShuffleReadMetrics(spark.sparkContext))
    shuffled.count()
  }
}
