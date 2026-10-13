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

package org.apache.spark.shuffle.streaming

import java.io.{FilterInputStream, InputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.{FutureTask, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.zip.CRC32C

import scala.reflect.ClassTag

import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.io.LZ4CompressionCodec
import org.apache.spark.network.TransportContext
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.{RpcHandler, StreamManager}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.MemoryShuffleBlockResolver

private object BatchMemoryShuffleProof {
  val consumedBeforeProducerEnd = new AtomicBoolean
  val producerEnded = new AtomicBoolean
  val failOnce = new AtomicBoolean
  val blocked = new AtomicBoolean
  val openCodecStreams = new AtomicLong
  val createdCodecStreams = new AtomicLong
}

class BatchMemoryShuffleSuite extends SparkFunSuite with LocalSparkContext {
  private def start(
      capacity: Long = 4 * 1024 * 1024L,
      settings: Seq[(String, String)] = Nil): MemoryShuffleBlockResolver = {
    sc = new SparkContext(new SparkConf(false).setMaster("local[1]")
      .setAppName("live-memory-shuffle-test").set("spark.ui.enabled", "false")
      .set(SHUFFLE_MEMORY_ENABLED, true).set(SHUFFLE_MEMORY_MAX_SIZE, capacity)
      .set(SHUFFLE_MANAGER_INCREMENTAL, "batch-memory")
      .set(STREAMING_SHUFFLE_NETWORK_BUFFER_SIZE, 1024)
      .set(EXECUTOR_TASK_RESIDENCY_MULTIPLIER, 8).setAll(settings))
    SparkEnv.get.blockingShuffleManager.shuffleBlockResolver
      .asInstanceOf[MemoryShuffleBlockResolver]
  }

  private def payload(buffer: ManagedBuffer): Array[Byte] = {
    try {
      val bytes = buffer.nioByteBuffer()
      val header = bytes.getLong()
      assert((header >>> 32).toInt == buffer.size())
      val expected = header & 0xffffffffL
      val checksum = new CRC32C
      checksum.update(bytes.duplicate())
      assert(checksum.getValue == expected)
      val result = new Array[Byte](bytes.remaining())
      bytes.get(result)
      result
    } finally {
      buffer.release()
    }
  }

  test("ready connections are reused and closed connections are replaced") {
    start()
    val transport = new TransportContext(
      SparkTransportConf.fromSparkConf(sc.getConf, "shuffle", 1), new RpcHandler {
        override def getStreamManager: StreamManager = null
        override def receive(
            client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback): Unit = {
          callback.onSuccess(message)
        }
      })
    val server = transport.createServer(0, java.util.Collections.emptyList())
    try {
      val manager = SparkEnv.get.pipelinedShuffleManager.asInstanceOf[BatchMemoryShuffleManager]
      val location = StreamingShuffleTaskLocation("peer", "127.0.0.1", server.getPort)
      val first = manager.client(location)
      assert(manager.client(location) eq first)
      first.close()
      eventually { assert(!first.isActive) }
      val replacement = manager.client(location)
      assert(replacement ne first)
      assert(replacement.isActive)
    } finally {
      server.close()
      transport.close()
    }
  }

  test("pages are readable before producer completion and survive successful completion") {
    val resolver = start()
    val output = new LiveShuffleOutput(resolver, 100, 1, 1, 1024,
      SparkTransportConf.fromSparkConf(sc.getConf, "shuffle", 1), _ => (), _ => ())
    try {
      val waiting = output.read(0, 0)
      assert(!waiting.isDone)
      output.output(0).write(Array[Byte](1, 2, 3))
      output.flush()
      assert(payload(waiting.get(10, TimeUnit.SECONDS)).toSeq == Seq[Byte](1, 2, 3))
      output.output(0).write(Array[Byte](4, 5))
      output.commit()
      output.abort(new IOException("successful task cleanup"))
      assert(payload(output.read(0, 1).get()).toSeq == Seq[Byte](4, 5))
      val end = output.read(0, 2).get()
      try assert(end.size() == 0) finally end.release()
    } finally output.dispose()
  }

  test("remote batches transfer ready pages without waiting for a full batch") {
    val resolver = start()
    val output = new LiveShuffleOutput(resolver, 100, 1, 1, 1024,
      SparkTransportConf.fromSparkConf(sc.getConf, "shuffle", 1), _ => (), _ => ())
    val bytes = Array.tabulate[Byte](3000)(_.toByte)
    try {
      output.output(0).write(bytes)
      val ready = output.readBatch(0, 0, 16)
      assert(ready.isDone)
      val buffer = ready.get()
      try {
        assert(buffer.size() == 2048)
        val batch = buffer.nioByteBuffer()
        val consumed = new java.io.ByteArrayOutputStream
        while (batch.hasRemaining) {
          val header = batch.getLong()
          val data = new Array[Byte]((header >>> 32).toInt - 8)
          batch.get(data)
          val checksum = new CRC32C
          checksum.update(data)
          assert(checksum.getValue == (header & 0xffffffffL))
          consumed.write(data)
        }
        assert(consumed.toByteArray.toSeq == bytes.take(2032).toSeq)
        assert(resolver.allocatedBytes == 3072)
      } finally buffer.release()
      assert(resolver.allocatedBytes == 1024)
      output.commit()
      assert(payload(output.readBatch(0, 2, 16).get()).toSeq == bytes.drop(2032).toSeq)
      assert(resolver.allocatedBytes == 0)
      val end = output.readBatch(0, 3, 16).get()
      try assert(end.size() == 0) finally end.release()
    } finally output.dispose()
  }

  test("a producer larger than the shared budget finishes before readers and spills safely") {
    val resolver = start(capacity = 2048)
    val disk = new AtomicLong
    val output = new LiveShuffleOutput(resolver, 100, 1, 2, 1024,
      SparkTransportConf.fromSparkConf(sc.getConf, "shuffle", 1), _ => (), disk.addAndGet)
    val bytes = Array.tabulate[Byte](65536)(i => i.toByte)
    try {
      output.output(0).write(bytes)
      output.output(1).write(bytes.reverse)
      output.commit()
      assert(disk.get() > 0)
      for (partition <- 0 until 2) {
        val result = new java.io.ByteArrayOutputStream
        var sequence = 0
        var done = false
        while (!done) {
          val page = output.read(partition, sequence).get(10, TimeUnit.SECONDS)
          sequence += 1
          if (page.size() == 0) { page.release(); done = true }
          else result.write(payload(page))
        }
        val expected = if (partition == 0) bytes else bytes.reverse
        assert(result.toByteArray.toSeq == expected.toSeq)
      }
    } finally output.dispose()
  }

  test("failure wakes a pending read and rejects late publication") {
    val resolver = start()
    val output = new LiveShuffleOutput(resolver, 100, 1, 1, 1024,
      SparkTransportConf.fromSparkConf(sc.getConf, "shuffle", 1), _ => (), _ => ())
    val read = output.read(0, 0)
    output.fail(new IOException("injected producer failure"))
    assert(read.isCompletedExceptionally)
    intercept[IOException] { output.output(0).write(1) }
    output.dispose()
  }

  private def shuffle[V: ClassTag](input: RDD[(Int, V)], width: Int): RDD[(Int, V)] = {
    new ShuffledRDD[Int, V, V](input, new HashPartitioner(width))
      .setSerializer(SparkEnv.get.serializer)
      .setPipelinedShuffle(true)
  }

  test("partial RDD consumption closes the active compression stream") {
    start(settings = Seq(
      IO_COMPRESSION_CODEC.key -> classOf[BatchMemoryCloseTrackingCodec].getName))
    BatchMemoryShuffleProof.openCodecStreams.set(0)
    BatchMemoryShuffleProof.createdCodecStreams.set(0)
    val input = sc.parallelize(0 until 10000, 1).map(i => (0, i))
    assert(shuffle(input, 1).mapPartitions(_.take(1)).collect().toSeq == Seq(0 -> 0))
    assert(BatchMemoryShuffleProof.createdCodecStreams.get() > 0)
    assert(BatchMemoryShuffleProof.openCodecStreams.get() == 0)
  }

  gridTest("real RDD consumers process rows before producers finish on a single CPU")(
      Seq(8192L, 4 * 1024 * 1024L)) { capacity =>
    val resolver = start(capacity = capacity)
    BatchMemoryShuffleProof.consumedBeforeProducerEnd.set(false)
    BatchMemoryShuffleProof.producerEnded.set(false)
    val input = sc.parallelize(0 until 100000, 1).mapPartitions { rows =>
      new Iterator[(Int, Int)] {
        override def hasNext: Boolean = {
          val result = rows.hasNext
          if (!result) BatchMemoryShuffleProof.producerEnded.set(true)
          result
        }
        override def next(): (Int, Int) = { val value = rows.next(); (value, value) }
      }
    }
    val result = shuffle(input, 4).mapPartitions { rows =>
      rows.map { pair =>
        if (!BatchMemoryShuffleProof.producerEnded.get()) {
          BatchMemoryShuffleProof.consumedBeforeProducerEnd.set(true)
        }
        pair
      }
    }.collect()
    assert(result.length == 100000)
    assert(result.map(_._2).sorted.toSeq == (0 until 100000))
    assert(BatchMemoryShuffleProof.consumedBeforeProducerEnd.get(),
      "downstream computation must start before the producer ends")
    eventually { assert(resolver.allocatedBytes == 0) }
  }

  test("more producer partitions than CPUs progress with fresh epochs on repeated actions") {
    start(capacity = 8192)
    val input = sc.parallelize(0 until 10000, 16).map(i => (i, i))
    val output = shuffle(input, 4)
    for (_ <- 0 until 2) {
      assert(output.collect().map(_._2).sorted.toSeq == (0 until 10000))
    }
  }

  test("readiness completes a shuffle whose producer streams are all empty") {
    val resolver = start(capacity = 2048)
    val input = sc.parallelize(0 until 4, 4).flatMap(_ => Iterator.empty[(Int, Int)])
    assert(shuffle(input, 2).collect().isEmpty)
    eventually { assert(resolver.allocatedBytes == 0) }
  }

  test("a failed live group cannot publish a partial result and a fresh action recomputes it") {
    val resolver = start()
    BatchMemoryShuffleProof.failOnce.set(true)
    val input = sc.parallelize(0 until 20000, 1).map { i =>
      if (i == 4096 && BatchMemoryShuffleProof.failOnce.compareAndSet(true, false)) {
        throw new IOException("injected failure after partial publication")
      }
      (i, i)
    }
    val output = shuffle(input, 4)
    intercept[SparkException] { output.collect() }
    assert(output.collect().map(_._2).sorted.toSeq == (0 until 20000))
    eventually { assert(resolver.allocatedBytes == 0) }
  }

  test("cancellation wakes an empty reader and returns CPU and page reservations") {
    val resolver = start()
    BatchMemoryShuffleProof.blocked.set(false)
    val input = sc.parallelize(0 until 100, 1).mapPartitions { rows =>
      new Iterator[(Int, Int)] {
        override def hasNext: Boolean = {
          BatchMemoryShuffleProof.blocked.set(true)
          val context = TaskContext.get()
          context.withReleasedCpu {
            while (true) {
              context.killTaskIfInterrupted()
              Thread.sleep(5)
            }
          }
          false
        }
        override def next(): (Int, Int) = { val value = rows.next(); (value, value) }
      }
    }
    val output = shuffle(input, 4)
    val work = new FutureTask[Unit](() => {
      sc.setJobGroup("cancel-live", "cancel live shuffle", interruptOnCancel = false)
      intercept[SparkException] { output.collect() }
    })
    val thread = new Thread(work, "live-shuffle-cancellation-test")
    thread.start()
    try {
      eventually { assert(BatchMemoryShuffleProof.blocked.get()) }
      sc.cancelJobGroup("cancel-live")
      work.get(10, TimeUnit.SECONDS)
      eventually {
        assert(resolver.allocatedBytes == 0)
        assert(SparkEnv.get.pipelinedShuffleManager
          .asInstanceOf[BatchMemoryShuffleManager].liveOutputCount == 0)
      }
      assert(shuffle(sc.parallelize(0 until 100, 4).map(i => (i, i)), 4).count() == 100)
    } finally {
      sc.cancelJobGroup("cancel-live")
      thread.interrupt()
      thread.join(10000)
    }
  }
  gridTest("serialized records larger than the budget survive compression and encryption")(
      Seq((true, false), (false, true))) { case (compress, encrypt) =>
    val resolver = start(capacity = 8192, settings = Seq(
      "spark.shuffle.compress" -> compress.toString,
      "spark.io.encryption.enabled" -> encrypt.toString))
    val bytes = new Array[Byte](128 * 1024)
    new scala.util.Random(42).nextBytes(bytes)
    val input = sc.parallelize(0 until 8, 4).map(i => (i, bytes))
    val result = shuffle(input, 4).collect().sortBy(_._1)
    assert(result.map(_._1).toSeq == (0 until 8))
    result.foreach { case (_, actual) => assert(java.util.Arrays.equals(actual, bytes)) }
    eventually { assert(resolver.allocatedBytes == 0) }
  }

  test("core RDD combining and ordering retain their ordinary shuffle semantics") {
    start()
    val input = sc.parallelize(0 until 10000, 4).map(i => (i % 101, 1))
    val output = new ShuffledRDD[Int, Int, Int](input, new HashPartitioner(4))
      .setPipelinedShuffle(true)
      .setKeyOrdering(Ordering.Int)
      .setAggregator(new Aggregator[Int, Int, Int](identity, _ + _, _ + _))
      .setMapSideCombine(true)
    val result = output.mapPartitions { pairs =>
      val values = pairs.toVector
      require(values.map(_._1) == values.map(_._1).sorted)
      values.iterator
    }.collect().toMap
    assert(result == (0 until 10000).groupMapReduce(_ % 101)(_ => 1)(_ + _))
  }

}

class BatchMemoryCloseTrackingCodec(conf: SparkConf) extends LZ4CompressionCodec(conf) {
  override def compressedInputStream(input: InputStream): InputStream = {
    new FilterInputStream(super.compressedInputStream(input)) {
      private var closed = false
      BatchMemoryShuffleProof.createdCodecStreams.incrementAndGet()
      BatchMemoryShuffleProof.openCodecStreams.incrementAndGet()

      override def close(): Unit = if (!closed) {
        closed = true
        try super.close() finally BatchMemoryShuffleProof.openCodecStreams.decrementAndGet()
      }
    }
  }
}
