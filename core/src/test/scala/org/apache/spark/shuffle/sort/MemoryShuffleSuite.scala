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

package org.apache.spark.shuffle.sort

import java.io.{File, IOException}
import java.lang.ref.WeakReference
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, TimeUnit}
import java.util.zip.Adler32

import _root_.io.netty.buffer.ByteBuf

import org.apache.spark.{HashPartitioner, LocalSparkContext, SparkConf, SparkContext, SparkEnv, SparkFunSuite, TaskContext}
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager, TestMemoryConsumer}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.ShuffleChecksumTestHelper
import org.apache.spark.storage.{ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.collection.OpenHashSet

class MemoryShuffleSuite extends SparkFunSuite with LocalSparkContext
  with ShuffleChecksumTestHelper {

  private def start(
      capacity: Long = 4 * 32768L,
      maxMap: Long = 4 * 32768L,
      automatic: Boolean = false,
      settings: Seq[(String, String)] = Nil): MemoryShuffleBlockResolver = {
    val conf = new SparkConf(false).setMaster("local[2]").setAppName("memory-shuffle-test")
      .set("spark.ui.enabled", "false")
      .set(SHUFFLE_MEMORY_ENABLED, true)
      .set(SHUFFLE_MEMORY_MAX_SIZE, capacity)
      .set(SHUFFLE_MEMORY_MAX_MAP_SIZE, maxMap)
    if (automatic) {
      conf.remove(SHUFFLE_MEMORY_MAX_SIZE.key)
      conf.remove(SHUFFLE_MEMORY_MAX_MAP_SIZE.key)
    }
    sc = new SparkContext(conf.setAll(settings))
    SparkEnv.get.blockingShuffleManager.shuffleBlockResolver
      .asInstanceOf[MemoryShuffleBlockResolver]
  }

  private def bytes(buffer: ManagedBuffer): Seq[Byte] = {
    val data = buffer.nioByteBuffer()
    val result = new Array[Byte](data.remaining())
    data.get(result)
    result.toSeq
  }

  test("small partitions do not each reserve a full transfer page") {
    val resolver = start(capacity = 1024 * 1024L, maxMap = 1024 * 1024L)
    val output = resolver.newMapOutput(100, 1L, 200, _ => ())
    for (partition <- 0 until 200) output.output(partition).write(partition)
    assert(output.isInMemory)
    assert(resolver.allocatedBytes <= 200 * 1024L)
    assert(output.commit(Array.emptyLongArray).forall(_ == 1L))
    val lease = resolver.getBlockData(ShuffleBlockBatchId(100, 1L, 0, 200), None)
    try assert(bytes(lease) == (0 until 200).map(_.toByte)) finally lease.release()
    resolver.removeDataByMap(100, 1L)
    assert(resolver.allocatedBytes == 0L)
  }

  test("sealed output and batch fetch survive producer completion and unregister") {
    val resolver = start()
    val output = resolver.newMapOutput(100, 7L, 3, _ => ())
    output.output(0).write(Array[Byte](1, 2))
    output.output(2).write(Array[Byte](3, 4, 5))
    assert(output.commit(Array.emptyLongArray).toSeq == Seq(2L, 0L, 3L))
    output.abort() // The producer task completion listener must not invalidate committed output.
    assert(!resolver.getDataFile(100, 7L).exists())
    assert(!resolver.getIndexFile(100, 7L).exists())
    val batch = resolver.getBlockData(ShuffleBlockBatchId(100, 7L, 0, 3), None)
    val empty = resolver.getBlockData(ShuffleBlockId(100, 7L, 1), None)
    try {
      resolver.removeDataByMap(100, 7L)
      assert(resolver.allocatedBytes == 2048L)
      assert(bytes(batch) == Seq[Byte](1, 2, 3, 4, 5))
      assert(empty.size() == 0L)
    } finally {
      batch.release()
      empty.release()
    }
    assert(resolver.allocatedBytes == 0L)
  }

  test("shared budget evicts sealed output into ordinary index data and checksum files") {
    val resolver = start(capacity = 1024L)
    val first = resolver.newMapOutput(100, 1L, 2, _ => ())
    val payload = Array[Byte](5, 9, 2)
    val checksum = new Adler32
    checksum.update(payload)
    first.output(0).write(payload)
    first.commit(Array(checksum.getValue, 1L))
    var evicted = 0L
    val second = resolver.newMapOutput(100, 2L, 2, n => evicted += n)
    second.output(1).write(Array[Byte](7))
    second.commit(Array.emptyLongArray)
    assert(evicted == payload.length)
    assert(!resolver.contains(100, 1L))
    assert(resolver.contains(100, 2L))
    assert(resolver.allocatedBytes == 1024L)
    val read = resolver.getBlockData(ShuffleBlockId(100, 1L, 0), None)
    try assert(bytes(read) == payload.toSeq) finally read.release()
    compareChecksums(2, "ADLER32", resolver.getChecksumFile(100, 1L, "ADLER32"),
      resolver.getDataFile(100, 1L), resolver.getIndexFile(100, 1L))
  }

  test("eviction copies do not block readers and retained pages stay charged") {
    start(capacity = 2048L)
    val copying = new CountDownLatch(1)
    val resume = new CountDownLatch(1)
    val resolver = new MemoryShuffleBlockResolver(sc.getConf,
        new ConcurrentHashMap[Int, OpenHashSet[Long]]) {
      override protected def writeMemoryOutput(data: ByteBuf, file: File): Unit = {
        copying.countDown()
        assert(resume.await(10, TimeUnit.SECONDS))
        super.writeMemoryOutput(data, file)
      }
    }
    val worker = Executors.newSingleThreadExecutor()
    var lease: ManagedBuffer = null
    try {
      for (id <- 1L to 2L) {
        val output = resolver.newMapOutput(100, id, 1, _ => ())
        output.output(0).write(id.toInt)
        output.commit(Array.emptyLongArray)
      }
      val copy = worker.submit(new Runnable {
        override def run(): Unit = {
          resolver.getChecksumFile(100, 1L, "ADLER32")
        }
      })
      assert(copying.await(10, TimeUnit.SECONDS))
      val readWorker = Executors.newSingleThreadExecutor()
      try {
        val read = readWorker.submit(new java.util.concurrent.Callable[ManagedBuffer] {
          override def call(): ManagedBuffer = {
            val unrelated = resolver.getBlockData(ShuffleBlockId(100, 2L, 0), None)
            try assert(bytes(unrelated) == Seq[Byte](2)) finally unrelated.release()
            resolver.getBlockData(ShuffleBlockId(100, 1L, 0), None)
          }
        })
        lease = read.get(5, TimeUnit.SECONDS)
      } finally {
        readWorker.shutdownNow()
        assert(readWorker.awaitTermination(10, TimeUnit.SECONDS))
      }
      resume.countDown()
      copy.get(10, TimeUnit.SECONDS)
      assert(!resolver.contains(100, 1L))
      assert(resolver.allocatedBytes == 2048L)
      assert(bytes(lease) == Seq[Byte](1))
      lease.release()
      lease = null
      assert(resolver.allocatedBytes == 1024L)
    } finally {
      resume.countDown()
      worker.shutdownNow()
      assert(worker.awaitTermination(10, TimeUnit.SECONDS))
      if (lease != null) lease.release()
      resolver.stop()
    }
  }

  gridTest("cleanup during an eviction copy never republishes the removed output")(
      Seq(false, true)) { shutdown =>
    start(capacity = 1024L)
    val copying = new CountDownLatch(1)
    val resume = new CountDownLatch(1)
    val resolver = new MemoryShuffleBlockResolver(sc.getConf,
        new ConcurrentHashMap[Int, OpenHashSet[Long]]) {
      override protected def writeMemoryOutput(data: ByteBuf, file: File): Unit = {
        copying.countDown()
        assert(resume.await(10, TimeUnit.SECONDS))
        super.writeMemoryOutput(data, file)
      }
    }
    val worker = Executors.newSingleThreadExecutor()
    try {
      val output = resolver.newMapOutput(100, 1L, 1, _ => ())
      output.output(0).write(7)
      output.commit(Array.emptyLongArray)
      val copy = worker.submit(new Runnable {
        override def run(): Unit = { resolver.getChecksumFile(100, 1L, "ADLER32") }
      })
      assert(copying.await(10, TimeUnit.SECONDS))
      if (shutdown) resolver.stop() else resolver.removeDataByMap(100, 1L)
      assert(resolver.allocatedBytes == 1024L)
      resume.countDown()
      copy.get(10, TimeUnit.SECONDS)
      assert(resolver.allocatedBytes == 0L)
      assert(!resolver.contains(100, 1L))
      assert(!resolver.getDataFile(100, 1L).exists())
      assert(!resolver.getIndexFile(100, 1L).exists())
    } finally {
      resume.countDown()
      worker.shutdownNow()
      assert(worker.awaitTermination(10, TimeUnit.SECONDS))
      resolver.stop()
    }
  }

  test("failed eviction preserves readable output and can be retried") {
    start(capacity = 1024L)
    var failCopy = true
    val resolver = new MemoryShuffleBlockResolver(sc.getConf,
        new ConcurrentHashMap[Int, OpenHashSet[Long]]) {
      override protected def writeMemoryOutput(data: ByteBuf, file: File): Unit = {
        if (failCopy) throw new IOException("injected eviction failure")
        super.writeMemoryOutput(data, file)
      }
    }
    try {
      val output = resolver.newMapOutput(100, 1L, 1, _ => ())
      output.output(0).write(7)
      output.commit(Array.emptyLongArray)
      intercept[IOException] { resolver.getChecksumFile(100, 1L, "ADLER32") }
      assert(resolver.allocatedBytes == 1024L)
      val lease = resolver.getBlockData(ShuffleBlockId(100, 1L, 0), None)
      try assert(bytes(lease) == Seq[Byte](7)) finally lease.release()
      failCopy = false
      resolver.getChecksumFile(100, 1L, "ADLER32")
      assert(resolver.allocatedBytes == 0L)
      val persisted = resolver.getBlockData(ShuffleBlockId(100, 1L, 0), None)
      try assert(bytes(persisted) == Seq[Byte](7)) finally persisted.release()
    } finally resolver.stop()
  }

  gridTest("automatic shuffle capacity follows storage without borrowing execution")(
      Seq(8L * 1024 * 1024, 256L * 1024 * 1024)) { heap =>
    val resolver = start(automatic = true, settings = Seq(
      "spark.testing.memory" -> heap.toString, "spark.testing.reservedMemory" -> "0",
      "spark.memory.fraction" -> "1", "spark.memory.storageFraction" -> "0.5"))
    val manager = SparkEnv.get.memoryManager
    val region = manager.storageMemoryRegionSize(MemoryMode.ON_HEAP)
    assert(region == heap / 2)
    assert(resolver.maxLiveReadBytes == region / 2)
    val output = resolver.newMapOutput(100, 1L, 1, _ => fail("unexpected eviction"))
    val chunk = new Array[Byte](32768)
    val length = region * 3 / 4
    for (_ <- 0L until length / chunk.length) output.output(0).write(chunk)
    assert(output.isInMemory)
    assert(output.commit(Array.emptyLongArray).toSeq == Seq(length))
    assert(resolver.allocatedBytes == length)
    val consumer = new TestMemoryConsumer(new TaskMemoryManager(manager, 123L))
    try {
      consumer.use(heap - region)
      assert(consumer.getUsed == heap - region)
      assert(resolver.contains(100, 1L))
    } finally {
      consumer.free(consumer.getUsed)
      resolver.removeDataByMap(100, 1L)
    }
    assert(resolver.allocatedBytes == 0L)
  }

  test("retained shuffle pages cannot borrow the execution region") {
    val resolver = start(capacity = 1024 * 1024L, maxMap = 1024 * 1024L,
      settings = Seq("spark.testing.memory" -> "1048576",
        "spark.testing.reservedMemory" -> "0", "spark.memory.fraction" -> "1",
        "spark.memory.storageFraction" -> "0.25"))
    for (mapId <- 1L to 2L) {
      val output = resolver.newMapOutput(100, mapId, 1, _ => ())
      output.output(0).write(new Array[Byte](200 * 1024))
      output.commit(Array.emptyLongArray)
    }
    val manager = SparkEnv.get.memoryManager
    assert(resolver.allocatedBytes <= manager.storageMemoryRegionSize(MemoryMode.ON_HEAP))
    val consumer = new TestMemoryConsumer(new TaskMemoryManager(manager, 123L))
    try {
      consumer.use(768 * 1024L)
      assert(consumer.getUsed == 768 * 1024L)
    } finally consumer.free(consumer.getUsed)
  }

  test("leased output stays charged and forces another active writer to disk") {
    val resolver = start(capacity = 1024L)
    val first = resolver.newMapOutput(100, 1L, 1, _ => ())
    first.output(0).write(Array[Byte](8))
    first.commit(Array.emptyLongArray)
    val lease = resolver.getBlockData(ShuffleBlockId(100, 1L, 0), None)
    try {
      val second = resolver.newMapOutput(100, 2L, 1, _ => fail("leased output evicted"))
      val payload = Array.fill[Byte](90000)(3)
      second.output(0).write(payload)
      second.commit(Array.emptyLongArray)
      assert(!second.isInMemory)
      assert(resolver.contains(100, 1L))
      val read = resolver.getBlockData(ShuffleBlockId(100, 2L, 0), None)
      try assert(bytes(read) == payload.toSeq) finally read.release()
      resolver.stop()
      assert(resolver.allocatedBytes == 1024L)
      assert(bytes(lease) == Seq[Byte](8))
    } finally lease.release()
    assert(resolver.allocatedBytes == 0L)
  }

  gridTest("an oversized map spills every partition including a partition opened after spill")(
      Seq(false, true)) { transferTo =>
    val resolver = start(maxMap = 32768L,
      settings = Seq("spark.file.transferTo" -> transferTo.toString))
    val output = resolver.newMapOutput(100, 1L, 3, _ => ())
    val payloads = Seq(Array[Byte](1, 2), Array.fill[Byte](90000)(4), Array[Byte](9))
    payloads.zipWithIndex.foreach { case (payload, partition) =>
      output.output(partition).write(payload)
    }
    assert(output.commit(Array.emptyLongArray).toSeq == payloads.map(_.length.toLong))
    assert(resolver.allocatedBytes == 0L)
    payloads.zipWithIndex.foreach { case (payload, partition) =>
      val read = resolver.getBlockData(ShuffleBlockId(100, 1L, partition), None)
      try assert(bytes(read) == payload.toSeq) finally read.release()
    }
  }

  test("abort releases pages and spilled temporary files without publishing a map") {
    val resolver = start(maxMap = 32768L)
    for (size <- Seq(3, 90000)) {
      val before = SparkEnv.get.blockManager.diskBlockManager.getAllBlocks().toSet
      val output = resolver.newMapOutput(100, size.toLong, 2, _ => ())
      output.output(0).write(new Array[Byte](size))
      output.abort()
      output.abort()
      assert(resolver.allocatedBytes == 0L)
      assert(!resolver.contains(100, size.toLong))
      assert(SparkEnv.get.blockManager.diskBlockManager.getAllBlocks().toSet == before)
      intercept[IllegalArgumentException] { output.commit(Array.emptyLongArray) }
    }
  }

  test("resolver shutdown rejects publication and the producer can release its active pages") {
    start()
    val resolver = new MemoryShuffleBlockResolver(sc.getConf,
      new ConcurrentHashMap[Int, OpenHashSet[Long]])
    val output = resolver.newMapOutput(100, 1L, 1, _ => ())
    output.output(0).write(Array[Byte](1))
    resolver.stop()
    intercept[IllegalArgumentException] { output.commit(Array.emptyLongArray) }
    output.abort()
    assert(resolver.allocatedBytes == 0L)
  }

  test("RDD partitioning and aggregation use ordinary readers with compression and fallback") {
    val resolver = start(capacity = 16 * 32768L, maxMap = 16 * 32768L)
    val input = (1 to 3000).map(i => (i % 7, i))
    val result = sc.parallelize(input, 3).partitionBy(new HashPartitioner(7)).collect()
    assert(result.toSeq.sorted == input.sorted)
    assert(resolver.allocatedBytes > 0L)
    val grouped = sc.parallelize(input, 3).groupByKey(7).mapValues(_.toSeq.sorted).collect().toMap
    assert(grouped == input.groupMap(_._1)(_._2).view.mapValues(_.sorted).toMap)
    val summed = sc.parallelize(input, 3).reduceByKey(_ + _, 7).collect().toMap
    assert(summed == input.groupMapReduce(_._1)(_._2)(_ + _))
  }

  test("sealed output releases the producer callback while retaining its data") {
    val resolver = start()
    def publish(): WeakReference[Array[Byte]] = {
      val taskState = new Array[Byte](1024 * 1024)
      val reference = new WeakReference(taskState)
      val output = resolver.newMapOutput(100, 1L, 1, n => taskState(0) = n.toByte)
      output.output(0).write(Array[Byte](7))
      output.commit(Array.emptyLongArray)
      reference
    }
    val reference = publish()
    for (_ <- 1 to 100 if !reference.refersTo(null)) {
      System.gc()
      if (!reference.refersTo(null)) Thread.sleep(10)
    }
    assert(reference.refersTo(null))
    val data = resolver.getBlockData(ShuffleBlockId(100, 1L, 0), None)
    try assert(bytes(data) == Seq[Byte](7)) finally data.release()
  }

  test("lost sealed output follows ordinary map-stage recomputation") {
    val resolver = start(capacity = 16 * 32768L, maxMap = 16 * 32768L)
    val input = (1 to 1000).map(i => (i % 3, i))
    val shuffled = sc.parallelize(input, 4).partitionBy(new HashPartitioner(3))
    assert(shuffled.collect().toSeq.sorted == input.sorted)
    val dependency = shuffled.dependencies.head.asInstanceOf[
      org.apache.spark.ShuffleDependency[Int, Int, Int]]
    val mapId = resolver.taskIdMapsForShuffle.get(dependency.shuffleId).iterator.next()
    assert(resolver.contains(dependency.shuffleId, mapId))
    resolver.removeDataByMap(dependency.shuffleId, mapId)
    assert(shuffled.collect().toSeq.sorted == input.sorted)
  }

  test("ordinary local readers release every lease before shuffle cleanup") {
    val resolver = start(capacity = 32 * 32768L, maxMap = 16 * 32768L)
    val shuffled = sc.parallelize((1 to 1000).map(i => (i % 3, i)), 2)
      .partitionBy(new HashPartitioner(3))
    assert(shuffled.count() == 1000)
    val dependency = shuffled.dependencies.head.asInstanceOf[
      org.apache.spark.ShuffleDependency[Int, Int, Int]]
    assert(resolver.allocatedBytes > 0L)
    SparkEnv.get.blockingShuffleManager.unregisterShuffle(dependency.shuffleId)
    assert(resolver.allocatedBytes == 0L)
  }

  gridTest("partition streams preserve records with codecs and encryption")(
      Seq((false, "lz4", false), (true, "zstd", false), (true, "lz4", true))) {
    case (compress, codec, encrypt) =>
      val resolver = start(capacity = 32 * 32768L, maxMap = 16 * 32768L,
        settings = Seq("spark.shuffle.compress" -> compress.toString,
          "spark.io.compression.codec" -> codec, "spark.io.encryption.enabled" -> encrypt.toString))
      val input = (1 to 5000).map(i => (i % 3, s"$i-" + "x" * 17))
      val shuffled = sc.parallelize(input, 2).partitionBy(new HashPartitioner(3))
      assert(shuffled.collect().toSeq.sorted == input.sorted)
      assert(resolver.allocatedBytes > 0L)
      val dependency = shuffled.dependencies.head.asInstanceOf[
        org.apache.spark.ShuffleDependency[Int, String, String]]
      val algorithm = sc.getConf.get(SHUFFLE_CHECKSUM_ALGORITHM)
      resolver.taskIdMapsForShuffle.get(dependency.shuffleId).iterator.foreach { mapId =>
        compareChecksums(3, algorithm,
          resolver.getChecksumFile(dependency.shuffleId, mapId, algorithm),
          resolver.getDataFile(dependency.shuffleId, mapId),
          resolver.getIndexFile(dependency.shuffleId, mapId))
      }
      assert(resolver.allocatedBytes == 0L)
  }

  test("failed producer releases partial serialized output and does not publish a map") {
    val resolver = start()
    val dependency = new org.apache.spark.ShuffleDependency[Int, Int, Int](
      sc.parallelize(Seq(1 -> 2), 1), new HashPartitioner(2))
    val handle = dependency.shuffleHandle.asInstanceOf[MemoryShuffleHandle[Int, Int]]
    val context = TaskContext.empty()
    val writer = new MemoryShuffleWriter(handle, 99L, context,
      context.taskMetrics.shuffleWriteMetrics, resolver)
    val records = Iterator(1 -> 2, 2 -> 3) ++ Iterator.continually {
      throw new IllegalStateException("producer failed")
    }
    val error = intercept[IllegalStateException] { writer.write(records) }
    assert(error.getMessage == "producer failed")
    writer.stop(success = false)
    assert(resolver.allocatedBytes == 0L)
    assert(!resolver.contains(handle.shuffleId, 99L))
    assert(!resolver.getDataFile(handle.shuffleId, 99L).exists())
  }

}
