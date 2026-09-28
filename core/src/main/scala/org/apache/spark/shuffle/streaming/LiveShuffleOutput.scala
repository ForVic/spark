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

import java.io.{File, IOException, OutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.CRC32C

import scala.collection.mutable

import io.netty.buffer.{ByteBuf, Unpooled}

import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NettyManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.shuffle.sort.MemoryShuffleBlockResolver
import org.apache.spark.storage.ShuffleBlockId

/**
 * Executor-owned incremental partition streams. Each page is published once and read once.
 * Serialization and compression span pages, so even a record larger than the budget is supported.
 * A full output budget spills new pages, allowing a producer to finish without resident readers.
 * Consumption releases memory; successful task completion does not invalidate published pages.
 */
private[spark] final class LiveShuffleOutput(
    resolver: MemoryShuffleBlockResolver,
    shuffleId: Int,
    mapId: Long,
    partitions: Int,
    pageSize: Int,
    transportConf: TransportConf,
    private var onMemoryBytes: Long => Unit,
    private var onDiskBytes: Long => Unit,
    private var onPressure: () => Unit = () => ()) {
  require(pageSize >= 1024)
  private val streams = Array.tabulate(partitions)(new PartitionOutput(_))
  private val files = mutable.ArrayBuffer.empty[File]
  @volatile private var failure: Throwable = _
  @volatile private var committed = false
  private val earlyReads = new AtomicLong

  def pagesReadBeforeCommit: Long = earlyReads.get()

  def output(partition: Int): OutputStream = streams(partition)
  def flush(): Unit = streams.foreach(_.flush())
  def lengths: Array[Long] = streams.map(_.length)
  def read(partition: Int, sequence: Int): CompletableFuture[ManagedBuffer] =
    streams(partition).read(sequence)
  def readBatch(partition: Int, sequence: Int, maxPages: Int)
      : CompletableFuture[ManagedBuffer] = {
    require(maxPages >= 1 && maxPages <= 16)
    streams(partition).readBatch(sequence, maxPages)
  }
  def abandon(partition: Int): Unit = streams(partition).abandon()

  def commit(): Unit = {
    streams.foreach(_.close())
    committed = true
    releaseProducerCallbacks()
  }

  def abort(error: Throwable): Unit = {
    if (!committed) fail(error)
  }

  def fail(error: Throwable): Unit = {
    failure = error
    streams.foreach(_.fail(error))
    releaseProducerCallbacks()
  }

  private def releaseProducerCallbacks(): Unit = {
    // Published pages outlive their producer. Do not retain its TaskContext, listeners or RDD
    // through metric and backpressure closures once it can no longer write.
    onMemoryBytes = _ => ()
    onDiskBytes = _ => ()
    onPressure = () => ()
  }

  def dispose(): Unit = {
    fail(new IOException("Live shuffle output removed"))
    files.synchronized { files.foreach(_.delete()); files.clear() }
  }

  private final class PartitionOutput(partition: Int) extends OutputStream {
    private val block = ShuffleBlockId(shuffleId, mapId, partition)
    private sealed trait Pages {
      def take(): ManagedBuffer
      def empty: Boolean
      def release(): Unit
    }
    private class MemoryPage(buffer: ManagedBuffer) extends Pages {
      private var taken = false
      override def take(): ManagedBuffer = { taken = true; buffer }
      override def empty: Boolean = taken
      override def release(): Unit = if (!taken) buffer.release()
    }
    private class DiskPages(var offset: Long, var end: Long) extends Pages {
      override def take(): ManagedBuffer = {
        val header = new RandomAccessFile(file, "r")
        val count = try { header.seek(offset); header.readInt() } finally header.close()
        require(count >= 8 && count <= pageSize && offset + 4 + count <= end)
        val buffer = new FileSegmentManagedBuffer(transportConf, file, offset + 4, count)
        offset += 4 + count
        buffer
      }
      override def empty: Boolean = offset == end
      override def release(): Unit = ()
    }
    private val queue = mutable.Queue.empty[Pages]
    private var pending: CompletableFuture[ManagedBuffer] = _
    private var nextSequence = 0
    private var page: ByteBuf = _
    private var file: File = _
    private var disk: RandomAccessFile = _
    private var diskStart = 0L
    private var spillOnPressure = false
    private var size = 0
    private val checksum = new CRC32C
    private var finished = false
    private var abandoned = false
    var length = 0L

    private def checkFailure(): Unit = if (failure != null) throw new IOException(failure)

    private def allocate(allowDisk: Boolean): Boolean = {
      resolver.newLivePage(block, pageSize) match {
        case Some(buffer) =>
          page = buffer
          page.writeLong(0L)
          spillOnPressure = false
        case None if !allowDisk && !spillOnPressure => return false
        case None =>
          spillOnPressure = true
          if (disk == null) {
            file = File.createTempFile("live-shuffle-", ".pages",
              resolver.getDataFile(shuffleId, mapId).getParentFile)
            files.synchronized { files += file }
            disk = new RandomAccessFile(file, "rw")
          }
          diskStart = disk.length()
          disk.seek(diskStart)
          disk.writeInt(0)
          disk.writeLong(0L)
      }
      size = 8
      checksum.reset()
      true
    }

    override def write(value: Int): Unit = write(Array(value.toByte), 0, 1)

    override def write(bytes: Array[Byte], offset: Int, count: Int): Unit = {
      java.util.Objects.checkFromIndexSize(offset, count, bytes.length)
      var position = offset
      val end = offset + count
      var allowDisk = false
      while (position < end) {
        val written = synchronized {
          checkFailure()
          if (abandoned) return
          require(!finished, "Writing a completed live partition")
          if (size == 0 && !allocate(allowDisk)) {
            0
          } else {
            val amount = math.min(end - position, pageSize - size)
            if (page != null) page.writeBytes(bytes, position, amount)
            else disk.write(bytes, position, amount)
            checksum.update(bytes, position, amount)
            size += amount
            length += amount
            if (size == pageSize) publish()
            amount
          }
        }
        if (written == 0) {
          // Never suspend with a partition lock held: readers need that lock to drain pages.
          // Publish partial pages first, then give ready consumers a chance to return capacity.
          // A bounded wait followed by spill also progresses when readers are not scheduled yet.
          LiveShuffleOutput.this.flush()
          onPressure()
          allowDisk = true
        } else {
          position += written
          allowDisk = false
        }
      }
    }

    private def publish(): Unit = {
      if (size == 0) return
      val pages: Pages = if (page != null) {
        page.setLong(0, (size.toLong << 32) | checksum.getValue)
        val result = new NettyManagedBuffer(page)
        page = null
        onMemoryBytes(size - 8)
        new MemoryPage(result)
      } else {
        val end = diskStart + 4 + size
        disk.seek(diskStart)
        disk.writeInt(size)
        disk.writeLong((size.toLong << 32) | checksum.getValue)
        disk.seek(end)
        onDiskBytes(size - 8)
        new DiskPages(diskStart, end)
      }
      size = 0
      if (pending != null) {
        if (!committed) earlyReads.incrementAndGet()
        val waiter = pending
        pending = null
        val buffer = pages.take()
        if (!waiter.complete(buffer)) buffer.release()
      } else {
        // Consecutive spilled frames use one range descriptor, not one heap object per page.
        (queue.lastOption, pages) match {
          case (Some(previous: DiskPages), next: DiskPages) if previous.end == next.offset =>
            previous.end = next.end
          case _ => queue.enqueue(pages)
        }
      }
    }

    override def flush(): Unit = synchronized {
      checkFailure()
      if (!abandoned) publish()
    }

    override def close(): Unit = synchronized {
      checkFailure()
      if (!finished) {
        if (!abandoned) publish()
        finished = true
        if (disk != null) { disk.close(); disk = null }
        if (pending != null) {
          pending.complete(new NioManagedBuffer(ByteBuffer.allocate(0)))
          pending = null
        }
      }
    }

    def read(sequence: Int): CompletableFuture[ManagedBuffer] = synchronized {
      checkFailure()
      require(!abandoned && pending == null && sequence == nextSequence,
        "Duplicate or out-of-order live shuffle page request")
      nextSequence += 1
      val result = new CompletableFuture[ManagedBuffer]
      if (queue.nonEmpty) {
        if (!committed) earlyReads.incrementAndGet()
        val pages = queue.front
        result.complete(pages.take())
        if (pages.empty) queue.dequeue()
      } else if (finished) result.complete(new NioManagedBuffer(ByteBuffer.allocate(0)))
      else pending = result
      result
    }

    def readBatch(sequence: Int, maxPages: Int): CompletableFuture[ManagedBuffer] = {
      read(sequence).thenApply { first =>
        synchronized {
          // Drain only ready memory pages. Never wait to fill a batch or copy spilled data.
          if (!first.isInstanceOf[NettyManagedBuffer] || maxPages == 1 ||
              !queue.headOption.exists(_.isInstanceOf[MemoryPage])) {
            first
          } else {
            val combined = Unpooled.compositeBuffer(maxPages)
            def append(buffer: ManagedBuffer): Unit = {
              try combined.addComponent(true, buffer.convertToNetty().asInstanceOf[ByteBuf])
              finally buffer.release()
            }
            try {
              append(first)
              while (combined.numComponents() < maxPages &&
                  queue.headOption.exists(_.isInstanceOf[MemoryPage])) {
                append(queue.dequeue().take())
                nextSequence += 1
                if (!committed) earlyReads.incrementAndGet()
              }
              new NettyManagedBuffer(combined)
            } catch {
              case error: Throwable => combined.release(); throw error
            }
          }
        }
      }
    }

    private def release(): Unit = {
      queue.foreach(_.release())
      queue.clear()
      if (page != null) { page.release(); page = null }
      if (disk != null) { disk.close(); disk = null }
      size = 0
    }

    def abandon(): Unit = synchronized {
      abandoned = true
      release()
      if (pending != null) {
        pending.completeExceptionally(new IOException("Live shuffle reader abandoned output"))
        pending = null
      }
    }

    def fail(error: Throwable): Unit = synchronized {
      release()
      if (pending != null) {
        pending.completeExceptionally(error)
        pending = null
      }
    }
  }
}
