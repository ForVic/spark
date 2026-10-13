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

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream, OutputStream}
import java.util.LinkedHashMap
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import _root_.io.netty.buffer.{ByteBuf, CompositeByteBuf, Unpooled, UnpooledByteBufAllocator, UnpooledHeapByteBuf}

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.config._
import org.apache.spark.memory.MemoryMode
import org.apache.spark.network.buffer.{ManagedBuffer, NettyManagedBuffer}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.{BlockId, ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashSet

/**
 * Executor-owned output with ordinary shuffle block identities and fetch semantics. Pages are
 * charged to storage memory before allocation and survive producer task completion. Pressure
 * moves sealed, unleased output to the ordinary disk format; active writers can also fall back.
 */
private[spark] class MemoryShuffleBlockResolver(
    conf: SparkConf,
    taskIds: ConcurrentHashMap[Int, OpenHashSet[Long]])
  extends IndexShuffleBlockResolver(conf, taskIdMapsForShuffle = taskIds) {

  private case class Output(data: CompositeByteBuf, lengths: Array[Long], checksums: Array[Long]) {
    var persistence: CompletableFuture[Long] = _
  }

  private val outputs = new LinkedHashMap[(Int, Long), Output](16, 0.75f, true)
  // These pages are not MemoryStore entries, so execution cannot evict them. Do not borrow
  // execution's guaranteed region until the resolver participates in operator memory reclamation.
  private lazy val capacity = {
    val storageRegion = memoryManager.storageMemoryRegionSize(MemoryMode.ON_HEAP)
    math.min(conf.get(SHUFFLE_MEMORY_MAX_SIZE).getOrElse(storageRegion), storageRegion)
  }
  private lazy val maxMapSize = conf.get(SHUFFLE_MEMORY_MAX_MAP_SIZE)
    .getOrElse(math.min(capacity, Int.MaxValue.toLong))
  private val minPageSize = 1024
  private val maxPageSize = 32 * 1024
  private var allocated = 0L
  private var liveWriteBytes = 0L
  private var closed = false
  private var persisting = false
  private lazy val memoryManager = SparkEnv.get.memoryManager

  // Keep the sealed buffer independent of its builder and producer task's captured callbacks.
  private class OutputBuffer
    extends CompositeByteBuf(UnpooledByteBufAllocator.DEFAULT, false, Int.MaxValue) {
    var ownedBytes = 0L

    override protected def deallocate(): Unit = {
      try super.deallocate() finally releaseBytes(ownedBytes)
    }
  }

  private[spark] def allocatedBytes: Long = synchronized { allocated }
  private[spark] def maxLiveReadBytes: Long = capacity / 2
  private[sort] def contains(shuffleId: Int, mapId: Long): Boolean = synchronized {
    outputs.containsKey((shuffleId, mapId))
  }

  private def releaseBytes(bytes: Long): Unit = synchronized {
    allocated -= bytes
    assert(allocated >= 0)
    if (bytes > 0) memoryManager.releaseStorageMemory(bytes, MemoryMode.ON_HEAP)
    notifyAll()
  }

  private[spark] def awaitLiveCapacity(bytes: Int): Unit = synchronized {
    if (!closed && (liveWriteBytes + bytes > capacity / 2 || allocated + bytes > capacity)) {
      wait(2)
    }
  }

  /** Reserve half the budget for receive credits so full output queues cannot starve readers. */
  private[spark] def newLivePage(block: ShuffleBlockId, size: Int): Option[ByteBuf] = {
    synchronized {
      if (liveWriteBytes + size > capacity / 2) return None
      liveWriteBytes += size
    }
    var charged = false
    try {
      charged = reserve(block, size, _ => ())
      if (!charged) {
        synchronized {
          liveWriteBytes -= size
          notifyAll()
        }
        return None
      }
      Some(new UnpooledHeapByteBuf(UnpooledByteBufAllocator.DEFAULT, size, size) {
        override protected def deallocate(): Unit = {
          try super.deallocate() finally {
            MemoryShuffleBlockResolver.this.synchronized {
              liveWriteBytes -= size
              releaseBytes(size)
            }
          }
        }
      })
    } catch {
      case error: Throwable =>
        synchronized {
          liveWriteBytes -= size
          if (charged) releaseBytes(size)
          notifyAll()
        }
        throw error
    }
  }

  private[spark] def reserveLiveRead(block: ShuffleBlockId, bytes: Int): Boolean = {
    require(bytes <= capacity / 2, "Live shuffle buffer exceeds half the memory budget")
    reserve(block, bytes, _ => ())
  }

  private[spark] def releaseLiveRead(bytes: Int): Unit = releaseBytes(bytes)

  protected def writeMemoryOutput(data: ByteBuf, file: File): Unit = {
    // These files are newly created and empty. Use append, as ordinary shuffle does: truncating
    // a temporary file can force early ext4 writeback when it is closed.
    Utils.tryWithResource(new FileOutputStream(file, true)) { stream =>
      data.getBytes(0, stream, data.readableBytes())
    }
  }

  private def persist(key: (Int, Long), output: Output): Long = {
    val (owner, completion) = synchronized {
      if (closed || (outputs.get(key) ne output)) return 0L
      if (output.persistence != null) {
        (false, output.persistence)
      } else {
        output.data.retain()
        output.persistence = new CompletableFuture[Long]
        (true, output.persistence)
      }
    }
    if (!owner) {
      completion.get()
      return 0L
    }

    var temporary: File = null
    try {
      // Readers can retain the immutable pages while their disk copy is written. Keep the
      // pages charged until both the persistence lease and every reader have released them.
      // Callers may hold a live partition lock, so do not yield task CPU ownership here.
      temporary = File.createTempFile("memory-shuffle-", ".data",
        getDataFile(key._1, key._2).getParentFile)
      writeMemoryOutput(output.data, temporary)
      val written = synchronized {
        // Cleanup can remove this map while the copy is in progress. Never resurrect it.
        if (closed || (outputs.get(key) ne output)) {
          0L
        } else {
          persisting = true
          try {
            writeMetadataFileAndCommit(key._1, key._2, output.lengths, output.checksums, temporary)
          } finally {
            persisting = false
          }
          val size = output.data.readableBytes().toLong
          outputs.remove(key)
          output.data.release()
          size
        }
      }
      completion.complete(written)
      written
    } catch {
      case error: Throwable =>
        completion.completeExceptionally(error)
        throw error
    } finally {
      synchronized { output.persistence = null }
      output.data.release()
      if (temporary != null && temporary.exists()) temporary.delete()
    }
  }

  private def reserve(
      block: ShuffleBlockId,
      bytes: Int,
      onEviction: Long => Unit): Boolean = {
    def acquire(): Boolean = synchronized {
      require(!closed, "Memory shuffle resolver is closed")
      if (allocated + bytes <= capacity &&
          memoryManager.acquireStorageMemory(block, bytes, MemoryMode.ON_HEAP)) {
        allocated += bytes
        true
      } else {
        false
      }
    }
    while (!acquire()) {
      // A leased page must remain charged until its last local or network reader releases it.
      val victim = synchronized {
        outputs.entrySet().asScala.find { entry =>
          entry.getValue.data.refCnt() == 1 && entry.getValue.persistence == null
        }.map(entry => (entry.getKey, entry.getValue))
      }
      if (victim.isEmpty) return false
      val (key, output) = victim.get
      val written = persist(key, output)
      if (written > 0) onEviction(written)
    }
    true
  }

  def newMapOutput(
      shuffleId: Int,
      mapId: Long,
      partitions: Int,
      onEviction: Long => Unit): MapOutput = synchronized {
    require(!closed, "Memory shuffle resolver is closed")
    new MapOutput(shuffleId, mapId, partitions, onEviction)
  }

  /** A single producer owns the builder until commit transfers its pages to the resolver. */
  final class MapOutput private[MemoryShuffleBlockResolver] (
      shuffleId: Int,
      mapId: Long,
      partitions: Int,
      onEviction: Long => Unit) {
    private val block = ShuffleBlockId(shuffleId, mapId, 0)
    private val buffers = Array.fill(partitions)(new PartitionOutput)
    private var reserved = 0L
    private var allocatedPageBytes = 0L
    private var disk = false
    private var committed = false
    private var aborted = false

    def output(partition: Int): OutputStream = buffers(partition)
    def isInMemory: Boolean = !disk
    def pageBytesAllocated: Long = allocatedPageBytes

    private def spill(): Unit = {
      disk = true
      buffers.foreach(_.spill())
    }

    private final class PartitionOutput extends OutputStream {
      val pages = new ArrayBuffer[ByteBuf]
      var size = 0L
      var file: File = null
      var stream: OutputStream = null

      def spill(force: Boolean = false): Unit = {
        if (stream == null && (pages.nonEmpty || force)) {
          file = File.createTempFile("memory-shuffle-", ".partition",
            getDataFile(shuffleId, mapId).getParentFile)
          stream = new BufferedOutputStream(new FileOutputStream(file, true),
            conf.get(SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024)
          pages.foreach(p => p.getBytes(0, stream, p.readableBytes()))
          releasePages()
        }
      }

      def releasePages(): Unit = {
        val bytes = pages.iterator.map(_.capacity().toLong).sum
        pages.foreach(_.release())
        pages.clear()
        if (bytes > 0) {
          reserved -= bytes
          releaseBytes(bytes)
        }
      }

      override def write(value: Int): Unit = write(Array(value.toByte), 0, 1)

      override def write(bytes: Array[Byte], offset: Int, length: Int): Unit = {
        require(!committed && !aborted, "Map output is no longer writable")
        java.util.Objects.checkFromIndexSize(offset, length, bytes.length)
        var position = offset
        val end = offset + length
        while (position < end) {
          if (!disk && (pages.isEmpty || !pages.last.isWritable)) {
            // Tiny partitions should not each pin a full transfer page. Grow geometrically for
            // small serializer writes, or directly to the incoming chunk size, up to the maximum.
            val previous = if (pages.isEmpty) 0 else pages.last.capacity()
            val needed = math.max(minPageSize,
              math.max(previous * 2, math.min(maxPageSize, end - position)))
            val rounded = math.min(maxPageSize, Integer.highestOneBit(needed - 1) << 1)
            val size = math.min(rounded.toLong, maxMapSize - reserved).toInt
            if (size == 0 || !reserve(block, size, onEviction)) {
              MapOutput.this.spill()
            } else {
              try {
                pages += Unpooled.buffer(size, size)
                reserved += size
                allocatedPageBytes += size
              } catch {
                case error: Throwable =>
                  releaseBytes(size)
                  throw error
              }
            }
          }
          if (disk) {
            if (stream == null) spill(force = true)
            stream.write(bytes, position, end - position)
            size += end - position
            position = end
          } else {
            val count = math.min(end - position, pages.last.writableBytes())
            pages.last.writeBytes(bytes, position, count)
            size += count
            position += count
          }
        }
      }

      override def close(): Unit = {
        if (stream != null) stream.close()
      }

      def delete(): Unit = {
        try close() finally {
          releasePages()
          if (file != null && file.exists()) file.delete()
        }
      }
    }

    def commit(checksums: Array[Long]): Array[Long] = {
      require(!committed && !aborted, "Map output already completed")
      buffers.foreach(_.close())
      val lengths = buffers.map(_.size)
      if (disk) {
        val temporary = File.createTempFile("memory-shuffle-", ".data",
          getDataFile(shuffleId, mapId).getParentFile)
        try {
          Utils.tryWithResource(new FileOutputStream(temporary, true)) { out =>
            buffers.foreach { partition =>
              if (partition.file != null) {
                Utils.tryWithResource(new FileInputStream(partition.file)) { in =>
                  Utils.copyStream(in, out, closeStreams = false,
                    transferToEnabled = conf.get(SHUFFLE_MERGE_PREFER_NIO))
                }
              }
            }
          }
          writeMetadataFileAndCommit(shuffleId, mapId, lengths, checksums, temporary)
        } finally {
          if (temporary.exists()) temporary.delete()
          buffers.foreach(_.delete())
        }
      } else {
        MemoryShuffleBlockResolver.this.synchronized {
          require(!closed && !outputs.containsKey((shuffleId, mapId)))
          val data = new OutputBuffer
          try {
            // Retain before adding: the builder keeps ownership until the whole transfer succeeds.
            buffers.foreach { partition =>
              partition.pages.foreach(p => data.addComponent(true, p.retain()))
            }
            outputs.put((shuffleId, mapId), Output(data, lengths.clone(), checksums.clone()))
          } catch {
            case error: Throwable =>
              data.release()
              throw error
          }
          buffers.foreach { partition =>
            partition.pages.foreach(_.release())
            partition.pages.clear()
          }
          data.ownedBytes = reserved
          reserved = 0
        }
      }
      committed = true
      lengths
    }

    def abort(): Unit = {
      if (!committed && !aborted) {
        aborted = true
        buffers.foreach(_.delete())
      }
    }
  }

  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    if (dirs.isDefined) return super.getBlockData(blockId, dirs)
    val (shuffleId, mapId, start, end) = blockId match {
      case b: ShuffleBlockId => (b.shuffleId, b.mapId, b.reduceId, b.reduceId + 1)
      case b: ShuffleBlockBatchId =>
        (b.shuffleId, b.mapId, b.startReduceId, b.endReduceId)
      case _ => return super.getBlockData(blockId, dirs)
    }
    synchronized {
      val output = outputs.get((shuffleId, mapId))
      if (output != null) {
        require(start >= 0 && end <= output.lengths.length && start < end)
        val offset = output.lengths.take(start).sum.toInt
        val length = output.lengths.slice(start, end).sum.toInt
        return new NettyManagedBuffer(output.data.retainedSlice(offset, length))
      }
    }
    // Eviction publishes disk metadata before removing the memory entry under the lock above.
    // Ordinary and spilled reads should not hold the memory lock while opening their files.
    super.getBlockData(blockId, dirs)
  }

  override def getChecksumFile(
      shuffleId: Int,
      mapId: Long,
      algorithm: String,
      dirs: Option[Array[String]] = None): File = {
    // Corruption diagnosis uses the ordinary checksum-file protocol. Materialize only on demand.
    val output = synchronized {
      if (persisting) None else Option(outputs.get((shuffleId, mapId)))
    }
    output.foreach(persist((shuffleId, mapId), _))
    super.getChecksumFile(shuffleId, mapId, algorithm, dirs)
  }

  override def removeDataByMap(shuffleId: Int, mapId: Long): Unit = synchronized {
    Option(outputs.remove((shuffleId, mapId))).foreach(_.data.release())
    super.removeDataByMap(shuffleId, mapId)
  }

  override def stop(): Unit = synchronized {
    closed = true
    outputs.values().asScala.foreach(_.data.release())
    outputs.clear()
    super.stop()
  }
}
