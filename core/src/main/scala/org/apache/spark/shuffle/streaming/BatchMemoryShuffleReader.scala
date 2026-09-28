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

import java.io.{InputStream, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.CRC32C

import org.apache.spark.{Aggregator, InterruptibleIterator, SparkEnv, TaskContext}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.serializer.DeserializationStream
import org.apache.spark.shuffle.{ShuffleReader, ShuffleReadMetricsReporter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

private[spark] final class BatchMemoryShuffleReader[K, C](
    handle: BatchMemoryShuffleHandle[K, _, C],
    startMap: Int,
    endMap: Int,
    partition: Int,
    context: TaskContext,
    metrics: ShuffleReadMetricsReporter,
    manager: BatchMemoryShuffleManager) extends ShuffleReader[K, C] {
  import BatchMemoryShuffleManager._

  private val run = epoch(context)
  private var active: PageInputStream = _
  private var activeDeserializer: DeserializationStream = _
  private var fetchWaitNs = 0L
  context.addTaskCompletionListener[Unit] { _ =>
    try closeDeserializer() finally {
      try {
        if (active != null) active.close()
      } finally {
        context.taskMetrics().mergeShuffleReadMetrics()
      }
    }
  }

  private def closeDeserializer(): Unit = {
    val stream = activeDeserializer
    activeDeserializer = null
    if (stream != null) stream.close()
  }

  private def addFetchWait(nanos: Long): Unit = {
    val previousMs = fetchWaitNs / 1000000L
    fetchWaitNs += nanos
    metrics.incFetchWaitTime(fetchWaitNs / 1000000L - previousMs)
  }

  private class ReceivedPage(val buffer: ManagedBuffer, releaseCredit: () => Unit) {
    private val closed = new AtomicBoolean
    def close(): Unit = {
      if (closed.compareAndSet(false, true)) {
        try buffer.release() finally releaseCredit()
      }
    }
  }

  /** One reserved batch per map stream bounds receiver memory, including canceled RPCs. */
  private class PageInputStream(map: Int) extends InputStream {
    private val block = ShuffleBlockId(handle.shuffleId, map, partition)
    private val location = {
      val start = System.nanoTime()
      try manager.location(handle.shuffleId, map, context) finally {
        val elapsed = System.nanoTime() - start
        handle.discoveryNs.add(elapsed)
        addFetchWait(elapsed)
      }
    }
    private val local = location.executorId == SparkEnv.get.executorId
    private lazy val client = manager.client(location, context)
    private var sequence = 0
    private var current: ReceivedPage = _
    private var batch: ByteBuffer = _
    private var data: ByteBuffer = _
    private var requestedBytes = manager.pageSize
    private var pending: CompletableFuture[ReceivedPage] = _
    private var ended = false
    private var closed = false

    def hasData: Boolean = (data != null && data.hasRemaining) || nextPage()

    private def requestPage(): CompletableFuture[ReceivedPage] = {
      handle.readRequests.add(1)
      val result = new CompletableFuture[ReceivedPage]
      if (local) {
        manager.readLocal(run, handle.shuffleId, map, partition, sequence)
          .whenComplete { (buffer, error) =>
            if (error != null) result.completeExceptionally(error)
            else {
              val page = new ReceivedPage(buffer, () => ())
              if (!result.complete(page)) page.close()
            }
          }
      } else {
        val maxBytes = math.min(Int.MaxValue.toLong, manager.resolver.maxLiveReadBytes)
        val maxPages = math.min(16L, maxBytes / manager.pageSize).toInt
        require(maxPages > 0, "Live shuffle page exceeds half the executor memory budget")
        var reserved = false
        def tryReserve(): Unit = {
          var pages = maxPages
          while (!reserved && pages > 0) {
            requestedBytes = manager.pageSize * pages
            reserved = manager.resolver.reserveLiveRead(block, requestedBytes)
            if (!reserved) pages /= 2
          }
        }
        try {
          context.killTaskIfInterrupted()
          tryReserve()
          if (!reserved) {
            context.withReleasedCpu {
              while (!reserved) {
                context.killTaskIfInterrupted()
                tryReserve()
                if (!reserved) Thread.sleep(5)
              }
            }
          }
        } catch {
          case error: Throwable =>
            if (reserved) manager.resolver.releaseLiveRead(requestedBytes)
            throw error
        }
        val reservedBytes = requestedBytes
        val creditReleased = new AtomicBoolean
        def releaseCredit(): Unit = {
          if (creditReleased.compareAndSet(false, true)) {
            manager.resolver.releaseLiveRead(reservedBytes)
          }
        }
        try {
          client.sendRpc(request(READ, run, handle.shuffleId, map, partition, sequence,
            requestedBytes / manager.pageSize),
            new RpcResponseCallback {
              override def onSuccess(buffer: ManagedBuffer): Unit = {
                val page = new ReceivedPage(buffer.retain(), () => releaseCredit())
                if (!result.complete(page)) page.close()
              }
              override def onSuccess(buffer: ByteBuffer): Unit = {
                val copy = ByteBuffer.allocate(buffer.remaining())
                copy.put(buffer).flip()
                onSuccess(new NioManagedBuffer(copy))
              }
              override def onFailure(error: Throwable): Unit = {
                releaseCredit()
                result.completeExceptionally(error)
              }
            })
        } catch {
          case error: Throwable => releaseCredit(); throw error
        }
      }
      result
    }

    private def discardPending(): Unit = {
      if (pending != null) {
        if (!pending.cancel(false) && !pending.isCompletedExceptionally) {
          val page = pending.getNow(null)
          if (page != null) page.close()
        }
        pending = null
      }
    }

    private def nextPage(): Boolean = {
      if (batch != null && batch.hasRemaining) return decodePage()
      if (current != null) { current.close(); current = null }
      if (ended || closed) return false
      val start = System.nanoTime()
      try {
        pending = requestPage()
        def await(): ReceivedPage = {
          var result: ReceivedPage = null
          while (result == null) {
            context.killTaskIfInterrupted()
            try result = pending.get(20, TimeUnit.MILLISECONDS) catch {
              case _: TimeoutException =>
            }
          }
          result
        }
        current = if (pending.isDone) await() else context.withReleasedCpu { await() }
        pending = null
        addFetchWait(System.nanoTime() - start)
        if (current.buffer.size() == 0) {
          ended = true
          current.close()
          current = null
          false
        } else {
          require(current.buffer.size() <= requestedBytes, "Live response exceeds reserved memory")
          batch = current.buffer.nioByteBuffer()
          decodePage()
        }
      } catch {
        case error: Throwable =>
          discardPending()
          if (current != null) { current.close(); current = null }
          error match {
            case e: ExecutionException => throw new IOException("Live shuffle fetch failed", e)
            case _ => throw error
          }
      }
    }

    private def decodePage(): Boolean = {
      require(batch.remaining() >= 8, "Truncated live shuffle page header")
      val header = batch.getLong()
      val length = (header >>> 32).toInt
      require(length >= 8 && length <= manager.pageSize && length - 8 <= batch.remaining(),
        "Invalid live shuffle page size")
      data = batch.slice()
      data.limit(length - 8)
      batch.position(batch.position() + length - 8)
      val checksum = new CRC32C
      checksum.update(data.duplicate())
      require(checksum.getValue == (header & 0xffffffffL), "Live shuffle page checksum mismatch")
      sequence += 1
      handle.readPages.add(1)
      if (local) metrics.incLocalBytesRead(data.remaining())
      else metrics.incRemoteBytesRead(data.remaining())
      true
    }

    override def read(): Int = {
      if ((data == null || !data.hasRemaining) && !nextPage()) -1
      else data.get() & 0xff
    }

    override def read(bytes: Array[Byte], offset: Int, length: Int): Int = {
      java.util.Objects.checkFromIndexSize(offset, length, bytes.length)
      if (length == 0) return 0
      if ((data == null || !data.hasRemaining) && !nextPage()) return -1
      val count = math.min(length, data.remaining())
      data.get(bytes, offset, count)
      count
    }

    override def close(): Unit = {
      if (!closed) {
        closed = true
        discardPending()
        if (current != null) { current.close(); current = null }
        data = null
        batch = null
        if (!ended) {
          if (local) manager.abandonLocal(run, handle.shuffleId, map, partition)
          else {
            client.sendRpc(request(ABANDON, run, handle.shuffleId, map, partition),
              new RpcResponseCallback {
                override def onSuccess(buffer: ByteBuffer): Unit = ()
                override def onFailure(error: Throwable): Unit = ()
              })
          }
        }
      }
    }
  }

  override def read(): Iterator[Product2[K, C]] = {
    val dependency = handle.dependency
    val serializer = dependency.serializer.newInstance()
    val maps = (startMap until endMap).iterator
    val records = maps.flatMap { map =>
      active = new PageInputStream(map)
      if (!active.hasData) {
        // Untouched partitions have no serializer, codec or encryption header.
        active.close()
        Iterator.empty
      } else {
        val stream = SparkEnv.get.serializerManager.wrapStream(
          ShuffleBlockId(handle.shuffleId, map, partition), active)
        val decoded = serializer.deserializeStream(stream)
        activeDeserializer = decoded
        CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
          decoded.asKeyValueIterator, closeDeserializer())
      }
    }.map { pair =>
      metrics.incRecordsRead(1)
      pair
    }
    val input = new InterruptibleIterator[(K, Nothing)](context,
      records.asInstanceOf[Iterator[(K, Nothing)]])
    val aggregator = if (dependency.mapSideCombine) {
      dependency.aggregator.map { a =>
        new Aggregator[K, C, C](identity, a.mergeCombiners, a.mergeCombiners)
          .asInstanceOf[Aggregator[K, Nothing, C]]
      }
    } else dependency.aggregator.asInstanceOf[Option[Aggregator[K, Nothing, C]]]
    if (dependency.keyOrdering.isDefined) {
      val sorter = new ExternalSorter[K, Nothing, C](context,
        aggregator,
        ordering = dependency.keyOrdering, serializer = dependency.serializer)
      sorter.insertAllAndUpdateMetrics(input)
    } else if (aggregator.isDefined) {
      aggregator.get.combineValuesByKey(input, context)
    } else input.asInstanceOf[Iterator[Product2[K, C]]]
  }
}
