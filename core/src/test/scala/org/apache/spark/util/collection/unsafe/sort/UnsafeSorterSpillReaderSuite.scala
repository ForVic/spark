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

package org.apache.spark.util.collection.unsafe.sort

import java.io.{DataOutputStream, File, FileOutputStream, FilterInputStream, InputStream, IOException}
import java.lang.ref.WeakReference
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{SparkConf, SparkFunSuite, TaskContext}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.storage.{BlockId, TempLocalBlockId}

class UnsafeSorterSpillReaderSuite extends SparkFunSuite {
  private def writeSpill(file: File): Unit = {
    val out = new DataOutputStream(new FileOutputStream(file))
    try {
      out.writeInt(1)
      out.writeInt(3)
      out.writeLong(17L)
      out.write(Array[Byte](7, 8, 9))
    } finally {
      out.close()
    }
  }

  private def manager(closes: AtomicInteger, failClose: Boolean): SerializerManager = {
    val conf = new SparkConf(false)
    new SerializerManager(new JavaSerializer(conf), conf) {
      override def wrapStream(blockId: BlockId, stream: InputStream): InputStream = {
        new FilterInputStream(stream) {
          override def close(): Unit = {
            closes.incrementAndGet()
            super.close()
            if (failClose) throw new IOException("test close failure")
          }
        }
      }
    }
  }

  private def readLastRecord(
      file: File,
      serializer: SerializerManager,
      failClose: Boolean): Seq[WeakReference[AnyRef]] = {
    val reader = new UnsafeSorterSpillReader(serializer, file, TempLocalBlockId(UUID.randomUUID()))
    if (failClose) {
      intercept[IOException](reader.loadNext())
    } else {
      reader.loadNext()
    }
    // loadNext closes the stream at EOF, but the caller still needs the last record's bytes.
    assert(!reader.hasNext)
    assert(reader.getKeyPrefix == 17L)
    assert(reader.getRecordLength == 3)
    assert(reader.getBaseObject.asInstanceOf[Array[Byte]].take(3).toSeq === Seq[Byte](7, 8, 9))
    Seq(new WeakReference[AnyRef](reader), new WeakReference[AnyRef](reader.getBaseObject))
  }

  test("closed spill readers release task ownership while preserving their last record") {
    withTempDir { dir =>
      val file = new File(dir, "spill")
      writeSpill(file)
      Seq(false, true).foreach { failClose =>
        val context = TaskContext.empty()
        val closes = new AtomicInteger()
        val refs = TaskContext.withTaskContext(context) {
          readLastRecord(file, manager(closes, failClose), failClose)
        }
        for (_ <- 1 to 100 if refs.exists(!_.refersTo(null))) {
          System.gc()
          if (refs.exists(!_.refersTo(null))) Thread.sleep(10)
        }
        assert(refs.forall(_.refersTo(null)))
        assert(!context.isCompleted())
        context.markTaskCompleted(None)
        assert(closes.get() == 1)
      }
    }
  }

  test("task completion still closes an abandoned spill reader exactly once") {
    withTempDir { dir =>
      val file = new File(dir, "spill")
      writeSpill(file)
      val context = TaskContext.empty()
      val closes = new AtomicInteger()
      val reader = TaskContext.withTaskContext(context) {
        new UnsafeSorterSpillReader(
          manager(closes, false), file, TempLocalBlockId(UUID.randomUUID()))
      }
      assert(closes.get() == 0)
      context.markTaskCompleted(None)
      assert(closes.get() == 1)
      reader.close()
      assert(closes.get() == 1)
    }
  }
}
