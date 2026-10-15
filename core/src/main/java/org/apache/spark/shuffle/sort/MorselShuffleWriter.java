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

package org.apache.spark.shuffle.sort;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.Checksum;

import scala.Product2;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Private;
import org.apache.spark.internal.config.package$;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.shuffle.checksum.RowBasedChecksum;
import org.apache.spark.shuffle.checksum.ShuffleChecksumSupport;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.ExposedBufferByteArrayOutputStream;
import org.apache.spark.util.Utils;

/**
 * Writes the map output of a morsel-executed map task (see
 * {@code org.apache.spark.scheduler.morsel.MorselScheduler}).
 * <p>
 * Every participant of the task (its own thread and any helpers) writes the records it produces
 * with {@link #write}, on its own thread and with its own memory. {@link #commit} then builds the
 * task's single map output by concatenating, for each reduce partition, the data of all
 * participants. This relies on the serializer supporting relocation and the compression codec
 * supporting concatenation of serialized streams, like the fast merge path of
 * {@link UnsafeShuffleWriter}.
 * <p>
 * Participants write the way the regular writer for the shuffle would: one file per reduce
 * partition where {@link BypassMergeSortShuffleWriter} would be used, otherwise sorted spill
 * files as in {@link UnsafeShuffleWriter}. A task that no helper joined therefore does the same
 * work as with the regular writer.
 */
@Private
public final class MorselShuffleWriter<K, V> implements ShuffleChecksumSupport {

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  private final BlockManager blockManager;
  private final ShuffleDependency<K, V, V> dep;
  private final long mapId;
  private final int numPartitions;
  private final Partitioner partitioner;
  private final SparkConf conf;
  private final ShuffleExecutorComponents shuffleExecutorComponents;
  private final boolean bypassMergeSort;
  private final int fileBufferSize;
  private final int initialSortBufferSize;

  MorselShuffleWriter(
      BlockManager blockManager,
      ShuffleDependency<K, V, V> dep,
      long mapId,
      SparkConf conf,
      ShuffleExecutorComponents shuffleExecutorComponents) {
    this.blockManager = blockManager;
    this.dep = dep;
    this.mapId = mapId;
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.conf = conf;
    this.shuffleExecutorComponents = shuffleExecutorComponents;
    this.bypassMergeSort = SortShuffleWriter.shouldBypassMergeSort(conf, dep);
    this.fileBufferSize =
      (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.initialSortBufferSize =
      (int) (long) conf.get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE());
  }

  /**
   * Writes the records of one participant. Called on the participant's thread with its own task
   * context, metrics, and copies of the shuffle dependency's serializer and row-based checksums
   * (serializers may update metrics, which must not be shared between threads).
   */
  public MorselSpills write(
      scala.collection.Iterator<Product2<K, V>> records,
      TaskContext context,
      ShuffleWriteMetricsReporter writeMetrics,
      Serializer serializer,
      RowBasedChecksum[] rowBasedChecksums) throws IOException, SparkException {
    if (!records.hasNext()) {
      return new MorselSpills(
        new ArrayList<>(), createChecksumValues(), rowBasedChecksums, 0L);
    }
    return bypassMergeSort ?
      writePerPartitionFiles(records, writeMetrics, serializer, rowBasedChecksums) :
      writeSorted(records, context, writeMetrics, serializer, rowBasedChecksums);
  }

  private long[] createChecksumValues() {
    return getChecksumValues(createPartitionChecksums(numPartitions, conf));
  }

  /** Writes each reduce partition to its own file, like BypassMergeSortShuffleWriter. */
  private MorselSpills writePerPartitionFiles(
      scala.collection.Iterator<Product2<K, V>> records,
      ShuffleWriteMetricsReporter writeMetrics,
      Serializer serializer,
      RowBasedChecksum[] rowBasedChecksums) throws IOException {
    final SerializerInstance serInstance = serializer.newInstance();
    final Checksum[] partitionChecksums = createPartitionChecksums(numPartitions, conf);
    final DiskBlockObjectWriter[] writers = new DiskBlockObjectWriter[numPartitions];
    final File[] files = new File[numPartitions];
    boolean success = false;
    try {
      final long openStartTime = System.nanoTime();
      for (int i = 0; i < numPartitions; i++) {
        final Tuple2<TempShuffleBlockId, File> blockIdAndFile =
          blockManager.diskBlockManager().createTempShuffleBlock();
        files[i] = blockIdAndFile._2();
        writers[i] = blockManager.getDiskWriter(
          blockIdAndFile._1(), files[i], serInstance, fileBufferSize, writeMetrics);
        if (partitionChecksums.length > 0) {
          writers[i].setChecksum(partitionChecksums[i]);
        }
      }
      writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

      while (records.hasNext()) {
        final Product2<K, V> record = records.next();
        final K key = record._1();
        final int partitionId = partitioner.getPartition(key);
        writers[partitionId].write(key, record._2());
        if (rowBasedChecksums.length > 0) {
          rowBasedChecksums[partitionId].update(key, record._2());
        }
      }

      final List<MorselSpills.PartitionedFile> result = new ArrayList<>(numPartitions);
      for (int i = 0; i < numPartitions; i++) {
        final FileSegment segment;
        try (DiskBlockObjectWriter writer = writers[i]) {
          segment = writer.commitAndGet();
        }
        writers[i] = null;
        result.add(new MorselSpills.PartitionedFile(files[i], i, new long[] {segment.length()}));
      }
      success = true;
      return new MorselSpills(
        result, getChecksumValues(partitionChecksums), rowBasedChecksums, 0L);
    } finally {
      if (!success) {
        for (int i = 0; i < numPartitions; i++) {
          if (writers[i] != null) {
            writers[i].closeAndDelete();
          } else if (files[i] != null && files[i].exists()) {
            files[i].delete();
          }
        }
      }
    }
  }

  /** Sorts records by partition into spill files, like UnsafeShuffleWriter. */
  private MorselSpills writeSorted(
      scala.collection.Iterator<Product2<K, V>> records,
      TaskContext context,
      ShuffleWriteMetricsReporter writeMetrics,
      Serializer serializer,
      RowBasedChecksum[] rowBasedChecksums) throws IOException, SparkException {
    final ShuffleExternalSorter sorter = new ShuffleExternalSorter(
      context.taskMemoryManager(), blockManager, context, initialSortBufferSize, numPartitions,
      conf, writeMetrics);
    sorter.setOutputIsMergeInput();
    boolean success = false;
    try {
      final ExposedBufferByteArrayOutputStream serBuffer =
        new ExposedBufferByteArrayOutputStream(UnsafeShuffleWriter.DEFAULT_INITIAL_SER_BUFFER_SIZE);
      final SerializationStream serOutputStream =
        serializer.newInstance().serializeStream(serBuffer);
      while (records.hasNext()) {
        final Product2<K, V> record = records.next();
        final K key = record._1();
        final int partitionId = partitioner.getPartition(key);
        serBuffer.reset();
        serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
        serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
        serOutputStream.flush();
        sorter.insertRecord(
          serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serBuffer.size(), partitionId);
        if (rowBasedChecksums.length > 0) {
          rowBasedChecksums[partitionId].update(key, record._2());
        }
      }
      final long peakMemoryUsedBytes = sorter.getPeakMemoryUsedBytes();
      final SpillInfo[] spills = sorter.closeAndGetSpills();
      final List<MorselSpills.PartitionedFile> result = new ArrayList<>(spills.length);
      long bytes = 0L;
      for (SpillInfo spill : spills) {
        result.add(new MorselSpills.PartitionedFile(spill.file, 0, spill.partitionLengths));
        bytes += spill.file.length();
      }
      // The files become part of the map output as they are.
      writeMetrics.incBytesWritten(bytes);
      success = true;
      return new MorselSpills(result, sorter.getChecksums(), rowBasedChecksums,
        peakMemoryUsedBytes);
    } finally {
      if (!success) {
        sorter.cleanupResources();
      }
    }
  }

  /**
   * Builds the map output from the files of all participants, the task's own thread first, and
   * deletes them. Bytes were already counted as written by {@link #write}.
   *
   * @param rowBasedChecksums the task's own row-based checksums; the other participants' are
   *                          merged into them.
   * @param outputVariesWithSplit whether the output rows depend on how the input was split among
   *                              participants (e.g. one partial aggregate row per key per
   *                              participant). The map status then carries no row-based checksum,
   *                              which would otherwise differ between attempts that split the input
   *                              differently and trigger needless retries of consumer stages.
   */
  public MapStatus commit(
      MorselSpills[] parts,
      ShuffleWriteMetricsReporter writeMetrics,
      RowBasedChecksum[] rowBasedChecksums,
      boolean outputVariesWithSplit) throws IOException {
    final long[] checksums = MorselSpills.combinedChecksums(parts, numPartitions,
      conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ALGORITHM()));
    for (MorselSpills part : parts) {
      if (part.rowBasedChecksums != rowBasedChecksums) {
        for (int i = 0; i < rowBasedChecksums.length; i++) {
          rowBasedChecksums[i].merge(part.rowBasedChecksums[i]);
        }
      }
    }
    final long[] partitionLengths;
    try {
      final MorselSpills.PartitionedFile single = singleFileWithAllPartitions(parts);
      final Optional<SingleSpillShuffleMapOutputWriter> singleFileWriter = single == null ?
        Optional.empty() :
        shuffleExecutorComponents.createSingleFileMapOutputWriter(dep.shuffleId(), mapId);
      if (singleFileWriter.isPresent()) {
        singleFileWriter.get().transferMapSpillFile(single.file, single.lengths, checksums);
        partitionLengths = single.lengths;
      } else {
        final ShuffleMapOutputWriter mapWriter = shuffleExecutorComponents
          .createMapOutputWriter(dep.shuffleId(), mapId, numPartitions);
        try {
          concatenate(parts, mapWriter, writeMetrics);
          partitionLengths = mapWriter.commitAllPartitions(checksums).getPartitionLengths();
        } catch (Exception e) {
          try {
            mapWriter.abort(e);
          } catch (Exception e2) {
            e.addSuppressed(e2);
          }
          throw e;
        }
      }
    } finally {
      for (MorselSpills part : parts) {
        part.delete();
      }
    }
    return MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths, mapId,
      outputVariesWithSplit ? 0L : RowBasedChecksum.getAggregatedChecksumValue(rowBasedChecksums));
  }

  /** The only file of all parts if it holds every partition, else null. */
  private MorselSpills.PartitionedFile singleFileWithAllPartitions(MorselSpills[] parts) {
    MorselSpills.PartitionedFile single = null;
    for (MorselSpills part : parts) {
      for (MorselSpills.PartitionedFile f : part.files) {
        if (single != null) return null;
        single = f;
      }
    }
    return single != null && single.firstPartition == 0 &&
      single.lengths.length == numPartitions ? single : null;
  }

  private void concatenate(
      MorselSpills[] parts,
      ShuffleMapOutputWriter mapWriter,
      ShuffleWriteMetricsReporter writeMetrics) throws IOException {
    final long writeStartTime = System.nanoTime();
    boolean threwException = true;
    try {
      for (int p = 0; p < numPartitions; p++) {
        final ShufflePartitionWriter writer = mapWriter.getPartitionWriter(p);
        boolean copyThrewException = true;
        final WritableByteChannelWrapper out = writer.openChannelWrapper()
          .orElseGet(() -> new StreamChannelWrapper(openStreamUnchecked(writer)));
        try {
          for (MorselSpills part : parts) {
            for (MorselSpills.PartitionedFile f : part.files) {
              final long length = f.length(p);
              if (length > 0) {
                Utils.copyFileStreamNIO(f.channel(), out.channel(), f.advance(length), length);
              }
              if (f.isLastPartition(p)) {
                f.closeChannel(false);
              }
            }
          }
          copyThrewException = false;
        } finally {
          com.google.common.io.Closeables.close(out, copyThrewException);
        }
      }
      threwException = false;
    } finally {
      for (MorselSpills part : parts) {
        for (MorselSpills.PartitionedFile f : part.files) {
          f.closeChannel(threwException);
        }
      }
      writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
    }
  }

  private static OutputStream openStreamUnchecked(ShufflePartitionWriter writer) {
    try {
      return writer.openStream();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final class StreamChannelWrapper implements WritableByteChannelWrapper {
    private final WritableByteChannel channel;

    StreamChannelWrapper(OutputStream stream) {
      this.channel = Channels.newChannel(stream);
    }

    @Override
    public WritableByteChannel channel() {
      return channel;
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }
}
