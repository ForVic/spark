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
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;

import com.google.common.io.Closeables;

import org.apache.spark.annotation.Private;
import org.apache.spark.internal.LogKeys;
import org.apache.spark.internal.MDC;
import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper;
import org.apache.spark.shuffle.checksum.RowBasedChecksum;

/**
 * The shuffle files one participant of a morsel-executed map task wrote, see
 * {@link MorselShuffleWriter}.
 */
@Private
public final class MorselSpills {

  private static final SparkLogger logger = SparkLoggerFactory.getLogger(MorselSpills.class);

  /**
   * A file holding the data of consecutive reduce partitions, starting at {@code firstPartition},
   * in partition order. Each partition's data is a separately compressed stream.
   */
  static final class PartitionedFile {
    final File file;
    final int firstPartition;
    final long[] lengths;
    // Read state while the file is being merged.
    private FileChannel channel;
    private long position;

    PartitionedFile(File file, int firstPartition, long[] lengths) {
      this.file = file;
      this.firstPartition = firstPartition;
      this.lengths = lengths;
    }

    long length(int partition) {
      int i = partition - firstPartition;
      return i >= 0 && i < lengths.length ? lengths[i] : 0L;
    }

    boolean isLastPartition(int partition) {
      return partition == firstPartition + lengths.length - 1;
    }

    FileChannel channel() throws IOException {
      if (channel == null) {
        channel = new FileInputStream(file).getChannel();
      }
      return channel;
    }

    long advance(long length) {
      long start = position;
      position += length;
      return start;
    }

    void closeChannel(boolean swallowIOException) throws IOException {
      if (channel != null) {
        Closeables.close(channel, swallowIOException);
        channel = null;
      }
    }
  }

  final List<PartitionedFile> files;
  // Per-partition checksums of the bytes in `files`; empty if shuffle checksums are disabled.
  final long[] checksums;
  // Order-independent checksums of the records this participant wrote, one per partition.
  final RowBasedChecksum[] rowBasedChecksums;
  private final long peakMemoryUsedBytes;

  MorselSpills(
      List<PartitionedFile> files,
      long[] checksums,
      RowBasedChecksum[] rowBasedChecksums,
      long peakMemoryUsedBytes) {
    this.files = files;
    this.checksums = checksums;
    this.rowBasedChecksums = rowBasedChecksums;
    this.peakMemoryUsedBytes = peakMemoryUsedBytes;
  }

  public long peakMemoryUsedBytes() {
    return peakMemoryUsedBytes;
  }

  public void delete() {
    for (PartitionedFile f : files) {
      try {
        f.closeChannel(true);
      } catch (IOException e) {
        // Swallowed by closeChannel.
      }
      if (f.file.exists() && !f.file.delete()) {
        logger.error("Error while deleting shuffle file {}",
          MDC.of(LogKeys.PATH, f.file.getPath()));
      }
    }
  }

  /**
   * Each partition of the merged output is the concatenation of that partition's bytes in every
   * part, in order. Combine the parts' checksums into checksums of the concatenation. Only
   * ADLER32 is combined; for other algorithms no checksums are written for the map output.
   */
  static long[] combinedChecksums(MorselSpills[] parts, int numPartitions, String algorithm) {
    for (MorselSpills part : parts) {
      if (part.checksums.length != numPartitions) {
        return ShuffleChecksumHelper.EMPTY_CHECKSUM_VALUE;
      }
    }
    if (parts.length == 1) {
      return parts[0].checksums;
    }
    if (!"ADLER32".equals(algorithm)) {
      return ShuffleChecksumHelper.EMPTY_CHECKSUM_VALUE;
    }
    long[] result = new long[numPartitions];
    for (int p = 0; p < numPartitions; p++) {
      long checksum = 1L; // Adler-32 of the empty sequence
      for (MorselSpills part : parts) {
        long length = 0L;
        for (PartitionedFile f : part.files) {
          length += f.length(p);
        }
        checksum = adler32Combine(checksum, part.checksums[p], length);
      }
      result[p] = checksum;
    }
    return result;
  }

  private static final long ADLER_BASE = 65521L;

  /** Adler-32 of the concatenation A + B, given adler(A), adler(B) and len(B) (as in zlib). */
  static long adler32Combine(long adler1, long adler2, long len2) {
    long rem = len2 % ADLER_BASE;
    long sum1 = adler1 & 0xffffL;
    long sum2 = (rem * sum1) % ADLER_BASE;
    sum1 += (adler2 & 0xffffL) + ADLER_BASE - 1;
    sum2 += ((adler1 >> 16) & 0xffffL) + ((adler2 >> 16) & 0xffffL) + ADLER_BASE - rem;
    if (sum1 >= ADLER_BASE) sum1 -= ADLER_BASE;
    if (sum1 >= ADLER_BASE) sum1 -= ADLER_BASE;
    if (sum2 >= (ADLER_BASE << 1)) sum2 -= (ADLER_BASE << 1);
    if (sum2 >= ADLER_BASE) sum2 -= ADLER_BASE;
    return sum1 | (sum2 << 16);
  }
}
