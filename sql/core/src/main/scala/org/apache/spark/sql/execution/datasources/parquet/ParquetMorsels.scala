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

package org.apache.spark.sql.execution.datasources.parquet

import java.util.Optional

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.FileMetaData.EncryptionType
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.util.Utils

/**
 * Splits Parquet files into morsels for morsel-driven execution (see
 * [[org.apache.spark.scheduler.morsel.MorselScheduler]]).
 *
 * A morsel is a row group, or for a row group larger than the target size, a range of pages of
 * one of its columns. The rows of consecutive pages of a column partition the row group, and the
 * vectorized reader reads only the pages of every column that overlap those rows, skipping the
 * other rows of those pages, like it does for column index filtering. This needs offset indexes,
 * which Parquet writes by default.
 */
object ParquetMorsels {

  /**
   * Key of a morsel's page range in [[PartitionedFile.otherConstantMetadataColumnValues]]. The
   * file's byte range selects the row group.
   */
  val PAGE_RANGE_KEY = "__spark_morsel_page_range"

  /** Pages `[firstPage, endPage)` of the column at `columnPath` (dot-separated). */
  case class PageRange(columnPath: String, firstPage: Int, endPage: Int)

  def pageRange(file: PartitionedFile): Option[PageRange] =
    file.otherConstantMetadataColumnValues.get(PAGE_RANGE_KEY).map(_.asInstanceOf[PageRange])

  /**
   * Key of the footer that splitting read, so that the reader of each morsel need not read it
   * again. All morsels of a file range share it, so adjacent ones can still be merged.
   */
  val FOOTER_KEY = "__spark_morsel_footer"

  /**
   * Opens a morsel for the vectorized reader with the footer read when splitting, if any: the
   * footer's row groups whose midpoint lies in the morsel's byte range, like Parquet's own range
   * filter.
   */
  def openWithCachedFooter(
      conf: Configuration,
      file: PartitionedFile): Option[OpenedParquetFooter] =
    file.otherConstantMetadataColumnValues.get(FOOTER_KEY).map { cached =>
      val footer = cached.asInstanceOf[ParquetMetadata]
      val end = file.start + file.length
      val rowGroups = footer.getBlocks.asScala.filter { block =>
        val midpoint = block.getStartingPos + block.getCompressedSize / 2
        midpoint >= file.start && midpoint < end
      }
      val inputFile = HadoopInputFile.fromPath(file.toPath, conf)
      new OpenedParquetFooter(new ParquetMetadata(footer.getFileMetaData, rowGroups.asJava),
        inputFile, Optional.of(inputFile.newStream()))
    }

  /**
   * Rows per morsel. Compressed bytes can badly underestimate the work in a row group (e.g.
   * highly compressible data), so row groups are also split by row count, like the ~100k-row
   * morsels of HyPer or DuckDB's 122,880-row row groups.
   */
  val ROWS_PER_MORSEL: Long = 1L << 20

  /**
   * Splits the row groups of a file range into morsels of about `targetSize` bytes, returning
   * each morsel with its size.
   *
   * @param splitRowGroups whether large row groups may be split into page ranges, which only the
   *                       vectorized reader supports.
   */
  def split(
      file: PartitionedFile,
      conf: Configuration,
      targetSize: Long,
      splitRowGroups: Boolean): Seq[(PartitionedFile, Long)] = {
    // Not worth reading the footer of tiny files. If the footer cannot be read, leave the file
    // whole, so that the reader handles the error (e.g. honoring ignoreMissingFiles).
    if (file.length * 64 < targetSize) {
      Seq((file, file.length))
    } else {
      try {
        splitRowGroupsOf(file, conf, targetSize, splitRowGroups)
      } catch {
        case NonFatal(_) => Seq((file, file.length))
      }
    }
  }

  private def splitRowGroupsOf(
      file: PartitionedFile,
      conf: Configuration,
      targetSize: Long,
      splitRowGroups: Boolean): Seq[(PartitionedFile, Long)] = {
    val options = HadoopReadOptions.builder(conf, file.toPath)
      .withRange(file.start, file.start + file.length)
      .build()
    Utils.tryWithResource(
        ParquetFileReader.open(HadoopInputFile.fromPath(file.toPath, conf), options)) { reader =>
      val footer = reader.getFooter
      // Readers of encrypted files decrypt footer metadata lazily, so they don't share it.
      val cachedFooter =
        if (footer.getFileMetaData.getEncryptionType == EncryptionType.UNENCRYPTED) {
          Map(FOOTER_KEY -> footer)
        } else {
          Map.empty
        }
      reader.getRowGroups.asScala.toSeq.flatMap { block =>
        // The row group's own byte range contains its midpoint, so it selects just this group.
        val rowGroup = file.copy(start = block.getStartingPos, length = block.getCompressedSize,
          otherConstantMetadataColumnValues =
            file.otherConstantMetadataColumnValues ++ cachedFooter)
        // Size by uncompressed bytes, which reflect the work better than compressed ones.
        val size = block.getTotalByteSize
        val columns = block.getColumns.asScala.toSeq
        // Page ranges are slower to read than whole row groups (rows outside the range are
        // skipped one page at a time), so only split row groups that are much larger than a
        // morsel.
        val bySize = if (size > 4 * targetSize) math.ceil(size.toDouble / targetSize) else 1.0
        val numMorsels =
          math.max(bySize, math.ceil(block.getRowCount.toDouble / ROWS_PER_MORSEL)).toInt
        if (!splitRowGroups || numMorsels < 2 || block.getRowCount < 2 ||
            columns.exists(_.getOffsetIndexReference == null)) {
          Seq((rowGroup, size))
        } else {
          // Split along the pages of the largest column, into ranges of about equal rows.
          val column = columns.maxBy(_.getTotalSize)
          val offsetIndex = reader.readOffsetIndex(column)
          val rowCount = block.getRowCount
          val boundaries = (0 +: (1 until numMorsels).map { m =>
            val firstRow = rowCount * m / numMorsels
            (0 until offsetIndex.getPageCount)
              .find(p => offsetIndex.getFirstRowIndex(p) >= firstRow)
              .getOrElse(offsetIndex.getPageCount)
          } :+ offsetIndex.getPageCount).distinct.sorted
          if (boundaries.length < 3) {
            Seq((rowGroup, size))
          } else {
            boundaries.sliding(2).map { case Seq(first, end) =>
              val endRow =
                if (end < offsetIndex.getPageCount) offsetIndex.getFirstRowIndex(end) else rowCount
              val rows = endRow - offsetIndex.getFirstRowIndex(first)
              val range = PageRange(column.getPath.toDotString, first, end)
              (rowGroup.copy(otherConstantMetadataColumnValues =
                rowGroup.otherConstantMetadataColumnValues + (PAGE_RANGE_KEY -> range)),
                size * rows / rowCount)
            }.toSeq
          }
        }
      }
    }
  }
}
