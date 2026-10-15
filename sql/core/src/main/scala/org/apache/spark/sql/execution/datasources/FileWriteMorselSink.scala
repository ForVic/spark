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

package org.apache.spark.sql.execution.datasources

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec, SparkHadoopWriterUtils}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.scheduler.morsel.MorselSink
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.datasources.FileFormatWriter.ConcurrentOutputWriterSpec
import org.apache.spark.util.Utils

/**
 * Writes the files of a write task such that the task can run as morsels (see
 * [[org.apache.spark.scheduler.morsel.MorselTask]]). Each participant writes its rows to its own
 * files, created through the job's commit protocol under the task's attempt and named uniquely
 * for the participant, and the task commits all of them with a single task commit, so the job
 * sees one task whatever the number of participants. A task that does not run as morsels writes
 * exactly as [[FileFormatWriter.executeTask]].
 */
private[sql] class FileWriteMorselSink(
    description: WriteJobDescription,
    jobTrackerID: String,
    committer: FileCommitProtocol,
    concurrentOutputWriterSpec: Option[ConcurrentOutputWriterSpec])
  extends MorselSink[InternalRow, FileWriteMorselSink.TaskWrite, FileWriteMorselSink.Part,
    WriterCommitMessage] with Serializable {

  import FileWriteMorselSink._

  override def write(records: Iterator[InternalRow], context: TaskContext): WriterCommitMessage =
    FileFormatWriter.executeTask(
      description,
      jobTrackerID,
      context.stageId(),
      context.partitionId(),
      context.taskAttemptId().toInt & Int.MaxValue,
      committer,
      records,
      concurrentOutputWriterSpec)

  override def open(context: TaskContext): TaskWrite = {
    // As in FileFormatWriter.executeTask.
    val jobId = SparkHadoopWriterUtils.createJobID(jobTrackerID, context.stageId())
    val taskId = new TaskID(jobId, TaskType.MAP, context.partitionId())
    val taskAttemptId = new TaskAttemptID(taskId, context.taskAttemptId().toInt & Int.MaxValue)
    val hadoopConf = description.serializableHadoopConf.value
    hadoopConf.set("mapreduce.job.id", jobId.toString)
    hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)
    val taskAttemptContext = new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    committer.setupTask(taskAttemptContext)
    TaskWrite(context, committer, taskAttemptContext)
  }

  override def writePart(
      task: TaskWrite,
      records: Iterator[InternalRow],
      context: TaskContext): Part = {
    val participant = if (context eq task.owner) 0 else task.participants.incrementAndGet()
    val partCommitter = new PartCommitProtocol(task.committer, participant)
    var dataWriter: FileFormatDataWriter = null
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      // As in FileFormatWriter.executeTask: only the task's own first partition writes a file
      // without rows, to save metadata for file formats like parquet.
      dataWriter = if ((participant != 0 || context.partitionId() != 0) && !records.hasNext) {
        new EmptyDirectoryDataWriter(description, task.taskAttemptContext, partCommitter)
      } else if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
        new SingleDirectoryDataWriter(description, task.taskAttemptContext, partCommitter)
      } else {
        concurrentOutputWriterSpec match {
          case Some(spec) =>
            new DynamicPartitionDataConcurrentWriter(
              description, task.taskAttemptContext, partCommitter, spec)
          case _ =>
            new DynamicPartitionDataSingleWriter(
              description, task.taskAttemptContext, partCommitter)
        }
      }
      dataWriter.writeWithIterator(records)
      Part(dataWriter.closeWithoutCommit(), partCommitter.files)
    })(catchBlock = {
      // Removes this participant's files only.
      if (dataWriter != null) {
        dataWriter.abort()
      } else {
        partCommitter.abortTask(task.taskAttemptContext)
      }
    }, finallyBlock = {
      if (dataWriter != null) {
        dataWriter.close()
      }
    })
  }

  override def commit(task: TaskWrite, parts: Seq[Part], context: TaskContext): WriteTaskResult = {
    val (taskCommitMessage, taskCommitTime) = Utils.timeTakenMs {
      task.committer.commitTask(task.taskAttemptContext)
    }
    description.statsTrackers.collect { case tracker: BasicWriteJobStatsTracker => tracker }
      .foreach(_.taskCommitTimeMetric += taskCommitTime)
    val summaries = parts.map(_.summary)
    val stats = description.statsTrackers.indices.map { i =>
      val partStats = summaries.map(_.stats(i).asInstanceOf[BasicWriteTaskStats])
      BasicWriteTaskStats(
        partitions = partStats.flatMap(_.partitions),
        numFiles = partStats.map(_.numFiles).sum,
        numBytes = partStats.map(_.numBytes).sum,
        numRows = partStats.map(_.numRows).sum)
    }
    WriteTaskResult(taskCommitMessage,
      ExecutedWriteSummary(summaries.flatMap(_.updatedPartitions).toSet, stats))
  }

  override def discard(task: TaskWrite, part: Part): Unit =
    deleteFiles(part.files, task.taskAttemptContext)

  override def abort(task: TaskWrite, context: TaskContext): Unit =
    task.committer.abortTask(task.taskAttemptContext)
}

private[sql] object FileWriteMorselSink extends Logging {

  /**
   * Why the tasks of a write may not run as morsels, if they may not.
   *
   * The stats of participants are combined into the task's, which needs to know the stats.
   * Files of aborted participants are deleted, which commit protocols do not expect for files in
   * custom partition locations: they keep track of those to move them when the job commits.
   */
  def unsupportedReason(description: WriteJobDescription): Option[String] = {
    if (description.statsTrackers.exists(_.getClass != classOf[BasicWriteJobStatsTracker])) {
      Some("custom write stats trackers")
    } else if (description.customPartitionLocations.nonEmpty) {
      Some("custom partition locations")
    } else {
      None
    }
  }

  /** A task's write, shared by its participants. */
  case class TaskWrite(
      owner: TaskContext,
      committer: FileCommitProtocol,
      taskAttemptContext: TaskAttemptContext) {
    val participants = new AtomicInteger(0)
  }

  /** The files one participant wrote. */
  case class Part(summary: ExecutedWriteSummary, files: Seq[String])

  private def deleteFiles(files: Seq[String], taskAttemptContext: TaskAttemptContext): Unit =
    files.foreach { file =>
      try {
        val path = new Path(file)
        path.getFileSystem(taskAttemptContext.getConfiguration).delete(path, false)
      } catch {
        case NonFatal(e) => logWarning(s"Failed to delete $file", e)
      }
    }

  /**
   * The commit protocol of one participant of a write task that runs as morsels. It creates the
   * participant's files with the task's commit protocol, which the participants share, under names
   * unique to the participant, and on abort deletes just those files. The task's own thread writes
   * files with the usual names.
   */
  private class PartCommitProtocol(task: FileCommitProtocol, participant: Int)
    extends FileCommitProtocol {

    private val created = new ArrayBuffer[String]

    def files: Seq[String] = created.toSeq

    private def unique(spec: FileNameSpec): FileNameSpec =
      if (participant == 0) spec else spec.copy(suffix = s"-m$participant${spec.suffix}")

    private def created(file: String): String = {
      created += file
      file
    }

    override def newTaskTempFile(
        taskContext: TaskAttemptContext,
        dir: Option[String],
        spec: FileNameSpec): String =
      created(task.synchronized(task.newTaskTempFile(taskContext, dir, unique(spec))))

    override def newTaskTempFileAbsPath(
        taskContext: TaskAttemptContext,
        absoluteDir: String,
        spec: FileNameSpec): String =
      created(task.synchronized(
        task.newTaskTempFileAbsPath(taskContext, absoluteDir, unique(spec))))

    override def abortTask(taskContext: TaskAttemptContext): Unit =
      deleteFiles(files, taskContext)

    override def deleteWithJob(fs: org.apache.hadoop.fs.FileSystem, path: Path,
        recursive: Boolean): Boolean =
      task.synchronized(task.deleteWithJob(fs, path, recursive))

    override def setupJob(jobContext: JobContext): Unit = unsupported()
    override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit =
      unsupported()
    override def abortJob(jobContext: JobContext): Unit = unsupported()
    override def setupTask(taskContext: TaskAttemptContext): Unit = unsupported()
    override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = unsupported()

    private def unsupported(): Nothing = throw new UnsupportedOperationException(
      "The task commits the files of its participants")
  }
}
