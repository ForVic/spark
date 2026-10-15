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

package org.apache.spark.scheduler.morsel

import java.lang.management.ManagementFactory
import java.util.concurrent.{Executors, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.{Partition, SparkConf, SparkEnv, SparkException, TaskContext, TaskContextImpl, TaskKilledException}
import org.apache.spark.executor.{Executor, TaskMetrics}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.memory.{SparkOutOfMemoryError, TaskMemoryManager}
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{AccumulatorV2, UninterruptibleThread}

/**
 * A piece of a task's input that can be processed independently of the rest of the input.
 *
 * @param payload source-specific description of the input, e.g. a file range.
 * @param sizeInBytes approximate input size, used to decide which task helpers should work on.
 */
private[spark] case class Morsel(payload: AnyRef, sizeInBytes: Long)

/**
 * An RDD whose partitions can be split into morsels. Computing a [[MorselPartition]] must
 * produce exactly the records of the morsels its iterator returns.
 */
private[spark] trait MorselSource {
  private[spark] def splitIntoMorsels(split: Partition, targetSize: Long): IndexedSeq[Morsel]

  /**
   * Splits a partition that is zipped with partitions of other sources, e.g. one side of a join,
   * into morsels whose payload identifies matching data in every aligned source (e.g. the same
   * reducer partition of two shuffles). Includes empty morsels. None if the partition cannot be
   * split this way.
   */
  private[spark] def splitIntoAlignedMorsels(split: Partition): Option[IndexedSeq[Morsel]] = None

  /**
   * Combines two morsels that are adjacent in the input into one that is cheaper to process
   * than both (e.g. consecutive byte ranges of a file, read with one open), if possible.
   */
  private[spark] def mergeMorsels(first: AnyRef, next: AnyRef): Option[AnyRef] = None

  /**
   * Splits a partition that is joined with partitions of other sources into morsels that each
   * join with the other sources' whole partitions, e.g. ranges of map outputs of a shuffle
   * partition, like AQE's skew join splitting. None if the partition cannot be split this way.
   */
  private[spark] def splitIntoOneSidedMorsels(
      split: Partition,
      targetSize: Long): Option[IndexedSeq[Morsel]] = None

  /** The approximate size of a partition's input, if known. */
  private[spark] def partitionSize(split: Partition): Option[Long] = None

  /** Identifies this source in [[MorselJoinSplit]]s, e.g. by its shuffle id. */
  private[spark] def joinSideId: Option[Int] = None
}

/**
 * Which sides of a join of two sources (identified by [[MorselSource.joinSideId]]) the planner
 * allows to split into morsels one-sidedly: each morsel holds part of one side's rows and joins
 * with all of the other side's, which every participant reads. That is valid for the sides AQE's
 * skew join handling may split (e.g. the left side of a left outer join), if nothing after the
 * join in the stage needs all rows of a key together.
 */
private[spark] case class MorselJoinSplit(
    leftSideId: Int,
    rightSideId: Int,
    splitLeft: Boolean,
    splitRight: Boolean)

/**
 * The input of a participant whose pipeline reads several aligned sources, e.g. both sides of a
 * join: the source that is read first claims morsels from the task's queue, and the others replay
 * the same morsels. This requires the first source to be drained before any other is read (e.g.
 * by a sort or a hash join build), which the planner guarantees; reading another source earlier
 * fails the task rather than producing wrong results.
 */
private[spark] class SharedMorselInput(claims: Iterator[AnyRef]) {
  private val claimed = new ArrayBuffer[AnyRef]
  private var claimerStarted = false
  private var claimerDone = false

  def sourceIterator(): Iterator[AnyRef] = new Iterator[AnyRef] {
    private var delegate: Iterator[AnyRef] = null

    private def input: Iterator[AnyRef] = {
      if (delegate == null) {
        delegate = if (!claimerStarted) {
          claimerStarted = true
          new Iterator[AnyRef] {
            override def hasNext: Boolean = {
              val more = claims.hasNext
              if (!more) claimerDone = true
              more
            }
            override def next(): AnyRef = {
              val morsel = claims.next()
              claimed += morsel
              morsel
            }
          }
        } else if (claimerDone) {
          claimed.iterator
        } else {
          throw new IllegalStateException(
            "A morsel source was read before the first source of its pipeline was drained")
        }
      }
      delegate
    }

    override def hasNext: Boolean = input.hasNext
    override def next(): AnyRef = input.next()
  }
}

/**
 * The input of one participant of a morsel-executed task: the morsels of `parent` that this
 * participant claims, one at a time, from the task's shared queue.
 *
 * @param zippedSide if the morsels split only one side of a zipped partition (see
 *                   [[MorselJoinSplit]]), that side; every participant reads the other side whole.
 */
private[spark] class MorselPartition(
    val parent: Partition,
    @transient val morsels: Iterator[AnyRef],
    val zippedSide: Option[Int] = None) extends Partition {
  override def index: Int = parent.index
}

/**
 * The context of a thread working on a task it does not own: a helper, or the task's own thread
 * running morsels of helpers that were aborted. It identifies as the owning task attempt and
 * shares its kill and fetch-failure state, but has its own memory manager, metrics and completion
 * listeners, so the participant's operators are isolated from the owner's.
 *
 * @param isAborted whether the participant was told to stop, see [[MorselJob.reclaimMemory]].
 */
private[spark] class MorselTaskContext(
    val owner: TaskContextImpl,
    taskMemoryManager: TaskMemoryManager,
    taskMetrics: TaskMetrics,
    isAborted: () => Boolean)
  extends TaskContextImpl(
    owner.stageId,
    owner.stageAttemptNumber,
    owner.partitionId,
    owner.taskAttemptId,
    owner.attemptNumber,
    owner.numPartitions,
    taskMemoryManager,
    owner.getLocalProperties,
    SparkEnv.get.metricsSystem,
    taskMetrics,
    owner.cpus,
    owner.resources) {

  override def isInterrupted(): Boolean = owner.isInterrupted() || isAborted()

  private[spark] override def killTaskIfInterrupted(): Unit = {
    owner.killTaskIfInterrupted()
    if (isAborted()) throw new TaskKilledException(MorselJob.ABORT_REASON)
  }

  private[spark] override def getKillReason(): Option[String] =
    owner.getKillReason().orElse(if (isAborted()) Some(MorselJob.ABORT_REASON) else None)

  private[spark] override def markInterrupted(reason: String): Unit = owner.markInterrupted(reason)

  private[spark] override def setFetchFailed(fetchFailed: FetchFailedException): Unit =
    owner.setFetchFailed(fetchFailed)

  private[spark] override def fetchFailed: Option[FetchFailedException] = owner.fetchFailed
}

/**
 * The morsels of one task attempt and the bookkeeping of the threads working on them.
 *
 * The owner (the task's own thread) takes morsels from the front of the queue; helpers steal
 * from the back. Every participant runs its own instance of the task's pipeline over the morsels
 * it claims, with its own operator state (e.g. partial aggregation and shuffle sort buffers), and
 * hands its part of the output to the owner, which combines all parts into the task's output. The
 * queue only ever holds this task attempt's input, so the task's output is exactly that of its
 * partition, whichever threads produced it.
 *
 * Helpers' memory counts towards the task's share of execution memory (see
 * [[TaskMemoryManager]]), so helping does not take memory from other tasks. Helpers give way to
 * the owner when the task runs short of memory: a helper that runs out of memory, and every helper
 * when the owner runs out (see [[reclaimMemory]]), is aborted. An aborted helper's part is
 * discarded and its morsels go back to the queue, so the owner processes them, like it would have
 * without helpers. Helpers therefore never wait for memory that the owner holds.
 *
 * @tparam P the part of the output written by one participant.
 */
private[spark] abstract class MorselJob[P](
    val owner: TaskContextImpl,
    morsels: IndexedSeq[Morsel],
    merge: (AnyRef, AnyRef) => Option[AnyRef] = (_, _) => None) extends Logging {

  private val queue = new java.util.ArrayDeque[Morsel](morsels.asJava)
  private var remainingBytes = morsels.iterator.map(_.sizeInBytes).sum
  // For the rate at which the task's own thread processes morsels: the bytes of the morsels it
  // finished between its second take and its last, and the time between those takes. The first
  // morsel is left out, as it also pays for setting up the pipeline.
  private var ownerTakes = 0
  private var rateStartNs = 0L
  private var lastTakeNs = 0L
  private var processedBytes = 0L
  private var lastTakenBytes = 0L
  // When closed, no new helpers may join.
  private var closed = false
  // Set when the task ran short of memory: no new helpers may join.
  private var memoryBound = false
  // Set when the owner needs the helpers' memory: active helpers stop and give their morsels back.
  @volatile private var helpersAborted = false
  private var activeHelpers = 0
  @volatile private var failure: Throwable = null
  private val results = new ArrayBuffer[(P, MorselTaskContext)]
  private val helperThreads = new java.util.HashSet[Thread]

  // Thread state of the owner that helpers need to deserialize and run the task.
  private val classLoader = Thread.currentThread().getContextClassLoader
  private val deserializationProps = Executor.taskDeserializationProps.get()

  // Helpers on the task's spare CPUs (see MorselScheduler), guarded by the scheduler's lock.
  private[morsel] var spareSlotHelpers = 0
  private[morsel] var slotsLent = false

  val numMorsels: Int = morsels.length
  private var _morselsStolen = 0
  private var _helperRuns = 0
  private var _helpersAborted = 0
  private var _helperCpuTimeNs = 0L

  /** Runs this task's pipeline over `morsels` on a participant's thread, writing its part. */
  protected def runPart(context: MorselTaskContext, morsels: Iterator[AnyRef]): P

  /** Discards a part that will not be combined into the task output. */
  protected def discardPart(part: P): Unit

  /** The owner's input: morsels taken from the front of the queue. */
  def ownerMorsels: Iterator[AnyRef] = new MorselIterator(null, fromFront = true, () => false, null)

  def fail(t: Throwable): Unit = synchronized {
    if (failure == null) failure = t
    closed = true
    notifyAll()
  }

  def failureOption: Option[Throwable] = Option(failure)

  /**
   * Stops new helpers from joining and waits until the active ones have finished. The wait ignores
   * thread interrupts, since returning early would release the task's resources while helpers
   * still use them. Helpers of a killed task are interrupted, as a helper blocked in I/O might not
   * check for the kill.
   */
  def closeAndAwaitHelpers(): Unit = synchronized {
    closed = true
    awaitHelpers()
  }

  private def awaitHelpers(): Unit = {
    var interrupted = false
    var helpersInterrupted = false
    while (activeHelpers > 0) {
      if (!helpersInterrupted && owner.isInterrupted()) {
        helperThreads.forEach(_.interrupt())
        helpersInterrupted = true
      }
      try {
        wait(100)
      } catch {
        case _: InterruptedException => interrupted = true
      }
    }
    if (interrupted) Thread.currentThread().interrupt()
  }

  /**
   * Frees the memory that helpers hold for the task, for the owner, which ran short of memory even
   * after spilling: aborts the active helpers, which give their morsels back, and waits for them
   * to release their memory. No helper joins afterwards. Returns whether any helper was aborted.
   * Called on the owner's thread, see [[TaskMemoryManager.setMemoryReclaimer]].
   */
  def reclaimMemory(): Boolean = synchronized {
    memoryBound = true
    if (activeHelpers == 0) {
      false
    } else {
      logInfo(s"Aborting $activeHelpers morsel helpers of task ${owner.taskAttemptId} to free " +
        "memory for the task's own thread")
      helpersAborted = true
      // Wake helpers blocked waiting, e.g. for memory from other tasks.
      helperThreads.forEach(_.interrupt())
      awaitHelpers()
      true
    }
  }

  /** Called for a helper that claimed a morsel but will never run (the executor is stopping). */
  private[morsel] def abandonHelper(): Unit = synchronized {
    fail(new SparkException("A morsel helper did not run because the executor is stopping"))
    activeHelpers -= 1
    notifyAll()
  }

  /**
   * Processes the morsels that aborted helpers gave back after the owner's own pipeline finished,
   * on the owner's thread. Must be called after [[closeAndAwaitHelpers]]; its part is one of the
   * [[helperResults]]. A failure fails the task.
   */
  def runRemainingMorsels(): Unit = {
    val remaining = synchronized(failure == null && !queue.isEmpty)
    if (remaining) {
      runParticipant(null, () => false, helper = false)
    }
  }

  /**
   * Parts of the participants other than the owner's pipeline that completed successfully. Only
   * valid after [[closeAndAwaitHelpers]].
   */
  def helperResults: Seq[P] = synchronized { results.map(_._1).toSeq }

  def morselsStolen: Int = synchronized(_morselsStolen)
  def helperRuns: Int = synchronized(_helperRuns)
  def helperAborts: Int = synchronized(_helpersAborted)
  def helperCpuTimeNs: Long = synchronized(_helperCpuTimeNs)

  /**
   * Adds the helpers' task metrics and accumulator updates (e.g. SQL metrics) to the owner's.
   * Must run on the owner's thread after the owner's pipeline and all helpers have finished.
   */
  def mergeHelperMetrics(): Unit = {
    val ownerMetrics = owner.taskMetrics
    val ownerAccums = ownerMetrics.withExternalAccums(_.map(a => a.id -> a).toMap)
    synchronized(results.toSeq).foreach { case (_, context) =>
      val m = context.taskMetrics
      ownerMetrics.inputMetrics.incBytesRead(m.inputMetrics.bytesRead)
      ownerMetrics.inputMetrics.incRecordsRead(m.inputMetrics.recordsRead)
      ownerMetrics.outputMetrics.setBytesWritten(
        ownerMetrics.outputMetrics.bytesWritten + m.outputMetrics.bytesWritten)
      ownerMetrics.outputMetrics.setRecordsWritten(
        ownerMetrics.outputMetrics.recordsWritten + m.outputMetrics.recordsWritten)
      ownerMetrics.shuffleWriteMetrics.incBytesWritten(m.shuffleWriteMetrics.bytesWritten)
      ownerMetrics.shuffleWriteMetrics.incRecordsWritten(m.shuffleWriteMetrics.recordsWritten)
      ownerMetrics.shuffleWriteMetrics.incWriteTime(m.shuffleWriteMetrics.writeTime)
      ownerMetrics.incMemoryBytesSpilled(m.memoryBytesSpilled)
      ownerMetrics.incDiskBytesSpilled(m.diskBytesSpilled)
      ownerMetrics.incPeakExecutionMemory(m.peakExecutionMemory)
      m.mergeShuffleReadMetrics()
      val read = m.shuffleReadMetrics
      if (read.recordsRead > 0 || read.totalBlocksFetched > 0) {
        val ownerRead = ownerMetrics.createTempShuffleReadMetrics()
        ownerRead.incRemoteBlocksFetched(read.remoteBlocksFetched)
        ownerRead.incLocalBlocksFetched(read.localBlocksFetched)
        ownerRead.incRemoteBytesRead(read.remoteBytesRead)
        ownerRead.incRemoteBytesReadToDisk(read.remoteBytesReadToDisk)
        ownerRead.incLocalBytesRead(read.localBytesRead)
        ownerRead.incFetchWaitTime(read.fetchWaitTime)
        ownerRead.incRecordsRead(read.recordsRead)
        ownerRead.incRemoteReqsDuration(read.remoteReqsDuration)
        ownerMetrics.mergeShuffleReadMetrics()
      }
      m.withExternalAccums(_.foreach { acc =>
        ownerAccums.get(acc.id).foreach { ownerAcc =>
          ownerAcc.asInstanceOf[AccumulatorV2[Any, Any]]
            .merge(acc.asInstanceOf[AccumulatorV2[Any, Any]])
        }
      })
    }
  }

  /**
   * How much a helper joining now would help: the share of the remaining input it would get, if
   * that is worth starting a helper for (deserializing the task and setting up its pipeline):
   * at least `minBytes`, or at least `minNanos` of work at the rate at which the task's own thread
   * has processed its morsels so far. 0 if not.
   */
  private[morsel] def stealPriority(minBytes: Long, minNanos: Long): Double = synchronized {
    if (closed || memoryBound || queue.isEmpty) {
      0.0
    } else {
      val share = remainingBytes.toDouble / (2 + activeHelpers)
      val shareNanos =
        if (processedBytes > 0) share * (lastTakeNs - rateStartNs) / processedBytes else 0.0
      if (share >= minBytes || shareNanos >= minNanos) share else 0.0
    }
  }

  /** Joins as a helper, claiming the first morsel to process; null if there is nothing left. */
  private[morsel] def tryJoin(): Morsel = synchronized {
    if (closed || memoryBound) {
      null
    } else {
      val morsel = queue.pollLast()
      if (morsel != null) {
        remainingBytes -= morsel.sizeInBytes
        _morselsStolen += 1
        activeHelpers += 1
      }
      morsel
    }
  }

  // After a failure every participant's input ends at its next morsel boundary; the owner rethrows
  // the failure once all participants have stopped.
  private def take(fromFront: Boolean): Morsel = synchronized {
    var morsel = if (failure != null) null
      else if (fromFront) queue.pollFirst()
      else queue.pollLast()
    if (morsel != null) {
      remainingBytes -= morsel.sizeInBytes
      if (!fromFront) _morselsStolen += 1
    }
    // While no helper competes for this task's morsels, the owner takes up to
    // MAX_OWNER_MERGE adjacent morsels at once, as one larger morsel when they can be merged
    // (e.g. byte ranges of one file, saving reopening the file). Helpers always take one.
    var merged = 1
    while (fromFront && morsel != null && activeHelpers == 0 &&
        merged < MorselJob.MAX_OWNER_MERGE && !queue.isEmpty) {
      val next = queue.peekFirst()
      merge(morsel.payload, next.payload) match {
        case Some(payload) =>
          queue.pollFirst()
          remainingBytes -= next.sizeInBytes
          morsel = Morsel(payload, morsel.sizeInBytes + next.sizeInBytes)
          merged += 1
        case None =>
          merged = MorselJob.MAX_OWNER_MERGE
      }
    }
    if (morsel != null && fromFront) {
      val now = System.nanoTime()
      ownerTakes += 1
      if (ownerTakes == 2) {
        rateStartNs = now
      } else if (ownerTakes > 2) {
        processedBytes += lastTakenBytes
        lastTakeNs = now
      }
      lastTakenBytes = morsel.sizeInBytes
    }
    morsel
  }

  /** Puts the morsels of an aborted helper back at the front of the queue. */
  private def giveBack(claimed: Seq[Morsel]): Unit = synchronized {
    claimed.reverseIterator.foreach { morsel =>
      queue.addFirst(morsel)
      remainingBytes += morsel.sizeInBytes
    }
  }

  /**
   * Runs the pipeline on the calling helper thread, starting with `first` and then stealing
   * more morsels until the queue is empty or `shouldYield` returns true.
   */
  private[morsel] def runAsHelper(first: Morsel, shouldYield: () => Boolean): Unit =
    runParticipant(first, shouldYield, helper = true)

  private def runParticipant(first: Morsel, shouldYield: () => Boolean, helper: Boolean): Unit = {
    val thread = Thread.currentThread()
    val previousClassLoader = thread.getContextClassLoader
    val previousContext = TaskContext.get()
    val threadMXBean = ManagementFactory.getThreadMXBean
    val cpuStart = threadMXBean.getCurrentThreadCpuTime
    if (helper) synchronized { helperThreads.add(thread) }
    // The morsels this participant claimed, to give back if it is aborted.
    val claimed = new ArrayBuffer[Morsel]
    var taskMemoryManager: TaskMemoryManager = null
    var context: MorselTaskContext = null
    var completed = false
    try {
      taskMemoryManager = new TaskMemoryManager(owner.taskMemoryManager)
      context = new MorselTaskContext(owner, taskMemoryManager, new TaskMetrics,
        if (helper) () => helpersAborted else () => false)
      if (helper) {
        thread.setContextClassLoader(classLoader)
        Executor.taskDeserializationProps.set(deserializationProps)
        InputFileBlockHolder.initialize()
      }
      TaskContext.setTaskContext(context)
      val input = if (helper) {
        new MorselIterator(first, fromFront = false, shouldYield, claimed)
      } else {
        new MorselIterator(null, fromFront = true, () => false, null)
      }
      val part = runPart(context, input)
      try {
        context.markTaskCompleted(None)
        synchronized { results += ((part, context)) }
        completed = true
      } finally {
        if (!completed) discardPart(part)
      }
    } catch {
      case t: Throwable =>
        if (context != null) {
          try {
            context.markTaskFailed(t)
            if (!completed) context.markTaskCompleted(Some(t))
          } catch {
            case t2: Throwable => if (t2 ne t) t.addSuppressed(t2)
          }
        }
        val runOutOfMemory = MorselJob.isOutOfMemory(t)
        if (helper && (helpersAborted || runOutOfMemory) && !owner.isInterrupted()) {
          // Give way to the owner: it processes this helper's morsels instead.
          synchronized {
            if (runOutOfMemory) memoryBound = true
            _helpersAborted += 1
          }
          giveBack(claimed.toSeq)
          logInfo(s"Morsel helper of task ${owner.taskAttemptId} gave back ${claimed.length} " +
            s"morsels: ${if (runOutOfMemory) t.toString else MorselJob.ABORT_REASON}")
        } else {
          fail(t)
        }
    } finally {
      val leaked = if (taskMemoryManager != null) {
        taskMemoryManager.cleanUpAllAllocatedMemory()
      } else {
        0L
      }
      if (leaked > 0) {
        val message = s"Managed memory leak detected; size = $leaked bytes, morsel helper " +
          s"of task ${owner.taskAttemptId}"
        if (SparkEnv.get.conf.get(config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK)) {
          fail(new SparkException(message))
        } else {
          logWarning(message)
        }
      }
      TaskContext.setTaskContext(previousContext)
      if (helper) {
        InputFileBlockHolder.unset()
        Executor.taskDeserializationProps.remove()
        thread.setContextClassLoader(previousClassLoader)
        val cpuTime = threadMXBean.getCurrentThreadCpuTime - cpuStart
        synchronized {
          helperThreads.remove(thread)
          _helperRuns += 1
          _helperCpuTimeNs += cpuTime
          activeHelpers -= 1
          notifyAll()
        }
        // Clear an interrupt from closeAndAwaitHelpers() or reclaimMemory() before the pool
        // reuses the thread. No interrupt can arrive after the thread was removed above.
        Thread.interrupted()
      }
    }
  }

  /**
   * The input of one participant. Once it reports no more input it stays exhausted, so a helper
   * that yielded does not resume.
   *
   * @param claimed if not null, records the morsels taken.
   */
  private class MorselIterator(
      private var pending: Morsel,
      fromFront: Boolean,
      shouldYield: () => Boolean,
      claimed: ArrayBuffer[Morsel]) extends Iterator[AnyRef] {
    private var exhausted = false
    if (pending != null && claimed != null) claimed += pending

    override def hasNext: Boolean = {
      if (pending == null && !exhausted) {
        if (shouldYield() || helpersAborted && claimed != null) {
          exhausted = true
        } else {
          pending = MorselJob.this.take(fromFront)
          exhausted = pending == null
          if (pending != null && claimed != null) claimed += pending
        }
      }
      pending != null
    }

    override def next(): AnyRef = {
      if (!hasNext) throw new NoSuchElementException("No more morsels")
      val morsel = pending
      pending = null
      morsel.payload
    }
  }
}

private[spark] object MorselJob {
  val MAX_OWNER_MERGE = 4

  val ABORT_REASON = "morsel helper aborted to free memory for its task"

  /** Whether `t` was caused by running out of execution memory. */
  def isOutOfMemory(t: Throwable): Boolean =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).take(16)
      .exists(_.isInstanceOf[SparkOutOfMemoryError])
}

/**
 * Executor-wide scheduler that lends idle CPU slots to running tasks.
 *
 * The driver assigns tasks to executor slots as usual and every task runs on its own thread. A
 * task whose work can be split registers a [[MorselJob]]. When a slot has been idle for
 * `helperDelay` (no task is running on it, e.g. in the tail of a stage or when a stage has fewer
 * tasks than the executor has cores), a helper thread joins the registered job with the most
 * remaining input per participant and steals morsels from it. Helpers yield their slot at the
 * next morsel boundary when a newly launched task needs it, so the driver's view of slot usage
 * stays accurate within one morsel.
 *
 * A task that the driver gave more than one CPU (`spark.task.cpus`) uses its spare CPUs for its
 * own helpers right away. Together with helpers charging their memory to their task, this lets an
 * executor run fewer, larger tasks, each with a larger share of memory, without leaving CPUs idle.
 */
private[spark] class MorselScheduler(val numSlots: Int, conf: SparkConf) extends Logging {

  val targetMorselSize: Long = conf.get(config.MORSEL_TARGET_SIZE)
  private val helperDelayNs = TimeUnit.MILLISECONDS.toNanos(conf.get(config.MORSEL_HELPER_DELAY))
  // A helper joins a task only for at least half a morsel or MIN_HELPER_WORK of work.
  private val minHelperBytes = targetMorselSize / 2
  private val minHelperNanos = TimeUnit.MILLISECONDS.toNanos(MorselScheduler.MIN_HELPER_WORK_MS)

  private val lock = new Object
  // CPU slots used by task threads that are running (not waiting for helpers).
  private var busySlots = 0
  // Helpers on idle slots, including those of tasks that lend their slots while they wait.
  private var activeHelpers = 0
  // Helpers on the spare CPUs of their own tasks.
  private var spareHelpers = 0
  private var slotFreedAtNs = 0L
  private var stopped = false
  private val jobs = new java.util.LinkedHashSet[MorselJob[_]]

  private val helperPool = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("morsel-helper-%d")
      .setThreadFactory((r: Runnable) => new UninterruptibleThread(r, "unused"))
      .build()
    Executors.newFixedThreadPool(numSlots, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  private val dispatcher = new Thread("morsel-dispatcher") {
    setDaemon(true)
    override def run(): Unit = dispatchLoop()
  }
  dispatcher.start()

  // Executor-wide counters, for monitoring and tests.
  val jobsRun = new AtomicLong
  val morselsStolen = new AtomicLong
  val helperAborts = new AtomicLong

  def taskStarted(cpus: Int): Unit = lock.synchronized {
    busySlots += cpus
  }

  def taskFinished(cpus: Int): Unit = lock.synchronized {
    busySlots -= cpus
    slotFreedAtNs = System.nanoTime()
    lock.notifyAll()
  }

  def register(job: MorselJob[_]): Unit = lock.synchronized {
    jobs.add(job)
    lock.notifyAll()
  }

  def unregister(job: MorselJob[_]): Unit = lock.synchronized {
    jobs.remove(job)
  }

  /**
   * Lends the slots of `job`'s task to helpers while it runs `body` (e.g. waits for them). Helpers
   * on the task's spare CPUs then count as helpers on idle slots.
   */
  def lendSlotWhile[T](job: MorselJob[_])(body: => T): T = {
    lock.synchronized {
      activeHelpers += job.spareSlotHelpers
      spareHelpers -= job.spareSlotHelpers
      job.spareSlotHelpers = 0
      job.slotsLent = true
    }
    taskFinished(job.owner.cpus)
    try body finally taskStarted(job.owner.cpus)
  }

  def numActiveHelpers: Int = lock.synchronized(activeHelpers + spareHelpers)

  def stop(): Unit = {
    lock.synchronized {
      stopped = true
      lock.notifyAll()
    }
    // Helpers that never ran hold a claimed morsel: fail their jobs rather than lose its rows.
    helperPool.shutdownNow().asScala.foreach {
      case run: HelperRun => run.claim._1.abandonHelper()
      case _ =>
    }
  }

  private class HelperRun(val claim: (MorselJob[_], Morsel), spareSlot: Boolean)
    extends Runnable {
    override def run(): Unit = helperLoop(claim, spareSlot)
  }

  private def stealPriority(job: MorselJob[_]): Double =
    job.stealPriority(minHelperBytes, minHelperNanos)

  private def hasStealableJob: Boolean = jobs.asScala.exists(stealPriority(_) > 0)

  /** Joins the job that benefits most from another helper; null if no job has work left. */
  private def claimMorsel(): (MorselJob[_], Morsel) = {
    val candidates = jobs.asScala.toSeq.map(j => (j, stealPriority(j))).filter(_._2 > 0)
    candidates.sortBy(-_._2).iterator.map { case (job, _) => (job, job.tryJoin()) }
      .find(_._2 != null).orNull
  }

  /** Joins a job that has a spare CPU of its own task; null if there is none. */
  private def claimSpareSlot(): (MorselJob[_], Morsel) = {
    jobs.asScala.iterator
      .filter(j => !j.slotsLent && j.spareSlotHelpers < j.owner.cpus - 1)
      .filter(stealPriority(_) > 0)
      .map(j => (j, j.tryJoin()))
      .find(_._2 != null).orNull
  }

  private def dispatchLoop(): Unit = lock.synchronized {
    while (!stopped) {
      val spare = claimSpareSlot()
      if (spare != null) {
        spare._1.spareSlotHelpers += 1
        spareHelpers += 1
        helperPool.execute(new HelperRun(spare, spareSlot = true))
      } else {
        val idle = busySlots + activeHelpers < numSlots
        val waitNs = slotFreedAtNs + helperDelayNs - System.nanoTime()
        val claim = if (idle && waitNs <= 0) claimMorsel() else null
        if (claim != null) {
          activeHelpers += 1
          helperPool.execute(new HelperRun(claim, spareSlot = false))
        } else if (idle && waitNs > 0 && hasStealableJob) {
          lock.wait(math.max(1L, TimeUnit.NANOSECONDS.toMillis(waitNs)))
        } else if (jobs.isEmpty) {
          lock.wait()
        } else {
          // Whether a task is worth helping changes as it runs.
          lock.wait(MorselScheduler.RECHECK_MS)
        }
      }
    }
  }

  /** Whether a helper still uses a spare CPU of its own task, which it never yields. */
  private def onSpareSlot(job: MorselJob[_], spareSlot: Boolean): Boolean =
    spareSlot && !job.slotsLent

  /** Called by a helper at morsel boundaries: gives up the slot if a task needs it. */
  private def tryYield(job: MorselJob[_], spareSlot: Boolean): Boolean = lock.synchronized {
    if (!onSpareSlot(job, spareSlot) && busySlots + activeHelpers > numSlots) {
      activeHelpers -= 1
      lock.notifyAll()
      true
    } else {
      false
    }
  }

  private def helperLoop(initialClaim: (MorselJob[_], Morsel), initialSpareSlot: Boolean): Unit = {
    var claim = initialClaim
    var spareSlot = initialSpareSlot
    while (claim != null) {
      val job = claim._1
      var yielded = false
      job.runAsHelper(claim._2, () => {
        yielded = tryYield(job, spareSlot)
        yielded
      })
      // A helper that yielded has already given up its slot. A spare CPU goes back to its task.
      claim = if (yielded) null else lock.synchronized {
        if (onSpareSlot(job, spareSlot)) {
          job.spareSlotHelpers -= 1
          spareHelpers -= 1
          lock.notifyAll()
          null
        } else {
          val next =
            if (stopped || busySlots + activeHelpers > numSlots) null else claimMorsel()
          if (next == null) {
            activeHelpers -= 1
            lock.notifyAll()
          }
          next
        }
      }
      spareSlot = false
    }
  }
}

private[spark] object MorselScheduler {
  private val current = new AtomicReference[MorselScheduler]()

  def get: Option[MorselScheduler] = Option(current.get())

  def set(scheduler: MorselScheduler): Unit = current.set(scheduler)

  def clear(scheduler: MorselScheduler): Unit = current.compareAndSet(scheduler, null)

  // Several times what starting a helper costs (deserializing a task and setting up its
  // pipeline, tens of milliseconds), so that helping does not delay the task.
  val MIN_HELPER_WORK_MS = 100L

  val RECHECK_MS = 10L
}
