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

import org.apache.spark.{Partition, SparkConf, SparkEnv, SparkException, TaskContext, TaskContextImpl}
import org.apache.spark.executor.{Executor, TaskMetrics}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.memory.TaskMemoryManager
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
}

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
 */
private[spark] class MorselPartition(
    val parent: Partition,
    @transient val morsels: Iterator[AnyRef]) extends Partition {
  override def index: Int = parent.index
}

/**
 * The context of a helper thread working on a task it does not own. It identifies as the owning
 * task attempt and shares its kill and fetch-failure state, but has its own memory manager,
 * metrics and completion listeners, so the helper's operators are isolated from the owner's.
 */
private[spark] class MorselTaskContext(
    val owner: TaskContextImpl,
    taskMemoryManager: TaskMemoryManager,
    taskMetrics: TaskMetrics)
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

  override def isInterrupted(): Boolean = owner.isInterrupted()

  private[spark] override def killTaskIfInterrupted(): Unit = owner.killTaskIfInterrupted()

  private[spark] override def getKillReason(): Option[String] = owner.getKillReason()

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
 * hands its result to the owner, which combines all results into the task's output. The queue
 * only ever holds this task attempt's input, so the task's output is exactly that of its
 * partition, whichever threads produced it.
 *
 * @tparam R the result of one helper, combined by the owner.
 */
private[spark] abstract class MorselJob[R](
    val owner: TaskContextImpl,
    morsels: IndexedSeq[Morsel],
    merge: (AnyRef, AnyRef) => Option[AnyRef] = (_, _) => None) extends Logging {

  private val queue = new java.util.ArrayDeque[Morsel](morsels.asJava)
  private var remainingBytes = morsels.iterator.map(_.sizeInBytes).sum
  // When closed, no new helpers may join.
  private var closed = false
  private var activeHelpers = 0
  @volatile private var failure: Throwable = null
  private val results = new ArrayBuffer[(R, MorselTaskContext)]
  private val helperThreads = new java.util.HashSet[Thread]

  // Thread state of the owner that helpers need to deserialize and run the task.
  private val classLoader = Thread.currentThread().getContextClassLoader
  private val deserializationProps = Executor.taskDeserializationProps.get()

  val numMorsels: Int = morsels.length
  private var _morselsStolen = 0
  private var _helperRuns = 0
  private var _helperCpuTimeNs = 0L

  /** Runs this task's pipeline on a helper thread over `morsels`, returning the helper result. */
  protected def runHelper(context: MorselTaskContext, morsels: Iterator[AnyRef]): R

  /** Releases a helper result that will not be combined into the task output. */
  protected def discardResult(result: R): Unit

  /** The owner's input: morsels taken from the front of the queue. */
  def ownerMorsels: Iterator[AnyRef] = new MorselIterator(null, fromFront = true, () => false)

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

  /** Called for a helper that claimed a morsel but will never run (the executor is stopping). */
  private[morsel] def abandonHelper(): Unit = synchronized {
    fail(new SparkException("A morsel helper did not run because the executor is stopping"))
    activeHelpers -= 1
    notifyAll()
  }

  /** Results of helpers that completed successfully. Only valid after closeAndAwaitHelpers(). */
  def helperResults: Seq[R] = synchronized { results.map(_._1).toSeq }

  def morselsStolen: Int = synchronized(_morselsStolen)
  def helperRuns: Int = synchronized(_helperRuns)
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

  /** How much a helper joining now would help: remaining input per participant. */
  private[morsel] def stealPriority: Double = synchronized {
    if (closed || queue.isEmpty) -1.0 else remainingBytes.toDouble / (1 + activeHelpers)
  }

  /** Joins as a helper, claiming the first morsel to process; null if there is nothing left. */
  private[morsel] def tryJoin(): Morsel = synchronized {
    if (closed) {
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
    morsel
  }

  /**
   * Runs the pipeline on the calling helper thread, starting with `first` and then stealing
   * more morsels until the queue is empty or `shouldYield` returns true.
   */
  private[morsel] def runAsHelper(first: Morsel, shouldYield: () => Boolean): Unit = {
    val thread = Thread.currentThread()
    val previousClassLoader = thread.getContextClassLoader
    val threadMXBean = ManagementFactory.getThreadMXBean
    val cpuStart = threadMXBean.getCurrentThreadCpuTime
    synchronized { helperThreads.add(thread) }
    var taskMemoryManager: TaskMemoryManager = null
    var context: MorselTaskContext = null
    var completed = false
    try {
      taskMemoryManager =
        new TaskMemoryManager(SparkEnv.get.memoryManager, MorselJob.nextHelperAttemptId())
      context = new MorselTaskContext(owner, taskMemoryManager, new TaskMetrics)
      thread.setContextClassLoader(classLoader)
      Executor.taskDeserializationProps.set(deserializationProps)
      TaskContext.setTaskContext(context)
      InputFileBlockHolder.initialize()
      val result = runHelper(context, new MorselIterator(first, fromFront = false, shouldYield))
      try {
        context.markTaskCompleted(None)
        synchronized { results += ((result, context)) }
        completed = true
      } finally {
        if (!completed) discardResult(result)
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
        fail(t)
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
      TaskContext.unset()
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
      // Clear an interrupt from closeAndAwaitHelpers() before the pool reuses the thread. No
      // interrupt can arrive after the thread was removed above.
      Thread.interrupted()
    }
  }

  /**
   * The input of one participant. Once it reports no more input it stays exhausted, so a helper
   * that yielded does not resume.
   */
  private class MorselIterator(
      private var pending: Morsel,
      fromFront: Boolean,
      shouldYield: () => Boolean) extends Iterator[AnyRef] {
    private var exhausted = false

    override def hasNext: Boolean = {
      if (pending == null && !exhausted) {
        if (shouldYield()) {
          exhausted = true
        } else {
          pending = MorselJob.this.take(fromFront)
          exhausted = pending == null
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

  // Helpers get their own TaskMemoryManager so the memory pool and spilling treat them like a
  // separate task. Use negative ids so they never collide with real task attempt ids.
  private val helperAttemptIds = new AtomicLong(0L)

  def nextHelperAttemptId(): Long = -1L - helperAttemptIds.getAndIncrement()
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
 */
private[spark] class MorselScheduler(val numSlots: Int, conf: SparkConf) extends Logging {

  val targetMorselSize: Long = conf.get(config.MORSEL_TARGET_SIZE)
  private val helperDelayNs = TimeUnit.MILLISECONDS.toNanos(conf.get(config.MORSEL_HELPER_DELAY))

  private val lock = new Object
  // CPU slots used by task threads that are running (not waiting for helpers).
  private var busySlots = 0
  private var activeHelpers = 0
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

  /** Lends the calling task's slot to helpers while it runs `body` (e.g. waits for them). */
  def lendSlotWhile[T](cpus: Int)(body: => T): T = {
    taskFinished(cpus)
    try body finally taskStarted(cpus)
  }

  def numActiveHelpers: Int = lock.synchronized(activeHelpers)

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

  private class HelperRun(val claim: (MorselJob[_], Morsel)) extends Runnable {
    override def run(): Unit = helperLoop(claim)
  }

  private def hasStealableJob: Boolean = jobs.asScala.exists(_.stealPriority > 0)

  /** Joins the job that benefits most from another helper; null if no job has work left. */
  private def claimMorsel(): (MorselJob[_], Morsel) = {
    val candidates = jobs.asScala.toSeq.map(j => (j, j.stealPriority)).filter(_._2 > 0)
    candidates.sortBy(-_._2).iterator.map { case (job, _) => (job, job.tryJoin()) }
      .find(_._2 != null).orNull
  }

  private def dispatchLoop(): Unit = lock.synchronized {
    while (!stopped) {
      if (busySlots + activeHelpers < numSlots) {
        val waitNs = slotFreedAtNs + helperDelayNs - System.nanoTime()
        if (waitNs > 0 && hasStealableJob) {
          lock.wait(math.max(1L, TimeUnit.NANOSECONDS.toMillis(waitNs)))
        } else {
          val claim = claimMorsel()
          if (claim != null) {
            activeHelpers += 1
            helperPool.execute(new HelperRun(claim))
          } else {
            lock.wait()
          }
        }
      } else {
        lock.wait()
      }
    }
  }

  /** Called by a helper at morsel boundaries: gives up the slot if a task needs it. */
  private def tryYield(): Boolean = lock.synchronized {
    if (busySlots + activeHelpers > numSlots) {
      activeHelpers -= 1
      lock.notifyAll()
      true
    } else {
      false
    }
  }

  private def helperLoop(initialClaim: (MorselJob[_], Morsel)): Unit = {
    var claim = initialClaim
    while (claim != null) {
      var yielded = false
      claim._1.runAsHelper(claim._2, () => {
        yielded = tryYield()
        yielded
      })
      // A helper that yielded has already given up its slot.
      claim = if (yielded) null else lock.synchronized {
        val next =
          if (stopped || busySlots + activeHelpers > numSlots) null else claimMorsel()
        if (next == null) {
          activeHelpers -= 1
          lock.notifyAll()
        }
        next
      }
    }
  }
}

private[spark] object MorselScheduler {
  private val current = new AtomicReference[MorselScheduler]()

  def get: Option[MorselScheduler] = Option(current.get())

  def set(scheduler: MorselScheduler): Unit = current.set(scheduler)

  def clear(scheduler: MorselScheduler): Unit = current.compareAndSet(scheduler, null)
}
