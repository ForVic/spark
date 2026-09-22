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
package org.apache.spark.scheduler.cluster.k8s

import java.lang.management.ManagementFactory
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicLongArray, AtomicReferenceArray}

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException}
import io.fabric8.kubernetes.client.dsl.PodResource
import io.fabric8.kubernetes.client.dsl.base.PatchContext
import org.jmock.lib.concurrent.DeterministicScheduler
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{doAnswer, mock, when}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, KubernetesExecutorSpec}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.ExecutorExited
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._
import org.apache.spark.util.{ManualClock, SizeEstimator}

/** Local replay through the production store, allocator and lifecycle manager. */
object ExecutorPodsReplay {
  case class Result(actions: Seq[String], failures: Int, removed: Set[Long], created: Int,
      elapsedMs: Double, producerMs: Double, lifecycleMs: Double, allocatorMs: Double,
      peakOutstanding: Long, maxBatch: Long, maxAgeMs: Double, peakHeapBytes: Long,
      allocatedBytes: Long,
      backlogBytes: Long)

  def run(count: Int, retain: Boolean, faults: Boolean, threaded: Boolean,
      measureBacklog: Boolean = false): Result = {
    require(count >= 8 && count % 4 == 0)
    ExecutorPodsSnapshot.setShouldCheckAllContainers(false)
    ExecutorPodsSnapshot.setSparkContainerName(TEST_SPARK_EXECUTOR_CONTAINER_NAME)
    val conf = new SparkConf(false)
      .set(KUBERNETES_DELETE_EXECUTORS, !retain)
      .set(KUBERNETES_EXECUTOR_PODTEMPLATE_CONTAINER_NAME, TEST_SPARK_EXECUTOR_CONTAINER_NAME)
      .set(KUBERNETES_ALLOCATION_BATCH_SIZE, count)
      .set(KUBERNETES_MAX_PENDING_PODS, count * 2)
      .set(KUBERNETES_ALLOCATION_BATCH_DELAY, 201L)
      .set(KUBERNETES_EXECUTOR_EVENT_PROCESSING_INTERVAL, 201L)
      .set(KUBERNETES_EXECUTOR_MISSING_POD_DETECT_DELTA, 1000L)
      .set("spark.executor.maxNumFailures", "100000")
    val bean = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    val allocated = new AtomicLong()
    val inFlight = new AtomicReferenceArray[Seq[ExecutorPodsSnapshot]](2)
    var backlogBytes = 0L
    val clock = new ManualClock(100L)
    val deterministic = new DeterministicScheduler()
    val executor = if (threaded) new ScheduledThreadPoolExecutor(2) else deterministic
    val delegate = new ExecutorPodsSnapshotsStoreImpl(executor, clock, conf)
    val produced = new AtomicLong(1)
    val finished = new AtomicLongArray(2)
    val callbackNs = new AtomicLongArray(2)
    val started = new AtomicLongArray(2)
    val consumers = new AtomicInteger()
    val peakOutstanding = new AtomicLong()
    val maxBatch = new AtomicLong()
    val maxAgeNs = new AtomicLong()
    val trace = new ConcurrentLinkedQueue[String]()
    val traceEnabled = java.lang.Boolean.getBoolean("spark.replay.trace")
    val terminalTimes = new ConcurrentHashMap[String, Long]()
    val eventTimes = new ConcurrentHashMap[Long, Long]()
    val errors = new ConcurrentLinkedQueue[Throwable]()
    eventTimes.put(0L, System.nanoTime())
    def maximum(value: AtomicLong, next: Long): Unit = {
      value.accumulateAndGet(next, (a: Long, b: Long) => math.max(a, b))
    }
    val store = new ExecutorPodsSnapshotsStore {
      override def addSubscriber(interval: Long)
          (callback: Seq[ExecutorPodsSnapshot] => Unit): Unit = {
        val index = consumers.getAndIncrement()
        delegate.addSubscriber(interval) { snapshots =>
          inFlight.set(index, snapshots)
          val first = started.getAndAdd(index, snapshots.size)
          maximum(maxBatch, snapshots.size)
          if (snapshots.nonEmpty) {
            maximum(maxAgeNs, System.nanoTime() - eventTimes.get(first))
          }
          val begin = System.nanoTime()
          val age = if (snapshots.nonEmpty) begin - eventTimes.get(first) else 0L
          val bytes = bean.getThreadAllocatedBytes(Thread.currentThread().getId)
          try {
            callback(snapshots)
          } catch {
            case e: Throwable => errors.add(e); throw e
          } finally {
            val duration = System.nanoTime() - begin
            if (traceEnabled) {
              trace.add(s"TRACE|$index|$first|${snapshots.size}|$age|$duration")
            }
            callbackNs.addAndGet(index, duration)
            allocated.addAndGet(bean.getThreadAllocatedBytes(Thread.currentThread().getId) - bytes)
            finished.addAndGet(index, snapshots.size)
            inFlight.set(index, null)
          }
        }
      }
      override def updatePod(pod: Pod): Unit = delegate.updatePod(pod)
      override def replaceSnapshot(pods: Seq[Pod]): Unit = delegate.replaceSnapshot(pods)
      override def notifySubscribers(): Unit = delegate.notifySubscribers()
      override def stop(): Unit = delegate.stop()
    }
    val actions = new ConcurrentLinkedQueue[String]()
    val apiPods = new ConcurrentHashMap[String, Pod]()
    val registered = new ConcurrentHashMap[String, Long]()
    val removed = ConcurrentHashMap.newKeySet[Long]()
    val attempts = new ConcurrentHashMap[String, AtomicInteger]()
    val created = new AtomicInteger()
    val client = mock(classOf[KubernetesClient])
    val pods = mock(classOf[PODS])
    val namespace = mock(classOf[PODS_WITH_NAMESPACE])
    val backend = mock(classOf[KubernetesClusterSchedulerBackend])
    val builder = mock(classOf[KubernetesExecutorBuilder])
    when(client.pods()).thenReturn(pods)
    when(pods.inNamespace(anyString())).thenReturn(namespace)
    when(backend.getExecutorIds()).thenAnswer(_ => registered.keySet().asScala.toSeq)
    when(backend.getExecutorsWithRegistrationTs())
      .thenAnswer(_ => registered.asScala.toMap)
    when(backend.isExecutorActive(anyString())).thenAnswer { call =>
      registered.containsKey(call.getArgument[String](0))
    }
    doAnswer { call =>
      val id = call.getArgument[String](0)
      val reason = call.getArgument[ExecutorExited](1)
      if (traceEnabled && terminalTimes.containsKey(id)) {
        trace.add(s"REMOVAL|$id|${System.nanoTime() - terminalTimes.get(id)}")
      }
      actions.add(s"remove:$id:${reason.toString.replace('\n', ' ')}")
      removed.add(id.toLong)
      registered.remove(id)
      null
    }.when(backend).doRemoveExecutor(anyString(), any())
    def operation(kind: String, name: String): Unit = {
      val key = s"$kind:$name"
      val attempt = attempts.computeIfAbsent(key, _ => new AtomicInteger()).incrementAndGet()
      val fail = faults && name == "spark-executor-3" && attempt == 1
      actions.add(s"$key:${if (fail) "failure" else "success"}")
      if (faults && threaded) Thread.sleep(2)
      if (fail) throw new KubernetesClientException("Injected replay API failure")
    }
    when(namespace.withName(anyString())).thenAnswer { call =>
      val name = call.getArgument[String](0)
      val resource = mock(classOf[PodResource])
      when(resource.get()).thenAnswer { _ =>
        actions.add(s"get:$name")
        apiPods.get(name)
      }
      when(resource.delete()).thenAnswer { _ =>
        operation("delete", name)
        java.util.Collections.emptyList[io.fabric8.kubernetes.api.model.StatusDetails]()
      }
      when(resource.patch(any[PatchContext], any[Pod])).thenAnswer { _ =>
        operation("patch", name)
        apiPods.get(name)
      }
      resource
    }
    when(builder.buildFromFeatures(any[KubernetesExecutorConf], any[KubernetesClient],
      any[ResourceProfile])).thenAnswer { call =>
      val c = call.getArgument[KubernetesExecutorConf](0)
      KubernetesExecutorSpec(executorPodWithId(c.executorId.toLong), Seq.empty)
    }
    when(namespace.resource(any[Pod])).thenAnswer { call =>
      val pod = call.getArgument[Pod](0)
      val resource = mock(classOf[PodResource])
      when(resource.create()).thenAnswer { _ =>
        actions.add(s"create:${pod.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL)}")
        apiPods.put(pod.getMetadata.getName, pod)
        created.incrementAndGet()
        pod
      }
      resource
    }
    val lifecycle = new ExecutorPodsLifecycleManager(conf, client, store, clock)
    val allocator = new ExecutorPodsAllocator(conf, new SecurityManager(conf), builder,
      client, store, clock)
    allocator.setExecutorPodsLifecycleManager(lifecycle)
    val profile = ResourceProfile.getOrCreateDefaultProfile(conf)
    var producerNs = 0L
    var peakHeap = 0L
    var revision = 0L
    def pump(delay: Int): Unit = {
      clock.advance(delay)
      if (threaded) {
        if (delay > 0) Thread.sleep(delay)
      } else {
        deterministic.tick(delay, TimeUnit.MILLISECONDS)
      }
    }
    def awaitCondition(condition: => Boolean): Unit = {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60)
      while (!condition && errors.isEmpty && System.nanoTime() < deadline) {
        if (threaded) Thread.sleep(2) else deterministic.tick(201, TimeUnit.MILLISECONDS)
      }
      require(errors.isEmpty, s"Subscriber errors: $errors")
      require(condition, "Replay did not converge")
    }
    def drain(): Unit = {
      store.notifySubscribers()
      val target = produced.get()
      awaitCondition(finished.get(0) >= target && finished.get(1) >= target)
    }
    def publish(body: => Unit): Unit = {
      val sequence = produced.getAndIncrement()
      eventTimes.put(sequence, System.nanoTime())
      val begin = System.nanoTime()
      val bytes = bean.getThreadAllocatedBytes(Thread.currentThread().getId)
      body
      producerNs += System.nanoTime() - begin
      allocated.addAndGet(bean.getThreadAllocatedBytes(Thread.currentThread().getId) - bytes)
      maximum(peakOutstanding, (0 until 2).map(i => produced.get() - finished.get(i)).max)
      val used = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed
      peakHeap = math.max(peakHeap, used)
      if (measureBacklog && sequence % 128 == 0) {
        // Diagnostic runs only: inspect real queues without exposing production test hooks.
        val field = delegate.getClass.getDeclaredField("subscribers")
        field.setAccessible(true)
        val subscribers = field.get(delegate).asInstanceOf[java.util.List[AnyRef]]
        val queues = subscribers.asScala.map { subscriber =>
          val buffer = subscriber.getClass.getDeclaredField("snapshotsBuffer")
          buffer.setAccessible(true)
          // Lists avoid SizeEstimator's sampling shortcut for large root arrays.
          buffer.get(subscriber).asInstanceOf[java.util.Queue[_]].iterator().asScala.toList
        }.toArray
        val active = (0 until 2).map(inFlight.get).filter(_ != null).map(_.toList).toArray
        backlogBytes = math.max(backlogBytes, SizeEstimator.estimate(Array(queues, active)))
      }
    }
    def update(pod: Pod, deliver: Boolean = true): Unit = {
      revision += 1
      val value = new PodBuilder(pod).editMetadata()
        .withCreationTimestamp("2026-01-01T00:00:00Z")
        .withResourceVersion(revision.toString).endMetadata()
        .editOrNewStatus().withStartTime("2026-01-01T00:00:00Z").endStatus().build()
      apiPods.put(value.getMetadata.getName, value)
      if (deliver) {
        if (traceEnabled && Set("failed", "succeeded").contains(value.getStatus.getPhase)) {
          terminalTimes.putIfAbsent(value.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL),
            System.nanoTime())
        }
        publish(store.updatePod(value))
      }
      if (revision % 16 == 0) pump(2)
    }
    def relist(): Unit = {
      publish(store.replaceSnapshot(apiPods.values().asScala.toSeq.sortBy(_.getMetadata.getName)))
    }
    def healthy(first: Int, last: Int): Unit = {
      (first to last).foreach { id =>
        update(runningExecutor(id))
        registered.put(id.toString, clock.getTimeMillis())
      }
    }
    val begin = System.nanoTime()
    eventTimes.put(0L, begin)
    try {
      lifecycle.start(backend)
      allocator.start(TEST_SPARK_APP_ID, backend)
      allocator.setTotalExpectedExecutors(Map(profile -> count))
      awaitCondition(created.get() == count)
      (1 to count).foreach(id => update(pendingExecutor(id)))
      healthy(1, count)
      drain()
      (1 to 3).foreach { _ => healthy(1, count); relist() }
      drain()

      allocator.setTotalExpectedExecutors(Map(profile -> (count / 2)))
      // The failure watch event is lost; the full list must repair it.
      update(failedExecutorWithoutDeletion(1), deliver = false)
      apiPods.remove("spark-executor-2")
      pump(1100)
      relist()
      drain()
      (3 to count / 2).foreach { id =>
        update(if (id <= count / 4) failedExecutorWithoutDeletion(id) else succeededExecutor(id))
      }
      drain()
      // Keep terminal pods present while healthy events trigger failed cleanup retries.
      healthy(count / 2 + 1, count)
      drain()
      (1 to count / 2).filter(_ != 2).foreach { id =>
        val name = s"spark-executor-$id"
        if (retain && id != 3) {
          update(new PodBuilder(apiPods.get(name)).editMetadata()
            .addToLabels(SPARK_EXECUTOR_INACTIVE_LABEL, "true").endMetadata().build())
        } else if (!retain) {
          apiPods.remove(name)
        }
      }
      relist()
      (1 to 3).foreach { _ => healthy(count / 2 + 1, count) }
      drain()
      // Executor 3's inactive-label acknowledgement remains pending during regrowth.
      allocator.setTotalExpectedExecutors(Map(profile -> count))
      awaitCondition(created.get() == count + count / 2)
      (count + 1 to count + count / 2).foreach(id => update(pendingExecutor(id)))
      healthy(count + 1, count + count / 2)
      if (retain) {
        update(new PodBuilder(apiPods.get("spark-executor-3")).editMetadata()
          .addToLabels(SPARK_EXECUTOR_INACTIVE_LABEL, "true").endMetadata().build())
      }
      relist()
      drain()
      if (traceEnabled) {
        // scalastyle:off println
        trace.asScala.foreach(println)
        // scalastyle:on println
      }
      require(removed.asScala.toSet == (1L to count / 2).toSet)
      require(lifecycle.getNumExecutorsFailed == count / 4 - 1)
      require(created.get() == count + count / 2)
      require(registered.size() == count)
      Result(actions.asScala.toSeq.sorted, lifecycle.getNumExecutorsFailed,
        removed.asScala.toSet, created.get(), (System.nanoTime() - begin) / 1e6,
        producerNs / 1e6, callbackNs.get(0) / 1e6, callbackNs.get(1) / 1e6,
        peakOutstanding.get(), maxBatch.get(), maxAgeNs.get() / 1e6, peakHeap,
        allocated.get(), backlogBytes)
    } finally {
      if (threaded) store.stop()
      ExecutorPodsSnapshot.setSparkContainerName(DEFAULT_EXECUTOR_CONTAINER_NAME)
      ExecutorPodsSnapshot.setShouldCheckAllContainers(false)
    }
  }

  def main(args: Array[String]): Unit = {
    val result = run(args(0).toInt, args(1).toBoolean, args(2).toBoolean,
      args(3) == "threaded", args.lift(4).contains("memory"))
    // scalastyle:off println
    result.actions.foreach(action => println(s"ACTION|$action"))
    println(s"RESULT|${result.failures}|${result.created}|${result.elapsedMs}|" +
      s"${result.producerMs}|${result.lifecycleMs}|${result.allocatorMs}|" +
      s"${result.peakOutstanding}|${result.maxBatch}|${result.maxAgeMs}|" +
      s"${result.peakHeapBytes}|${result.allocatedBytes}|${result.backlogBytes}")
    // scalastyle:on println
  }
}

class ExecutorPodsReplaySuite extends SparkFunSuite {
  for (retain <- Seq(false, true); faults <- Seq(false, true)) {
    test(s"Complete executor lifecycle: retained=$retain, API failures=$faults") {
      val result = ExecutorPodsReplay.run(32, retain, faults, threaded = false)
      assert(result.created == 48)
      assert(result.failures == 7)
      assert(result.removed == (1L to 16L).toSet)
      assert(result.actions.count(_.startsWith("remove:")) == 16)
      if (faults) {
        assert(result.actions.exists(_.endsWith(":failure")))
        val operation = if (retain) "patch" else "delete"
        assert(result.actions.contains(s"$operation:spark-executor-3:success"))
      }
    }
  }
}
