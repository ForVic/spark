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

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import org.mockito.Mockito.{mock, when}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Constants.SPARK_EXECUTOR_INACTIVE_LABEL
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._
import org.apache.spark.util.SizeEstimator

/**
 * Local replay of pod events through the real snapshot and lifecycle implementations.
 * No Kubernetes server or executor processes are started. Pod construction is outside timing;
 * Snapshot construction and lifecycle processing are timed separately. Run on both revisions:
 * build/sbt -Pkubernetes "kubernetes/Test/runMain
 *   org.apache.spark.scheduler.cluster.k8s.ExecutorPodsLifecycleBenchmark running"
 */
object ExecutorPodsLifecycleBenchmark {
  private class ReplayStore extends ExecutorPodsSnapshotsStore {
    var consume: Seq[ExecutorPodsSnapshot] => Unit = _
    override def addSubscriber(interval: Long)
        (callback: Seq[ExecutorPodsSnapshot] => Unit): Unit = { consume = callback }
    override def stop(): Unit = {}
    override def notifySubscribers(): Unit = {}
    override def updatePod(pod: Pod): Unit = throw new UnsupportedOperationException
    override def replaceSnapshot(pods: Seq[Pod]): Unit = throw new UnsupportedOperationException
  }

  def main(args: Array[String]): Unit = {
    ExecutorPodsSnapshot.setShouldCheckAllContainers(false)
    val bean = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    val threadId = Thread.currentThread().getId
    val measureCpu = args.lift(4).contains("cpu")
    // scalastyle:off println
    println("SAMPLE,scenario,pods,updates,iteration,build_ns,process_ns,allocated_bytes")
    // scalastyle:on println
    for {
      scenario <- args.headOption.toSeq
      count <- args.lift(1).map(n => Seq(n.toInt)).getOrElse(Seq(1000, 10000))
    } {
      val retainedPercent = if (scenario == "retained50") 50 else {
        if (scenario == "retained100") 100 else 0
      }
      def podFor(i: Int): Pod = {
        val pod = runningExecutor(i.toLong)
        if (i <= count * retainedPercent / 100) {
          pod.getMetadata.getLabels.put(SPARK_EXECUTOR_INACTIVE_LABEL, "true")
        }
        pod
      }
      val pods = (1 to count).map(podFor)
      val base = ExecutorPodsSnapshot(pods, 0)
      val batchSize = if (scenario == "running1") 1 else {
        if (scenario == "running10") 10 else 100
      }
      val updates = (1 to batchSize).map { i =>
        if (scenario == "deletions" && i % 10 == 0) {
          new PodBuilder(succeededExecutor(i.toLong)).editMetadata()
            .withDeletionTimestamp("2026-01-01T00:00:00Z").endMetadata().build()
        } else {
          new PodBuilder(podFor(i)).editMetadata()
            .withResourceVersion(i.toString).endMetadata().build()
        }
      }
      val backend = mock(classOf[KubernetesClusterSchedulerBackend])
      when(backend.getExecutorsWithRegistrationTs()).thenReturn(Map.empty[String, Long])
      val client = mock(classOf[KubernetesClient])
      val store = new ReplayStore
      val manager = new ExecutorPodsLifecycleManager(new SparkConf(false), client, store)
      manager.start(backend)
      store.consume(Seq(base))
      // Estimate the reachable snapshot graph, not cumulative allocation or peak heap.
      // scalastyle:off println
      println(s"MEMORY,$scenario,$count,${SizeEstimator.estimate(base)}")
      // scalastyle:on println
      val warmupMillis = args.lift(2).map(_.toLong).getOrElse(2000L)
      val measuredBatches = args.lift(3).map(_.toInt).getOrElse(25)
      val warmupEnd = System.nanoTime() + warmupMillis * 1000000L
      var fullSnapshotTs = 0L
      var iteration = -20
      while (iteration < measuredBatches) {
        val allocated = bean.getThreadAllocatedBytes(threadId)
        val startCpu = if (measureCpu) bean.getCurrentThreadCpuTime else 0L
        val start = System.nanoTime()
        val snapshots = if (scenario == "full") {
          fullSnapshotTs += 1
          Seq(ExecutorPodsSnapshot(pods, fullSnapshotTs))
        } else {
          var current = base
          updates.map { pod =>
            current = current.withUpdate(pod)
            current
          }
        }
        val built = System.nanoTime()
        val builtCpu = if (measureCpu) bean.getCurrentThreadCpuTime else 0L
        store.consume(snapshots)
        val end = System.nanoTime()
        val endCpu = if (measureCpu) bean.getCurrentThreadCpuTime else 0L
        val bytes = bean.getThreadAllocatedBytes(threadId) - allocated
        if (iteration >= 0) {
          // scalastyle:off println
          println(s"SAMPLE,$scenario,$count,${snapshots.size},$iteration," +
            s"${built - start},${end - built},$bytes")
          if (measureCpu) {
            println(s"CPU,$scenario,$count,$iteration," +
              s"${builtCpu - startCpu},${endCpu - builtCpu}")
          }
          // scalastyle:on println
        }
        if (iteration != -1 || System.nanoTime() >= warmupEnd) {
          iteration += 1
        }
      }
    }
  }
}
