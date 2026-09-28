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

package org.apache.spark.sql.execution.exchange

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager
import org.apache.spark.sql.execution.{CoalesceExec, CollectLimitExec, CollectTailExec, DeserializeToObjectExec, ExternalRDDScanExec, RDDScanExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.joins.CartesianProductExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Shared environment and execution-boundary checks for batch pipelining. The original local
 * flag selects the channel-only policy; the generic batch flag requires a transport that explicitly
 * supports finite batch groups. Row ownership is specified separately by requiresDetachedRecords.
 */
private[sql] object PipelinedShuffleEligibility extends Logging {

  // The flag/manager mismatch is a start-up misconfiguration, so warn once per JVM rather than on
  // every query planned in the session.
  private val mismatchWarned = new AtomicBoolean(false)

  /** Operators whose consumers cannot safely drain a bounded, single-reader channel. */
  def isUnsupportedConsumer(plan: SparkPlan): Boolean = plan match {
    case _: CoalesceExec | _: CartesianProductExec | _: DeserializeToObjectExec |
        _: CollectLimitExec | _: CollectTailExec | _: TakeOrderedAndProjectExec => true
    case _ => false
  }

  /**
   * Estimate the complete group's task demand without executing or materializing the plan.
   * Each exchange contributes its producer width; the root contributes the consumer width.
   * Unknown widths conservatively retain regular execution. The scheduler still checks actual
   * RDD widths and free slots at submission, including contention from other jobs.
   */
  def fitsLocalCapacity(plan: SparkPlan, candidates: Set[Int]): Boolean = {
    val exchanges = plan.collect {
      case s: ShuffleExchangeExec if s.pipelined || candidates.contains(s.id) => s
    }.groupBy(_.id).values.map(_.head).toSeq
    val widths = plan.outputPartitioning.numPartitions +:
      exchanges.map(_.child.outputPartitioning.numPartitions)
    val sc = plan.session.sparkContext
    val slots = sc.maxNumConcurrentTasks(sc.resourceProfileManager.defaultResourceProfile)
    val demand = widths.map(_.toLong).sum
    val fits = widths.forall(_ > 0) && demand <= slots
    if (!fits) {
      logDebug(s"Pipelined shuffle: estimated stage widths ${widths.mkString(",")} cannot " +
        s"fit $slots local task slots; retaining regular shuffles.")
    }
    fits
  }

  /** Whether this environment supports batch pipelining, independent of exchange selection. */
  def enabled(plan: SparkPlan, conf: SQLConf): Boolean = {
    if (!conf.localPipelinedShuffleEnabled && !conf.batchPipelinedShuffleEnabled) {
      return false
    }
    if (plan.session == null ||
        (!conf.batchPipelinedShuffleEnabled && !plan.session.sparkContext.isLocal)) {
      return false
    }
    // Batch only. `IncrementalExecution.preparations` inherits QueryExecution's list, so without
    // this gate a streaming plan would be rewritten here: every micro-batch exchange (the
    // state-store shuffles, the static side of a stream-static join) would be flipped to pipelined
    // BEFORE `MarkPipelinedShuffleForRealTimeMode` runs. That contradicts what the Real-Time Mode
    // rule deliberately does -- it leaves the static side regular, because pulling it into the gang
    // would demand slots for stages that must instead finish first, failing admission. Streaming
    // marks its own pipelined boundaries; this opt-in batch path must not pre-empt that decision.
    // (`logicalLink.exists(_.isStreaming)` is the same signal InsertAdaptiveSparkPlan uses to keep
    // AQE off streaming plans.)
    // Dataset.rdd exposes arbitrary consumers beyond the SQL plan, including RDD shuffles,
    // multi-partition reads and repeated reads of the same partition. Cached inputs also hide
    // shuffle lineage: cache hits skip those readers, while misses may require regular stages.
    // RDD scans can hide arbitrary shuffle lineage and sharing outside the SQL plan as well.
    def hasUnsupportedBoundary(p: SparkPlan): Boolean = p match {
      case _: DeserializeToObjectExec | _: InMemoryTableScanExec |
          _: RDDScanExec | _: ExternalRDDScanExec[_] => true
      case q: QueryStageExec => hasUnsupportedBoundary(q.plan)
      case other => other.children.exists(hasUnsupportedBoundary)
    }
    if (hasUnsupportedBoundary(plan)) return false
    if (plan.exists(_.logicalLink.exists(_.isStreaming))) {
      logDebug("Pipelined shuffle: the plan is a streaming plan; leaving it to the streaming " +
        "engine's own pipelined-shuffle marking.")
      return false
    }
    if (conf.batchPipelinedShuffleEnabled) {
      val sc = plan.session.sparkContext
      return !sc.conf.get(config.SPECULATION_ENABLED) &&
        !org.apache.spark.util.Utils.isDynamicAllocationEnabled(sc.conf) &&
        SparkEnv.get.pipelinedShuffleManager.supportsBatchExecution
    }
    if (!SparkEnv.get.pipelinedShuffleManager.isInstanceOf[PipelinedChannelShuffleManager]) {
      // WARN, not DEBUG, and once per JVM: the user asked for this feature and is silently not
      // getting it, which no plan or metric reveals. The flag alone cannot select the transport --
      // the manager is a separate, start-up-only config -- so the mismatch is a misconfiguration
      // the user has to act on, unlike the plan-shape fallbacks (reuse, coalesce) which are normal
      // outcomes and stay at DEBUG.
      if (mismatchWarned.compareAndSet(false, true)) {
        logWarning(s"${SQLConf.LOCAL_PIPELINED_SHUFFLE_ENABLED.key} is enabled but " +
          s"${config.SHUFFLE_MANAGER_INCREMENTAL.key} is not the in-process channel manager, so " +
          s"no shuffle will be pipelined. Set it to " +
          s"${classOf[PipelinedChannelShuffleManager].getName} to enable the feature, or unset " +
          s"the SQL flag to silence this warning.")
      }
      return false
    }
    true
  }
}
