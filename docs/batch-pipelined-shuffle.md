---
layout: global
title: Experimental batch pipelined shuffle
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Experimental batch pipelined shuffle

This opt-in physical planning rule joins adjacent stages with live shuffle streams when their
tasks can run concurrently. Core scheduling owns admission and readiness, with or without AQE.
It works with the in-process channel transport on a multicore machine and the RPC transport on a
static cluster. It is intended for research on latency and materialization overhead.

The September 17 follow-up uses distributed RPC measurements to isolate operator-memory and
scheduling costs. Correctness and a faster transport do not establish a general query speedup:
the retained memory/readiness/admission implementation passes both 103-query runs, with coalesced
summed medians 0.68% lower than ordinary with catalog statistics and 0.92% higher without. Neither
establishes a suite-wide gain. Readiness removes a reproducible spill pattern, but one full-run q72
sample still spills. A further internal-reader sizing extension removed tasks without improving
the measured critical path and was reverted.
See the [current results and profiles](batch-pipelined-shuffle-research.html#distributed-follow-up-september-17-2026)
for controls, source versions and fractional-CPU results. Earlier local results measure their
recorded implementations and do not validate the latest distributed changes.

## Enable

Disable speculation and dynamic allocation, and set:

```
spark.sql.shuffle.pipelined.enabled=true
```

For `local[N]`, select the channel transport at SparkContext creation:

```
spark.shuffle.manager.incremental=org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager
```

For a cluster, use the default RPC transport, or select it explicitly:

```
spark.shuffle.manager.incremental=org.apache.spark.shuffle.streaming.StreamingShuffleManager
```

Choose input and shuffle parallelism appropriate to the available cores. Every task in a connected
pipelined group must fit simultaneously. For example, two producers with two partitions each and
a consumer with two partitions require six task slots. The rule preserves requested partition
counts and retains materialization boundaries when a larger group would not fit. An exchange with
200 partitions will normally remain materialized on a small machine. Runtime admission checks
actual RDD widths and queues groups while other work occupies their slots.

### Estimated live reader coalescing

An additional opt-in experiment reduces the physical reader count before writers start:

```
spark.sql.shuffle.pipelined.coalescePartitions.enabled=true
```

It uses estimated physical output sizes with Spark's existing adaptive partition-size, parallelism
and maximum reducer fan-in settings. It also requires
`spark.sql.adaptive.coalescePartitions.enabled`,
but does not require adaptive execution. Compatible join inputs receive the same layout; unknown
estimates, explicit partition counts and incompatible fixed inputs retain their current layout.

Partial aggregates need special handling: their logical link can describe the final result, not
the intermediate rows entering the shuffle. The rule uses Catalyst's existing group-cardinality
estimator on the physical grouping keys and buffer outputs, accounts for groups repeated across
producers, and caps grouped output at the input row count. Without column statistics it retains
an input-sized estimate adjusted for row width. Opaque aggregate buffers and grouping keys absent
from the logical input retain their existing reader count. No query names or fitted selectivities enter
the policy. Standard catalog statistics can improve its estimates. When an adaptive stage provides
runtime rows and bytes without column statistics, the sizing calculation can recover the original
catalog estimate. It preserves observed empty inputs and does not change AQE's runtime statistics.
Catalog estimates can be stale; they choose concurrency, never which rows to execute.

The optional setting
`spark.sql.shuffle.pipelined.coalescePartitions.parallelismFirst` controls live-reader sizing.
When unset, it inherits `spark.sql.adaptive.coalescePartitions.parallelismFirst`. Setting only the
live option to `false` lets live readers use the advisory size while ordinary AQE retains its
parallelism policy. An explicit `spark.sql.adaptive.coalescePartitions.minPartitionNum` takes
precedence over both. The advisory size and maximum reducer fan-in remain standard Spark settings.

`CoalescedHashPartitioning` keeps the original hash buckets and groups contiguous buckets into
physical readers. Writers route directly to the physical reader, which still consumes one stream.
This avoids sequentially draining several bounded streams and needs no new transport protocol.
The layout is fixed before execution. Estimates are not runtime shuffle statistics and cannot
justify dropping empty-looking buckets or splitting skewed keys.

This first implementation coalesces exchanges already selected for pipelining. It does not make an
otherwise over-capacity, 200-partition exchange eligible. Poor size estimates can also preserve
unnecessary tasks or reduce useful parallelism. The feature is disabled by default while measured.

The earlier [coalescing measurements](batch-pipelined-shuffle-research.html#estimated-live-reader-coalescing-september-16-follow-up)
separate ordinary shuffle, the preceding pipeline and reader coalescing. The distributed tests use
four-GiB executor heaps with `spark.memory.fraction=0.4` and
`spark.unsafe.sorter.spill.merge.factor=8` in all modes. These are experimental settings, not new
defaults: bounded merging adds I/O, and reducing managed memory reserves heap for other buffers.
The accompanying spill-reader fix releases task-completion ownership when a reader closes while
preserving its last record for the caller. Neither bounded transport nor fractional CPU admission
bounds the memory used by downstream operators.

### Memory and initial input readiness

Default task pages use the existing memory-page formula with admitted task concurrency as well as
executor cores. Fractional CPU reservations can therefore select smaller pages. Each task captures
the size for its actual CPU reservation; an explicit `spark.buffer.pageSize` still takes precedence.
This controls allocation granularity, not the task's total memory allowance.

Finite batch live readers wait for first input or EOF before returning their iterator to downstream
operator initialization. This avoids registering idle sorts and aggregate maps in managed-memory
sharing before they can consume a row. The reader preserves its first row and existing cleanup,
failure and interruption behavior. It still owns a Spark task slot and transport buffers while
waiting; this is not cooperative task suspension.

### Reuse RPC connection workers

An independent transport experiment reuses connection workers across completed readers:

```
spark.shuffle.streaming.reuseClientWorkers.enabled=true
```

Set this when creating the SparkContext. Each active reader exclusively borrows an executor with
its own connection queue and the existing connection parallelism. Workers return to the manager
only after discovery has terminated and an explicit count confirms that every submitted connection
and its callbacks have finished. Cancellation retires executors with outstanding work. Repeated
reader cleanup cannot interrupt a subsequent borrower, and manager shutdown closes all executors.
Idle worker threads expire after a minute.

The option is disabled by default. It targets RPC reader setup overhead; live task admission,
operator memory pressure and the finite-batch failure contract still apply.

## Execution contract

- `PipelinedShuffleManager.supportsBatchExecution` opts a transport into finite batch groups.
  Each scheduled reduce partition has one reader, and successful batch readers finish the stream before
  releasing transport resources. The existing `requiresDetachedRecords` capability controls row
  copying for transports that share objects across threads.
- `supportsLiveReducePartitionHints` lets both transports honor partial actions such as scalar
  subqueries and `executeTake`. The scheduler maps the requested result subset to reduce IDs;
  writers send to and await only those readers. Each later partial job starts fresh producers.
  An unknown mapping is rejected before producers start.
- `SparkPlan.estimatedPartitionCount` describes execution width without claiming a distribution.
  File scans plan their splits without creating readers or broadcasting configuration. Plain unions
  sum their input widths without claiming a distribution. Runtime pruning keeps the estimate
  unknown until its inputs are ready.
- `PlanBatchPipelinedShuffle` selects internal edges using reader compatibility, exchange sharing
  and concurrent task demand. It retains statistics needed for adaptive broadcast joins, unless
  the join strategy is hinted or broadcasting is disabled. Global aggregates emitting at most one
  row per producer remain materialized. An unfinished broadcast keeps the probe's input boundary
  so its producers can run independently. There are no query names or benchmark-specific costs.
- `PipelinedExecutionPlan` groups actual stages connected by live dependencies; durable edges
  connect groups. DAGScheduler can reserve a group together with its unfinished inputs when they
  fit, letting independent producers overlap a materialized sibling. Otherwise it runs input groups
  first. A finite ordinary group reserves one progress slot and may execute in waves; barriers and
  live groups retain their full concurrent demand. Reservations
  include consumers that have not started yet. A completed stage releases its reservation;
  unfinished members retain theirs. A failed execution cannot retry a completed member in place.
- `ShuffleExchangeExec.materializeForAdaptivePlanning` distinguishes an AQE statistics boundary
  from a durable input executed inside the submitted graph. The latter still writes a normal
  shuffle; only the per-input submission barrier is removed. Join statistics, broadcasts, sharing
  and unsupported readers retain their existing boundaries. Ordinary `ShuffleQueryStageExec`
  continues to provide adaptive statistics, coalescing and reuse at the retained boundaries.
- Top-N keeps its driver merge for collection. Its executor-side shuffle can use a pipelined
  dependency when it consumes live input. The planner budgets that extra producer stage even
  when collection ultimately does not need it. Projection and offset are unchanged.

Range-partition sampling, shared exchanges, unsupported partial mappings, multi-partition readers,
cache construction and arbitrary RDD consumers retain materialized boundaries. This is a
compatibility policy, not a prediction that every eligible exchange will be faster.

Failures abort the execution containing the live groups. Individual transient stages are not
retried, and loss of a materialized group output can require retrying the query. Concurrent actions cannot share a live
shuffle. Dynamic allocation, speculation and non-default resource profiles are unsupported.
Bounded transport buffers do not imply a bound on the memory used by downstream operators.

## Validate and measure

```
build/sbt 'core/testOnly *DAGSchedulerSuite -- -z "pipelined shuffle"'
build/sbt 'sql/testOnly *CoalescePipelinedShufflePartitionsSuite *BatchPipelinedShuffleSuite *BatchPipelinedShuffleClusterSuite'

```

For the benchmark, remove SBT's test-only execution settings. In Bash or Zsh, define this helper
and use it for the benchmark commands below:

```bash
spark_benchmark() {
  env -u SPARK_TESTING build/sbt "$@" 'project sql' \
    'set Test / envVars ~= (_ - "SPARK_TESTING")' \
    'set Test / javaOptions ~= (_.filterNot(o => o.startsWith("-Dspark.testing") || o.startsWith("-Dspark.memory.debugFill") || o.startsWith("-ea")) :+ "-Dspark.memory.debugFill=false")' \
    'set Test / javaOptions += "-XX:ReservedCodeCacheSize=512m"' \
    "Test/runMain org.apache.spark.sql.execution.benchmark.BatchPipelinedShuffleBenchmark ${BENCHMARK_ARGS:-}"
}
spark_benchmark
```

The harness clears the testing property set by `BenchmarkBase` and prints its effective runtime
settings. Require `memoryDebugFill=false, testing=false, assertions=false` in benchmark logs.
Debug memory filling adds large page writes under the heap allocator's lock and can distort the
concurrency comparison. Keep these settings enabled for correctness tests.

The cluster suite launches separate executor JVMs with the RPC transport. The benchmark checks
rows against ordinary AQE and reports the number of actual pipelined and materialized exchanges
before timing. Diagnostic runs also capture exchange counts from every timed plan after execution;
AQE can make different choices between repetitions. Both modes use the same source data and
partition settings. Zero pipelined exchanges means fallback, not a demonstrated pipelining benefit.

With `--diagnose`, `spark.shuffle.benchmark.coalescingAblation=true` compares ordinary shuffle,
pipelining, and pipelining with estimated reader coalescing in the same JVM. It rotates their
measurement order and validates every mode during warmup. `spark.shuffle.benchmark.warmups`
controls warmup repetitions per mode, and `spark.shuffle.benchmark.iterations` controls measured
repetitions. Sample files record logical bucket counts, physical readers, tasks and CPU time;
`--profile` additionally records JFR data. Keep profiled and unprofiled timing results separate.

`spark.shuffle.benchmark.catalogStatistics=true` registers external Parquet tables, runs standard
`ANALYZE TABLE ... FOR ALL COLUMNS`, and enables Spark's existing CBO for every benchmark mode.
Setup time is printed separately and excluded from query timings. Report this as a separate
experiment: statistics can change join plans and live-exchange eligibility as well as reader size.

For small TPC-DS experiments, generate data with `dsdgen`, convert it once, then compare:

```
BENCHMARK_ARGS="--prepare-tpcds /path/to/raw /path/to/parquet" spark_benchmark
BENCHMARK_ARGS="--tpcds /path/to/parquet" spark_benchmark
```

An optional comma-separated query list after the Parquet path restricts a diagnostic run. Use the
full query set for reporting, include fallbacks and regressions, and report the scale, partition
settings, transport, executor layout and both timings. These are research measurements, not audited
TPC-DS results. The harness collects results in both modes, so use it with result sets that fit in
driver memory. Add `--explain` to inspect both executed plans outside the timed region.

For a full suite in the SBT test JVM, set `HEAP_SIZE=12g` (or size it for the workload). The harness
limits retained UI history so hundreds of repeated executions do not accumulate query plans.
The scan parallelism also matters: `spark.sql.files.minPartitionNum` defaults to the machine's
parallelism. A scan occupying every slot cannot overlap its consumers. Compare a lower scan
parallelism with identical settings in both modes before changing the selection policy.

## Earlier measurements and runtime correction

The measurements below predate removal of SBT's debug memory filling and testing settings.
Local runs used debug filling in task execution; distributed executor JVMs did not, but their
SBT driver did use test settings. Retain these as historical diagnostic evidence. The controlled
scheduling experiment in the [research notes](batch-pipelined-shuffle-research.html) repeats the
comparison with normal runtime settings. Do not combine these trials into one estimate.

## AWS SF1 validation: September 15, 2026

The local run used an m7i.2xlarge (8 vCPUs, 4 physical cores with SMT), Java 21, `local[8]`,
the channel transport, 12 GiB heap, 512 MiB code cache, and two scan/shuffle partitions. All 103
queries returned the same rows as ordinary AQE. Eight alternating pairs per query followed three
warmups per mode, in one JVM without JFR during timing. The data occupied 363 MiB in Parquet at SF1
and fit in the hosts' page caches. These are warm-cache latency measurements.

| Measure | Ordinary AQE, ms | Pipelined mode, ms | Change |
| --- | ---: | ---: | ---: |
| Sum of means, all 103 | 64,261 | 63,305 | -1.49% |
| Sum of medians, all 103 | 63,489 | 62,475 | -1.60% |

This is a modest one-trial gain. Ten queries improved by more than 10% and ten regressed by more
than 10%, counting all queries. For example, q51 improved from 1,068 to 841 ms, while q28 regressed from 381 to 439 ms and
q49 from 343 to 391 ms. The complete per-query results are in
[`BatchPipelinedShuffleBenchmark-tpcds-sf1-aws-local.csv`](../sql/core/benchmarks/BatchPipelinedShuffleBenchmark-tpcds-sf1-aws-local.csv).

For q28, median aggregate task CPU fell from 1,349 to 1,163 ms while elapsed time rose.
The pipeline ran 30 tasks instead of 24, and waiting consumers occupied slots that otherwise
ran independent producers. In one measured pair, the last CPU-heavy producer stage started at
307 ms with pipelining versus 202 ms with regular execution. Aggregate fetch wait is not a
critical-path measurement. This supports work on reader coalescing and cooperative scheduling;
it does not justify a query-specific optimizer exception. Regular AQE can coalesce materialized
partitions, while live readers currently preserve them.

The original harness classified queries using a separate correctness pass: 72 had pipelines and
31 had none in that pass. Those labels do not reliably describe every timed repetition. For
example, q57 and q75 had no pipelines in correctness checking but did in their first timed plans.
The full-suite latency totals above are unaffected, but snapshot-based subgroup totals should not
be treated as pure pipeline/fallback comparisons. Subsequent experiments record exchange counts
for every timing sample. Among the 72 snapshot-tagged queries, 71 launched more tasks and one the
same number as regular AQE; this is descriptive, not a causal subgroup estimate.

The distributed follow-up uses two executors on separate hosts in one availability zone, with
four cores and 4 GiB heap each. Driver/SBT and the colocated executor are pinned to disjoint
physical cores. Both hosts have identical runtime jar and data checksums. Its first attempt exposed
a missing RPC partial-read contract; see the [research notes](batch-pipelined-shuffle-research.html).

After that fix, the complete distributed baseline checked all 103 queries and ran four alternating
pairs per query. This version has shared client I/O resources but still computes CRC incrementally
per row. Every query used both hosts in each execution mode. The m7i.2xlarge hosted the driver
and one executor; a c7i.xlarge hosted the other executor.

| Distributed baseline | Ordinary AQE, ms | Pipelined mode, ms | Change |
| --- | ---: | ---: | ---: |
| Sum of means, all 103 | 78,853 | 78,123 | -0.93% |
| Sum of medians, all 103 | 78,253 | 77,395 | -1.10% |

Sixty queries had a lower median; 16 improved by more than 10% and 12 regressed by more than 10%.
The largest examples include q31 (548 to 700 ms) and q56 (350 to 430 ms) regressions, versus
q3 (296 to 221 ms) and q51 (1,067 to 992 ms) improvements. These are one-trial observations,
including fallbacks, with the same snapshot-classification limitation described above.
[`BatchPipelinedShuffleBenchmark-tpcds-sf1-aws-cluster-before-checksum.csv`](../sql/core/benchmarks/BatchPipelinedShuffleBenchmark-tpcds-sf1-aws-cluster-before-checksum.csv)
retains every query and marks the classification source explicitly.

### Final distributed run after transport fixes

The same two hosts then ran all 103 queries again with shared reader I/O, partial-reader support,
batched CRC32C and the serializer buffering hint. There were four alternating pairs per query,
three warmups per mode, and no JFR during timing. Every query used both hosts in both modes and
returned matching rows. The harness now records exchange counts for every measured execution:
72 queries used pipelines in all four repetitions and 31 used none; there were no mixed cases.

| Final distributed run | Ordinary AQE, ms | Pipelined mode, ms | Change |
| --- | ---: | ---: | ---: |
| Sum of means, all 103 | 78,593 | 78,503 | -0.11% |
| Sum of medians, all 103 | 77,961 | 76,974 | -1.27% |
| Sum of means, 72 consistently pipelined queries | 58,992 | 59,033 | +0.07% |
| Sum of medians, 72 consistently pipelined queries | 58,704 | 57,690 | -1.73% |

**The full-suite mean is effectively flat.** The geometric mean of median speedups was 1.028x;
63 queries had a lower median. Among the 72 consistently pipelined queries, 19 improved by more
than 10% and six regressed by more than 10%. Counting fallbacks as well gives 23 and 11, respectively;
fallback differences are controls, not evidence of live-shuffle benefits.

The largest contribution to the mean regression was q72: 6,305 to 7,583 ms mean. One pipeline
sample took 10,136 ms; the other three took 6,632–6,837 ms. The slow sample also consumed more task
CPU (20,081 ms versus 13,216–14,012 ms). Its exact cause was not isolated; it remains in all totals.
Neither executor reported allocation failure or code-cache exhaustion. Host monitoring recorded
no swapping and at most 1% CPU steal in a sample. Four pairs in one JVM do not establish a stable small-percentage improvement.

Other representative medians were q51 1,096 to 941 ms, q31 579 to 577 ms, q49 431 to 412 ms,
q28 427 to 434 ms, q4 1,444 to 1,577 ms, and q56 355 to 375 ms. These are within-run comparisons;
changes between separate JVMs cannot all be attributed to the transport patch. The isolated
profiles establish removal of specific CPU/allocation work, while the full run exposes remaining
planning and scheduling costs. There is no universal speedup claim.

[`BatchPipelinedShuffleBenchmark-tpcds-sf1-aws-cluster.csv`](../sql/core/benchmarks/BatchPipelinedShuffleBenchmark-tpcds-sf1-aws-cluster.csv)
contains every query, per-mode variation, task metrics, host counts and actual timing classifications.
The [research notes](batch-pipelined-shuffle-research.html) contain separate checksum/serializer and
broadcast-policy comparisons, plus the mixed-input readiness diagnosis.

The two temporary EC2 instances, their root volumes, the security group and the SSH key pair were
removed after collection; deletion was verified. Runtime-based on-demand compute cost was about
$1.32, excluding the small EBS and public IPv4 charges. Raw samples, plans, JFR recordings, source
manifests and cleanup receipts are retained locally under `/tmp/spark-shuffle-review/aws/`.

## Full paired run after the planning and Top-N fixes

The complete SF1 run checked all 103 queries in one JVM, with eight alternating-order pairs per
query, three warmup executions per mode, a 12 GiB heap and no JFR recording during timing. Other
settings match the initial experiment below. It completed without allocation failures. The separate correctness snapshots classified 77
queries as pipelined and 26 as materialized. As with the original AWS measurements, these historical
labels do not establish the selection in every timed plan.

| Measure | Ordinary AQE, ms | Pipelined mode, ms | Change |
| --- | ---: | ---: | ---: |
| Sum of per-query means, all 103 | 49,461 | 48,464 | -2.0% |
| Sum of per-query medians, all 103 | 47,755 | 47,115 | -1.3% |
| Sum of medians, 77 snapshot-tagged pipeline queries | 36,744 | 35,637 | -3.0% |
| Sum of medians, 26 snapshot-tagged fallback queries | 11,010 | 11,477 | +4.2% |

The geometric mean of per-query median speedups was 1.017x. Among the snapshot-tagged pipeline queries,
17 improved by more than 10% and 12 regressed by more than 10%. Examples of median speedups:
q99 1.53x, q98 1.42x, q51 1.29x, q39a 1.24x; regressions include q79 0.82x, q56 0.83x
and q49 0.83x. Every query is included in the aggregate, including fallbacks and regressions.

These are modest overall gains in one full trial. The fallback variation, including high variance
on q97 despite the same task count and nearly identical task CPU, is a reason not to claim a stable
universal speedup. The isolated planning ablation and profiles identify concrete overhead reductions;
more independent full trials would be needed for a strong small-percentage performance claim.

[`BatchPipelinedShuffleBenchmark-tpcds-sf1-paired.csv`](../sql/core/benchmarks/BatchPipelinedShuffleBenchmark-tpcds-sf1-paired.csv)
contains per-query means, medians, standard deviations, task counts, CPU, waits and exchange counts.
The earlier CSV below is retained as historical evidence, not overwritten.

## Initial experiment, before profiling fixes: September 15, 2026

Based on Spark trunk `9b4e4f7547c07c5eba3d2b0822a6d3dbb023a150`, with the initial patch.
This historical result predates the profiling fixes described below.
The measurements used an Apple M5 Pro, macOS 26.4, OpenJDK 21.0.12.1, `local[8]`, the channel
transport, an 8 GB test JVM heap, a 512 MB code cache, two shuffle partitions and
`spark.sql.files.minPartitionNum=2`. AQE was enabled in both modes. The benchmark contains no
query-specific selection settings.

TPC-DS data was generated at SF1 with dsdgen 2.13.0 and converted to approximately 379 MiB of
Parquet. All 103 query variants in Spark's `tpcds` test resources returned matching rows.
89 query plans used pipelined exchanges; 14 retained materialized execution.

| Workload | Ordinary AQE average, ms | Pipelined mode average, ms |
| --- | ---: | ---: |
| Synthetic aggregation and Top-N | 135 | 50 |
| Synthetic shuffled join and aggregation | 93 | 57 |
| TPC-DS SF1, sum of per-query averages | 46,429 | 48,167 |

The synthetic cases improved by 2.70x and 1.63x. TPC-DS was **3.7% slower in aggregate**;
the geometric mean of per-query speedups was 0.947x. Among the 89 queries using pipelining,
13 had more than 10% lower average latency and 28 had more than 10% higher latency.
This demonstrates working execution, but does not establish an overall TPC-DS speedup.

The full per-query timings, standard deviations and exchange counts are in
[`BatchPipelinedShuffleBenchmark-tpcds-sf1.csv`](../sql/core/benchmarks/BatchPipelinedShuffleBenchmark-tpcds-sf1.csv).
Speedup is ordinary average time divided by pipelined-mode average time. A zero pipelined
exchange count is a fallback, so its timing difference cannot be attributed to pipelining.

These are exploratory paired measurements: ordinary AQE ran first, each case warmed up for one
second, and timing ran for at least three iterations and three seconds. They were not randomized
or repeated across independent full trials. A network-interface change interrupted q50 during
timing; q50 and q51-q99 were rerun with the driver bound to loopback in fresh JVMs. No interrupted
timing was retained. An earlier run with a 4 GB heap encountered allocation failures and is not
included in this table. The results do not isolate the causes of regressions; lost AQE decisions,
group concurrency and operator overhead require profiling before adding a profitability policy.

The recorded configuration can be reproduced with Java 21 and an existing Parquet dataset:

```
HEAP_SIZE=8g build/sbt \
  -Dspark.sql.files.minPartitionNum=2 \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  'project sql' \
  'set Test / javaOptions += "-XX:ReservedCodeCacheSize=512m"' \
  'Test/runMain org.apache.spark.sql.execution.benchmark.BatchPipelinedShuffleBenchmark --tpcds /path/to/parquet'
```

Validation also passed all 241 `DAGSchedulerSuite` cases, 27 new local/channel, local/RPC and
separate-executor RPC cases, the selected existing AQE and shuffle SQL regression suites, and
Scala style checks for core, Catalyst and SQL. That initial experiment did not cover production
failure recovery or separate-executor TPC-DS performance; the later RPC subset is reported below.


## Profiling and upstream review

`--diagnose <directory>` writes individual alternating-order pairs, separate initial-planning and
execution times, final physical plans, and per-task timings. `--profile` additionally records JFR
CPU, allocation and blocking events after warmup. `spark.shuffle.benchmark.iterations` selects the
number of pairs. Each timed action uses a fresh query plan. Correctness checking and plan inspection
are outside the timed region. Adaptive replanning is included in execution time.

```
HEAP_SIZE=12g \
BENCHMARK_ARGS="--diagnose /tmp/pipeline-profile --profile --tpcds /path/to/parquet q9,q28,q31,q82,q83" \
spark_benchmark \
  -Dspark.sql.files.minPartitionNum=2 \
  -Dspark.driver.host=127.0.0.1 \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -Dspark.shuffle.benchmark.iterations=12
```

JFR timings are diagnostic, not headline latency results. Export stacks with
`jfr print --stack-depth 128`; its default output truncates important callers. Restrict CPU/allocations to
sample intervals and compare both modes; an inclusive `ChannelShuffleWriter.write` frame includes
all upstream scan and aggregation work and is not evidence that the queue itself is expensive.
Idle thread-pool parks must also be excluded when attributing waits to shuffle.

### Findings and changes

- **Eager reader creation:** width estimation used `inputRDD.getNumPartitions`. File readers
  broadcast their Hadoop configuration while being constructed, including on scan instances that
  AQE later discards. JFR stacks confirmed this path. File-split planning is now separate from
  reader/RDD creation, and V2 width estimation does not create a reader factory. A regression test
  asserts that estimating width never calls the file format's reader builder.
- **Lost adaptive join choices:** q82 and q83 retained sort-merge joins where ordinary AQE chose
  broadcast joins. q82's task CPU increased from about 624 to 802 ms in the initial profiling run.
  Join inputs now retain their statistics boundaries when adaptive broadcasting remains possible.
- **Extra Top-N work:** explicitly inserting a local Top-N plus a single-partition exchange added
  a task to collection. The existing Top-N operator now creates a pipelined dependency only on its
  executor path. Collection retains the driver merge. Tests cover task count, projection, offset,
  bounded buffers and both collection and writes on both transports.
- **Unproductive overlap:** a global aggregate emits one row per producer only after scanning its
  entire input. Its waiting reducers take slots from independent scans. These exchanges now remain
  materialized. Grouped aggregates can still benefit; no blanket aggregate exclusion is applied.
- **Missing read metrics:** the channel reader did not merge temporary task read metrics at task
  completion. Short jobs reported zero reads/waits unless a heartbeat or another reader happened
  to merge them. Completion now publishes the metrics, including successful draining. Submillisecond
  batch waits accumulate before conversion to milliseconds.
- **RPC thread multiplication:** the task-owned writer server inherited an executor-sized Netty
  worker pool. The synthetic aggregate's 12 profiled pipeline runs created 384 writer worker
  threads (four producers per run, eight workers each). Its default now uses one worker, matching
  the task-scoped reader's sizing; an explicit RPC I/O thread override is preserved. The protocol,
  acknowledgments, cancellation and byte-buffer ownership remain unchanged.
- **Remaining group contention:** q28/q31 have multiple independent aggregate branches. Pipelining
  uses more tasks and prevents coalescing, while reducers wait for producers. Faster transport does
  not eliminate that scheduling tradeoff. Do not interpret remaining regressions as queue CPU cost
  or introduce query-specific exclusions to conceal them. Summed task wait time can exceed query
  wall time and is not itself a critical-path measurement; faster queries may have more aggregate
  wait time because more tasks overlap.

### Upstream decisions checked

- [Concurrent-stage design discussion #57092](https://github.com/apache/spark/pull/57092): dependency
  type expresses the execution contract; regular shuffles delimit groups; reviewers specifically
  discussed queued admission, resource accounting, group failure and shared consumers. This patch
  extends mixed groups and queued admission while retaining single-reader and whole-group failure.
- [Local channel implementation/review #58097](https://github.com/apache/spark/pull/58097): retain
  batching and detached row ownership. The review explains why per-row queue operations, eager
  allocation and repeated graph walks are expensive. It also identifies hidden limit shuffles;
  making those dependencies pipeline-aware was explicitly deferred. The Top-N change follows
  that extension while retaining its cheaper collection path.
- [Shared RPC transport #56387](https://github.com/apache/spark/pull/56387): preserve direct ByteBuf
  sends and error propagation back to the task thread. Do not bypass buffer ownership, credit
  control or termination acknowledgments in a throughput optimization.
- [Writer review #56878](https://github.com/apache/spark/pull/56878) and
  [reader review #56879](https://github.com/apache/spark/pull/56879): acknowledgments confirm receipt
  through the final sequence number and cannot simply time out into successful completion.
  Buffer-memory limits are best-effort for oversized rows. Thread-pool sizing was not justified as
  a throughput requirement in these discussions; the change above is based on measured creation.
- [Open custom shuffle metrics #57355](https://github.com/apache/spark/pull/57355): proposes
  transport-specific write metrics in the SQL UI; read metrics are deferred. Useful for future
  queue/connection telemetry, but it does not fix the existing temporary-read-metric merge issue.
- [Open execution-memory fix #58747](https://github.com/apache/spark/pull/58747): addresses concurrent
  acquisition/release registration. Relevant to concurrent memory consumers, but the local profiles
  did not identify it as the latency bottleneck. It is not silently included in this patch.

PR state and discussions were checked on September 15, 2026. Spark's merged PRs often appear as
closed in GitHub because Spark uses its own merge script.


### Transport validation

The default RPC writer pool change passed all 59 reader, writer, manager and end-to-end transport
tests. JFR confirmed writer-worker thread starts fell from 384 to 48 over 12 aggregate repetitions,
an 87.5% reduction. Absolute timings changed substantially between profiling JVMs, including the
regular-shuffle controls; those separate runs do not establish the latency benefit of pool sizing.

With two separate executors (`local-cluster[2,4,1024]`, four cores and 1 GiB heap each), 12 alternating
pairs without JFR measured the synthetic aggregate/Top-N at 177.9 to 141.3 ms median, and the shuffled
join/aggregate at 144.0 to 99.4 ms. Both returned matching results and exercised RPC pipelines.
This tests process boundaries on one host, not a multi-host network or cluster failure recovery.

An eight-query SF1 subset also passed on two four-core executors with 4 GiB heaps, six alternating
pairs per query and two scan/shuffle partitions. This includes the original diagnostic regressions,
fallback controls and two larger positive cases:

| Query | Ordinary median, ms | Pipelined mode median, ms | Live exchanges |
| --- | ---: | ---: | ---: |
| q3 | 216.9 | 228.4 | 1 |
| q9 | 354.0 | 347.5 | 0 |
| q28 | 444.3 | 444.2 | 6 |
| q31 | 603.8 | 610.0 | 6 |
| q51 | 800.4 | 679.0 | 5 |
| q82 | 440.2 | 389.9 | 1 |
| q83 | 326.2 | 278.9 | 0 |
| q99 | 225.2 | 208.3 | 1 |

All are separate JVMs on the same host and share the local Parquet dataset. q9 and q83 are fallbacks;
their timing differences are controls, not RPC-pipelining gains. This selected diagnostic subset
must not be presented as a distributed full-suite TPC-DS speedup. Full figures are in
[`BatchPipelinedShuffleBenchmark-tpcds-sf1-rpc-subset.csv`](../sql/core/benchmarks/BatchPipelinedShuffleBenchmark-tpcds-sf1-rpc-subset.csv).

Earlier transport validation passed 22 channel tests, 56 SQL tests, and 72 existing AQE, pipelined
SQL, coalescing and shuffled-RDD tests. After adding core execution groups and releasing completed
stages' reservations, validation passed all 246 `DAGSchedulerSuite` tests, 42 batch SQL tests
(including separate executors), 52 existing pipelined SQL tests, and ten real-time-mode shuffle
cases. The shared transport passed 56 streaming-shuffle tests and 26 plain/TLS network tests.
Core and SQL production/test Scala style checks passed. Some suites emitted possible-thread-leak
warnings during shutdown; passing tests do not establish leak-free execution.

### Remaining performance work

The current live-reader contract fixes one reduce partition per reader. Reusing AQE's coalescing
over existing streams requires fair multiplexing; sequentially draining them can deadlock. Another
option is to agree on a coalesced destination mapping before launching producers and route directly
to those physical readers. Both approaches need consistent partitioning and scheduler accounting.
A queue-size change cannot solve this loss of coalescing.

RPC readers now share executor-owned client I/O workers and allocators, with an independently owned
handler and channel for each connection. The writer experiment similarly shares I/O workers and
an allocator, while each writer owns its listener, accepted connections and protocol state.
Discovery, connection-creation workers and flush timers remain task-owned. Combining listeners
would additionally need explicit per-shuffle/task routing and is not part of this change.

Selection can use actual statistics from retained materialized boundaries as the experiment grows.
The present policy deliberately has no fitted thresholds, TPC-DS query identifiers or scale-factor
exceptions. Profiles show why capacity and reader compatibility are necessary but insufficient to
predict which blocking stage groups will improve latency.

See [the MPP research and follow-up experiments](batch-pipelined-shuffle-research.html) for the
primary sources, resource-lifetime changes, broadcast readiness and remaining scheduler abstractions.
