---
layout: global
title: Batch pipelined shuffle development and experiment handoff
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

# Batch pipelined shuffle: development and experiment handoff

This document records the September 2026 implementation, experiments, rejected changes,
and remaining work. Publication and CI status were checked on October 13, 2026. Start with
the [usage and design guide](batch-pipelined-shuffle.md) for configuration and restrictions.

## What is in the fork

The implementation adds opt-in streaming shuffle to finite batch execution in Spark core
and SQL/AQE. Eligibility depends on execution graphs, capabilities, resources, and partition
properties. It contains no query-name selection or workload-specific optimizer/codegen rules.
It is experimental, disabled by default, and does not change Structured Streaming planning.

- Review: [ForVic/spark PR #1](https://github.com/ForVic/spark/pull/1), a draft in the fork.
- Branch: `codex/batch-streaming-shuffle`.
- Implementation commit: `40395a2336701637c11790d57a719dde3e450807`.
- Review base: `29da84aa64eb807f6127b1bcd4edad90ad9ea0e9`, pinned by the fork branch
  `codex/batch-streaming-shuffle-base`. This is not the latest upstream `master`.

Keep three things separate:

| Artifact | Status and meaning |
| --- | --- |
| Published implementation | Rebased, simplified code for review; targeted local tests passed, but CI has failures listed below. |
| Frozen integrated runtime | Earlier, hash-verified binary runtime used for the completed all-103-query SF100 comparison. |
| Isolated follow-up candidates | Statistics, placement, reader sizing, and codegen experiments; their results did not justify promotion. |

**The historical 8.81% result does not measure the published implementation commit.**
Rebuilding this branch, or the original development checkout, does not recreate the frozen
runtime. A new controlled comparison is required before assigning that result to the PR head.

## Where to read the code

Paths below are relative to the repository root. The test suites are useful entry points
for the invariants, including cancellation and repeated actions.

| Responsibility | Main implementation | Relevant suites |
| --- | --- | --- |
| Execution groups and admission | `core/.../scheduler/PipelinedExecutionPlan.scala`, `DAGScheduler.scala`, `TaskSchedulerImpl.scala` | `DAGSchedulerSuite`, `TaskSchedulerImplSuite` |
| Executor CPU permits and task residency | `core/.../executor/ExecutorTaskScheduler.scala`; local and coarse-grained scheduler backends | `ExecutorTaskSchedulerSuite`, backend suites |
| Completed memory output and disk fallback | `core/.../shuffle/sort/MemoryShuffleBlockResolver.scala`, `MemoryShuffleWriter.scala` | `MemoryShuffleSuite` |
| Incremental pages, receive capacity, and reader lifetime | `core/.../shuffle/streaming/BatchMemoryShuffleManager.scala`, `BatchMemoryShuffleReader.scala`, `BatchMemoryShuffleWriter.scala` | `BatchMemoryShuffleSuite` |
| SQL eligibility, live boundaries, and estimated layouts | `sql/core/.../execution/exchange/PlanBatchPipelinedShuffle.scala`, `PipelinedShuffleEligibility.scala`, `EnablePipelinedShuffle.scala`, `CoalescePipelinedShufflePartitions.scala` | `BatchPipelinedShuffleSuite`, `CoalescePipelinedShufflePartitionsSuite` |
| Adaptive execution | `sql/core/.../execution/adaptive/AQEEnablePipelinedShuffle.scala` | `BatchMemoryAdaptiveShuffleSuite` |

Here `core/...` expands to `core/src/main/scala/org/apache/spark`, and `sql/core/...`
expands to `sql/core/src/main/scala/org/apache/spark/sql`.

Important invariants are executor ownership of completed output, charged page references
until the last reader releases them, receive capacity that producers cannot consume, and
execution epochs that reject late publication. The scheduler must reserve reader progress
without admitting every ordinary task at once. SQL must retain materialized boundaries
needed by adaptive decisions, reuse, and unsupported consumers.

Blocked tasks lend CPU permits while retaining their JVM threads, stacks, and operator
state. This is not resumable operator execution, and the shuffle-page budget does not bound
all query memory. Live failures abort the affected execution instead of independently retrying
producers whose uncommitted data may already have been consumed.

## How the implementation evolved

The early experiments used local and distributed SF1, followed by larger distributed probes.
They often compared ordinary, sealed-memory, and live modes within a modified build. Those
controls are useful for mechanisms, but they are not clean upstream Spark.

1. **Transport and ownership.** Early task-owned streaming required connection setup,
   polling, copies, and coordination across task lifetimes. Work moved toward executor-owned
   completed memory output and an executor transport endpoint, with bounded pages, spilling,
   and retained-buffer ownership. Optional connection-worker reuse reduced setup activity
   without improving the complete suite; that option was excluded from the public patch.
2. **Reader layout before admission.** Logical hash buckets were separated from physical
   reader tasks so compatible inputs could share a layout chosen before producers started.
   Same-layout materialized controls were needed to distinguish reduced task counts from
   transport gains. Unknown estimates and explicit partition counts keep conservative layouts.
3. **Memory under greater residency.** A page-size formula based only on physical cores
   could allocate large pages while many more operators were resident. Admission-aware page
   sizing reduced some spill. Waiting readers also registered execution-memory allocations
   before receiving rows, reducing producer allowances. Delaying downstream initialization
   until first input or EOF removed a reproducible q72 spill pattern: all 24 baseline samples
   spilled about 143 MiB, while all 24 candidate samples avoided it. Latency remained mixed.
4. **Finite mixed-stage graphs.** Source waves reserve capacity for readers. Ordinary
   materialized groups need minimum progress capacity rather than simultaneous admission of
   every task. An early q80 control demonstrated overlap with independent materialized siblings.
   This did not establish a general benefit from more concurrency.
5. **Avoid optimizing task count alone.** An extension sizing internal materialized readers
   reduced q80 tasks from 68 to 54 but lengthened its critical path. Its small control reported
   a 7.6% regression after ordinary-control normalization. The extension was reverted. Two
   earlier complete SF1 suites also showed no established suite-wide pipelining win: coalesced
   mode changed summed medians by -0.68% with statistics and +0.92% without statistics.
6. **Separate correctness, latency, and diagnostics.** Later experiments froze production
   jars and dependencies, audited actual driver/executor class origins, ran unprofiled timing
   separately from profiles, and used balanced whole-application order. This led to the clean
   upstream headline and the isolated follow-ups below.

These are development observations, not a sequence of interchangeable benchmark scores.
The early SF1 runs used different heaps, CPU allocation, observers, and protocols from SF100.

## Experiment ledger

Negative percentages mean less elapsed time. Unless stated otherwise, each percentage is
the change in the sum of per-query means. Query sets, baseline runtimes, and application
history differ between rows; do not combine them into one score.

| Experiment | Scope and comparator | Result | Decision |
| --- | --- | --- | --- |
| Early local statistics retention | SF1; local retention diagnostic | +43.39%, with 79 regressions | Retained as negative evidence; not a distributed SF100 comparison. |
| Dedicated statistics retention | All 103 SF1 queries; candidate versus frozen streaming | -0.43%; pairs -0.73% / -0.13%; 29 queries regress over 5% | Not promoted. Smaller broadcast builds can deepen dependencies. |
| Independent-source scheduling | 13 SF100 queries; candidate versus frozen | -0.18%; pairs -0.83% / +0.47%; q15 +14.51% | Not promoted. Better source placement did not consistently shorten the downstream path. |
| Reader width | Seven SF100 queries; candidate versus frozen | -0.71%, with negligible q1 stage benefit | Not promoted. |
| CHAR-IN selectivity | Seven SF100 queries; candidate versus frozen | +2.28%; q14b/q94 remain problematic | Not promoted. This planning experiment is distinct from the later string-IN codegen experiment. |
| Clean upstream headline | All 103 SF100 queries; one frozen integrated runtime versus unmodified upstream | **-8.81%; pairs -9.99% / -7.59%** | Completed historical headline; all repeats and regressions retained. |
| Nested-locality retest | 11 SF100 queries; nested candidate versus frozen | +3.58%; pairs +0.68% / +6.71% | Not promoted, despite q94 -16.27%. q51 +10.02% and q81 +16.91%, both worse in both pairs. |
| String-IN codegen prefix | 16 SF100 queries, q1 through q15 including q14a/q14b; candidate versus frozen | -0.87%; pairs -2.10% / +0.34% | Not promoted. q14b +6.49%; q15 +3.64%, including a +9.99% pair. |

The 11-query locality retest also measured frozen versus upstream at -0.23%
(+4.56% / -4.90%), and nested versus upstream at +3.35% (+5.27% / +1.48%).
These focused results do not replace the all-query headline.

The final 16-query prefix measured frozen versus upstream at -11.96% and string-IN versus
upstream at -12.73%. Its fixed unprofiled order was upstream / frozen / string-IN /
string-IN / frozen / upstream, followed by separate frozen and string-IN diagnostic apps.
Each query had an initial answer, two warmups, and two measured actions. All eight apps
completed; collection was later recovered without repeating any benchmark work.

Validation was part of each result, not inferred from an application's exit status:

| Collected experiment | Checked answers | Successful task events | Actual JVM runtime audits | Verified collected files |
| --- | ---: | ---: | ---: | ---: |
| Dedicated retention/source probes | 2,720 | 260,915 | 30 | 9,196 |
| Profile and reader/CHAR-IN follow-up | 1,912 | 307,398 | 48 | 6,967 |
| All-query headline | 2,060 | 679,662 | 12 | 6,956 |
| Nested-locality retest | 594 | 160,356 | 27 | 2,500 |
| Final string-IN prefix | 640 | 287,899 | 24 | 5,133 |

Counts include each protocol's warmups, measured actions, and initial answer checks as
applicable. The prefix's two observer smokes separately passed ten answers, 394 tasks, and
six runtime audits. All listed experiments reached verified collection and cleanup.

## The all-query SF100 result

The unmodified upstream control was commit `8e1f4f755337339a81602fccd644df87fca37cf5`.
Both runtimes used the same dedicated driver, two eight-core workers, 48 GiB executor heaps,
188 external dependencies, Java version, input data, neutral observer, and common settings.
One globally selected frozen integrated runtime ran every one of the 103 TPC-DS query variants.
This is a TPC-DS-derived research experiment, not an audited TPC-DS result.

Four applications ran sequentially: upstream A / streaming A / streaming B / upstream B.
The first pair compares the first two applications; the second compares the last two,
reversing execution order. Each query had an initial checked answer, two warmups, and two
measured actions in each application. Thus each runtime has four measured actions per query.
The combined result gives the two pairs equal weight; it does not select their better samples.

| Observation | Upstream sum | Frozen streaming sum | Change |
| --- | ---: | ---: | ---: |
| First application pair | 807.100 s | 726.488 s | -9.99% |
| Second application pair | 785.147 s | 725.520 s | -7.59% |
| Combined | 796.123 s | 726.004 s | -8.81% |

![Historical SF100 comparison, both application pairs and combined mean][headline-chart]

[Chart and benchmark provenance][results-readme] are on a separate results branch.
These totals are sums of query means, including planning, not total application wall time.
The small number of application pairs does not establish an independent-run confidence interval.

Ninety-four queries improved on average. The largest percentage improvement was q26,
2.638 s to 1.194 s (-54.76%, about 2.21x); both pairs improved by more than 53%.
The largest absolute saving was q64, 26.361 s to 17.116 s, about 9.245 s (-35.07%).

The integrated result includes planning, scheduling, sealed-memory output, and live transport.
Live-channel payload accounted for 2.67% of candidate shuffle output across 57 queries;
43 queries recorded pages read before producer commit. Published payload is not an exact
measure of bytes consumed before commit. The 8.81% gain cannot be attributed to streaming alone.

## Regression investigations and rejected fixes

### q94: locality can dominate a cheaper plan

q94 was the only headline mean regression above 5%: +12.90%, with pairs +26.51% / +0.02%.
Three of four frozen final joins put all 17 tasks on one executor; upstream always split 9/8.
The stage took 2.14-2.50 s on one executor versus 1.17-1.30 s on two. Task CPU fell 15.06%,
and no task disk spill was reported, so lower CPU did not imply lower elapsed time.

A 27.18 MB web-returns input supplied locality while nested zipped and mapped RDD inputs
hid a balanced 432.61 MB web-sales input. An isolated metadata propagation change preserved
conservative boundaries for cached, checkpointed, unknown, live, and partial reads. Its
focused assertion failed before the fix; 83 RDD tests, a SQL locality test, and styles passed.

The distributed retest confirmed balanced placement and a 16.27% q94 improvement versus
frozen, but q51 and q81 regressed in both pairs. q51 included one-executor live readers,
a long subsequent window stage, and higher CPU. q81 combined placement, a CPU spike, and
an AQE plan variation. These observations do not prove the metadata change caused every
regression. The candidate remains excluded from this PR.

### q15: generated-code history matters

The original headline slow action had 17.99 s task CPU versus 10.03 s in its neighbor with
the same final plan. That timing run did not record compilation or exact generated bytes.
Shortened-history profiles could not settle its cause.

A later locality diagnostic reproduced a CPU spike with generated join deoptimization and
C2 recompilation. Reconstructing the code aligned 17 method descriptors and bytecode locations,
but reconstruction alone could not prove the exact historical class bytes.

The final prefix therefore retained continuous JDK compilation/GC/Java-IO history and captured
loaded generated classes with typed JDK class and loader identities. Native CPU/wall recording
covered q15 only. A slow frozen measured action took 1.479 s and 15.557 s task CPU, versus
0.943 s and 10.090 s next. Both joins were balanced across workers; the slow action had no
GC pause. Four C2 `unstable_if` events mapped to individual `UTF8String.binaryEquals`
branches in the exact saved class, with recompilation overlapping the join.

The isolated string-IN candidate combined literal binary-string equalities using Boolean OR,
preserving null behavior and other collations/nonliteral short-circuit behavior. It changed
only `In.class` in the frozen Catalyst jar, without query-specific values or InSet thresholds.
It passed 72 predicate tests and all 103 SF1 queries (309 answers, 7,505 tasks), the latter
as local correctness checks only.

The reconstructed method shrank from 687 to 595 bytes, with 43 to 25 conditional branches.
An earlier report's 35-to-23 count was a parser error; the corrected count preserves the
original bytecode and does not change any performance sample. All twelve equalities still run,
so removing early exits can increase work. The candidate's measured profile lacked those
per-literal deoptimizations, but warmups still deoptimized at the iterator backedge and the
combined predicate. Unprofiled q15 CPU rose 8.04%, and its unfavorable pair remains evidence.

This is strong evidence for a generated-code instability mechanism in the later diagnostic,
not proof of the exact original headline action or a successful code fix. Neither string-IN
prototype is included in the public implementation.

### Other unresolved results

The headline pair-only regressions above 5% are q2, q15, q32, q37, q81, q85, and q99.
Together with q94, all eight remain in the investigation. q38/q51/q87 served as pipeline
controls. The final prefix adds q9/q13 pair regressions versus upstream and q3/q14b/q15
regressions for string-IN versus frozen; it cannot settle later full-history q51/q81 behavior.

q14b retained the same final plan while task CPU increased from 181.97 to 194.80 s across
large scans and joins. Planning added only 16.2 ms against 919.8 ms additional elapsed time.
q3 and q14b do not exercise the changed literal binary-string IN expression, so direct rule
causation is unproven. Their history and generated-code evidence need further review.

Never sum concurrent task CPU, task waits, shuffle timers, or overlapping stage spans into
query latency. Check per-host/action sample coverage, sample conservation, and stale method
IDs before attributing CPU categories. Earlier SF1 profiles contained stale symbols; affected
categories were withheld. Later locality and prefix profiles passed those checks with zero
stale samples. Java IO recordings omit native IO; resolver writes and missing task eviction
updates cannot establish how much retained output was evicted. Disappearance after warmup,
or on unchanged code, is not a demonstrated fix.

## Failed runs and measurement infrastructure

Failures were preserved rather than counted as benchmark successes:

- The first locality run completed remotely but lost most collection before host expiry.
  No SF100 timing directories survived; it supplies no performance result. Two intact SF1
  correctness applications were independently salvaged (618 answers, 32,744 tasks).
- The first prefix observer failed before Spark tasks because executor hostname lookup was
  unavailable. The next attempt answered its smoke queries but failed to finalize executor
  class mappings. Neither started a timing application.
- A later setup received input HTTP 404 before Spark started. Bucket expiration was consistent
  with deletion, but no deletion-event audit proved its cause. Another attempt lost a long
  setup SSH connection; one worker's exact outcome remained unknown. Neither yielded timing.
- The final detached-setup attempt completed all eight applications, then local authentication
  expired during collection and cleanup. Durable archives survived host retirement. After
  authentication returned on September 28, the unchanged collector recovered all 5,133 files,
  cleanup was independently verified, and the original analysis owner completed. No benchmark
  application was repeated and the expiry was not extended.

The resulting harness used exclusive detached host setup, source/role/nonce-bound completion
records, and collected setup evidence before starting Spark. Diagnostic capture finalized the
original JVMs through JDK Attach while they were alive, before releasing Spark shutdown.
Typed JDK identities were not equated with async-profiler IDs. Distributed smokes validated
actual runtime/observer origins, generated bytes, raw JFR, and bounded recording lifetimes.

Hosts uploaded result archives only after timing ended. Collection checked compressed hashes,
gzip footers, every archive member, and every persisted original before deleting remote copies.
This permits collection after EC2 expiry. Storage guards retained an 8 GiB reserve for analysis;
completed derived logs were compressed only after full decompression/hash verification.
These harnesses and operational receipts remain private artifacts, not shipped Spark features.

## Simplification before publication

The public preparation reduced the original 171-file development set to 115 changed files
at the implementation commit. It retained the transport, memory ownership, admission,
SQL/AQE integration, aggregate metrics, and tests for important lifetime and failure cases.

Removed from the public patch were workload-oriented optimizer/codegen changes, join-estimate
and hash-join experiments, independent-source and weighted/nested-locality prototypes,
raw diagnostic timeline/trace classes, benchmark CSVs and local harnesses, research diaries,
and unrelated configuration serialization changes. Unnecessary ready-map notifications,
cooperative-yield switches, and optional connection-worker reuse were also removed.
Original source, failed attempts, and negative results were preserved privately.

The pass also fixed a compression decoder leak on partial shuffle consumption: the new test
failed before the fix and passed afterward. Reader cleanup tracks and closes its deserialization
stream on exhaustion or completion. Local scheduler CPU accounting was made consistent with
actual task CPU reservations, and graph/capacity checks were simplified.

Local validation on the cleaned branch used JDK 21: 173 shuffle/runtime tests, 323 SQL/AQE
tests, 684 scheduler/storage/RDD tests, and 48 network tests passed (1,228 total). One existing
off-heap test skipped itself on Apple Silicon. Core/SQL production and test compilation,
core/SQL/Catalyst Scala style, and Checkstyle on 273 maintained Java files passed. Possible
daemon-thread-leak warnings remain recorded. This was targeted validation, not full CI.

## CI failures to address first

As checked on October 13, the [original implementation CI run][ci-run] has failures in
core, SQL-other, and Hive-other test jobs. Its [test-result annotations][ci-report] list:

- `CoarseGrainedExecutorBackendSuite` and its SSL variant: allocated-resource tracking
  times out; two cancellation tests in each suite hit a null `Executor.localTaskScheduler()`
  result. Inspect real behavior and test setup before changing mocks or assertions.
- `BlockStoreShuffleReaderSuite`: completion does not meet the resource-release assertion
  (`0 did not equal 1`).
- `SparkConfigBindingPolicySuite`: missing binding-policy declarations for
  `spark.shuffle.memory.enabled`, `spark.shuffle.memory.maxMapSize`, and
  `spark.shuffle.memory.maxSize`. Choose the appropriate policy; do not hide new settings
  in frozen-exception lists.
- `ExchangeSuite`: exchange string arguments end with `materializeForAdaptivePlanning`
  where the test expects `pipelined`; the existing positional `stringArgs` convention needs
  attention.

These are observed failures, not established root causes. Inspect every failed job, including
Hive; the annotations are not proof that every job failure has been explained. Linters,
documentation generation, precompilation, and the Java 25 Maven check passed on that run.
The branch is **not CI-green**. Enumerate current checks again after any new commit.

## Continuing from a cloud checkout

1. Check out the PR head and read the usage guide, this handoff, repository instructions,
   and `docs/building-spark.md`. Preserve the pinned base when reviewing this diff; updating
   to current upstream is a separate change. Follow repository test/build setup instructions.
2. Triage all CI failures above, make focused fixes, run the relevant suites and required
   style/build checks, and rerun CI. Do not claim the local test count proves current CI passes.
3. Audit memory/CPU accounting, cancellation, partial consumption, failure epochs, and
   adaptive boundaries before widening compatibility. Keep the feature opt-in.
4. If continuing performance work, first obtain the missing artifact bundle below and
   inspect completed results. Do not repeat completed runs merely because they are absent
   from this checkout, or promote the rejected locality/codegen candidates by assumption.
5. A benchmark of changed code needs a newly frozen runtime, clean unmodified upstream,
   matched hardware/data/dependencies/settings, balanced unprofiled application order, answer
   and actual-runtime validation, retained unfavorable repeats, and separate diagnostics.
   Choose one integrated runtime globally; never select a winner per query. Review every
   mean or pair regression above 5% and visually inspect all complete charts.

The recurring experiment schedule was paused at the owner's request. Historical AWS cleanup
was verified on September 28, including all known experiment compute and volumes; the shared
input data was retained. This document is not a current cloud resource inventory. A cloud
checkout does not transfer permission to restart that paused schedule or access private data.

## Evidence available and evidence to request

The fork contains the reviewable source, tests, usage guide, this history, and a separately
published aggregate chart. It does **not** contain the raw JFR recordings, full sample/event
logs, answers, Parquet inputs, frozen production jars, original launch/analysis harnesses,
or cloud credentials. It is not a turnkey reproduction bundle.

Ask the owner for a sanitized artifact transfer if that level of investigation is needed.
The following are artifact-relative identifiers for finding it; they are not paths or links
available in this checkout. Earlier diaries can contain superseded execution state, so use
the final synthesis and validation receipts for conclusions.

| Private artifact group | Start with |
| --- | --- |
| `batch-shuffle-generalization-20260924/final-comparison-ondemand` | `synthesis.md`, `protocol-spec.json`, `runtime-manifests.json`, `validated.json`, `analysis.json`, `regression-review.json`; original samples and all query charts |
| `batch-shuffle-generalization-20260924/candidate-probes-ondemand` | `synthesis.md`, `investigation.json` and validation/collection receipts |
| `batch-shuffle-generalization-20260924/candidate-followup-ondemand` | `synthesis.md`, profile review, unprofiled interpretation, stale-symbol evidence |
| `batch-shuffle-generalization-20260924/nested-locality-retest-ondemand` | `synthesis.md`, `regression-review.json`, `q15-jit-review.json`, raw profiles and exports |
| `batch-shuffle-generalization-20260924/string-in-codegen-v2` | `README.md`, `candidate-manifest.json`, `sf1-correctness-validated.json` |
| `batch-shuffle-generalization-20260924/string-in-prefix-detached-setup-ondemand` | `synthesis.md`, `regression-review.json`, `q15-exact-code-review/review.json`, `q15-exact-code-review/branch-count-correction.json`, collection/cleanup receipts |
| `batch-shuffle-public-pr-20260928` | `scope-removals.json`, `public-source-manifest.json`, `validation.json`, preservation checks and local test logs |
| Earlier development checkout | Excluded `docs/batch-pipelined-shuffle-research.md` and `docs/batch-pipelined-shuffle-runtime.md`; chronological mechanism notes, not current benchmark authority |

Keep original failures, partial collections, negative results, and raw recordings when moving
the evidence. Request only the needed sanitized subset, verify its hashes, and keep private
deployment identifiers, auth material, and signed URLs out of the public fork.

[headline-chart]: https://raw.githubusercontent.com/ForVic/spark/0f8de419be7ead8b565c9d5fc68a408f970e427d/headline-sf100.png
[results-readme]: https://github.com/ForVic/spark/blob/0f8de419be7ead8b565c9d5fc68a408f970e427d/README.md
[ci-run]: https://github.com/ForVic/spark/actions/runs/36468324338
[ci-report]: https://github.com/ForVic/spark/runs/109139479813
