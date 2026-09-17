---
layout: global
title: Batch pipelining research and performance experiments
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

# Batch pipelining research and performance experiments

This continues the [batch shuffle experiment](batch-pipelined-shuffle.html). The aim is generic
batch execution on local multicore and static distributed executors. Neither the optimizer nor
transport identifies benchmark queries or scale factors. Research was checked on September 15–17,
2026; code links below identify the inspected revisions. The follow-up dates and run times use UTC.

## Distributed follow-up: September 17, 2026

The latest work separates transport setup, physical partition sizing, operator memory and stage
admission. The follow-up uses only the RPC transport on the same two AWS hosts. The earlier local
measurements below belong to their recorded source versions; they do not measure the latest code.
The admission control and both V13 complete-suite runs are complete. A subsequent V14 extension
was tested and reverted after it failed to improve the measured critical path.

The resource controls are eight executor vCPUs, two four-GiB executor heaps, scan/shuffle width eight,
`spark.task.cpus=0.25`, memory fraction 0.4 and spill merge factor eight. Driver and colocated executor
use disjoint CPU affinity. Ordinary AQE stays parallelism-first; live reader sizing uses the existing
64-MiB advisory size. Connection-worker reuse is disabled. All modes share each application's build,
input and settings. These are warm-cache SF1 research measurements, not audited TPC-DS results.

### What the profiles changed

**Connection-worker reuse did not improve the complete suite.** The optional implementation remains
disabled. Preserving connection ownership, cancellation and asynchronous error propagation matters,
but fewer thread creations did not establish lower query latency.

**Fractional admission exposed a page-size mismatch.** Spark's default memory-page formula used
executor cores, even when fractional task reservations admitted more simultaneous operators. Two
diagnostic q11 applications failed to acquire 256 KiB while tasks retained multiple 16-MiB pages.
The candidate applies the existing formula to the greater of cores and admitted task concurrency,
snapshots that page size in each TaskMemoryManager, and makes task-owned SQL operators use it.
Explicit page-size overrides still win. This introduces no query threshold or new memory limit.

An ABBA configuration experiment isolated page size before changing source. Smaller pages eliminated
all measured q4/q11 spill in the two candidate applications, with the same plans. q4's uncoalesced
pipeline task CPU fell from about 9.9–10.2 seconds to 8.5–8.6 seconds. Spill-reader CPU samples
disappeared; transport samples stayed small. Ordinary execution also improved, so comparisons must
use the new ordinary control. The implemented default, without an explicit override, subsequently
passed both complete 103-query suites. Its summed medians were:

| Page-size implementation (V10) | Ordinary | Uncoalesced pipeline | Coalesced pipeline |
| --- | ---: | ---: | ---: |
| Catalog statistics | 51.276 s | 52.508 s | 52.003 s |
| No catalog statistics | 54.745 s | 54.720 s | 55.667 s |

These totals do not establish a suite-wide pipelining win. A separate q67 page-size ABBA reproduced
two execution bands: resident pipeline samples used about 9.0 seconds of task CPU, versus 13.9 seconds
when spilling. Nineteen of 32 old-page pipeline samples spilled, versus 11 of 32 smaller-page samples;
ordinary samples did not spill. This small experiment does not establish a universal spill
probability. The slow band already existed before the page-size change.

**Waiting readers also reduced producer memory allowances before receiving any rows.** A small
managed allocation registers a task in execution-memory sharing. A reader can therefore reduce a
producer's allowance while blocked on that producer. V12 waits for first input or EOF inside finite
batch ShuffledRowRDD construction, after installing completion cleanup and before downstream
operators initialize. It uses the existing iterator's `hasNext`, preserving its first row, error
handling and cancellation. There is no additional per-row wrapper.

The distributed V10/V12/V12/V10 control confirmed this mechanism. All 24 baseline pipelined q72
samples spilled about 143 MiB; all 24 candidate samples avoided spill. The inventory-input Sort's
median task peak allocation rose from 83,885,888 to 130,023,184 bytes, matching ordinary execution.
All 192 baseline pipelined Sort tasks reported SQL spill; all 192 candidate tasks reported zero.
JFR spill CPU samples disappeared, and initial reader waits moved into input construction.
Summed concurrent task waits are not query wall time.

Latency was mixed despite that concrete memory improvement. Ordinary q72 also became faster
between applications, and q4's initial apparent gain did not repeat consistently. q67 did not spill
in either treatment in this control, so it supplies no evidence of a q67 spill-frequency benefit.
The readiness change still holds its Spark task slot and transport buffers while waiting.

### Ordinary inputs need progress capacity, not simultaneous admission

V13 changes three core scheduler/test files relative to V12. Each ordinary finite group reserves
one slot for progress through its task waves. Live groups and barriers retain their full demand,
including consumers that have not started. An ordinary task cannot be blocked on a live input:
such a dependency places it in a live group. Additional ordinary tasks can use spare capacity.
This changes admission of the existing tasks; it does not implement task suspension.

The V12/V13/V13/V12 distributed control covered q4/q11/q72/q80 with six warmups and six rotating
triples per application. All 288 timed samples avoided spill. Each application recorded 11,327
successful tasks, 2,190 successful jobs and no executor removals. Linux passed 249 DAGScheduler
and 75 SQL tests, including capacity boundaries, barriers and multiple ordinary waves.

Coalesced q80 has eight live producer tasks, 17 Union consumer tasks and two ordinary eight-task
inputs. The old reservation requires 41 slots; the new one requires 27 and fits the 32 available.
The uncoalesced live group alone requires 32, so its ordinary siblings still execute first.

| Coalesced q80: median within each application | V12 A | V12 B | V13 A | V13 B |
| --- | ---: | ---: | ---: | ---: |
| Producer submission after job submission, ms | 181.5 | 242.0 | 1.0 | 4.5 |
| Producer launch after last sibling finishes, ms | 4.0 | 4.0 | -296.5 | -248.0 |
| Combined group task span, ms | 380.5 | 464.0 | 374.0 | 340.5 |

All 12 candidate samples overlap the independent producer with its materialized siblings; none
of the 12 controls do. The mean application-median group span falls about 15%. Whole-query latency
falls from 899 to 826 ms, or a descriptive 6.9% after normalization to ordinary controls. Task CPU
increases about 4.8%. Each producer runs longer while sharing CPU, but the combined group finishes
sooner. That tradeoff warrants complete-suite validation, not a claim that extra concurrency is free.

q4/q11/q72 live jobs contain no ordinary groups within their submitted job graph in this control.
Their timing changes cannot be attributed to the new reservation rule. Uncoalesced q80 also keeps
the same admission behavior yet becomes slower between applications. These negative controls show
substantial timing variation. Two applications per treatment do not establish an independent-run
confidence interval, and the selected profile cannot establish a suite-wide win.

The full [admission findings](../artifacts/pipelined-shuffle-client-workers-20260916/research/admission-repro/findings.md)
include CPU, actual task intervals, JFR waits, source hashes and all control timings.

### Complete-suite result after memory and admission changes

All 103 query variants passed result comparison with and without catalog statistics. Each full run
uses three warmups per mode and four rotating measured triples, without JFR profiling.

| V13 | Ordinary | Uncoalesced pipeline | Coalesced pipeline |
| --- | ---: | ---: | ---: |
| Catalog statistics | 50.554 s | 50.618 s | 50.207 s |
| No catalog statistics | 54.839 s | 55.122 s | 55.344 s |

These are sums of per-query medians. Coalesced pipelining changes the ordinary total by -0.68% with
statistics and +0.92% without. Both within-application bootstrap intervals include zero; they do
not estimate variation between independent cluster runs. There is no established suite-wide win.
Summed median task CPU is 1.4% higher with coalesced pipelining under catalog statistics and 2.7%
higher without; concurrency does not make that work free.
Initial planning medians sum to 3.04 seconds ordinary versus 3.25 seconds coalesced with statistics;
subsequent adaptive planning remains inside execution time. Transport tuning alone cannot explain
the remaining difference.

All 1,236 timed samples in the statistics run avoided disk spill. In the no-statistics run, 1,235
of 1,236 avoided spill; one coalesced q72 sample spilled 149,744,223 bytes. Thus the reproducible
controlled q72 spill pattern was removed, but contention is not eliminated in every execution.
q67 did not spill in either full run. The event logs record 91,273 and 94,565 successful tasks,
respectively, with no failed jobs or executor removals.

The [verified analysis and figure](../artifacts/pipelined-shuffle-client-workers-20260916/aws/analysis/)
keep all twelve full-suite applications separate by source and configuration. Queries with no live
exchange also show appreciable individual timing variation. Their aggregate coalesced-mode change
is +0.58% with statistics and +0.43% without; the live-query subsets change -1.11% and +1.07%.
These subsets are descriptive and are not used to tune eligibility.

### Residual placement pressure

The rare q72 full-run spill placed all eight heavy producers on one executor, while other
repetitions used four per host. A final on/off/off/on experiment toggled only existing
`spark.shuffle.reduceLocality.enabled`, with source V13 unchanged, no catalog statistics,
six warmups and twenty measured triples per application.

All 120 locality-off samples were balanced and avoided spill. Locality-on had one packed sample
among 120: eight producers on the worker, about 143 MiB disk spill and 6.26-second query latency.
Its catalog-input maps split seven to one, while inventory maps split four to four. All packed
producers were NODE_LOCAL; the Sort allowance again fell from 130,023,184 to 83,885,888 bytes.
The earlier full-run incident packed the other host, so the effect is not confined to one machine.

This supports a placement-pressure explanation, but neither locality-off application produced
comparable input skew. The experiment therefore does not prove that the setting prevents packing.
Mean application-median changes were +0.21% ordinary, -0.96% uncoalesced and -2.12% coalesced;
coalesced task CPU changed -0.12%. The default remains unchanged, and no new placement rule was
added. The next causal test needs fixed input placement and observed resource offers.

The [locality findings and profiles](../artifacts/pipelined-shuffle-client-workers-20260916/research/locality-repro/findings.md)
include all 240 samples, operator allocations, input-host bytes, actual task locality and both
executors' per-iteration JFR evidence. These focused profiles do not replace the full-suite result.

### Internal materialized inputs also need reader sizing

Coalesced V13 q80 retains eight readers on each of two internal durable inputs. Inlining these
exchanges removed their separate AQE statistics boundaries and consequently lost ordinary AQE's
coalescing opportunity. The downstream Union has 17 partitions instead of the ordinary plan's three.

The V14 candidate extends the existing estimated-layout rule to those internal durable exchanges.
It leaves ordinary AQE statistics boundaries intact, aligns join inputs together, and sizes plain
Union inputs independently. It uses the same estimates, advisory size and conservative handling of
unknown or explicitly fixed layouts. There is no query-specific threshold or new configuration.
All 80 local and 80 Linux SQL tests passed, including mixed live/durable joins on both transports
and separate executor JVMs. The V13/V14/V14/V13 control confirmed Q80's task reduction from 68 to
54 and durable readers from sixteen to two. Nevertheless, mean application-median latency increased
from 789 to 823 ms, or 7.6% after ordinary-control normalization. Group task span increased from
299.5 to 326 ms while group task CPU decreased about 2.2%. The consumer finished about 1 ms after
the producer in both versions, so removing its extra tasks did not shorten the critical tail.

All 288 timed samples avoided disk spill. Executor GC-pause sums were larger in the candidates,
and these small controls do not establish a universal regression. They also provide no basis for
claiming a performance win. The three-file extension was reverted; the retained source is V13.
The [candidate patch and full findings](../artifacts/pipelined-shuffle-client-workers-20260916/research/inline-sizing-repro/findings.md)
remain available. No V14 full-suite run was launched.

### Remaining sizing error

The current V13 q72 plan estimates 367,988,800 bytes entering its live aggregate exchange, retaining
eight physical readers under the standard 64-MiB advisory policy. Measured task SQL data-size
counters total 327,720 UnsafeRow bytes in every one of six coalesced repetitions. These are different
representations, not identical network-byte metrics, but the discrepancy remains substantial.
The other measured live outputs are about 32.2 MB across q4's four exchanges, 17.0 MB across q11's
two, and 4,992 bytes in q80. The exact estimates and reader layouts are archived.

This identifies a remaining statistics/concurrency problem. It does not justify fitting a new
threshold to q72 or assuming its final aggregate output size at planning time. Layout selection
still precedes live writer startup. A later runtime layout decision would need a different routing
and buffering contract; ordinary AQE can use completed-map statistics precisely because its data
has already been materialized. No late rewrite or new query-specific estimate was added here.

### How this relates to other engines and upstream

[Trino's HashAggregationOperator](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/operator/HashAggregationOperator.java)
creates its aggregation builder on input. That is the useful principle behind deferring Spark's
downstream allocation. Trino and [Velox](https://velox-lib.io/blog/velox-primer-part-2/) additionally
yield blocked execution to worker threads; this branch does not add that resumable-operator contract.
Changing `spark.task.cpus` cannot supply it.

The base already includes optional adaptive partial aggregation
([#57742](https://github.com/apache/spark/pull/57742),
[#58106](https://github.com/apache/spark/pull/58106)). It can switch to pass-through when reduction is
poor. The review notes that a high-cardinality prefix can make that one-way choice unhelpful later.
It remains disabled here. Bounded early emission of mergeable partial maps is a separate, unimplemented
possibility; the measured q72 spill was in Sort and did not justify changing aggregate semantics.

The [concurrent-stage discussion](https://github.com/apache/spark/pull/57092#issuecomment-4909059646)
separates admission, resource profiles and failure units. The scheduler experiment follows that
separation in core. The [reader](https://github.com/apache/spark/pull/56879) and
[writer connection-wait review](https://github.com/apache/spark/pull/58089#discussion_r3827227334)
also require preserving asynchronous failure notification; connection-worker reuse does not alter
that path. These sources inform the contracts; the measurements determine whether a change helps.

The [artifact record](../artifacts/pipelined-shuffle-client-workers-20260916/README.md) retains source
manifests, controls, failures, full plans, task events and both executors' JFR recordings.

## Earlier coalescing result: September 16, 2026

The earlier V6 experiment adds generic pre-launch reader coalescing and fixes spill-reader retention.
All 103 SF1 queries pass locally and across two hosts, with and without catalog statistics.
At scan/shuffle width eight, the complete pipeline reduces summed per-query medians by 5.16%
locally with statistics and 4.49% without. RPC is 0.22% slower with statistics and 3.03% slower
without. Coalescing specifically improves q28/q44, but does not establish a general suite-wide
incremental gain. Pipelining and reader coalescing remain opt-in; the spill-reader fix also
applies to ordinary execution.

See [the complete coalescing results and profiles](#estimated-live-reader-coalescing-september-16-follow-up)
for the three-mode comparison, unchanged-layout controls, memory pressure, remaining Union
admission restriction and unresolved q72 regression. The earlier fractional-CPU matrix remains
[reported separately](#corrected-fractional-cpu-results): pipeline totals were effectively flat
at task CPU reservations 1, 0.5 and 0.25. Widths, memory settings and statistics differ between
experiments; their absolute times must not be pooled.

## Earlier scheduling experiment: width two

Core execution-group admission, shared writer I/O resources, and release of completed stages'
reservations work end to end. The final implementation passed all 103 SF1 query-result comparisons
on both transports. With normal runtime settings, sums of per-query medians changed as follows:

| Final implementation | `spark.task.cpus` | Ordinary, seconds | Pipelined, seconds | Median change | Mean change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Two-host RPC, eight executor vCPUs | 0.5 | 73.856 | 72.860 | -1.35% | -1.88% |
| Local channel, `local[8]` | 0.25 | 54.532 | 53.156 | -2.52% | -2.43% |

These are small same-setting gains, with remaining regressions. Each full run uses three warmups
per mode and four alternating measured pairs. Seventy-six queries used live exchanges in every
measured pipelined plan; 27 retained materialization. Among those 76, four RPC queries and three
local queries regressed by more than 10%. No query-specific policy was added to avoid them.
The controls use the same experimental build with pipelining disabled, not a pristine upstream
binary. This warm-cache SF1 experiment is not an audited TPC-DS result.

The four full-suite pair aggregates ranged from -4.31% to -0.09% for RPC and -3.17% to -1.67% for
local execution. RPC improved by 3.30% when pipelining ran second, versus 0.43% when it ran first;
local execution improved by 2.44% and 2.42%, respectively. Alternating order reduces this bias but
does not make four pairs independent JVM replications. Fallback queries also varied, so small
differences between implementation variants should not be assigned wholly to the code change.

The scheduling abstraction belongs in core: the mixed-input example now starts independent
producers promptly with AQE on or off. That does not solve blocked readers consuming task slots.
The profiles below distinguish those two effects and explain why completed-stage release has
little opportunity to help q28. There is no demonstrated suite-wide incremental speedup from that
release over the preceding implementation; it is retained as a tested reservation-contract fix.
More generally, the core changes increased live-query coverage from 72 to 76 in the fractional
runs but did not establish a larger suite-wide gain than the original admission policy. Fixing
the specific readiness restriction and improving overall query latency are separate outcomes.

Relevant scheduler, batch SQL, transport, existing pipelined SQL and real-time compatibility suites
passed 432 tests. This is a research implementation with whole-execution failure handling; the
tests do not establish production robustness or the absence of resource leaks.

## Lessons from existing engines

### Execution readiness matters more than starting every stage

[Presto: SQL on Everything, sections IV.E-F](https://trino.io/Presto_SQL_on_Everything.pdf) describes
cooperative execution: an empty input, full output or unavailable memory causes a driver to yield
its worker thread. Spark's current pipelined tasks instead keep their scheduler slots while waiting.
Consequently, selecting the largest group that fits is not sufficient to maximize useful work.

Trino's [phased stage scheduler](https://github.com/trinodb/trino/blob/8aaa92e2173c083e29ff124d39336c7ac1b596f9/core/trino-main/src/main/java/io/trino/execution/scheduler/policy/PhasedExecutionSchedule.java)
records build-before-probe dependencies and can start downstream stages when output buffers fill.
Its [execution-policy documentation](https://trino.io/docs/current/admin/properties-query-management.html#query.execution-policy)
explicitly distinguishes phased scheduling from scheduling all stages immediately.

**Application to this experiment:** inspect the prerequisites of a group, not just its task count.
A pending broadcast above a live exchange delays the producer along with its consumer. Keeping
that exchange materialized lets the producer run independently of the broadcast. This preserves
Spark's existing asynchronous stage execution; it does not implement Trino's resumable drivers.
Pipelines within the producer remain eligible, and a completed broadcast adds no dependency wait.

### Share transport machinery, retain stream ownership

Trino's [DirectExchangeClientFactory](https://github.com/trinodb/trino/blob/8aaa92e2173c083e29ff124d39336c7ac1b596f9/core/trino-main/src/main/java/io/trino/operator/DirectExchangeClientFactory.java)
passes shared HTTP, scheduler and callback-executor resources to exchange clients. Each exchange
still has its own buffer and failure context. The relevant lesson for Spark is separating resource
lifetime from stream lifetime, not copying a particular pool size.

**Application:** extend `TransportClientFactory.createUnmanagedClient` with a connection-specific
`RpcHandler`. Streaming readers can share an executor-owned client factory and allocator while
each connection retains its sequence numbers, receive quota, termination acknowledgment and task
error notifier. Readers close their channels; the shuffle manager closes the factory. Directly
constructed readers still own and close their factory. Late connection completion after task
cleanup must close the new channel immediately.

The first two-host Linux run exposed an ordering race in this change: Netty's connect future can
complete before `channelActive` runs. Registering ownership only in `channelActive` made discovery
incorrectly report a missing client and abort the group. Registration is now idempotent from both
paths: it precedes readiness notification and also precedes discovery completion. A deterministic
test covers connect completion with the activation callback withheld.

Writer ports and protocol handlers remain task-owned: the current control message identifies
writer and reader IDs but not the shuffle/attempt needed to route multiple shuffles through one
port. An additional experiment shares their Netty workers and allocator through
`TransportServerFactory`, while retaining separate listeners. Each server tracks and closes its
accepted channels; closing a writer does not shut down other writers' workers. The manager owns
the factory and transport context. Plain TCP and TLS tests verify independent handlers, closing
one writer, opening another, and shutting down the shared factory. This removes repeated I/O pool
creation without changing the wire protocol; its performance is measured separately below.

### Partial results still need a complete transport lifecycle

The first distributed SF1 sweep stopped at a scalar subquery in q6. Both writers were waiting for
termination acknowledgments from reader 1, but `executeTake` had scheduled only reader 0. Spark's
local channel transport already supports the scheduler's live-reducer subset; the RPC writer did
not. This is a generic action contract, not a query-planning special case.

**Application:** RPC now opts into that existing capability. The writer skips destinations outside
the job's mapped reducer subset and waits only for scheduled readers. A later partial-read job gets
fresh producers and tracker locations. Reader identity comes from the requested reduce partition,
not the surrounding task's index. Unmappable partial reads fail before producers start. Tests cover
mapped scheduler hints, rejection of unknown mappings, `executeTake`, `executeTail`, and scalar
subqueries across both transports and separate executor JVMs.

### Move transport work from individual rows to completed buffers

The RPC allocation profile includes `DirectByteBuffer.slice` beneath
`ShuffleChecksum.updateChecksum`, called from the writer for every row. Updating the checksum in
small increments creates a direct-buffer view and invokes CRC32C repeatedly for each network batch.
The [wire protocol](https://github.com/apache/spark/pull/55620) and
[writer review](https://github.com/apache/spark/pull/56878) define a CRC32C over the serialized message
payload; they do not require a per-row checksum boundary. The review specifically emphasizes
written-byte bounds and correct reference-count ownership in tests.

**Application:** calculate CRC32C once when sending a completed buffer, after closing its serializer
and before handing it to Netty. The sender still exclusively owns the buffer at that point. Hashing
the same ordered bytes together produces the same wire checksum and includes serializer headers
and trailers. Sequence checks, termination acknowledgments and reader verification stay in place.
The wire test now exercises multiple buffers and the final flush through the actual write loop,
checking each payload independently with Java's CRC32C. Performance measurement follows separately;
sampled allocation weights are not treated as exact allocated-byte totals.

### Avoid buffering twice at the serializer boundary

Fresh Linux executor profiles put `BufferedOutputStream.write/flush` and its locking underneath
`UnsafeRowSerializer` on the RPC write path. The RPC writer already owns a Netty buffer and flushes
its serialization stream after every record to inspect its size. An additional Java buffer cannot
combine records under that contract, and adds copying and synchronization.

**Application:** an internal, advisory `SerializerInstance.serializeStream` overload describes
whether buffering is useful for the destination. Existing serializers can ignore it. UnsafeRow's
implementation writes directly into an already buffered destination; the ordinary shuffle path
retains its original buffering. The RPC writer still calls `flush`, so custom serializers keep their
existing contract. No serializer type check or SQL dependency is introduced in the transport.
A test compares the exact wire bytes for short and oversized rows, verifies immediate visibility,
and decodes them through the ordinary reader. CRC and this change are measured separately.

### A blocked pipeline is not an occupied CPU

[Morsel-Driven Parallelism](https://db.in.tum.de/~leis/papers/morsels.pdf) separates fine-grained
input work from a fixed degree of parallelism baked into a query plan. Workers execute a pipeline
until a blocking operator; available work and locality guide subsequent scheduling.

DuckDB's [pipeline executor](https://github.com/duckdb/duckdb/blob/d025f5fc8389953f491373506a5a51d779ac7fe9/src/parallel/pipeline_executor.cpp)
retains state when sources or sinks block and returns control to the scheduler. Its
[sorting implementation article](https://duckdb.org/2025/09/24/sorting-again) explains the distinct
sink, combine, finalize and output phases of blocking operators.

StarRocks' [driver executor](https://github.com/StarRocks/starrocks/blob/7056a8d56c940a1474eb8571ddbdc9188b7335de/be/src/exec/pipeline/pipeline_driver_executor.cpp)
also distinguishes runnable, blocked and finishing drivers. These are runtime contracts, not just
optimizer preferences. The [Velox paper, section 4.5](https://www.vldb.org/pvldb/vol15/p3372-pedreira.pdf)
similarly describes drivers moving on and off worker threads while retaining operator state.

**Implication for Spark:** reducing task resource requests or launching unlimited tasks would not
provide these semantics. The next scheduler abstraction needs explicit readiness, suspension and
resumption, cancellation, and memory ownership. A compiled Spark iterator cannot simply unwind
while blocked and later resume from an arbitrary call site. A first implementation should target
exchange boundaries and preserve the existing whole-stage loops within each runnable segment.

### Vectorization and pipelining are separate choices

[Compiled and Vectorized Queries](https://www.vldb.org/pvldb/vol11/p2209-kersten.pdf) compares both
execution models using the same parallelization framework. Parallel scheduling and row-versus-batch
execution are separate axes. The paper also discusses cases where controlled materialization
between compiled fragments helps memory access.

**Implication:** preserve Spark's generated loops and upstream channel batching. Replacing them
with per-row queues would add overhead. A future batch ownership interface should make byte size,
release, and cross-thread lifetime explicit. Removing `UnsafeRow.copy()` without transferring
ownership is unsafe because producers reuse their row buffers.

## Evidence that selected these experiments

The previous complete SF1 run showed that several slowdowns had nearly unchanged task CPU but
longer elapsed time. For example, one q79 pair had about 200 ms of producer CPU in both modes.
Its regular producer stage began 57 ms after query start; the live group began around 115 ms,
after the consumer's extra broadcast prerequisite. This observation motivates the readiness rule;
one sample is not a latency estimate.

The q28 timeline showed a different problem: ordinary execution overlapped more independent scan
tasks, whereas the pipelined groups occupied slots with waiting aggregate consumers. The broadcast
readiness rule does not solve that case. Fair multiplexed readers and finer scheduling remain
separate work.

A fresh RPC baseline with 12 aggregate repetitions created 96 client I/O worker threads during
the recorded intervals, in addition to 96 connection-creation workers. The first resource-sharing
experiment targets the I/O workers and allocator. Connection-creation workers and discovery threads
remain task-owned, so the experiment must not be described as eliminating all transport setup.

## Measurement discipline

1. Capture the current implementation with ordinary AQE controls before editing.
2. Check results and bounded-buffer, failure, cleanup and TLS behavior before timing.
3. Use JFR only for diagnostic comparisons; count resource creation and inspect CPU/wait stacks.
4. Compare fresh queries in alternating order, with identical data, parallelism and heap settings.
5. Record actual exchange counts for every measured plan, outside timing. AQE can choose
   differently between repetitions; a separate correctness snapshot does not classify all samples.
   Report mixed repetitions separately and rerun the full query set after a policy change.
6. Validate process boundaries separately; same-host executors do not measure network scaling.
7. Remove SBT Test's debug memory filling, assertions and testing property/environment from
   benchmark launches. `BenchmarkBase` also sets `spark.testing`; the end-to-end harness clears it.
   Record effective settings. Keep correctness tests on the normal test settings.
8. Treat task CPU as task-thread CPU, not whole-executor CPU. RPC I/O work runs on Netty threads.
   The current RPC transport does not publish complete ordinary shuffle byte/wait metrics; zeros
   in those fields do not mean no network traffic or waiting. Use executor JFR for those paths.
9. Copy large artifacts only during explicit idle periods between experiments. An artifact-transfer
   audit found overlap with part of an earlier full baseline and some profile samples. Those runs
   are retained with provenance and repeated; affected results are excluded from the final table.
10. Do not use restarted local recordings' allocation weights as per-query allocation totals. In
    the normal local q28 recording, first samples from each thread contributed about 110,345 MiB
    of weight, versus 16,665 MiB from later samples. These startup outliers are excluded from
    allocation conclusions. CPU samples, task timelines and unprofiled latency are separate
    evidence. The reported allocation sample weights are statistical estimates, not byte counters.

The current diagnostic set includes negative cases q28/q31/q49/q56/q79 and positive controls
q51/q99. This list belongs only in the harness. It is not an input to the selection rule.

## RPC transport ablations on two AWS hosts

The three synthetic profiling variants used identical executors, data and parallelism: shared reader
I/O with the original row-level checksum; batched checksum; then batched checksum plus the serializer
buffering hint. Each fresh JVM ran three warmups per mode and 12 alternating pairs per query.
Executor events below are restricted to those measured intervals and combined across both hosts.
JFR allocation weights are statistical estimates, not exact allocated-byte counts.

| Aggregate/Top-N pipeline profile | Row CRC + buffered serializer | Batch CRC + buffered serializer | Batch CRC + direct serializer |
| --- | ---: | ---: | ---: |
| Checksum stack allocation sample weight, MiB | 485.3 | 0.8 | 0.7 |
| Checksum stack CPU samples | 22 | 1 | 0 |
| UnsafeRow output-buffer stack CPU samples | 72 | 75 | 0 |
| All executor CPU samples | 332 | 335 | 258 |
| Median task CPU, ms | 398.8 | 385.2 | 331.1 |
| Median ordinary / pipeline latency, ms | 268.5 / 233.6 | 258.5 / 221.4 | 268.1 / 208.3 |

The join profile showed task CPU of 270.5, 267.1 and 235.1 ms in the same order. The targeted output
buffer frames disappeared there too. Shared reader I/O created no new worker threads during these
post-warmup intervals. Writer servers, discovery and connection creation still create task-scoped
threads; this is not a claim that all transport setup disappeared.

A separate eight-pair run per variant, without JFR, compared batch CRC alone to batch CRC plus the
buffering hint:

| Workload | Batch CRC: ordinary / pipeline median, ms | Plus buffering hint: ordinary / pipeline median, ms |
| --- | ---: | ---: |
| Aggregate/Top-N | 253.1 / 229.8 | 258.9 / 210.8 |
| Join/aggregate | 200.9 / 162.0 | 217.6 / 184.4 |

Aggregate task CPU fell from 395.9 to 345.1 ms; join task CPU from 255.7 to 239.2 ms. The join elapsed
time nevertheless rose in the second JVM, including its ordinary-shuffle control. These measurements
support removal of the identified CPU/allocation work, not a uniform latency gain from each change.
The final full TPC-DS run is a separate measurement with both changes enabled.

The complete synthetic measurements and profile counters are in
[`BatchPipelinedShuffleBenchmark-aws-transport-ablation.csv`](../sql/core/benchmarks/BatchPipelinedShuffleBenchmark-aws-transport-ablation.csv).
Remaining synthetic samples include input buffering, generated row iteration and task-scoped
transport setup. In particular, `LinkedList` allocations came from `BufferedRowIterator.append`,
not the transport's message queue. Inclusive shuffle stacks must not be mistaken for transport-only
costs. Changing that generic generated-code boundary would require its own ordinary-shuffle controls.

## Broadcast-readiness ablation on AWS

A fresh `local[8]` JVM per variant ran eight alternating pairs on 12 queries, with the same channel
transport, data, two scan/shuffle partitions and heap settings as the full SF1 run. Eight queries
changed selection and four were controls. Actual exchange counts were captured after each timed
execution. This ablation changes only the pending-broadcast guard.

| Query | Without guard: ordinary / pipeline ms | With guard: ordinary / pipeline ms | Live exchanges before / after |
| --- | ---: | ---: | ---: |
| q44 | 351 / 420 | 338 / 403 | 2 / 1 |
| q46 | 345 / 364 | 361 / 321 | 1 / 0 |
| q57 | 620 / 597 | 601 / 611 | 1 / 0 |
| q65 | 378 / 350 | 391 / 383 | 2 / 1 |
| q68 | 286 / 322 | 280 / 276 | 1 / 0 |
| q75 | 688 / 748 | 671 / 718 | 0–1 / 0 |
| q77 | 370 / 337 | 355 / 329 | 2 / 1 |
| q79 | 250 / 294 | 242 / 251 | 1 / 0 |

The guard removes the observed regressions in q68/q79, but also loses a small benefit in q57/q65.
Across all 12 queries, summed medians changed from 5,480 / 5,440 ms without the guard to
5,457 / 5,287 ms with it. Summed means show a smaller change: the pipeline/ordinary difference
was -1.49% without the guard and -1.83% with it. Unchanged q28 also improved substantially between
runs, while q99 lost a small benefit. This is evidence of between-JVM variation, not an effect of
the guard. Do not attribute the entire aggregate change to the rule.

The small generic rule is retained because an unfinished build is a real prerequisite and retaining
the probe's materialization boundary permits useful producer work during that wait. It is not a
universal performance guarantee. The complete 12-query results, including controls, are in
[`BatchPipelinedShuffleBenchmark-tpcds-sf1-aws-broadcast-policy.csv`](../sql/core/benchmarks/BatchPipelinedShuffleBenchmark-tpcds-sf1-aws-broadcast-policy.csv).

## What remains after the transport fixes

Eight-pair executor/driver profiles of six diagnostic SF1 queries used the final transport changes.
These queries were selected to investigate regressions and a positive control; their aggregate is
not a full-suite performance result. All timed executions exercised pipelines.

| Query | Ordinary / pipeline median, ms | Ordinary / pipeline task CPU, ms | Ordinary / pipeline tasks |
| --- | ---: | ---: | ---: |
| q4 | 1,659 / 1,826 | 4,243 / 3,459 | 30 / 37 |
| q28 | 539 / 571 | 1,411 / 1,229 | 24 / 30 |
| q31 | 756 / 799 | 1,026 / 975 | 28 / 40 |
| q49 | 522 / 582 | 644 / 591 | 18 / 21 |
| q51 | 1,127 / 988 | 2,140 / 2,144 | 17 / 18 |
| q56 | 423 / 538 | 473 / 470 | 15 / 16 |

Several regressions have lower task CPU. q51 shows the converse: useful overlap reduces latency
without reducing task CPU. Executor profiles remain necessary because task CPU excludes Netty.

q56 exposes a separate readiness issue. Its final plan has three independent sales branches feeding
a union: two use live exchanges and the third remains materialized. `createNonResultQueryStages`
propagates `allChildStagesMaterialized` across those branches; the result group cannot start until
the materialized sibling finishes. The three sales producer stages start within a median 6 ms of
one another under ordinary AQE, versus 141.5 ms with the mixed live group, across eight repetitions.
In the first pipeline sample, the materialized producer starts at 231 ms; the two live producers
start at 379 and 381 ms. This uses task launch timestamps, not deferred completion timestamps.
The broadcast guard does not remove this later materialized-input prerequisite.

**Inference:** group formation can lengthen the critical path before transport throughput matters.
A general extension must distinguish prerequisites for the whole result from prerequisites for an
individual producer. AQE currently cannot expose that early producer work through an independently
materializable live exchange. Introducing it also changes AQE's stage cancellation/replanning
assumption that child stages finish before parent stages are created. It needs an explicit group
lifecycle and capacity reservation, not just starting extra tasks or increasing buffer sizes.

Trino's [phased scheduler](https://github.com/trinodb/trino/blob/8aaa92e2173c083e29ff124d39336c7ac1b596f9/core/trino-main/src/main/java/io/trino/execution/scheduler/policy/PhasedExecutionSchedule.java)
separates fragment topology from scheduling dependencies and starts downstream stages when output
buffers block. That is a useful model for this next experiment; it does not establish that Spark's
stack-based iterators can suspend safely. The same distinction applies on one machine and across
hosts, without using query IDs or scale factors in planning.

## Validation of the RPC extensions

This pass exercised 408 distinct Scala tests and 24 Java transport tests, including TLS variants.
Coverage includes the scheduler and routing suites, writer/reader/manager lifecycle, mapped partial
reads, repeated actions, scalar subqueries, union partition offsets, ordinary and AQE pipelined SQL,
and separate executor JVMs. Core and SQL production/test style checks passed.

After adding the serializer hint, 45 core writer/end-to-end tests, 42 serializer/batch/cluster SQL
tests, and 45 existing pipelined SQL tests were rerun successfully. The serializer test verifies
wire equivalence and immediate visibility for short and oversized rows. The checksum test sends
multiple buffers through the actual writer and verifies each with an independent CRC32C.
Some Spark suites still report possible shutdown-thread leaks; passing suites do not prove that
all runtime resources are leak-free. The shared client factory has an explicit shutdown check.

## Next abstraction work

1. **Multiplexed live readers.** One task must fairly consume all its assigned reduce streams.
   Sequentially draining one bounded stream can deadlock a producer blocked on another. Coalesced
   partition specifications also need an explicit writer-to-reader mapping for RPC discovery and
   complete scheduler accounting. An alternative is to freeze that mapping before any producer
   launches and route rows directly into the final physical readers, avoiding separate streams
   for the original buckets. This needs consistent partitioning metadata across join inputs and
   accurate group widths. It cannot change the mapping after writers have started. Reuse AQE's
   partition sizing where actual statistics exist.
2. **Cooperative task execution.** Core execution groups now separate producer readiness from
   whole-result readiness, as described below. Parked readers still hold task slots. A further
   extension would track input-ready and output-space-ready events and retain operator state while
   yielding a worker. It must preserve the guarantee that bounded streams can make progress; stage
   admission alone is not an implementation of resumable operators.
3. **Shared writer transport.** Register per-attempt streams at an executor endpoint. Keep sequence,
   credit/backpressure, termination, cancellation and buffer release scoped to the registered stream.
4. **Batch ownership.** Measure row-copy/allocation CPU before replacing representation. A batch
   interface should transfer ownership and account for bytes, with ordinary row iterators as an
   adapter. It should work for both channel and RPC transports.

These are distinct hypotheses to test incrementally. None requires a TPC-DS-fitted cost model.

## Core execution groups and fractional CPU experiment

The mixed-input restriction is not inherent to AQE or to the upstream dependency API.
`PipelinedExecutionPlan` now forms groups from the actual core stage graph. Live edges connect
members of one group; ordinary shuffle edges connect groups. `DAGScheduler` admits a group with
its unfinished input groups if all reservations fit, otherwise admits input groups first. A large
ordinary input can run in waves. Reservations include consumers waiting on ordinary inputs, so an
unrelated task set cannot take their future slots. Tests cover both admission paths, multiple live
groups separated by a durable edge, deferred success and cancellation cleanup. An additional
refinement releases completed stages' reservations while retaining those for unfinished members.
It is measured separately from the initial whole-group reservation policy and writer I/O sharing.

SQL still decides which exchanges can use streams. `PlanBatchPipelinedShuffle` is now an exchange
rule usable with AQE disabled. `materializeForAdaptivePlanning=false` keeps selected durable sibling
inputs inside the execution graph when their complete statistics are not needed by AQE. They still
write ordinary shuffle data. Broadcast construction and join-strategy statistics retain their
boundaries. Plain unions estimate their execution width without claiming an output distribution.

This builds on the separation of live regions and durable edges described by
[Flink's region scheduler](https://flink.apache.org/2020/12/02/improvements-in-task-scheduling-for-batch-workloads-in-apache-flink-1.12/).
Flink's documented policy waits for blocking inputs before admitting a region. This experiment also
permits overlap with unfinished ancestors when the whole reservation fits. That extra admission
choice is ours; it should not be attributed to Flink. It does not make Spark's pull iterators
resumable, and parked readers still consume task slots. Fractional task CPUs test that separate
constraint by allowing more tasks to share the same hardware.

The new AWS experiment keeps eight executor vCPUs fixed across `spark.task.cpus=1`, `0.5`, and
`0.25`. Driver and colocated executor have disjoint CPU affinity. Each setting compares ordinary
execution and pipelining with identical settings, first on six diagnostic queries (12 alternating
pairs), then all 103 SF1 queries (four pairs). Per-timed-plan exchange counts distinguish live
execution from fallback. JFR runs are separate from the unprofiled latency trials.

The local channel trial uses `local[8]` on the whole driver host, with tasks and driver sharing its
eight vCPUs and a 12 GiB heap. The distributed trial has two four-vCPU, 4 GiB executors and separate
driver CPU capacity. Compare pipelining on/off within each configuration; the difference in absolute
time between these local and distributed trials is not an isolated transport comparison.

At `0.25`, the original implementation's full AWS trial passed all 103 correctness checks. Summed
medians were 77,715 / 75,866 ms (ordinary / pipelined, -2.38%); summed means were 78,070 / 76,561 ms
(-1.93%). The six-query diagnostic subset improved by 4.79%, illustrating why it is not a substitute
for the full suite. The initial complete matrix gave -0.45% at `1`, -3.26% at `0.5`, and -2.38% at `0.25`.
These runs still used test settings in the driver. The corrected repeats below supersede them for
latency comparisons.

### Corrected fractional-CPU results

All 103 SF1 queries returned matching results at each setting. These runs use the original
admission policy and normal runtime settings, with eight executor vCPUs held fixed. Each query has
three warmups per mode and four alternating measured pairs. Times below sum the per-query medians;
the mean column instead compares sums of per-query means.

| `spark.task.cpus` | Ordinary, seconds | Pipelined, seconds | Median change | Mean change |
| --- | ---: | ---: | ---: | ---: |
| 1 | 73.677 | 72.644 | -1.40% | -1.28% |
| 0.5 | 74.609 | 72.587 | -2.71% | -2.53% |
| 0.25 | 74.400 | 72.875 | -2.05% | -2.67% |

The `1` row is the clean full-suite repeat. Its earlier run overlapped a profile transfer and
is superseded. The fractional runs in this table did not overlap those transfers.

The pipelined totals themselves are nearly flat: 72.644, 72.587 and 72.875 seconds. Each percentage
uses its own setting's ordinary control, whose measured total also changes. Thus the larger
relative gain at `0.5` does not demonstrate an overall latency benefit from lowering task CPU
reservations. The profiles establish reduced launch delays; the full suite does not establish
a net gain from that additional concurrency across all queries.

Seventy-two queries used live exchanges in every measured pipelined plan; 31 fell back. At `0.25`,
the live subset's summed medians improved by 2.48%, while the fallback controls improved by 0.72%.
Of the 72 live queries, 16 improved by more than 10% and five regressed by more than 10%. Fractional
reservations reduce a scheduling restriction; this matrix does not show a universal speedup or
establish that one reservation is optimal. Four pairs and changing AQE choices limit precision.

### Complete normal-runtime comparison

Each row is a separate full 103-query run with its own same-build ordinary control.
The changes are cumulative; the last variant is the final implementation. The local and
distributed hardware/driver arrangements differ as described above.

| Implementation | Transport | Task CPUs | Ordinary, seconds | Pipelined, seconds | Median change | Mean change | Live queries |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Original policy | rpc | 1 | 73.677 | 72.644 | -1.40% | -1.28% | 72 |
| Original policy | rpc | 0.5 | 74.609 | 72.587 | -2.71% | -2.53% | 72 |
| Original policy | rpc | 0.25 | 74.400 | 72.875 | -2.05% | -2.67% | 72 |
| Core admission | rpc | 1 | 73.898 | 73.611 | -0.39% | -0.68% | 74 |
| Core admission | rpc | 0.25 | 74.283 | 72.889 | -1.88% | -2.17% | 76 |
| Core admission | channel | 0.25 | 55.582 | 53.608 | -3.55% | -3.08% | 76 |
| Shared writer I/O | rpc | 1 | 74.623 | 74.211 | -0.55% | -0.51% | 74 |
| Shared writer I/O | rpc | 0.5 | 74.381 | 73.028 | -1.82% | -2.13% | 76 |
| Shared writer I/O | rpc | 0.25 | 73.335 | 71.859 | -2.01% | -1.98% | 76 |
| Completed-stage release | rpc | 0.5 | 73.856 | 72.860 | -1.35% | -1.88% | 76 |
| Completed-stage release | channel | 0.25 | 54.532 | 53.156 | -2.52% | -2.43% | 76 |

Per-query results are preserved in
[`BatchPipelinedShuffleBenchmark-tpcds-sf1-scheduling.csv`](../sql/core/benchmarks/BatchPipelinedShuffleBenchmark-tpcds-sf1-scheduling.csv).
The evidence artifact also contains raw recordings, input data, frozen source overlays, and
per-sample plans. Small cross-row differences do not establish the incremental effect of a change.

### Admission example

For a two-task live producer, a two-task consumer, and a two-task ordinary sibling input:

| Available task slots | Submission behavior |
| --- | --- |
| 4 | Run the ordinary input first, then admit the four-task live group. |
| 6 | Admit all six; start both producers, then start the consumer when its ordinary input completes. |

The reservation includes the consumer before it starts, so unrelated work cannot take the slots
needed to drain the live producer. These paths are covered directly in `DAGSchedulerSuite` and the
mixed SQL execution is tested with AQE both enabled and disabled. This is admission of existing
Spark tasks. It does not release their task slots while an iterator waits for input.

Completed stages need a different rule from waiting tasks. With six slots and two independent
four-task live groups, the first group initially reserves four slots. Once its two producers
finish, its two consumers retain their reservations and the second group can fit. The initial
implementation kept all four reservations until the consumers finished. A regression test
reproduced that unnecessary delay; the refinement releases completed stages immediately. This
is safe under the existing whole-execution failure contract, which does not restart a completed
stage in place. It still does not release a blocked, unfinished reader's task slot.

The runtime profiles distinguish that bookkeeping correction from a solution to waiting readers.
With one CPU per task and the same final q28 plan in all eight samples, shared writer I/O before
the correction had a 306 ms median source-start spread; completed-stage release had 333 ms.
Reducing task CPU reservations to a quarter on the corrected implementation reduced it to 34 ms,
again with the same plan in all eight samples. Thus the reservation correction did not remove this
observed slot bottleneck. In the matched q4 plan, the corrected implementation's spread was 934 ms
at one CPU per task (four samples) and 65.5 ms at a quarter CPU (eight samples). These are JFR
diagnostics, not unprofiled latency estimates. q56 retained the core readiness improvement, with
its independent producers launching over a median 5 ms at one CPU per task.

The completion timestamps explain q28's limited opportunity for this particular correction. At
one CPU per task, its six two-stage live groups produced 48 observed overlapping parent/child
edges across eight repetitions. In the corrected implementation, the median interval between
parent and child stage completion was only 1 ms, with a maximum of 7 ms. For 25 of those 48 edges,
all child task finishes were already recorded before the parent's logical completion; child
success was waiting for that dependency. The long reader waits occurred earlier. Returning a
completed producer's reservation therefore cannot recover that earlier capacity. q51 had longer
parent/child completion gaps, a median 58.5 ms across 40 observed overlapping edges, illustrating
why completed-stage release can still matter for other graph shapes. These are driver-observed
task finishes and logical stage completions, not listener arrival order or sampled CPU time.

Earlier overlap is not necessarily less CPU work. In those matched q28 samples, median task CPU
rose from 1,321 to 1,572 ms when task reservations changed from one CPU to a quarter, while profiled
latency fell from 615 to 523 ms. The CPU increase's cause was not isolated. Fractional reservations
are therefore a useful scheduling experiment, not evidence that more concurrent tasks are free.

The corrected implementation's six-query, 12-pair diagnostic subset was 11.88% slower at one CPU
per task and 4.71% faster at a quarter CPU. That selected subset cannot establish the full-suite
effect. In particular, returning completed stages' reservations does not make blocking iterators
yield their task slots, and it should not be presented as cooperative task execution.

### Runtime correction discovered during profiling

SBT's test runner enables `spark.memory.debugFill`, assertions, and testing flags. The old local
profiles therefore included page filling inside the synchronized heap allocator. In q28's eight
paired runs, the pipelined path accumulated about 1,623 ms waiting on that allocator; after removing
debug filling, that wait was absent. Under normal settings, the local q28 profile still recorded
about 4,215 ms of aggregate live-reader parking across eight repetitions, with 145 CPU samples in
the pipelined path versus 144 in ordinary execution. These are summed task waits and sampled CPU,
not critical-path times. Parquet decoding and aggregation dominate the CPU stacks.

The distributed executor JVMs did not inherit debug filling or assertions, but the SBT driver did.
The complete fractional matrix was preserved in
[`BatchPipelinedShuffleBenchmark-tpcds-sf1-fractional-initial.csv`](../sql/core/benchmarks/BatchPipelinedShuffleBenchmark-tpcds-sf1-fractional-initial.csv).
Normal-runtime repeats remove these flags in the driver as well. Correctness tests retain their
usual debug settings. The benchmark prints effective settings and refuses to time with debug
filling or the testing environment enabled.

### What fractional CPUs change in the profile

The original-policy distributed q4 profiles keep eight executor vCPUs fixed; the quarter-CPU
profile is a clean repeat. Reducing `spark.task.cpus` from `1` to `0.25` increases admission
capacity from 8 to 32 tasks. Median sales-source launch spread in the pipelined path fell from
772.5 to 62 ms. Ordinary execution's spread changed from 301.5 to 28.5 ms.
Pipelined profiled latency changed from 1,826 to 1,493 ms, while aggregate reader-input parking
across eight samples changed from 27.7 to 51.3 seconds. Summed task waits are not critical-path time:
earlier readers can wait alongside more useful work.

Within the most common shared final plan shape (7 samples at `1`, 6 at `0.25`), launch spread
changed from 779 to 60.5 ms and profiled latency from 1,834 to 1,434 ms. These small diagnostic
groups support the scheduling explanation; use the unprofiled full-suite matrix for latency claims.
Other queries can gain live exchanges when the larger admission budget makes them eligible, and
AQE build-side choices vary. Per-sample exchange counts and final plans distinguish selection
changes from scheduling changes within the same plan.

### What moving admission into core changes

With the original policy and one CPU per task, q56's independent sales producers launched over
157 ms in the profile. Core admission reduced that spread to 6 ms with the same 16 tasks and two
live exchanges. Aggregate reader input waits fell from 1,561 to 568 ms across eight repetitions.
This confirms that keeping a durable sibling inside the core execution graph can remove an
unnecessary producer dependency. It does not establish a general latency improvement: q56 gained
7.9% in the 12-pair diagnostic run but regressed 9.8% in the four-pair full run at that setting.

Core admission also makes additional graphs eligible. At one CPU per task, q75 and q80 gained
live exchanges but regressed by 18.7% and 29.4% in the full run. Their profiles showed fewer Java
CPU samples yet more tasks and more widely separated source starts. At a quarter CPU per task,
the source-start spreads fell to 11 and 6 ms. Removing an admission restriction can expose useful
overlap, but admitting more waiting tasks can also delay useful work on fixed hardware.

A clean repeat of the quarter-CPU core q4 profile gave a 49 ms sales-source launch spread, versus
736 ms at one CPU per task. Its pipelined median remained close to ordinary execution, 1,583 versus
1,571 ms. These are diagnostic timings with JFR enabled, separate from the full-suite table.

### Scan width and shuffle width are different controls

The following core-admission RPC experiments use the same eight executor vCPUs, a quarter CPU per
task, and 12 alternating pairs of six diagnostic queries. The scan setting is
`spark.sql.files.minPartitionNum`; it is a suggested minimum, not a guarantee of each scan's width.

| Scan minimum | Shuffle partitions | Ordinary, ms | Pipelined, ms | Change |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 2 | 4,180.19 | 4,104.52 | -1.81% |
| 8 | 8 | 3,911.43 | 4,355.92 | +11.36% |
| 8 | 2 | 4,078.68 | 4,104.34 | +0.63% |

At width eight, q28 used 102 pipelined tasks versus 60 ordinary tasks; q31 used 115 versus 67.
Keeping eight scan partitions but reducing shuffle width to two reduced the pipelined counts to
66 and 79. Ordinary AQE can coalesce materialized readers; the current live-reader contract keeps
one complete reduce partition per reader. The eight-wide trial also changes which groups fit, so
this is evidence of partitioning and admission sensitivity, not a pure transport comparison.
It motivates a generic partition-width and reader-coalescing contract, rather than a benchmark
query exception. The diagnostic subset is not a replacement for a full-suite result.

On this subset, ordinary execution at eight scan/shuffle partitions was also faster than pipelined
execution at two (3,911 versus 4,105 ms). Consequently, the small full-suite gains at identical
partition settings do not establish a win against an ordinary baseline tuned independently for
its best parallelism. A stronger performance claim needs that comparison as well.

### Shared writer I/O: the targeted overhead was removed

The executor-owned server factory creates no writer I/O threads during the measured intervals
after warmup. The preceding core-only implementation still created a boss and worker for each
writer. Both profiles below use a quarter CPU per task and eight measured repetitions per query.

| Query | Writer I/O thread starts, core / shared | All thread starts, core / shared |
| --- | ---: | ---: |
| q28 | 192 / 0 | 597 / 388 |
| q31 | 192 / 0 | 583 / 388 |
| q75 | 256 / 0 | 651 / 384 |
| q80 | 192 / 0 | 516 / 321 |

The remaining q28 starts include 192 connection-creation workers, 96 discovery workers, and
96 flush timers. Per-stream listeners and connections also remain. Sharing I/O workers is a
measured reduction in resource creation, not elimination of transport setup. Profiled latencies
remain mixed, and the separate unprofiled full-suite runs determine the reported latency effect.

Exclusive sampled CPU leaves and thread roles help avoid another attribution error: a stack
containing `StreamingShuffleWriter.write` may be executing its upstream Parquet scan or aggregate.
For example, the quarter-CPU core q28 profile had 667 Java execution samples, including 652 on task
threads and seven on streaming transport threads. Parquet decoding and generated aggregation
dominated the leaves. These sample counts do not measure native thread-creation cost, and are not
an exact CPU-time breakdown. They explain why reducing transport setup alone need not substantially
change the query's compute time.

### Blocking operators still bound the overlap

The recorded q28 plan places a grouped partial `HashAggregate` directly below each of its six
live exchanges. With the settings used here, the generated aggregate builds its map from the
partition's input before returning aggregate rows. A streaming exchange does not turn that build
into incremental output. Reducers can overlap with earlier-finishing producers and avoid the
materialized shuffle, but cannot consume rows that an upstream operator has not emitted.

This distinction also appears in the MPP pipeline models above: blocking operators split the
work into build and output phases. A generic next step should represent those phases and their
readiness, rather than infer useful CPU work from the number of admitted Spark tasks. Rejecting
every grouped aggregate would be too broad: eliminating materialization and overlapping unequal
producer durations can still help. The source base also contains an opt-in adaptive partial
aggregation path that can bypass ineffective pre-aggregation; it is disabled in these trials and
its interaction with live exchanges is not measured here.

### Final local profile and repeatability

The final channel profile uses eight paired samples per query, separately from the full suite.
For q28, ordinary and pipelined execution each had 396 sampled Java execution events; generated
aggregation and Parquet decoding dominated the exclusive leaves. Median task CPU was 1,223 and
1,211 ms, while the live readers accumulated 16.1 seconds of parking across all eight samples.
Those simultaneous waits are not critical-path time. Ordinary and pipelined plans used 24 and
30 tasks respectively, so avoiding materialization does not imply fewer tasks.

q49's 16.6% regression in the unprofiled full run did not reproduce in this profile: ordinary and
pipelined medians were 379 and 318 ms, with task CPU of 448 and 460 ms. Each mode retained one
final plan shape across its eight profile samples. CPU leaves were largely hash aggregation,
hash-join probes and decoding. This separate diagnostic run therefore does not isolate the cause
of the unprofiled regression. It is evidence against treating a single short-query slowdown, or
a favorable profile repeat, as a stable performance property.

## Estimated live reader coalescing: September 16 follow-up

### What changed

A new opt-in physical rule assigns the live shuffle's physical reader layout before any writer
starts. It reuses `CoalescedHashPartitioning`, `CoalescedPartitioner` and the existing adaptive
partition sizing/fan-in policy. Writers hash into the original logical buckets and then map those
buckets to physical readers. Each reader still owns one complete stream, so neither transport
needs a new multiplexing protocol. Compatible join inputs receive identical boundaries.

This implements the pre-launch mapping alternative described above. It works with AQE on or off,
and with local channels or distributed RPC. The core scheduler sees the physical task count.
Explicit partition counts, unknown estimates and incompatible fixed sibling layouts are preserved.
The planner can also size an already-selected, unexecuted live edge that still has its original
hash layout when a later pass provides more information. Already-coalesced layouts are retained;
executed query stages remain opaque.

The feature flag is `spark.sql.shuffle.pipelined.coalescePartitions.enabled`, default false.
It requires the existing adaptive coalescing flag, but does not require adaptive execution itself.
It currently coalesces already-admitted live edges. A 200-partition exchange that is rejected by
initial admission remains materialized. Making sizing and admission a joint decision is separate
follow-up work; these results do not demonstrate automatic tuning of Spark's default width.

### Physical statistics, not final-result statistics

The first estimator used the exchange child's logical-link size. Profiling and plan inspection
exposed a semantic problem: Spark can attach the final Aggregate's logical link to its physical
partial aggregate. A scalar result can be a few bytes while its intermediate distinct-key output
contains many rows. Treating those bytes as shuffle size reduced too much parallelism.

The corrected implementation unwraps generated wrappers first and treats partial aggregates
explicitly. It calls Catalyst's existing `AggregateEstimation` with physical grouping keys and
buffer outputs, accounts for groups repeated across producer partitions, and caps grouped rows
at the logical input row count. Without column statistics it uses an input-sized estimate adjusted
for physical row width. Opaque growing buffers and grouping keys absent from the logical input remain unknown.
This uses no query names, TPC-DS scale factors, or fitted selectivities.

The archived v1-v3 runs are exploratory estimator ablations and do not measure the final code.
The v4/v5 local diagnostic results use the corrected estimator. Final v6 results also include
the unknown-projection guard and spill-reader ownership fix described below.

### Measurement controls

Each experiment compares ordinary AQE, the preceding pipelined implementation, and pipelining
with estimated reader coalescing. All modes share the same data, source/shuffle width, CPU setting,
CBO setting and JVM. Four measured triples follow three warmups per mode for each of all 103 SF1
queries. The order ABC/CBA/BCA/ACB balances each pair's relative order. Diagnostic profiles use
more repetitions and stay separate from unprofiled latency results. Warmup actions validate rows
against ordinary execution in every mode.

The measured source is the archived v6 overlay on commit
`9b4e4f7547c07c5eba3d2b0822a6d3dbb023a150`. A final upstream check found 11 newer commits
through `66a28acd5b1cf34458de74f5ff6703a4851ad751`, with no changes to the relevant shuffle,
scheduler or sorter implementations. All three benchmark modes include the spill-reader fix;
ordinary mode is a control in this checkout, not a pristine-trunk binary.

Both scan minimum and shuffle width are eight, with `spark.task.cpus=0.25` and
`spark.sql.adaptive.coalescePartitions.parallelismFirst=true`. Other adaptive sizing settings
retain their defaults. The local path uses
`local[8]` on an 18-core Apple M5 Pro without CPU affinity: eight is its scheduler budget,
not a hard eight-core OS limit. The RPC path has two four-vCPU executors on separate hosts and driver affinity disjoint
from its colocated executor. The same 363 MiB Parquet data fits in memory; these are warm-cache
research measurements, not audited TPC-DS results. Assertions, testing mode and debug memory fill
are disabled for timing and enabled in correctness tests.

The catalog-statistics condition runs ordinary `ANALYZE TABLE ... FOR ALL COLUMNS` and enables
Spark CBO for every mode. This setup is excluded from query timing. It can change join plans and
which exchanges are eligible, so catalog/no-catalog results are separate experiments. Local Mac
measurements are supporting evidence; the dedicated AWS hosts provide the distributed comparison.

### Memory-pressure diagnostic before the distributed comparison

The initial catalog-statistics cluster run failed on q11 before reaching coalescing. Event-log
properties identify the first heap OOM in ordinary execution, followed by failure in the preceding
pipeline implementation. Both modes were using four-GiB executor heaps and quarter-CPU tasks.
The ordinary attempt reported about 4.37 GiB of aggregate memory spilling but only 34 MiB of
physical spill bytes; these metrics are not the same quantity. Executor stacks failed while
opening `UnsafeSorterSpillReader`, after aggregation/sorting had already caused memory pressure.

This source has an existing bounded spill merge, controlled by
`spark.unsafe.sorter.spill.merge.factor`, but its default is -1 (unbounded). Each spill reader
allocates a one-MiB row array, and default read-ahead adds two one-MiB heap buffers. The underlying
NIO stream also owns a direct buffer. Opening many
readers across concurrent tasks can therefore exhaust heap despite spilling managed execution
memory. The default Tungsten page-size calculation also uses executor cores, not fractional task
slot capacity. More admitted tasks need not have smaller allocation granularity.

Bounded merge alone passed isolated q11 but failed after the q1-q10 prefix. Lowering the managed
memory fraction to 0.4 also failed. That second diagnostic produced a 4.17-billion-byte HPROF dump.
A parser found 1,079 spill readers, including 1,025 with null input streams. Their retained row arrays
accounted for exactly 1 GiB of payload, and 1,075 task-completion callbacks referenced spill
readers. The source confirmed the ownership path: each callback captured the reader; `close()`
cleared its streams but left the last-record buffer reachable until task completion. Bounded
merge rounds therefore accumulated closed-reader buffers while the task was still running.

The fix makes the callback capture a releasable reader reference. `close()` releases the task's
ownership even if stream closure throws. The iterator itself retains the last record because
`loadNext()` closes at EOF before its caller consumes that record. Abandoned readers still remain
owned by task completion and are closed there. A regression test fails on the old implementation
and passes with the fix; it verifies garbage collection while the task is still active, last-row
readability, close failures and abandoned-reader cleanup. This is a general spill-reader fix,
independent of coalescing or TPC-DS planning.

The post-fix run at memory fraction 0.6 still encountered pressure. Its new heap contained 86
spill readers, 51 with null input streams and 50 MiB of associated row buffers, versus 1 GiB before.
These snapshots have different memory settings and failure points; their counts are not a
controlled memory-reduction ratio. The regression test establishes the ownership fix.
The remaining payload was dominated by active 32/64-MiB long arrays and additional one-MiB
buffers. A subsequent task reported failure to acquire a 64-KiB managed allocation. The cleanup
fix addresses retention; it does not make arbitrary concurrent operator allocations fit.

The next controlled condition passed both the q1-q11 pressure sequence and the complete
103-query catalog-statistics run by combining the fix with
memory fraction 0.4 and merge factor eight,
keeping the original four-GiB heaps and quarter-CPU task setting in all three modes. Lowering the
fraction reserves more heap outside managed execution memory and also reduces the default
Tungsten page size in this configuration. Earlier failures remain separate diagnostic evidence. The original instances reached their
shutdown backstop before final artifact collection, so both raw heap dumps were lost. The pre-fix
heap-parser JSON was already copied locally; the post-fix counts above were observed during the
run. A separate local replay retains the failing-old/passing-new regression logs. Final
distributed latency results come from a fresh pair with automatic private artifact backup before
shutdown, rather than the lost first matrix. Memory-page sizing still uses executor cores, and
bounded transport buffers still do not bound downstream operator memory.

### Final unprofiled results

The table sums the median planning-plus-execution time for each of all 103 queries. It is not
elapsed wall time for the benchmark process, which includes warmups and diagnostics.

| Runtime / statistics | Ordinary | Previous pipeline | Coalesced pipeline | vs previous pipeline |
|---|---:|---:|---:|---:|
| Local / catalog | 21.167 s | 19.973 s | 20.074 s | +0.51% |
| Local / no catalog | 22.220 s | 21.334 s | 21.222 s | -0.53% |
| RPC / catalog | 51.732 s | 53.280 s | 51.845 s | -2.69% |
| RPC / no catalog | 54.936 s | 56.182 s | 56.601 s | +0.74% |

The new coalescer is effectively flat across the local full suite. Against ordinary shuffle, the
complete pipeline is 5.16% faster with catalog statistics and 4.49% faster without them. Those
improvements include the preceding pipeline implementation and must not all be credited to
coalescing. Conditional paired-bootstrap intervals for coalescing versus the previous pipeline
span zero: [-1.31%, +1.84%] with statistics and [-2.53%, +0.60%] without. These resample four
repetitions within fixed queries, not independent JVM or cluster trials.

In the local runs, with catalog statistics, 71 of 103 queries use at least one live exchange;
without statistics,
74 do. The remaining queries retain materialized execution or have no eligible shuffle.
With catalog statistics, nine queries change layout; their summed medians improve 6.40% versus
the previous pipeline, and summed median task counts fall from 491 to 352.5. Fractional task
counts here are medians across adaptive runs, not fractional tasks. q81 only coalesces in half of
its four samples. The other 94 queries are 0.93% slower in aggregate despite unchanged layouts.
Without catalog statistics, only q6 and q54 change layout; their combined improvement is 2.87%.
The per-query CSV includes all regressions and fallback cases.

Locally, coalescing closes only part of the task-count gap with ordinary AQE. With catalog statistics,
summed median task counts are 3,611 ordinary, 4,419 preceding pipeline and 4,280.5 coalesced.
The coalesced path still launches 18.5% more tasks than ordinary AQE; without statistics it launches
26.4% more (4,446 versus 3,517). Ordinary AQE sizes from completed output, whereas live layouts
must be fixed in advance. Whole-suite task CPU is nearly flat versus the previous pipeline:
77.29 to 77.01 seconds with statistics and 67.55 to 67.20 seconds without. The reader reduction
helps specific plans but does not remove the dominant work throughout the suite.


The distributed result is weaker than the local result. With catalog statistics, coalescing is
2.69% faster than the preceding pipeline, but the complete pipeline is 0.22% slower than ordinary
shuffle. Without statistics it is 0.74% slower than the preceding pipeline and 3.03% slower than
ordinary shuffle. Conditional intervals versus the preceding pipeline are [-3.92%, +0.07%] and
[-1.84%, +2.96%], respectively. The no-statistics comparison against ordinary shuffle has an
interval of [+0.51%, +4.58%]. These results do not establish a general distributed speedup.

The nine changed RPC catalog plans improve 6.11% in aggregate, with median task totals 491 to
350. However, about 1.25 seconds of the 1.43-second full-suite reduction comes from the other 94
queries, whose layouts did not change. It cannot all be assigned to reader coalescing. The two
changed no-statistics queries improve task count 154 to 115 but regress 1.34% in summed medians.
Across the complete RPC suite, coalesced task counts still exceed ordinary AQE by 18.4% with
statistics (4,278 versus 3,612) and 26.5% without (4,448 versus 3,516.5).

Unprofiled RPC q28 and q44 improve 17.5% and 19.3% against the preceding pipeline, but are only
0.5% and 2.5% faster than ordinary shuffle. q54 instead regresses 4.2% with statistics and 7.3%
without them. In the latter case, median planning increases about 10 ms, CPU decreases 44 ms,
and task-reported GC increases. Those overlapping task GC totals are not pause time and cannot
quantify how much of the 34-ms regression they explain. The regression remains in the results.

All four full experiments, including fallback queries and regressions, are in
[`BatchPipelinedShuffleBenchmark-tpcds-sf1-coalescing.csv`](../sql/core/benchmarks/BatchPipelinedShuffleBenchmark-tpcds-sf1-coalescing.csv).


### Local diagnostic profile

The v5 local profile, before the spill-reader fix, uses 12 balanced triples and six warmups per mode on q28, q44,
q49, q77 and the unchanged-layout control q89. The profile is separate from full-suite timings.
Relative to the preceding pipeline, medians changed by -19.3%, -15.4%, -2.1%, -0.6% and +2.5%
respectively. The earlier four-round q77 regression did not reproduce; no query-specific exclusion
was added. q89's earlier large unchanged-layout swing also narrowed in the repeat.

q28 tasks fell from 102 to 60 and median aggregate task CPU from 634 to 549 ms. Across the 12
measured executions, sampled Java execution events fell from 213 to 137; decoding and generated
aggregation remained prominent exclusive leaves. Channel-reader parking was 532 versus 478 ms
summed across tasks and repetitions. These waits are thresholded lower bounds, not query critical
path time. Coalescing does not make a blocking partial aggregate emit rows before its build ends.

q44 shows an additional benefit beyond fewer simultaneous readers. Its scalar subquery used
`executeTake` in three probes of 1, 4 and 3 reduce partitions. Each fresh partial live job restarted
the same eight producer tasks. The first measured preceding-pipeline plan has source stages
2177/2179/2181 scanning the same RDD 4391, paired with reader widths 1/4/3. With one coalesced
reader, stages 2194/2195 execute eight producers and one reader once. This removes 16 repeated
producer tasks plus seven reader tasks: query total 46 to 23. The matching CPU reduction, 354 to
226 ms, and fewer Parquet CPU samples therefore include avoided repeated scans. The data path
still follows the existing partial-read contract; it does not cache or share a live shuffle.

This suggests a broader partial-action scheduling question: avoiding repeated production for
incremental `take` probes without forcing every reader to process all data for a small limit.
Always scheduling all readers would trade away the current ability to discard unrequested reduce
streams, so the present change does not impose that policy.

q77 reader count fell eight to two, but most of the query remained scans, joins and materialized
preparation. The repeated median was effectively flat. The q89 control retained 30 tasks, similar
CPU leaves, and similar aggregate channel waits (2,628 versus 2,649 ms across 12 executions).

### Local regression repeats on final v6

The final full runs showed q6 about 9% slower with coalescing in both statistics conditions.
Separate 12-triple profiles did not reproduce that magnitude: without statistics, medians were
183.83 versus 185.96 ms (+1.16%); with statistics, 154.86 versus 132.57 ms (-14.40%). The
unprofiled slowdown remains recorded; the repeats do not justify a stable regression claim or a
query-specific exception.

q6 coalesces the tiny date-dimension scalar subquery, not the main eight-reader aggregate. Three
partial reads previously reran three producers each; one coalesced reader removes six repeated
producer tasks and seven readers. Without statistics, the main store-sales shuffle and joins
dominate: total task CPU barely changes, 650.5 to 643.2 ms, and aggregate channel waits remain
about 1.56 versus 1.61 seconds across all 12 repeats. With statistics, that preparation changes
and total task CPU falls 174.3 to 149.2 ms. This explains why identical task savings need not
produce identical query speedups.

The unchanged q50 control had a 22% slowdown in the short full run but only +0.60% in its repeat;
CPU samples were 322 versus 321 and both retained 31 tasks. q81's repeated layout reduces 36 to
29 tasks and task CPU 217.9 to 150.9 ms, but latency improves only 3.9%. Its ordinary-shuffle
baseline still finishes faster. Fewer tasks are useful evidence, not a guarantee of lower latency.

### Distributed diagnostic profile

The separate final RPC profile uses eight balanced triples and six warmups per mode. It retains
JFR recordings from the driver and both executor JVMs. The following comparisons are against the
preceding pipeline; profiling timings are not pooled into the unprofiled results.

| Query | Median latency change | Tasks before / after | Task CPU before / after |
|---|---:|---:|---:|
| q28 | -20.2% | 102 / 60 | 1,501 / 1,378 ms |
| q44 | -17.0% | 46 / 23 | 640 / 416 ms |
| q49 | -15.6% | 69 / 59 | 723 / 686 ms |
| q77 | +2.4% | 58 / 52 | 552 / 546 ms |

q28's measured executor thread starts fall from 2,305 to 624 across all eight repetitions.
The breakdown identifies the mechanism: connection-creation threads fall 1,536 to 192 and
reader-discovery threads 384 to 48; writer-flush threads remain 384. Aggregate reader parking
falls from 29.68 to 4.40 seconds. Sampled executor Java execution events fall 603 to 498, with
Parquet decoding and decimal operations still prominent. These thread and wait counts are
summed across both executors and all repetitions, not per-query critical-path durations.

q44 reproduces the partial-read effect on RPC. In iteration zero, producer stages 4745/4747/4749
scan RDD 10661 three times, with reader stages of width 1/4/3. Coalescing uses producer 4762 and
reader 4763 once. Median measured input bytes fall from 3.61 to 1.80 MB, CPU samples from 307 to
180, and reader parking from 1.36 to 0.21 seconds across eight repetitions. Thread starts fall
512 to 104. The saved work includes repeated producers and their flush threads, not just readers.

q49's reader parking falls 9.24 to 2.77 seconds and thread starts 976 to 544, but its CPU sample
count changes little (265 to 256). q77's reader parking also falls, from 1.57 to 0.39 seconds,
yet latency does not improve. Planning is about 11 ms slower in that repeat and most task work
is unchanged. Lower waiting totals alone are not a sufficient performance claim.

### Remaining distributed bottlenecks

**Overlapping operator memory is a material cost.** In the unprofiled catalog run, q4 writes
median physical spill bytes of 14.6 MiB with ordinary shuffle, 106.2 MiB with the preceding
pipeline, and 99.9 MiB with coalescing. Task CPU is 8.55/9.65/9.44 seconds respectively.
The profile repeat also shows substantially more spilling in both pipelined modes; its dominant
leaves include buffered spill reads, row copying and aggregation. The coalescer changes no q4
layout. Concurrent operators can compete for managed memory even when the task group fits.
q11 and q72 likewise spill in the pipelined full runs while ordinary execution's median is zero.
Releasing closed readers prevents retention but does not remove that active-memory tradeoff.

**Whole-stage admission remains conservative around Union.** q80 keeps an eight-task live
producer and a 24-task Union consumer. Together they consume the 32-slot budget at quarter-CPU
tasks. Its two eight-task materialized sibling producers must therefore finish before that group
is admitted. Across the profile repeats, the live source starts a median 209 ms after the earliest
sibling source; ordinary execution's corresponding gap is 47.5 ms. All layouts are unchanged.
The coalesced mode is 16.7% slower than ordinary in that profile, with 9.28 seconds of summed
reader parking across the eight executions. This is a task-group granularity restriction,
not a requirement imposed by AQE. A partition-aware admission contract could distinguish Union
partitions that actually require live peers from independent partitions, but that extension is
not implemented here. It must preserve bounded-stream progress and partial-read semantics.

**Some within-pipeline variation remains unexplained.** Without statistics, q72 is 19.8% slower
with the coalescing flag despite zero coalesced exchanges and identical normalized final plans.
Both pipelined variants spill about 143.5 MiB per execution; additional spill volume does not
explain their difference. The expensive downstream stage's median span grows from 2.32 to
2.91 seconds and its summed task CPU from 16.04 to 17.67 seconds. Planning does not account for
that gap. The full-run task records locate it in execution, but no dedicated q72 JFR repeat was
recorded, so its additional cause is unresolved. q19 also has a large unchanged-layout swing.
Neither result is removed from the aggregate or dismissed as a proven measurement artifact.

The evidence supports generic improvements to reader layout and lifetime management. The next
performance questions are memory-aware admission and partition-level readiness, alongside
reducing per-reader connection/discovery and per-writer timer setup. A shared blocking connection
pool can serialize progress, so thread-count reduction by itself does not justify such a change.
Joint sizing and admission would also let the planner consider an initially over-capacity exchange;
the current rule only resizes edges already admitted. None requires a TPC-DS-specific cost model.


### Validation and limits

The fresh distributed full event logs contain 94,309 and 95,187 successful task-end events,
all jobs successful, exactly two executors added per application and no executor removals.
Both diagnostic applications also contain only successful task ends and jobs. The source overlay's
57 hashes were rechecked after measurement; both executor runtime hashes and all 210 input-file
hashes match. Both final archives were downloaded, verified and copied into the persistent
artifact before deleting the temporary backup bucket and upload role. The two replacement VMs,
volumes, security group, key pair and local private key were also removed.


The 73-test SQL suite passes on Linux, including actual separate executor JVMs. An additional
62 core tests pass on both macOS and Linux: spill-reader cleanup, unsafe external sorting, radix
sorting and read-ahead input streams. Core Scala style and Java Checkstyle pass on both platforms. The preceding
72-test suite passed on macOS; its final 10 planner tests were rerun after the sentinel guard. Production
and test Scala style checks pass. New checks cover uneven writer bucket routing for ordinary and
live shuffle, partial reads and repeated actions, AQE on/off, matching join layouts, live inputs
inside broadcast preparation, fixed siblings, original bucket coverage, aggregate estimates and
revisiting an already-selected edge. These counts overlap across platforms and must not be added as distinct tests.

Waiting readers still occupy task slots, and blocking aggregate builds still delay first output.
Coalescing reduces the number of readers and associated setup, but cannot turn Spark's iterator
stack into resumable operator tasks. It can also reduce useful parallelism when estimates are
wrong. The whole-group admission and finite-batch failure contract remain unchanged.

Fractional CPU settings remain admission controls. In the earlier clean 103-query RPC matrix at
width two, pipelined totals were 72.644/72.587/72.875 seconds at task CPUs 1/0.5/0.25. That is
essentially flat despite much faster producer launch in some profiles. The new width-eight
experiments use a quarter CPU throughout and do not establish a new fractional-CPU speedup.

### Upstream and MPP context

The implementation follows the [upstream bounded-queue deadlock discussion](https://github.com/apache/spark/pull/58097#issuecomment-5376850523):
it maps buckets before stream creation instead of sequentially draining multiple live streams.
It also preserves the requested-width and whole-group constraints discussed in the
[upstream scope review](https://github.com/apache/spark/pull/58097#issuecomment-5614533527).
The use of standard catalog statistics is consistent with
[Trino's documented dependence on connector statistics](https://trino.io/docs/current/optimizer/cost-based-optimizations.html).
The [bounded spill merge review](https://github.com/apache/spark/pull/55275) explicitly weighs
extra merge I/O against peak memory and keeps the default disabled to avoid regressions in jobs
already provisioned with sufficient memory. Its manual-merge comment and the code in this
checkout confirm its integration; a closed PR alone would not establish that. The present
experiment opts in consistently for all distributed modes rather than changing Spark's default.
The [current upstream spill reader](https://github.com/apache/spark/blob/66a28acd5b1cf34458de74f5ff6703a4851ad751/core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeSorterSpillReader.java)
still has the task-completion capture addressed by the new ownership fix.
The separate [execution-memory concurrency fix](https://github.com/apache/spark/pull/58747)
was reviewed but not incorporated; this change adds no concurrent TaskMemoryManager calls or
cooperative task runtime. Two other current proposals are relevant to the observed pressure:
[OOM retry CPU growth](https://github.com/apache/spark/pull/57329) reduces retry concurrency, which
cannot be applied blindly to a live group whose peers must run together; and
[optional execution-memory reclamation](https://github.com/apache/spark/pull/58763) provides
reservations and reclaimers for disposable work such as read-ahead, but does not automatically
make existing readers use those APIs. Neither is incorporated in this experiment.
The open [loser-tree spill merger proposal](https://github.com/apache/spark/pull/56804) targets
comparison overhead in k-way merges. Its merger microbenchmark does not establish a query-level
benefit here, and changing the merge algorithm would not release retained reader buffers. It is
a candidate only if the spill profile identifies comparison work as a material bottleneck. The
related [iterator-count contract fix](https://github.com/apache/spark/pull/58801) is also separate
from the ownership problem. Neither change is included in these measurements.
None of those references proves a Spark performance improvement;
the measurements above test that question directly.
