# Morsel-driven execution in Spark executors

Status: working prototype, behind two flags, off by default. It has unit tests, a correctness
sweep over Spark's SQL suites, a skew benchmark suite, and TPC-DS SF10/SF100 runs on 16-, 32- and
64-vCPU Graviton machines.

## What it does

Spark's distributed scheduling, stages, tasks, shuffle files and fault tolerance are unchanged.
What changes is execution inside an executor:

- **Morsels per task.** An eligible map task splits its input into morsels and puts them on its
  own queue.
- **Idle slots help.** When an executor CPU slot has been idle for `helperDelay`, a helper thread
  steals morsels from the back of the task with the most remaining input per participant. The
  task's own thread keeps taking morsels from the front. This is the classic work-stealing
  layout: a task owns a range and others steal only once they run out of their own work, as in
  HyPer and Cilk.
- **Preemption at morsel boundaries.** When the driver launches a new task on the executor,
  helpers give the slot back at their next morsel boundary.
- **Morsel sources:**
  - **File scans:** Parquet row groups, or for large row groups, page ranges of one column (from
    its offset index). The vectorized reader reads only the rows of those pages through
    `readFilteredRowGroup` with explicit `RowRanges`, the mechanism used by column index filtering.
    Morsels are sized by uncompressed bytes and by rows (about 1M per morsel), because compressed
    bytes badly underestimate the work in highly compressible data.
  - **Shuffle reads that AQE coalesced:** whole reducer partitions. Each holds every row of its
    keys, so final aggregations, windows, global sorts and sort-merge joins can process it on its
    own.
  - **AQE local shuffle reads** (after a join was turned into a broadcast join): map outputs, for
    row-wise pipelines.

The central invariant, and the lesson of Sail's
[#2171](https://github.com/lakehq/sail/pull/2171), is that work is shared only within one task
attempt. The queue holds that attempt's input and nothing else, and every participant's output is
written into that attempt's single map output. So the following behave exactly as before:

- map output identity and MapStatus
- retries, speculation and fetch-failure recomputation

**Row-based checksum caveat.** Spark compares an order-independent row checksum
(`spark.sql.shuffle.orderIndependentChecksum`) across attempts and rolls back consumer stages when
it changes. Row-wise and key-grouped pipelines produce the same rows per reducer however their
input is split, so their checksums are kept. Partial aggregations and partial window group limits
don't: they emit rows per participant. Their morsel-executed map outputs report no checksum (0),
which every attempt reports the same way. Changes upstream are still caught by the upstream
stage's own checksum.

## Design

| Piece | Where | Notes |
|---|---|---|
| `MorselScheduler` | `core/.../scheduler/morsel/MorselScheduler.scala` | One per executor. Tracks busy slots (task threads plus helpers). A dispatcher thread starts helpers on slots idle for `helperDelay`; helpers yield at morsel boundaries when the executor is oversubscribed. |
| `MorselJob` | same file | The per-task-attempt deque. The owner takes from the front, helpers steal from the back. While no helper competes, the owner takes up to 4 adjacent morsels at once, merged when possible. Also handles failure propagation (inputs end, then the owner rethrows), waiting for helpers, and merging helper metrics and accumulators. |
| `MorselTaskContext` | same file | A helper's TaskContext. It shares the owner's identity and kill/fetch-failure state but has its own `TaskMemoryManager` (negative synthetic attempt id), metrics and listeners. |
| `SharedMorselInput` | same file | Both sides of a join must see the same reducer partitions: the side read first claims morsels and the other side replays them. |
| Helper lifecycle | same file | Helpers of a killed task are interrupted, so one blocked in I/O can't stall the kill. Helper runs that never started when the executor stops fail their task instead of losing a claimed morsel. |
| `MorselShuffleMapTask` | `core/.../scheduler/morsel/MorselShuffleMapTask.scala` | Hooks into `ShuffleMapTask.runTask`. Walks the RDD chain (MapPartitions / Union / Zipped over a `MorselSource`) and groups adjacent small morsels up to the target size. Runs the owner and helpers; each helper deserializes its own copy of the task, so operator state is never shared across threads. |
| `MorselShuffleWriter` | `core/.../shuffle/sort/MorselShuffleWriter.java` | Each participant writes the way stock Spark would: per-partition files where bypass-merge applies (≤200 partitions), sorted spills otherwise. Commit concatenates each partition across participants and combines ADLER32 and row-based checksums. With no helpers it does the same work as stock. |
| `MorselSafety` | `sql/core/.../exchange/MorselSafety.scala` | Marks a `ShuffleDependency` `morselSafe`, or gives the blocking operator. Three input kinds, below. |
| Sources | `FileScanRDD`, `ParquetMorsels`, `ShuffledRowRDD` | `splitIntoMorsels` / `splitIntoAlignedMorsels` / `mergeMorsels`. Parquet files are split on the executor from their footer, and the footer is handed to each morsel's reader so it isn't read twice (except for encrypted files). Page ranges are only used by the vectorized reader and only without row-index metadata columns; the row-based reader fails if it is ever given one. Files whose footer can't be read stay whole, so the reader's error handling (e.g. `ignoreMissingFiles`) applies. Files aren't split when the `_metadata.file_block_start/length` columns are read. Shuffle reads aren't split when they include push-merged blocks. |

`MorselSafety`'s three input kinds:

| Input kind | Allowed operators |
|---|---|
| File ranges | Deterministic row-wise operators, streamed sides of broadcast joins, local sorts, partial aggregates, partial window group limits, unions |
| Coalesced reducer partitions | The above, plus key-grouped operators (final aggregates, windows, global sorts) and sort-merge joins of two shuffles with a sort under each side, so the side read first is drained before the other is read. Shuffled hash joins are excluded: their hash table can't spill and is freed only when the task completes, so a helper could wait forever for memory that the waiting owner holds. |
| Map outputs of local reads | Same as file ranges |

Configs:

| Config | Scope | Default |
|---|---|---|
| `spark.executor.morsel.enabled` | executor | false |
| `spark.executor.morsel.slots` | executor | executor cores |
| `spark.executor.morsel.targetSize` | executor | 16m |
| `spark.executor.morsel.helperDelay` | executor | 20ms |
| `spark.sql.execution.morsel.enabled` | per query | false |

### Prior art and how it maps

- **HyPer** ([Leis et al., SIGMOD'14](https://db.in.tum.de/~leis/papers/morsels.pdf)): workers own
  ranges and steal at morsel boundaries, with worker-local state merged at the end. Here a Spark
  task plays the role of the owned range. HyPer uses ~100k-tuple morsels; these are row groups,
  page ranges of about 1M rows, or reducer partitions grouped up to 16 MB.
- **DuckDB:** sink/combine/finalize with thread-local state. Here each participant has its own
  pipeline instance (partial-aggregate tables, sort buffers, shuffle sinks), and the owner does
  the combine step (concatenating shuffle segments).
- **DataFusion [#21351](https://github.com/apache/datafusion/pull/21351):** shared file queues
  across sibling partitions; 7–9% on ClickBench, roughly neutral on TPC-DS SF1. This is the
  per-task-ownership variant of that idea.
- **Sail [#2171](https://github.com/lakehq/sail/pull/2171):** independently deserialized task plans
  each built their own shared queue and reread files. Scoping the queue to one task attempt avoids
  that by construction.

## Results

Setup:
- On-Demand m8gd.4xlarge / 8xlarge / 16xlarge (16 / 32 / 64 Graviton4 vCPU), local NVMe.
- `local[N]`: one executor with N slots.
- Default AQE.
- Each run alternates morsels off and on per query in the same JVM and compares
  order-insensitive result hashes.
- CPU busy is user + system time from `vmstat`, averaged over each run.
- The numbers predate fixes from an independent review in the final commit. Those fixes shouldn't
  change what these benchmarks run: they don't choose shuffled hash joins under default settings
  and use no push-based shuffle, block metadata columns or encryption, and the serializer and
  checksum fixes don't affect speed.

### Skew suite

Synthetic Parquet data, about 3.4 GB per table, 4 measured reps per mode.

| Query | What it tests |
|---|---|
| Density skew | 56 ordinary files plus 8 highly compressible files holding twice as many rows. Spark packs splits by compressed bytes, so one or two tasks get most of the rows. |
| Few large tasks | 4 equal 850 MB files read as 4 tasks: a stage with fewer tasks than cores, not skew. |
| One row group | One file with a single ~1 GB row group: one task. |
| Balanced, short tasks, shuffle-all, trivial count | Overhead checks. |

| Query | 16 vCPU (v10) | 32 vCPU (v10) | 64 vCPU (v10) | 64 vCPU (v12) |
|---|---|---|---|---|
| Density skew | 8.8× (CPU 11% → 91%) | 15.3× | 28.2× | **26.5×** |
| Few large tasks | 3.6× | 6.5× | 10.3× | **9.2×** |
| One row group | 11.5× | 16.7× | 17.7× | **16.4×** |
| Balanced / short tasks / shuffle-all | 0.97–1.03× | 0.99–1.03× | 0.99–1.09× | **0.99–1.00×** |
| Trivial count (0.2–0.3 s) | 0.85× | 0.90× | 0.85× | **0.96×** |

On 16 vCPU with v12, density skew reaches 9.0× (CPU 11% → 94%) and the balanced cases
1.00–1.03×.

"Few large tasks" has a ceiling of cores ÷ 4. Stock Spark could recover much of that gain with a
smaller `maxPartitionBytes`; that tuned baseline wasn't run.

### TPC-DS

Spark's `GenTPCDSData`, partitioned Parquet, all 103 queries. One warmup and one measured run per
mode at SF100, two at SF10.

| Run | Total speedup | Geomean | CPU busy off → on |
|---|---|---|---|
| SF100, 64 vCPU, **v12** | **1.117×** | **1.106×** | 37.1% → 44.0% |
| SF100, 64 vCPU, v10 | 1.094× | 1.094× | 39.4% → 46.9% |
| SF100, 32 vCPU, v10 | 1.046× | 1.081× | 49.4% → 55.5% |
| SF100, 16 vCPU, v10 | 1.035× | 1.062× | 61.4% → 65.8% |
| SF10, 16 vCPU, v10 | 1.012× | 0.986× | 47.5% → 48.6% |

- **Best (64 vCPU, v12):** q79 3.5×, q30 3.3×, q73 2.3×, q81 2.0×, q34 1.9×, q6 1.9×.
- **v10's regressions and v12's fixes:**

  | Query | v10 | v12 |
  |---|---|---|
  | q59 | 0.74× | 1.16× |
  | q71 | 0.75× | 1.10× |
  | q53 | 0.88× | 1.00× |
  | q51 | 0.87× | 0.95× |
  | q5 | 0.87× | 0.94× |
  | q92 | 0.80× | 0.94× |

  The cause was one reducer partition per morsel: a task that AQE coalesced from about 50 small
  reducers set up about 50 separate fetches. v12 groups adjacent small morsels up to the target
  size; both sides of a join get the same groups.
- **Where the gain comes from:** mostly stage tails, wave quantization (for example 17 scan tasks
  on 16 cores) and uneven reduce tasks. TPC-DS data is uniform, so there's little real skew. Gains
  grow with core count because bigger executors have more idle cores in those phases.
- **q65 mismatch:** flagged on every machine. All four stock runs also returned different top-100
  rows, including two identical 64-vCPU machines with the same data and build. Its
  `ORDER BY s_store_name, i_item_desc LIMIT 100` has ties at the limit, so q65 is nondeterministic
  in stock Spark.

### Correctness

- **Unit tests:** `MorselExecutionSuite`, 17 tests, all passing. They cover:
  - partial and final aggregates, windows, window group limits
  - broadcast joins, and joins of two shuffles (SMJ split; SHJ not split; inner, outer, anti)
  - which outputs keep a row-based checksum
  - unions, expand and generate, range partitioning
  - Parquet page ranges (vectorized and row-based, with and without row indexes) and merged row
    groups
  - local shuffle reads
  - metrics, cancellation and error propagation
- **Spark's SQL suites with morsels forced on** (helpers start immediately, 1 KB morsels):
  JoinSuite, DataFrameAggregateSuite, DataFrameWindowFunctionsSuite, AdaptiveQueryExecSuite and
  SQLQueryTestSuite. 1,087 passed, 4 failed. All 4 failures are row-order differences on queries
  without a total order (ties under `ORDER BY`), e.g. `SELECT b FROM testData GROUP BY a, b
  ORDER BY a`; none returned different rows.
- **TPC-DS SF1 with the same forced settings:** all 103 queries return identical results, across
  3,543 morsel tasks and 7,501 stolen morsels.

### Coverage

Morsel eligibility in the AQE plans that actually executed (TPC-DS SF1, before local shuffle reads
were supported):

| Stage type | Eligible |
|---|---|
| Scan-rooted map stages | 221 / 221 |
| Reduce stages | 75 / 95 |
| All | 296 / 316 (94%) |

Of the 20 blocked reduce stages, 17 were local shuffle reads, which v12 now covers. The other 3 are
2 unions of shuffles and 1 multi-way join.

### Build history

| Build | Change | Effect |
|---|---|---|
| v1 | Scan morsels, every participant writes sorted shuffle spills | TPC-DS SF10 1.009×; q72/q50/q82 15% slower; shuffle-heavy scans up to 54% slower |
| v3 | Writer matches stock (bypass files for ≤200 partitions), union support | Regressions gone; SF10 1.018×, density skew 5.9× |
| v7 | Parquet page-range morsels | Not benchmarked |
| v10 | Reducer, join and local-read morsels; Parquet morsels sized by rows | Density skew 8.8× (16 vCPU); SF100 1.094× (64 vCPU); small fixed overhead on short scans |
| v12 | Footer reuse, page splits only for large row groups, grouping of small morsels, owner merging | SF100 1.117× (64 vCPU); trivial-count overhead 15% → 4% |

## Limitations and next steps

1. **One hot key stays in one reducer partition,** a single morsel. AQE's skew-join split handles
   joins; aggregations would need a HyPer-style partitioned merge.
2. **Executor-local only.** Work isn't moved between executors.
3. **Only shuffle map tasks run as morsels.** Result tasks and broadcast collection don't.
4. **Row order changes.** Rows within one map output come out in participant order, so ties under
   `ORDER BY` and order-dependent aggregates (`first`, `collect_list`) can differ from stock. Spark
   guarantees neither, but golden-file tests that encode stock's incidental order fail when morsels
   are forced on.
5. **Multi-way joins** in one stage are rejected, because claims happen while the pipeline runs.
   Fixing a participant's claims up front, with one pipeline instance per claimed batch, would lift
   this.
6. **Smaller known issues** from an independent review:
   - A helper that yields its slot keeps running until it finishes its current morsel, so the
     executor is briefly oversubscribed.
   - Helpers may start immediately after executor start, without `helperDelay`.
   - Metrics and accumulators of a helper that failed are dropped.
   - Push-merged shuffle read metrics and the write time of merged files aren't added to the task.
   - The row-based Parquet reader picks row groups by midpoint from the file's own metadata. A
     file whose row group sizes are reported inconsistently could make a row group fall outside
     every morsel.
7. **Not measured yet:**
   - multi-executor standalone mode
   - concurrent queries on one executor
   - object storage (S3), where parallel reads of one file should help more
   - a stock baseline with `maxPartitionBytes` tuned for the few-large-tasks case

## Testing

`build/sbt "sql/testOnly *MorselExecutionSuite"`
