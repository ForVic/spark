# Morsel-driven execution in Spark executors

Status: working prototype, behind two flags, off by default.

Morsels run in:
- shuffle map tasks and file writes
- scans of files, data source V2 and coalesced shuffles

Helpers share their task's memory. Tested with unit tests and Spark's SQL suites with morsels
forced on, in local-cluster mode, and on a 3-node standalone cluster with executor kills.
Benchmarked on TPC-DS SF10 (V1 and V2), ClickBench and targeted cases.

## What it does

Spark's distributed scheduling, stages, tasks, shuffle files, commit protocols and fault tolerance
are unchanged. What changes is execution inside an executor:

- **Morsels per task.** An eligible task splits its input into morsels and puts them on its own
  queue.
- **Idle cores help.** When an executor CPU slot has been idle for `helperDelay`, a helper thread
  steals morsels from the back of the task with the most remaining input per participant. The
  task's own thread keeps taking morsels from the front. This is HyPer/Cilk-style work stealing: a
  task owns a range and others steal only when they run out of their own work.
- **Helpers only join when it pays.** Starting a helper costs tens of milliseconds: it
  deserializes the task and sets up its own pipeline. So a helper joins only if its share of the
  remaining input is at least half a morsel, or at least 100 ms of work at the rate the task's
  own thread processes morsels.
- **Spare CPUs of multi-CPU tasks.** A task given several CPUs (`spark.task.cpus`) runs helpers of
  its own on them right away. Because helpers share their task's memory, an executor can run
  fewer, larger tasks, each with a larger share of memory, without leaving cores idle.
- **Preemption at morsel boundaries.** When the driver launches a new task on the executor,
  helpers on idle slots give the slot back at their next morsel boundary.

Which tasks run as morsels:

| Task | Output | How the participants' output becomes the task's |
|---|---|---|
| Shuffle map task | Map output | Each participant writes its own spill files. They are concatenated per reduce partition into the task's single map output. |
| File write: planned V1 writes, i.e. `WriteFilesExec` (`INSERT`, CTAS, `DataFrameWriter` to file sources and Hive tables) | Files plus a commit message | Each participant writes its own files through the task's commit protocol, and the task commits them all with one task commit. |

Morsel sources:

- **File scans, V1 and V2 Parquet:** row groups. For large row groups, morsels are page ranges of
  one column; only the V1 vectorized reader reads page ranges.
  - Sizing: morsels are sized by the uncompressed bytes of the columns the scan reads, plus a byte
    per row. Compressed bytes underestimate the work in compressible data, and unread columns are
    no work.
  - Footer reuse: the footer read when splitting is handed to each morsel's reader.
- **Data source V2 scans:**
  - Connectors opt in with `SplittableInputPartition` (below).
  - Built-in V2 file sources split their `FilePartition`s into files and Parquet row groups.
- **Shuffle reads that AQE coalesced:** whole reducer partitions. Each holds every row of its keys,
  so key-grouped operators can process it on its own.
- **Joins of two shuffles, one side at a time:** when a join task's input is one dominant reducer
  partition (a skewed key, or a slice of one that AQE's skew handling split off), the task may
  instead split one side into ranges of map outputs. Each range joins with the other side's whole
  partition, which every participant reads, like AQE's skew join splitting but at run time and
  within the task. This applies when all of the following hold:
  - the join type allows splitting that side, by AQE's rules (e.g. only the left side of a left
    outer join)
  - nothing after the join in the stage needs all rows of a key together
  - the other side is at most half as large
- **AQE local shuffle reads:** map outputs.

A task whose input is one morsel (or none) runs on its own thread with the normal writer. It still
reuses what splitting found out, such as the footer, or that a file range holds no row group.

The central invariant, and the lesson of Sail's
[#2171](https://github.com/lakehq/sail/pull/2171): work is shared only within one task attempt.
The queue holds that attempt's input and nothing else, and every participant's output becomes part
of that attempt's single output: its map output, or the files of its single task commit. So the
following behave exactly as before:

- the task's result (MapStatus, or WriteTaskResult)
- output commit coordination
- retries, speculation and fetch-failure recomputation

**Row-based checksum caveat.** Spark compares an order-independent row checksum
(`spark.sql.shuffle.orderIndependentChecksum`) across attempts and rolls back consumer stages when
it changes. Row-wise and key-grouped pipelines produce the same rows per reducer however their
input is split, so their checksums are kept. Partial aggregations and partial window group limits
don't: they emit rows per participant. Their morsel-executed map outputs report no checksum (0),
which every attempt reports the same way.

## Design

| Piece | Where | Notes |
|---|---|---|
| `MorselScheduler` | `core/.../scheduler/morsel/MorselScheduler.scala` | One per executor. Tracks busy slots (task threads, helpers on idle slots, helpers on tasks' spare CPUs). A dispatcher thread starts helpers; helpers on idle slots yield at morsel boundaries when the executor is oversubscribed. |
| `MorselJob` | same file | A task attempt's deque. Owner takes from the front, helpers steal from the back; uncontended, the owner takes up to 4 adjacent morsels at once, merged when possible. Handles failure propagation (inputs end, then the owner rethrows), waiting for helpers, aborting helpers and taking back their morsels (see Memory), and merging helper metrics and accumulators. |
| `MorselTaskContext` | same file | A participant's TaskContext. Same identity, kill and fetch-failure state as the owner's, with its own `TaskMemoryManager` (sharing the task's memory), metrics and completion listeners. |
| `SharedMorselInput` | same file | Both sides of a join see the same reducer partitions: the side read first claims morsels, the other side replays them. |
| `MorselSink`, `MorselSinkRDD` | `core/.../scheduler/morsel/MorselSink.scala` | What a task writes: `open` once per task, `writePart` per participant (with the participant's own deserialized copy of the sink), `commit` once with all parts, `discard`/`abort` on failure. A result task whose RDD is a `MorselSinkRDD` can run as morsels. |
| `MorselTask` | `core/.../scheduler/morsel/MorselTask.scala` | Hooks into `ShuffleMapTask` and `ResultTask`. Walks the RDD chain (MapPartitions / Union / Zipped over a `MorselSource`), groups adjacent small morsels up to the target size, and runs owner and helpers over a `MorselSink`. Each participant deserializes its own copy of the task, so operator state is never shared across threads. Contains the shuffle sink. |
| `MorselShuffleWriter` | `core/.../shuffle/sort/MorselShuffleWriter.java` | Participants write like stock Spark: per-partition files where bypass-merge applies, sorted spills otherwise. Commit concatenates each partition across participants and combines ADLER32 and row-based checksums. |
| `FileWriteMorselSink` | `sql/core/.../datasources/FileWriteMorselSink.scala` | The file-write sink, below. Without morsels it runs `FileFormatWriter.executeTask` unchanged. |
| `TaskMemoryManager(parent)`, `setMemoryReclaimer` | `core/.../memory/TaskMemoryManager.java` | A manager for a thread helping another task: its memory is charged to that task in the pool and counted in its peak memory, but it spills and cleans up only its own consumers. A reclaimer hook lets the owner free memory that helpers hold. |
| `MorselSafety` | `sql/core/.../exchange/MorselSafety.scala` | Decides whether a shuffle's map side, or a write, may run as morsels, or gives the blocking operator. |
| Sources | `FileScanRDD`, `DataSourceRDD`, `ParquetMorsels`, `ShuffledRowRDD` | `splitIntoMorsels` / `splitIntoAlignedMorsels` / `mergeMorsels`. Splitting falls back to whole files on errors, and doesn't happen when `_metadata.file_block_start/length` are read, for push-merged shuffle blocks, or for V2 Parquet scans with pushed-down aggregates (their readers answer from the footer of the whole file). |

### Memory

A helper's memory is charged to the task it helps. The execution memory pool sees one task, so
helping neither shrinks other tasks' shares (each of N active tasks may use up to 1/N) nor gets
more than the task's own share. Each participant still has its own `TaskMemoryManager`, so its
consumers spill independently, and cleaning up a helper releases only its own memory.

Participants of one task can't spill each other's consumers (another thread is using them), so
helpers give way to the task's own thread whenever the task runs short:

- **A helper that runs out of memory is aborted.** It is never failed: its part of the output is
  discarded and its morsels go back to the queue, and no new helper joins that task.
- **When the task's own thread runs short**, even after spilling its own consumers, it aborts the
  task's helpers, waits for them to release their memory, and retries. This goes through
  `TaskMemoryManager.setMemoryReclaimer`.
- **Morsels given back after the owner's pipeline finished** run afterwards on the owner's thread,
  as one more participant.

Helpers never wait for memory that their own task's thread holds, so the old deadlock is gone.
That deadlock was a helper waiting in the pool while the owner held its memory and waited for the
helper. Shuffled hash joins, previously excluded because of it, now run as morsels. In the worst
case a task does what it would have done without helpers.

### File writes

`WriteFilesExec` writes through a `MorselSinkRDD` when `spark.sql.execution.morsel.enabled` is set
and the write is safe to split:

- The task's own thread sets up the task attempt and its commit protocol once.
- Every participant writes its rows with its own `FileFormatDataWriter` and stats trackers. It
  creates files through the task's commit protocol: a per-participant wrapper serializes calls to
  it and makes the names unique (`part-…-m<N>-c000…`; the task's own thread keeps stock names).
- The task then commits once. Its commit message covers every participant's files and dynamic
  partitions, and its stats are those of all participants combined.
- An aborted helper deletes its files. A failed task aborts its task attempt as usual.

Writes are not split when:
- a stats tracker other than Spark's basic one is used: their per-task stats can't be combined
  generically, e.g. Delta's
- the table has custom partition locations: commit protocols track those files for the job commit
- the writer needs the rows of each output partition or bucket adjacent (dynamic partitions or
  buckets without concurrent writers) and the stage doesn't sort them itself, i.e. it relies on the
  order of its input

### Data source V2

`org.apache.spark.sql.connector.read.SplittableInputPartition` (`@Evolving`) is a mix-in with one
method, `InputPartition[] split(long targetSizeInBytes)`. The contract:
- The returned partitions together hold exactly the input partition's records.
- Each is readable by the same `PartitionReaderFactory`, in the same row/columnar mode.
- They may be read concurrently and in any order.
- Pieces that implement `HasPartitionStatistics` report their size.

Spark doesn't split partitions of scans that report an ordering or a key-grouped partitioning. For
example, an Iceberg scan task group can split into its file scan tasks.

`DataSourceRDD` is a morsel source. A task's input partition splits into:
- the files of a built-in V2 file source's `FilePartition`, further into Parquet row groups, whose
  readers reuse the footer like V1's
- the pieces of a `SplittableInputPartition`
- otherwise, one morsel

### What `MorselSafety` allows

| Input kind | Allowed operators |
|---|---|
| File ranges, V2 input partitions | Deterministic row-wise operators, streamed sides of broadcast joins, local sorts, partial aggregates, partial window group limits, unions |
| Coalesced reducer partitions, and AQE's skew-split slices of them | The above, plus key-grouped operators (final aggregates, windows, global sorts) and joins of two shuffles: sort-merge joins with a sort under each side, and shuffled hash joins. In both, the side read first is drained before the other is read. One-sided splitting of a join is allowed only if no key-grouped operator follows the join in the stage. |
| Map outputs of local reads | Same as file ranges |

A shuffle's partitioning must place each row independently of the others (hash, range, single).
A write additionally needs the conditions above.

### Configs

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
  task plays the role of the owned range.
- **DuckDB:** sink/combine/finalize with thread-local state. Here each participant has its own
  pipeline instance and sink part (`MorselSink.writePart`), and the owner does the combine step
  (`commit`). DuckDB's memory manager also gives operators reservations from one budget, which
  is what charging helpers to their task does.
- **DataFusion [#21351](https://github.com/apache/datafusion/pull/21351):** shared file queues
  across sibling partitions; 7–9% on ClickBench, roughly neutral on TPC-DS SF1. This is the
  per-task-ownership variant of that idea.
- **Sail [#2171](https://github.com/lakehq/sail/pull/2171):** independently deserialized task plans
  each built their own shared queue and reread files. Scoping the queue to one task attempt avoids
  that by construction.

## Results (round 2)

Setup:
- Each run alternates morsels off and on per query in one JVM, with one warmup and two measured
  runs per mode; medians are reported.
- On-Demand m8gd.4xlarge (16 Graviton4 vCPU, 64 GB), local NVMe, `local[16]` with a 48 GB
  driver, default AQE.
- A listener records:
  - task CPU time, including helpers' CPU
  - spill
  - each task's peak execution memory, including its helpers'
  - executor JVM heap peaks, from 0.5 s heartbeats
- CPU busy is user + system time from `vmstat` over each run.
- Results are compared with order-insensitive hashes.
- Builds: TPC-DS V1, ClickBench and the cluster ran the final build. The data source V2,
  targeted and trade-off runs ran the build just before scans began sharing the splitter's
  broadcast (last row of the overheads table below). That costs about 7.5 ms per V1 Parquet scan
  per query and only shows in the shortest queries, such as the 0.3 s count.

### General case: should not regress

| Benchmark | Queries | Stock (s) | Morsels (s) | Total speedup | Geomean | Task CPU on/off | Results match |
|---|---|---|---|---|---|---|---|
| TPC-DS SF10, catalog tables (V1 Parquet) | 103 | 154.7 | 154.3 | 1.003× | 1.001× | 1.018× | 103/103 |
| TPC-DS SF10 through data source V2 (temp views over V2 Parquet) | 103 | 192.0 | 195.9 | 0.980× | 0.986× | 1.037× | 103/103 |
| ClickBench (hits.parquet, 14 GB) | 43 | 113.3 | 112.5 | 1.007× | 0.998× | 1.006× | 40/43 |
| TPC-DS SF10, 3-node standalone cluster (6 executors × 8 cores, each node reading an identical local copy of the data), first 87 queries* | 87 | 83.9 | 83.7 | 1.002× | 1.001× | 1.012× | 86/86 |

- \* The VMs shut themselves down at their deadline before the last 16 queries' results were copied off. On an NFS share instead of local copies, the same cluster was dominated by random 5 s read stalls in single scan tasks (in both modes, unrelated to morsels). Its results all matched (103/103), but its timings aren't meaningful.
- The 3 ClickBench mismatches aren't wrong rows:
  - cb32 and cb39 give a different answer on every stock run.
  - cb30 has a tie at its `LIMIT 10` (ranks 10 and 11 both count 1,058), so which row it keeps
    depends on arrival order.
  - cb31 (also ties) happened to match this time; round 1's q65 is the same case.
- At this scale TPC-DS is uniform and its tasks are short, so there is little for helpers to do.
  Round 1 measured 1.117× on SF100 with 64 vCPU, where stage tails and wave quantization matter
  more.
- Data source V2 costs about 4% more CPU, which round 2 didn't remove: one reader per morsel
  instead of one per task, plus helper startup on V2's many small files.

Overheads that benchmarking turned up and that round 2 fixed:

| Problem | Seen as | Fix |
|---|---|---|
| Morsels sized by all columns of wide files. ClickBench has 105 columns, so a 1M-row row group became ~19 page ranges for a 1-column query. | ClickBench simple scans 0.78×, +20% CPU | Size by the columns read, plus a byte per row |
| Tasks with zero or one morsel took the stock path and read Parquet footers again | ClickBench splits holding no row group | Run them from the split input with the normal writer |
| V2 Parquet reader ignored the footer read during splitting | TPC-DS V2 0.97×, +4.6% CPU | Reuse it, as V1 does |
| Helpers joined tiny tasks: their startup (deserializing the task) outweighed the work | TPC-DS short stages up to 3× CPU | Join only for at least half a morsel, or 100 ms of work at the rate the owner processes morsels after its first |
| A Hadoop configuration broadcast per Parquet scan for the splitter: ~7.5 ms of driver time each | TPC-DS short queries +26 ms; worse on clusters, where executors fetch each broadcast | Scans with the same configuration share one broadcast |

### Targeted cases

Synthetic Parquet data (`gen_skew.scala`, 120M rows per table), 16 vCPU:

| Query | What it tests | Stock (s) | Morsels (s) | Speedup | Task CPU on/off | CPU busy off → on | Task peak memory off → on (MB) | Spill off → on (MB) |
|---|---|---|---|---|---|---|---|---|
| balanced | balanced scan + aggregate (general case) | 3.6 | 3.6 | 1.00× | 1.00× | 78% → 77% | 64 → 64 | 0 → 0 |
| small_tasks | 231 short tasks (general case) | 2.3 | 2.3 | 0.98× | 1.00× | 67% → 69% | 64 → 64 | 0 → 0 |
| shuffle_all | scan + repartition + aggregate (general case) | 2.6 | 2.6 | 0.99× | 1.03× | 70% → 68% | 193 → 193 | 0 → 0 |
| count | 0.3 s count (general case) | 0.3 | 0.4 | 0.93× | 1.02× | 26% → 25% | 0 → 0 | 0 → 0 |
| density | density skew: 8 compressible files hold 2/3 of the rows | 80.3 | 8.9 | 9.01× | 1.01× | 10% → 88% | 64 → 1028 | 0 → 0 |
| few | 4 large tasks on 16 cores | 13.3 | 3.7 | 3.58× | 1.01× | 26% → 75% | 64 → 514 | 0 → 0 |
| one_rowgroup | one 1 GB row group | 10.3 | 0.9 | 12.01× | 1.06× | 8% → 36% | 64 → 1028 | 0 → 0 |
| v2_balanced | balanced, data source V2 | 3.6 | 3.6 | 1.00× | 1.00× | 79% → 80% | 64 → 64 | 0 → 0 |
| v2_density | density skew, data source V2 | 80.5 | 8.9 | 9.04× | 1.01× | 10% → 84% | 64 → 1028 | 0 → 0 |
| v2_few | 4 large tasks, data source V2 | 13.3 | 3.8 | 3.56× | 1.01× | 25% → 70% | 64 → 385 | 0 → 0 |
| write_balanced | write 120M rows (general case) | 8.1 | 8.5 | 0.96× | 1.02× | 84% → 85% | 0 → 0 | 0 → 0 |
| write_few | write from 4 large tasks | 27.7 | 8.0 | 3.46× | 1.02× | 25% → 82% | 0 → 0 | 0 → 0 |
| write_density_partitioned | partitioned write of skewed input | 123.3 | 23.7 | 5.20× | 1.00× | 11% → 56% | 20992 → 16736 | 7 → 36 |
| write_after_agg | aggregate (400 partitions) then write | 63.3 | 25.7 | 2.46× | 1.03× | 14% → 47% | 19328 → 18688 | 6 → 17 |
| shj | shuffled hash join | 1.1 | 1.2 | 0.93× | 1.04× | 45% → 48% | 67 → 67 | 0 → 0 |
| smj | sort-merge join | 1.2 | 1.2 | 0.97× | 1.02× | 44% → 47% | 180 → 180 | 0 → 0 |
| window | window over skewed partitions | 59.4 | 25.8 | 2.30× | 1.01× | 34% → 78% | 1094 → 1094 | 0 → 0 |
| agg_high_card | 120M-group aggregate | 11.4 | 11.4 | 1.00× | 1.00× | 83% → 83% | 512 → 512 | 0 → 0 |

- **Writes:** the two big wins come from tasks that write much more than others: a few large
  files, or density-skewed partitions with dynamic partitioning, where each participant sorts its
  own rows by partition.
- **Data source V2** gets the same gains as V1 for the same data.
- **Peak memory per task** grows with the number of participants, because helpers' memory now
  counts towards their task, while executor heap peaks are unchanged.
- **Joins:** shuffled hash and sort-merge joins over coalesced shuffle partitions now run as
  morsels; these join queries are too short (~1 s) to gain.

### Skewed joins

One key holds 30% (or 10%) of 200M fact rows, joined with a 10M-row dimension table. There are
16 shuffle partitions on 16 cores (tasks = cores, coalescing off), so one join task lags. The
"before" column is the build before join tasks could split one side.

| Query | Stock (s) | Morsels (s) | Speedup | Before |
|---|---|---|---|---|
| 30% hot key, sort-merge join, AQE skew handling off | 16.8 | 8.4 | **2.00×** | 1.13× |
| 30% hot key, shuffled hash join, AQE skew handling off | 12.2 | 6.7 | **1.82×** | 0.99× |
| 30% hot key, left outer join, AQE skew handling off | 16.8 | 8.6 | **1.96×** | 1.04× |
| 30% hot key, sort-merge join, AQE splits it | 8.0 | 7.4 | 1.09× | 1.13× |
| 30% hot key, shuffled hash join, AQE splits it | 8.1 | 6.5 | 1.24× | 1.14× |
| 10% hot key, below AQE's skew thresholds | 12.4 | 8.5 | **1.46×** | 1.14× |
| 30% hot key, aggregated by the join key in the same stage (must not split) | 23.0 | 21.1 | 1.09× | 1.09× |

- **Without AQE's split, morsels reach what AQE achieves** (about 8 s), at run time and within the
  task, for skew AQE ignores (the 10% case) or doesn't detect.
- **On top of AQE's split there is little left.** AQE cut the hot partition into ~64 MB slices
  across more tasks than cores, which leaves only stage tails.
- **Cost:** task CPU is about 5% higher, from every participant re-reading the dimension side.
- **Correctness:** every result matched. A key-grouped aggregate after the join keeps the join
  unsplit, as it must.

### Fewer, larger tasks: memory vs parallelism

Same machine with a 16 GB heap: 16 tasks of one CPU each vs 4 tasks of 4 CPUs each
(`spark.task.cpus=4`). With 4 CPUs a task gets a quarter of execution memory instead of a
sixteenth. Stock Spark then leaves 12 cores idle; with morsels each task runs helpers on its 3
spare CPUs. Every result matched.

| Query | 1 CPU/task, stock | 1 CPU/task, morsels | 4 CPUs/task, stock | 4 CPUs/task, morsels | Spill (GB): 1 stock / 1 morsels / 4 stock / 4 morsels |
|---|---|---|---|---|---|
| Aggregate (400 partitions), then write | 79.5 s | 23.6 s | 98.7 s | **19.6 s** | 2.1 / 2.1 / 0.1 / 0.1 |
| Window over skewed partitions | 70.6 s | 31.5 s | 120.6 s | **39.1 s** | 1.2 / 1.1 / 0.0 / 0.0 |
| 120M-group aggregate | 10.6 s | 10.6 s | 36.0 s | **12.9 s** | 0.0 / 0.0 / 0.0 / 0.0 |
| Density skew | 85.3 s | 9.4 s | 93.0 s | **9.7 s** | 0.0 / 0.0 / 0.0 / 0.0 |
| 4 large tasks | 13.9 s | 4.0 s | 14.0 s | **4.4 s** | 0.0 / 0.0 / 0.0 / 0.0 |
| Shuffled hash join | 1.2 s | 1.3 s | 3.2 s | **2.4 s** | 0.0 / 0.0 / 0.0 / 0.0 |

With morsels, fewer, larger tasks spill far less: the aggregate-then-write spill drops from 2.1 to
0.1 GB, and the window's 1.2 GB spill disappears. And unlike stock, they keep every core busy. In
this run, aggregate-then-write is 4× faster than stock's default setup. It is a trade-off rather
than a free win, though: for the window and the high-cardinality aggregate, one-CPU tasks with
morsels were still faster (31.5 vs 39.1 s and 10.6 vs 12.9 s). Fewer, larger tasks pay off when
spilling or the per-task memory limit is the bottleneck.

### Multi-executor correctness

A standalone cluster of 3 m8gd.4xlarge nodes: one master, 3 workers, and 2 executors of 8 cores
(24 GB) per node, so 6 executors and 48 cores. Executors fetch each other's shuffle output over
the network. Input and output live on an NFS share served by the first node; the performance run
below reads identical local copies instead.

| Run | Queries | Results match |
|---|---|---|
| TPC-DS SF10, all queries, both modes | 103 | 103/103 |
| Targeted suite, including partitioned writes and writes after aggregates to the shared file system | 18 | 18/18 |
| Fault injection: a worker kills one of its executors every 30 s (17 kills, 45 fetch failures and recomputed map stages), targeted subset with writes, joins and windows | 7 | 7/7 |
| Fault injection, 10 of the heaviest TPC-DS queries (q4, q11, q14a, q23a, q64, q67, q72, q74, q78, q95) | 10 | 10/10 |

Writes are checked by reading back what each run wrote.

### Correctness

- **Unit tests:**
  - `MorselExecutionSuite`: 29 tests, including skew joins split by AQE, a skewed partition
    AQE doesn't split, and joins that must not be split.
  - `MorselMultiCpuTaskSuite`: spare-CPU helpers.
  - `MorselLocalClusterSuite`: 2 executor JVMs, including an executor that dies mid-stage, which
    leads to fetch failures and morsel recomputation.
  - Core: `MorselJobSuite` (abort and give-back, reclaim, catch-up runs, spare CPUs, minimum
    work) and `TaskMemoryManagerSuite` (shared accounting, reclaimer).
  - All pass.
- **Spark suites with morsels forced on** (1 KB morsels, helpers start immediately): 788 passed,
  0 failed. The suites: InsertSuite, PartitionedWriteSuite, BucketedWriteWithoutHiveSupportSuite,
  FileFormatWriterSuite, DataSourceV2Suite, FileBasedDataSourceSuite, JoinSuite, OuterJoinSuite,
  InnerJoinSuite, ExistenceJoinSuite, DataFrameAggregateSuite, ParquetV2QuerySuite.
- **The same suites with morsels off** (stock paths unchanged): 310 passed.
- **After adding one-sided join morsels**, also with morsels forced on: JoinSuite, OuterJoinSuite,
  InnerJoinSuite, ExistenceJoinSuite, AdaptiveQueryExecSuite (including AQE's skew join tests),
  DataFrameJoinSuite, InsertSuite and DataFrameAggregateSuite. 707 passed, 0 failed.
- **Round 1:** JoinSuite, DataFrameAggregateSuite, DataFrameWindowFunctionsSuite,
  AdaptiveQueryExecSuite and SQLQueryTestSuite with morsels forced on. 1,087 passed and 4 failed,
  all ORDER BY tie-order differences.

## Round 1 results (shuffle map tasks only, earlier build)

| Run | Speedup |
|---|---|
| TPC-DS SF100, 64 vCPU | 1.117× total, 1.106× geomean |
| Density skew, 64 vCPU | 26.5× |
| Few large tasks, 64 vCPU | 9.2× |
| One row group, 64 vCPU | 16.4× |
| Balanced cases | 0.99–1.00× |

## Limitations and next steps

1. **Data source V2 writes** are not split. That needs a connector opt-in:
   - several `DataWriter`s per task, each with a writer index so outputs don't collide
   - a task commit message made of its writers' messages, which `BatchWrite.commit` receives
     flattened
   The `MorselSink` abstraction fits it: `writePart` creates a writer, and `commit` asks the
   output commit coordinator once and commits every writer. Spark's own V2 file writes fall back
   to V1, so this only pays off with connectors such as Iceberg.
2. **A single hot key in an aggregation or window stays one morsel** (one reducer partition);
   that would need a HyPer-style partitioned merge. Joins split one side, but no finer than one
   map output's block, and every participant reads the other side's whole partition (a shuffled
   hash join builds its own copy of the hash table).
3. **Executor-local only.** Work isn't moved between executors.
4. **Row order changes**, so ties under `ORDER BY` and order-dependent aggregates (`first`,
   `collect_list`) can differ from stock, as can results that are nondeterministic in stock.
5. **Multi-way joins** in one stage aren't split, and neither are writes whose stats trackers
   aren't Spark's basic one (e.g. Delta's) or that use custom partition locations.
6. **Data source V2's remaining ~4% CPU overhead** and the minimum-work threshold are heuristics
   that larger clusters and object storage should tune. Object storage (S3) wasn't measured; there,
   parallel reads of one file should help more.
7. **Smaller known issues:**
   - A helper that yields its slot finishes its current morsel first, so the executor is briefly
     oversubscribed.
   - Push-merged shuffle read metrics aren't added to the task.
   - Scan input-bytes metrics from helpers are approximate.

## Testing

```
build/sbt "core/testOnly *MorselJobSuite *TaskMemoryManagerSuite" \
  "sql/testOnly *MorselExecutionSuite *MorselMultiCpuTaskSuite *MorselLocalClusterSuite"
```

