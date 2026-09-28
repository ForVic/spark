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

Batch pipelining allows eligible downstream tasks to consume shuffle output before their
producers finish. It works with Spark core RDD execution and SQL, with or without adaptive
query execution (AQE). Selection depends on the execution graph and available resources.
The feature is disabled by default and does not change Structured Streaming planning.

## Enable

Set these properties when creating the SparkContext:

```properties
spark.shuffle.memory.enabled=true
spark.shuffle.manager.incremental=batch-memory
spark.executor.taskScheduler.residencyMultiplier=8
spark.sql.shuffle.pipelined.enabled=true
spark.speculation=false
spark.dynamicAllocation.enabled=false
```

The batch memory transport requires the built-in sort shuffle manager, static executor resources,
and memory shuffle storage. External shuffle services, push shuffle, decommissioning,
synchronous shuffle writes, the old fetch protocol and custom shuffle I/O must be disabled.
The residency multiplier limits resident pipelined tasks relative to physical task capacity. It does not
increase the number of CPUs or bound total operator memory. Ordinary tasks remain limited
by physical CPU capacity. A multiplier of one keeps the existing scheduling policy.

Core RDD callers can opt a `ShuffledRDD` into pipelining with `setPipelinedShuffle(true)`.
Its serializer, aggregation and ordering contracts still apply. Ordinary RDD transformations
keep their default shuffle behavior.

## Storage and transport

The memory resolver owns completed shuffle output independently of producer tasks. Eligible
bypass-sort shuffles can retain their serialized partition streams in memory while keeping
ordinary map statuses, fetch semantics and completed-stage AQE decisions. Consumers can start
later, after the producer has released its execution resources.

The batch memory manager also publishes checksummed pages incrementally through one transport
endpoint per executor. Connections are reused. Local readers retain pages directly; remote
readers reserve receive capacity and retain transport buffers until consumption finishes.
Compression and encryption remain part of the normal serialization path.

Pages are charged to Spark storage memory before allocation. The executor limit cannot exceed
the protected on-heap storage region. `spark.shuffle.memory.maxSize` and
`spark.shuffle.memory.maxMapSize` optionally lower the executor and individual map limits.
Half the live budget is reserved for receive capacity so producers cannot occupy all space
needed by their readers. Completed output can move to the ordinary shuffle file format;
active writers spill when readers cannot release capacity. A record larger than the budget
can therefore make progress without retaining the complete record in shuffle pages.

These limits cover shuffle page capacity. Serializer state, metadata, task stacks and operator
state use additional memory. Memory retained by a reader remains charged until its last
reference is released, including during output cleanup or a disk copy.

Ordinary shuffles that need unsupported storage features retain the existing disk path. This
includes map-side combining, serialized sort above the bypass threshold, external shuffle
services, push shuffle, decommissioning, synchronous writes and custom shuffle I/O.

## Scheduling and adaptive execution

`PipelinedExecutionPlan` groups stages connected by live dependencies. DAGScheduler reserves
reader capacity and admits source tasks in bounded waves. Materialized dependencies connect
these groups and retain their readiness requirements. Groups wait for occupied capacity;
a group whose minimum reservation exceeds the cluster's capacity is rejected.

With executor-local scheduling enabled, a task waiting for input or buffer space hands its
CPU permit to ready work on the same executor. The driver retains the allocated capacity,
so this handoff cannot allocate the same CPU twice. Source tasks acquire a separate admission
permit before initializing operator state. Blocked tasks retain their JVM thread and stack;
this implementation does not provide resumable operator continuations.

SQL keeps boundaries needed for adaptive join decisions, exchange reuse or unsupported
consumers. Independent live inputs can prepare their RDDs concurrently without submitting
producer jobs; DAGScheduler owns execution admission. Completed memory output supplies the
same completed-stage statistics as ordinary shuffle output. Partial live output is never
reported as exact completed-stage statistics.

Optional estimated reader coalescing is enabled with:

```properties
spark.sql.shuffle.pipelined.coalescePartitions.enabled=true
```

It also requires `spark.sql.adaptive.coalescePartitions.enabled`. Compatible hash exchanges
share a layout fixed before producers start. Logical hash buckets retain their identity and
route directly to physical readers. Unknown estimates, explicit partition counts and
incompatible inputs keep their layouts. Estimates choose concurrency only; they cannot drop
rows, omit apparently empty buckets or justify skew splitting. The standard advisory size,
parallelism and reducer fan-in settings apply. The optional
`spark.sql.shuffle.pipelined.coalescePartitions.parallelismFirst` setting overrides the
parallelism preference for live layouts alone.

## Failure and compatibility limits

Live consumers can observe uncommitted producer output. A failure aborts the affected execution;
stages that participated in live consumption are not independently retried. A new action
starts a fresh execution epoch. Cancellation and failure wake pending readers, release pages
and reject late publication into a closed epoch.

Speculation, dynamic allocation, non-default resource profiles, live fan-out and reliable
checkpoints inside a live group are unsupported. Barrier execution is unsupported with the
executor-local scheduler. SQL cache construction, cached inputs and RDD exports retain regular
execution. Operators that cannot safely drain a live input retain materialized boundaries.

The feature remains experimental. Selecting a live exchange does not guarantee overlap or a
speedup: scheduling, partition sizes, operator state, data distribution and AQE decisions all
matter. Concurrent task CPU, shuffle and wait timers are not additive query latency.
