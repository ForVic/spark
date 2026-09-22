# Review of the retained-pod waiting-time observation

## Conclusion

The increased maximum batch-entry wait is real, but it is not evidence by itself
that executor-removal handling became slower. The metric excludes time spent
inside a batch and is sensitive to where the subscriber drains its queue.
Per-batch tracing locates the spike in the lifecycle subscriber, while it waits
for an earlier batch's synchronous, artificially delayed patch calls to finish.
There is no corresponding allocator wait spike in these traced runs.

Before: all 498 terminal updates enter one callback about 102 ms after the oldest
event; the callback then takes 1,471-1,522 ms. The original metric reports the
roughly 102 ms wait for that batch and ignores its long processing duration.

After: the same terminal updates split into 295+203 or 359+139. The first batch
starts earlier (63-72 ms), then spends 866-1,039 ms processing. The second batch
waits 866-1,039 ms for it. That is the source of the reported maximum wait. The
second batch's 386-548 ms processing duration then completes the same work.

The production store drains the queue before calling each subscriber and holds
that subscriber's lock during the callback. New events cannot join the already
drained batch. This behavior is unchanged by the patch. Changing processing costs
and producer costs can change the drain boundary. No synthetic delay occurs in
the new index code; it occurs in the existing synchronous cleanup path's API stub.

An earlier diagnostic after-run grouped all 498 updates into one batch and did
not show the wait spike, further supporting dependence on the drain boundary.
Those early observations were recorded in the conversation; the saved final trace
logs contain the two instrumented runs described above.

## Check the actual outcome latency

We additionally timestamped each delivered terminal event before publishing and
measured time until the backend's doRemoveExecutor call. All 498 expected terminal
removals were captured per run. Missed-event/relist cases are excluded from this
latency table because their publication reference differs; their correctness
assertions still pass. Values below are milliseconds, with p95 the lower observed
95th percentile (no interpolation).

| Version/run | Median | p95 | Maximum |
| --- | ---: | ---: | ---: |
| before 1 | 846.7 | 1429.4 | 1515.6 |
| before 2 | 801.5 | 1387.5 | 1463.5 |
| after 1 | 736.2 | 1308.8 | 1373.5 |
| after 2 | 756.0 | 1332.1 | 1398.8 |

The actual removal-latency distributions improve in these two before/after pairs,
despite worse batch-entry wait. This is evidence against interpreting that metric
as slower executor-removal handling in this workload. It does not establish all
production latency distributions, API completion latency, or scheduler throughput.

## PR implication

Do not present the maximum batch-entry wait as an unexplained production regression.
Document that the delayed-API replay changes batch boundaries and report terminal
handling latency alongside any queue-wait statistic. No additional production fix
is justified by this observation. Keep the main performance claim scoped to reduced
snapshot/lifecycle processing costs, with producer and memory overhead disclosed.

Four replay regression tests and test style checks pass after adding optional
tracing. No production source was changed for this investigation. Trace logs,
`trace.py`, and `removal-latency.json` retain the evidence. The tracer records data
in memory and prints after the replay; it does not print from timed callbacks.
