# Documentation

Guides on go-conveyor's pipeline features, split out from the [README](../README.md).

- [1. Retain previous stage longer](1_retain-previous-stage.md) — keep holding a stage after moving on, and join the work later
- [2. Queues](2_queues.md) — give a stage a waiting room instead of blocking on entry
- [3. Shared Stages](3_shared-stages.md) — let more than one item into a stage at a time
- [4. Fan-out (scatter/gather)](4_fan-out.md) — parallel branches (pools and lanes) with ordering guarantees and `Detach()`
- [5. Lanes: when a branch is a pipeline](5_lanes.md) — a branch that is itself a multi-step sub-pipeline, with its own ordering guarantees
- [6. Conditional MoveTo](6_conditional-move-to.md) — enter a stage only if it's immediately free, with `TryMoveTo`
- [7. Observability](7_observability.md) — pull-based `Stats()`, what the gauges mean, and what to alert on
- [8. Benchmarks](8_benchmarks.md) — throughput vs. a classical channel pipeline across topologies
