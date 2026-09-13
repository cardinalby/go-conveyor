# Documentation

Guides on go-conveyor's pipeline features, split out from the [README](../README.md).

| Operation     | Meaning                                                                     |
|---------------|-----------------------------------------------------------------------------|
| `MoveTo`      | Advance the item into a node; leaving a fan-out joins its work.             |
| `Schedule`    | Register parallel work for a fan-out, before or after entering it.          |
| `FanOut.Wait` | Join the work scheduled so far without leaving the fan-out.                 |
| `Retain`      | Let unfinished work keep the current node occupied while the item moves on. |
| `Wave.Wait`   | Wait for retained work later in the item's path.                            |

- [1. Retain previous stage longer](1_retain-previous-stage.md) — keep holding a stage after moving on, and wait for the work later with `Wave.Wait`
- [2. Queues](2_queues.md) — give a stage a waiting room instead of blocking on entry
- [3. Shared Stages](3_shared-stages.md) — let more than one item into a stage at a time
- [4. Fan-out (scatter/gather)](4_fan-out.md) — parallel branches (pools and lanes): `Schedule` before or after entry, rounds and follow-ups with `Wait`, `SetBackpressure`, ordering guarantees, `Retain()` and `Wave.Wait`
- [5. Lanes: when a branch is a pipeline](5_lanes.md) — a branch that is itself a multi-step sub-pipeline, with its own ordering guarantees
- [6. Conditional MoveTo](6_conditional-move-to.md) — enter a stage only if it's immediately free, with `TryMoveTo`
- [7. Observability](7_observability.md) — pull-based `Stats()`, what the gauges mean, and what to alert on
- [8. Benchmarks](8_benchmarks.md) — throughput vs. a classical channel pipeline across topologies
- [9. Design FAQ](9_design_faq.md) — why the API looks the way it does
