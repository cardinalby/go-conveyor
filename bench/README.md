# conveyor/bench

A standalone benchmark comparing the `conveyor` package against a hand-rolled
channels/goroutines pipeline that is **functionally equivalent** (same topology,
same ordering guarantee, same per-node concurrency limits). It is a separate Go
module so its charting dependency (`gonum.org/v1/plot`) never reaches the service.

## What it measures

For three topologies, and a sweep of a base delay `D`, it measures the
steady-state interval between item completions (`T/N` over the middle of the run,
fill and drain excluded) for both implementations. The CSV records that interval
in milliseconds; the chart plots its inverse — **throughput in items/sec**
(higher = better) — for both implementations on one chart, on log-log axes so the
small-D region (where overhead is visible next to the work) is not compressed.

A **D=0 point** is always prepended to the sweep: with all work zeroed, the
measured interval is the pure coordination cost of each machinery. It is drawn at
half the smallest nonzero D on the log X axis, under a tick labeled "0"
(saturation is not expected or checked there).

Sub-millisecond D values (the default `-dmin` is 10µs) are subject to OS timer
resolution: a requested 10–100µs sleep overshoots by a scheduler-dependent
amount, so absolute throughput there is indicative. The comparison stays fair —
both implementations run the identical sleep — which is what the charts read.

Because the "work" in every stage is an identical `time.Sleep(D·multiplier)`,
the sleeps cancel out: the gap between the two curves is the conveyor's extra
**coordination overhead** (its single mutex + broadcast-on-every-change) relative
to dedicated per-stage/per-slot goroutines. Where the curves overlap, that
overhead is negligible next to the work; it is most likely to appear at the small
end of `D`, where a stage's coordination cost is comparable to its work.

Scenarios (all five nodes long):

1. **linear** — five exclusive stages.
2. **shared** — a shared stage (`-limit` slots) in the middle, fed by fast
   exclusive stages. Traditional equivalent: a pool of `limit` goroutines + a
   reorder buffer.
3. **fanout** — a fan-out stage in the middle: two branches, `limit` slots each,
   run in parallel per item and joined before it proceeds. Traditional
   equivalent: two goroutine pools + a per-item join + a reorder buffer.

### Saturation

To keep all `limit` slots of a concurrent node busy, that node must be `limit×`
slower than the exclusive stage feeding it — an exclusive feeder emitting one item
every `f` seconds keeps only `service/f` items in a downstream node (Little's
law). So the concurrent node runs at `limit·D` while its feeders run at `D`,
making arrival rate (`1/D`) and service rate (`limit/(limit·D) = 1/D`) match and
the node fill to its limit. The tool checks the achieved peak occupancy and warns
if a node stayed under-loaded.

This is also why `-dmax` defaults to 100ms: at `limit·D` a shared node already
takes `limit×` as long, and a larger `D` makes fill/drain and total runtime grow
without adding information.

## The abstraction

The harness is implementation-agnostic; only the machinery under test differs.

- **`pipeline.Spec`** — a declarative topology (`[]StageSpec`). One `Spec` is
  interpreted by every implementation.
- **`pipeline.Pipeline`** — `NewConveyor(spec, obs)` and `NewTraditional(spec,
  obs)` both build one; the harness calls only `Run(ctx, n)`.
- **`pipeline.Observer`** — collects per-item completion times and per-stage
  occupancy. Both implementations report through the *same* calls (via the shared
  `stageRuntime`, which is the only thing that actually sleeps), so every metric
  is derived from identical data. It is lock-free (each item index written once;
  atomic gauges), so its cost is the same on both sides.

`internal/bench` sweeps `D` and builds the CSV; `internal/chart` renders the PNG.
`pipeline`'s test asserts both implementations finish all items, in order, and
saturate — the guarantee that makes them interchangeable.

## Usage

```sh
cd pkg/basics/conveyor/bench
go run .                 # full sweep with defaults (prints a runtime estimate first)
go run . -h              # list flags
go run . -steps=10 -n=200 -repeats=1   # a quicker, coarser sweep
go run . -impl=chans     # measure only the channels variant (raw CSVs, no charts)
```

The command runs in two modes:

- **Orchestrating parent** (no `-impl`): re-executes itself once per variant —
  `chans` first, then `conv` — waits for each, then reads their CSV files and
  renders one `<scenario>.png` per scenario. Each variant measures in a **fresh
  process**, so no shared Go runtime state (heap/GC pacing, OS thread pool,
  timer pressure) couples one variant's numbers to the other's.
- **Measuring child** (`-impl=chans|conv`): sweeps every scenario with that one
  implementation and writes raw data to `<out>/<scenario>_<impl>.csv` (e.g.
  `fanout_conv.csv`), one row per D point: `D_ms,interval_ms,min_sat_frac`
  (empty interval = skipped point). Running a child by hand is useful to
  re-measure one side after a change and re-render charts via the parent.

Key flags: `-n` (items per run), `-drop` (steady-state window trim), `-repeats`
(best-of), `-dmin`/`-dmax`/`-steps` (the `D` axis — values are **log-spaced**, so
samples concentrate at small `D` where overhead is visible, matching the chart's
log axis), `-limit` (concurrent-node capacity), `-out` (output dir).

Output lands in `results/`: `<scenario>_<impl>.csv` raw data plus a
`<scenario>.png` chart per scenario.
