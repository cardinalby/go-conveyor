# Observability

Observability is **pull-based**: the conveyor never pushes metrics and takes no callbacks. You ask it for a snapshot
with [Stats](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Conveyor.Stats), from any goroutine, at any time.
Outside a run — before the first one, or after one returns — it reports the zero value.

Name your nodes with `OptName` if you're going to chart them: an unnamed node gets a positional name
(`"stage 2"`, `"fan-out 3.1"`) which moves when you insert something in front of it.

```go
c := conveyor.NewConveyor()
write := c.AddStage(conveyor.OptName("write")).SetQueueSize(2)
dbs := c.AddFanOut(conveyor.OptName("dbs")).SetLimit(2)
db1 := dbs.AddPool(conveyor.OptName("db1")).SetLimit(2)
db2 := dbs.AddPool(conveyor.OptName("db2")).SetLimit(3)
commit := c.AddStage(conveyor.OptName("commit"))

// ONE reader, on a timer (see the warning below)
go func() {
    for range time.Tick(10 * time.Second) {
        s := c.Stats()
        metrics.Gauge("conveyor.items_in_flight", s.InFlight.Max)
        metrics.Gauge("conveyor.workers", s.LiveWorkers.Max)
        for _, u := range s.Units {
            node := u.Unit.String() // "write", "dbs", "db1", ... — or "start" for the implicit read stage
            metrics.Gauge("conveyor.node.occupied", u.Occupied.Max, "node", node)
            metrics.Gauge("conveyor.node.limit", u.Limit, "node", node)
            metrics.Gauge("conveyor.node.queued", u.Queued.Max, "node", node)
        }
    }
}()
```

## The gauges are windows, not point samples

`InFlight`, `LiveWorkers`, `Occupied` and `Queued` are `Gauge{Min, Max, Last}`: the range the quantity reached
**since the previous `Stats` call**, plus its value at this one. A spike between two scrapes shows up in `Max` instead
of being missed, which is what you want from a 10-second timer watching a pipeline that moves in milliseconds.

> ⚠️ Reading **resets** the windows, so `Stats` assumes a **single consumer**. Two readers split the window between
> them and both see nonsense. Wire exactly one.

## What each number means

| Field         | Meaning                                                                                                                                                                                                                                                                        |
|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `InFlight`    | live items — one per running `ItemProcessor` call, including one sitting at the implicit read stage. Child items of a lane's work are **not** counted; their work shows up as branch occupancy instead, so this stays bounded by your own nodes however wide an item fans out. |
| `LiveWorkers` | goroutines in the pool driving those items (branch workers are not counted). Compare with `InFlight` to see the pool being reused.                                                                                                                                             |
| `Units`       | one entry per node **and per branch** (pool or lane), in creation order; index 0 is the implicit read stage. Match an entry back to what you built by `==` on `u.Unit` or by its name.                                                                                         |

And per node, in `UnitStat`:

| Field      | Meaning                                                                                                                                                                                |
|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Occupied` | items running a stage's code / items with work outstanding in a fan-out / pieces of a branch's work in flight (for a lane: children at its entrance).                                  |
| `Limit`    | that node's capacity — the denominator. It travels with `Occupied` because 3 is saturated at limit 3 and idle at limit 300, and the two are read under one lock, so they always agree. |
| `Queued`   | what is piled up **in front of** the node: items in a stage's or fan-out's waiting room, or — for a branch — items whose work it has accepted but not yet started.                     |

`Queued` needs no capacity to interpret: any queueing at all means that node is not keeping up with its input, which
is why the configured waiting-room size is not reported here. Read it back from the handle (`Stage.QueueSize` /
`FanOut.QueueSize`) if a dashboard wants the denominator; a branch's backlog has no size of its own, being bounded by
its fan-out's limit.

So the two signals worth alerting on are `Occupied.Max == Limit` (saturated) and a `Queued` that never returns to
zero (falling behind) — and the node they point at is the one to give more capacity, with `SetLimit` or
`SetQueueSize`, both of which take effect on a running conveyor.

---

| Prev                                              | Next                             |
|---------------------------------------------------|----------------------------------|
| [⬅ Conditional MoveTo](6_conditional-move-to.md) | [Benchmarks ➡](8_benchmarks.md) |
