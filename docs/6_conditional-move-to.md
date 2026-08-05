# Conditional MoveTo

Sometimes a stage is **optional**: you want it if it's free, but you'd rather skip it than queue behind it.
[TryMoveTo](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Stage.TryMoveTo) is the conveyor's equivalent of a `select`
with a `default` case — it never waits, and tells you whether the item got in.

```go
c := conveyor.NewConveyor()
enrich := c.AddStage().SetLimit(4) // optional: asks a slow metadata service
write := c.AddStage()
commit := c.AddStage()

c.Run(ctx, func(ctx context.Context) error {
    batch := read()

    // Enter "enrich" only if it has a free slot right now. Never blocks.
    entered, err := enrich.TryMoveTo(ctx)
    if err != nil {
        return err
    }
    if entered {
        // (2) fetch metadata and attach it to the batch
        batch.meta = fetchMeta(ctx, batch)
    }
    // If we didn't enter, we are still in "read" and the batch just goes out un-enriched.

    if err := write.MoveTo(ctx); err != nil {
        return err
    }
    // (3) write to DB

    if err := commit.MoveTo(ctx); err != nil {
        return err
    }
    // (4) commit offsets / ack messages
    return nil
})
```

`entered == false` means **nothing happened**: the item stays where it was, still holding the previous stage, and
**enrich** is left unentered — so you can try it again later, or enter it with a blocking `MoveTo` after all. That is
the whole promise of the call, and it's what makes "skip it under load" and "take a different path" safe to express.

Two things are worth knowing:

- A **waiting room** in front of the stage (`SetQueueSize`) is deliberately **not** used.
- `TryMoveTo` also won't jump an item already waiting in front of the stage. Item order still holds.

A fan-out has the same variant, and there it does one thing more — on `entered == false` the tasks are left
**unclaimed**, so the very same `Tasks` value can be submitted later, here or somewhere else:

```go
tasks := conveyor.Tasks{db1Write.NewTask(writeDb1)}

entered, err := dbsWrite.TryMoveTo(ctx, tasks)
if err != nil {
    return err
}
if !entered {
    // Nothing was scheduled and nothing was consumed: the same value is still submittable.
    if err := dbsWrite.MoveTo(ctx, tasks); err != nil {
        return err
    }
}
```

Waves passed to either variant are joined only if the item entered, so a `TryMoveTo` that declines never waits on
anything.

---

| Prev                   | Next                                   |
|-------------------------|----------------------------------------|
| [⬅ Lanes](5_lanes.md) | [Observability ➡](7_observability.md) |
