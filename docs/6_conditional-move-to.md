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

## Into a fan-out

A fan-out has the same variant. Because the tasks are built and scheduled *after* entering, a declined entry leaves
nothing to clean up: no tasks were built for nothing, and nothing was claimed.

```go
entered, err := enrich.TryMoveTo(ctx)
if err != nil {
    return err
}
if entered {
    if err := enrich.Schedule(ctx, lookup.NewTasks(len(batch), lookupMeta)); err != nil {
        return err
    }
}
// not entered: the item is still in the previous node; enrich may be entered later with MoveTo
```

Neither variant takes waves, so a `TryMoveTo` that declines never waits on anything. To wait for a `Retain` or
`Detach` wave after a conditional move, call `Wave.Wait` once `entered` is true.

## Out of a fan-out

`TryMoveTo` called while the item is **inside a fan-out** has one more reason to decline: the item's body there is
still **busy** (tasks queued or running). Leaving would mean waiting for them, which is exactly what the call promises
not to do, so it returns `(false, nil)` and touches nothing — the body stays open and the item may keep scheduling.
With an idle body it behaves like the stage variant: it leaves if the target has room, and otherwise declines, again
leaving the body open. A body whose task has failed has already canceled the item, so the call returns
`(false, cause)`; the node-qualified error is what `Wait` or the blocking `MoveTo` report.

---

| Prev                   | Next                                   |
|------------------------|----------------------------------------|
| [⬅ Lanes](5_lanes.md) | [Observability ➡](7_observability.md) |
