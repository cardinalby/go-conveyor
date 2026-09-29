# Shutdown

Shutdown begins when the context passed to `Run` is canceled, or when an item returns an error. By default:
- no new items are started
- items already in the pipeline are allowed to finish
- after an item error, all later items are canceled with a `ShutdownError`

## No-abort point

Often the first stages have no side effects (read from a broker), and the later ones do (write, commit).
An item that has not started the side effects yet can be aborted at once instead of running the whole pipeline:

```go
c := conveyor.NewConveyor()
// "read" is the starting stage: it long-polls the broker
write := c.AddStage()
commit := c.AddStage()
c.SetNoAbortPoint(write)
```

When shutdown begins, every item that has not entered `write` (or a later node) yet is aborted: canceled with a
`ShutdownError`. The item reading from the broker returns at once. An item waiting in `write`'s queue or blocked
in `write.MoveTo()` has not entered it, so it is aborted too. An item that skipped `write` and is in `commit`
(or in `commit`'s queue) has passed the point and is not aborted.

The point can be a stage or a fan-out of the main pipeline (not a node inside a lane). The default is the starting
stage: every item enters it first, so no item is aborted. `c.SetNoAbortPoint(c.StartingStage())` restores it.

`SetNoAbortPoint` can be called at any time, also on a running conveyor. An item that has entered the point is
not aborted, even if the point is moved to a later node. It can still be canceled for other reasons: an earlier
item failed (all later items are canceled), or the `OptShutdownContext` context is done. Moving the point to an earlier node protects the items
that have already entered it.

## Grace period

Items past the point (all items, by default) finish on their own. To limit how long they may run, use
[OptShutdownContext](https://pkg.go.dev/github.com/cardinalby/go-conveyor#OptShutdownContext): once its context
is done, the contexts of all remaining items are canceled.

Cancellation works through the item's context: code that ignores `ctx` is not interrupted.

---

| Prev                             | Next                              |
|----------------------------------|-----------------------------------|
| [⬅ Benchmarks](8_benchmarks.md) | [Design FAQ ➡](10_design_faq.md) |