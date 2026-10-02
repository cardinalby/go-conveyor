# Shutdown

Shutdown begins when the context passed to `Run` is canceled, or when an item returns an error. It has three steps:
1. **Shutdown begins**: no new items are started, and contexts from [`UntilShutdown`](#untilshutdown) are done.
2. **Grace period**: items already in the pipeline finish.
3. **Grace period ends** (only with [`OptGracePeriod` / `OptGracePeriodFunc`](#grace-period)): the contexts of the remaining items are canceled.

After an item error, all later items are canceled with a `ShutdownError` at once.

## What Run returns

`Run` returns what began the shutdown, the first event only:

| Result                      | When                                 | `errors.Unwrap` gives            |
|-----------------------------|--------------------------------------|----------------------------------|
| `ItemError`                 | an item failed first                 | the item's own error             |
| `ShutdownError`             | the `Run` context was canceled first | the context's cancellation cause |
| `ErrConveyorAlreadyRunning` | `Run` is already running             | -                                |
| `nil`                       | nothing happened                     | -                                |

```go
err := c.Run(ctx, proc)

var ie conveyor.ItemError
var se conveyor.ShutdownError
switch {
case err == nil:
case errors.As(err, &ie):
    // an item failed. errors.Is(err, yourErr) works. ie.Unit() is the node where it failed.
case errors.As(err, &se):
    // stopped from outside. errors.Is(err, context.Canceled) or your own cause works.
}
```

Both are `RunError`s and have two more getters:
- `ItemErrors()`: failures of other items during the shutdown. If the `Run` context is canceled and an
  in-flight item then fails to commit, `Run` still returns a `ShutdownError`, so check this list.
- `DrainError()`: nil if the in-flight items finished on their own. If the grace period ended first, it is the cause of
  the grace-period context.

Items aborted by the conveyor are not failures. An item is aborted when it returns a `ShutdownError` (for example,
from a node method), or an error that wraps `context.Canceled` after shutdown has begun (for example, a driver call
interrupted by an `UntilShutdown` context). Items see a `ShutdownError` as the cause of their context. It unwraps to
the reason: the `Run` context's cause, or the `ItemError` of the item that failed first. `DrainError()` and
`ItemErrors()` are always empty there.

An item that fails or is aborted cancels all younger items. So a younger item never enters a node that an older
dropped item did not enter. This matters for cumulative commits like Kafka offsets: do the commit in its own stage,
and a younger item never commits past an older dropped one. An item that sees shutdown but goes on and returns nil is
not aborted and cancels nothing.

## UntilShutdown

Often the first steps of an item have no side effects (read from a broker), and the later ones do (write, commit).
An item that has not started the side effects yet can be dropped at once instead of running the whole pipeline.
[`conveyor.UntilShutdown(ctx)`](https://pkg.go.dev/github.com/cardinalby/go-conveyor#UntilShutdown) returns a context
derived from the item's `ctx` that is also done when shutdown begins. Its cause is then a `ShutdownError`.

```go
c.Run(ctx, func(ctx context.Context) error {
    pre := conveyor.UntilShutdown(ctx) // dropped if shutdown begins before write
    msg, err := reader.Fetch(pre)
    if err != nil {
        return err
    }
    if err := write.MoveTo(pre); err != nil { // last call with pre
        return err
    }
    // from here on use ctx: the item finishes, bounded by the grace period
    if err := db.Write(ctx, msg); err != nil {
        return err
    }
    if err := commit.MoveTo(ctx); err != nil {
        return err
    }
    return broker.Commit(ctx, msg)
})
```

Use `pre` while dropping the item is still safe: nothing is done yet that must be finished or undone. Switch to
`ctx` at the first step that must finish. `MoveTo(pre)` only affects the wait to enter the node (queue,
backpressure). Never use `pre` for the side effect itself.

### Partial batch

Read a batch with `pre`. When shutdown begins, stop reading and go on with what was read, using `ctx`:

```go
c.Run(ctx, func(ctx context.Context) error {
    pre := conveyor.UntilShutdown(ctx)
    var batch []Msg
    for len(batch) < batchSize {
        msg, err := reader.Fetch(pre)
        if err != nil {
            if ctx.Err() != nil {
                return err // the item itself is canceled: abort
            }
            if pre.Err() != nil {
                break // shutdown: go on with what was read
            }
            return err
        }
        batch = append(batch, msg)
    }
    if len(batch) == 0 {
        return nil
    }
    if err := write.MoveTo(ctx); err != nil {
        return err
    }
    // ...
})
```

To tell an abort of the item from a shutdown, check `ctx.Err()` before `pre.Err()`: when `ctx` is done, `pre` is
done too. Don't judge by the error itself: it looks the same in both cases. Return nil if nothing was read.

`UntilShutdown` returns `ctx` unchanged if `ctx` is not an item's context.

## Grace period

By default, items finish on their own. To limit how long they may run after shutdown begins, use
[`OptGracePeriod`](https://pkg.go.dev/github.com/cardinalby/go-conveyor#OptGracePeriod):

```go
c := conveyor.NewConveyor(conveyor.OptGracePeriod(30 * time.Second))
```

After 30 seconds the contexts of the remaining items are canceled. `d <= 0` cancels them at once.

If the limit depends on the shutdown cause, or comes from an outside deadline, use
[`OptGracePeriodFunc`](https://pkg.go.dev/github.com/cardinalby/go-conveyor#OptGracePeriodFunc). The function is
called when shutdown begins. When the returned context is done, the item contexts are canceled. A nil context means
no limit. The two options override each other: the last one wins.

Cancellation works through the item's context: code that ignores `ctx` is not interrupted.

---

| Prev                             | Next                              |
|----------------------------------|-----------------------------------|
| [⬅ Benchmarks](8_benchmarks.md) | [Design FAQ ➡](10_design_faq.md) |