# Shutdown

Shutdown begins when the context passed to `Run` is canceled, or when an item returns an error. It has three steps:
1. **Shutdown begins**: no new items are started, and contexts from [`UntilShutdown`](#untilshutdown) are canceled.
2. **Drain**: items already in the pipeline finish.
3. **Drain times out** (only with [`OptDrainTimeout` / `OptDrainContextFunc`](#drain-timeout)): the contexts of the remaining items are canceled.

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

Both are `RunError`s. `DrainError()` tells how the items in flight when shutdown began finished:
- nil: they all finished without failing (aborts are not failures).
- an `ItemError`: one of them failed first. For example, the `Run` context is canceled and an in-flight item then
  fails to commit: `Run` returns a `ShutdownError`, and `DrainError()` has the commit failure.
- the cause of the drain context (for example, `context.DeadlineExceeded`): the drain timed out first, and
  the remaining items were canceled.

`DrainError()` is only the first problem and never repeats what began the shutdown. To see every failure, log it where
it happens: in the ItemProcessor, a task, a lane callback, or a `RetainFor` callback.

Items aborted by the conveyor are not failures and are not reported. An item is aborted when:
- its error is or wraps a `ShutdownError`: from a node method, or `context.Cause(pre)` of an
  [`UntilShutdown`](#untilshutdown) context;
- the conveyor canceled it (younger than a dropped item, or the drain timed out). Then any error it returns is an
  abort.

Any other error is a failure, also one that wraps `context.Canceled`. A driver call interrupted by an `UntilShutdown`
context returns such an error: return `context.Cause(pre)` instead to abort (see the example below).

Items see a `ShutdownError` as the cause of their context. It unwraps to the reason: the `Run` context's cause, or
the `ItemError` of the item that failed first. `DrainError()` is always nil there.

Any error the ItemProcessor returns, an abort too, cancels the item's context, and with it the item's tasks and
`RetainFor` work that still run. Only a nil return lets them finish.

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
        if pre.Err() != nil {
            return context.Cause(pre) // a ShutdownError: an abort, not a failure
        }
        return err
    }
    if err := write.MoveTo(pre); err != nil { // last call with pre
        return err // a ShutdownError once shutdown has begun
    }
    // from here on use ctx: the item finishes, bounded by the drain timeout
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

After a failed call with `pre`, check `pre.Err()` and return `context.Cause(pre)`. The call's own error wraps
`context.Canceled`: returned as is, it is a failure, not an abort.

Use `pre` only before the stage where side effects start, not inside it. An item dropped there is aborted and cancels
all younger items, also those in the same stage (with `SetLimit(n > 1)`) that already started their side effects.

`pre.Done()` may close a moment after shutdown begins. Node methods called with `pre` fail from the moment shutdown
begins.

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
                return err // the conveyor canceled the item: any error is an abort
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

Call `UntilShutdown` once per item path and derive other contexts from its result, e.g. `context.WithValue(pre, k, v)`.
Each call with a context other than the item's own makes a new context that lives until that context or the item is
done. So `UntilShutdown(context.WithValue(ctx, k, v))` in a loop of a long-running item grows memory.

### Tasks and lane children

Any error a pool task or a lane child returns fails its item at once: the item's context and its sibling tasks are
canceled. This includes the error of a call interrupted by an `UntilShutdown` context. Return the error only if
dropping the whole item is fine. If sibling work must finish, return nil on shutdown and tell the item through the
task's result:

```go
c.Run(ctx, func(ctx context.Context) error {
    var stopped error // set by the task, read after Wait
    err := fo.Schedule(ctx,
        writes.NewTask(func(ctx context.Context) error {
            return db.Write(ctx, msg) // must finish
        }),
        fetches.NewTask(func(ctx context.Context) error {
            pre := conveyor.UntilShutdown(ctx)
            for _, page := range pages {
                if err := fetchPage(pre, page); err != nil {
                    if ctx.Err() == nil && pre.Err() != nil {
                        stopped = context.Cause(pre) // shutdown: stop, don't cancel the write
                        return nil
                    }
                    return err
                }
            }
            return nil
        }),
    )
    if err != nil {
        return err
    }
    if err := fo.MoveTo(ctx); err != nil {
        return err
    }
    if err := fo.Wait(ctx); err != nil {
        return err
    }
    if stopped != nil {
        return stopped // a ShutdownError: skip the commit, the item is aborted
    }
    return commit.MoveTo(ctx)
})
```

`ctx.Err()` is checked first, as in the partial batch: it tells an abort of the item from a shutdown. A task that
returns nil counts as a success, so `Wait` returns nil. If the item decides to commit when all its tasks are done,
the task must report that it stopped early.

## Drain timeout

By default, items finish on their own. To limit how long they may run after shutdown begins, use
[`OptDrainTimeout`](https://pkg.go.dev/github.com/cardinalby/go-conveyor#OptDrainTimeout):

```go
c := conveyor.NewConveyor(conveyor.OptDrainTimeout(30 * time.Second))
```

30 seconds after shutdown begins, the contexts of the remaining items are canceled. `d <= 0` cancels them at once.

If the limit depends on the shutdown cause, or comes from an outside deadline, use
[`OptDrainContextFunc`](https://pkg.go.dev/github.com/cardinalby/go-conveyor#OptDrainContextFunc). The function is
called when shutdown begins. When the returned context is done, the item contexts are canceled. A nil context means
no limit. The two options override each other: the last one wins.

Cancellation works through the item's context: code that ignores `ctx` is not interrupted.

---

| Prev                             | Next                              |
|----------------------------------|-----------------------------------|
| [⬅ Benchmarks](8_benchmarks.md) | [Design FAQ ➡](10_design_faq.md) |