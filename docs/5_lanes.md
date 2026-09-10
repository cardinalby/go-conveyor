# Lanes: when a branch is a pipeline

A fan-out fans out to **branches**, and there are two kinds of them:

| branch   | built with         | what it is                                      | capacity                    |
|----------|--------------------|-------------------------------------------------|-----------------------------|
| **pool** | `FanOut.AddPool()` | one step: a task runs there and is done         | `SetLimit(n)` — n at a time |
| **lane** | `FanOut.AddLane()` | a sub-pipeline: the task travels its own stages | its stages' own limits      |

Most fan-outs need only pools. Lanes are for the case where one branch's work is itself a multi-step
path. Start with pools; you'll know when you need a lane.

Both kinds satisfy [Branch](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Branch).
[`FanOut.Branches()`](https://pkg.go.dev/github.com/cardinalby/go-conveyor#FanOut.Branches) returns them all, 
in creation order.

- **pool** is one step and can accommodate multiple tasks from different items at once (up to its limit)
- **lane** is a sub-pipeline (with entrance limit = 1) that can have its own internal stages with their own limits.
It's a push-based sub-conveyor.

```go
c := conveyor.NewConveyor()
split := c.AddFanOut().SetLimit(2)

// A lane, not a pool: each message travels two steps with different capacities
messages := split.AddLane()
enrich := messages.AddStage().SetLimit(6) // 6 concurrent HTTP calls
write := messages.AddStage()              // but only 1 DB write at a time, in message order

report := split.AddPool().SetLimit(3) // 3 concurrent reporting tasks

commit := c.AddStage()

c.Run(ctx, func(ctx context.Context) error {
    batch := read() // (1) read a batch of messages

    // (2) enter the fan-out, then run the fetch+write and report tasks on each message
    if err := split.MoveTo(ctx); err != nil {
        return err
    }
    err := split.Schedule(ctx,
        messages.NewTasks(len(batch), func(cctx context.Context, i int) error {
            // `cctx` is the CHILD's context, not the item's — use it for the lane's own stages
            if err := enrich.MoveTo(cctx); err != nil {
                return err
            }
            meta := fetchMeta(cctx, batch[i]) // 6 messages can be in here at once

            if err := write.MoveTo(cctx); err != nil {
                return err
            }
            return writeDB(cctx, batch[i], meta) // one at a time, in batch order
        }),

        report.NewTasks(len(batch), func(cctx context.Context, i int) error {
            return reportDB(cctx, batch[i]) // 3 at a time
        }),
    )
    if err != nil {
        return err
    }

    // (3) waits for every child and task of this item to finish
    if err := commit.MoveTo(ctx); err != nil {
        return err
    }
    return nil // (4) commit offsets / ack messages
})
```

## [★ Interactive Demo](https://cardinalby.github.io/go-conveyor/#%7B%22v%22%3A2%2C%22startDelayMs%22%3A850%2C%22nodes%22%3A%5B%7B%22kind%22%3A%22fanout%22%2C%22name%22%3A%22split%22%2C%22limit%22%3A2%2C%22queueSize%22%3A0%2C%22branches%22%3A%5B%7B%22kind%22%3A%22lane%22%2C%22name%22%3A%22messages%22%2C%22tasksPerItem%22%3A3%2C%22delayMs%22%3A50%2C%22nodes%22%3A%5B%7B%22kind%22%3A%22stage%22%2C%22name%22%3A%22enrich%22%2C%22limit%22%3A6%2C%22queueSize%22%3A0%2C%22delayMs%22%3A3150%7D%2C%7B%22kind%22%3A%22stage%22%2C%22name%22%3A%22write%22%2C%22limit%22%3A1%2C%22queueSize%22%3A0%2C%22delayMs%22%3A1400%7D%5D%7D%2C%7B%22kind%22%3A%22pool%22%2C%22name%22%3A%22report%22%2C%22limit%22%3A3%2C%22delayMs%22%3A2100%2C%22tasksPerItem%22%3A2%7D%5D%7D%2C%7B%22kind%22%3A%22stage%22%2C%22name%22%3A%22commit%22%2C%22limit%22%3A1%2C%22queueSize%22%3A0%2C%22delayMs%22%3A1150%7D%5D%2C%22mode%22%3A%22run%22%2C%22showCode%22%3Afalse%2C%22showLegend%22%3Afalse%7D)

## The child is an item

Everything you know about an item applies to a child, one level down:

- It gets **its own context** (`cctx` above). Use it for the lane's stages, and pass it to whatever you call so
  cancellation reaches it.
- It may move **only through its own lane's nodes**. Reaching a conveyor node from a child — or a lane's node from
  the conveyor — is a **panic** (`errWrongScope`).
- Its error is fail-fast: it cancels the whole item (siblings included).
- A lane may have its own interior fan-outs; a child enters them with `MoveTo` and adds work with `Schedule` and
  `Wait`, exactly like the ItemProcessor does on the conveyor.

## A child may add work to its parent's fan-out

A child may call `Schedule` on the fan-out **its lane belongs to** (`split` above), before its callback returns. The
work joins the parent item's body, like a follow-up scheduled from a pool task (see
[Trees](4_fan-out.md#trees-a-task-schedules-follow-ups)), and the parent's next `MoveTo` waits for it too. A child
may not call `Wait` there — that panics, for the same reason a pool task may not wait: it holds a slot.

## The lane's entrance is limit 1, like the conveyor's start

A `Lane` has **no `SetLimit`** — its entrance always admits one child at a time, exactly like the implicit start
stage that paces item creation on the conveyor. The next
child isn't created until the previous one has moved off the entrance.

The parallelism comes from the **interior stages**, not the entrance — which is why `enrich` above carries the
`SetLimit(6)`.

## Order: every child takes a ticket

When a piece of a lane's work is **pulled** off the lane's queue, a child is created and takes a numbered ticket.
Interior stages admit strictly by ticket number — a stage's limit says how many may be *inside* at once, never
*who goes next*.

Children are pulled from the lane's queue in this order:

1. **Older item first** — a child is never created for a younger item's work while an older item has work queued
   on the lane.
2. **Then `Schedule` order**, and within one `Schedule` call the order you listed the tasks.
3. **Then index order** within a `NewTasks` / generator / channel.

So for `Schedule(ctx, messages.NewTask(A), messages.NewTasks(2, B))` from item 1 and
`Schedule(ctx, messages.NewTasks(2, C))` from item 2, every interior stage is entered in the order
`A, B0, B1, C0, C1`.

The ticket is taken at creation, and a child holds the lane's entrance only until its first move. So work an item
adds later — a second round, or a follow-up scheduled by a child after it has moved off the entrance — is queued
ahead of younger items' queued work, but a younger item's child that was already created keeps its earlier ticket.
Strict "all of item 1, then all of item 2" holds for the pattern above: one `Schedule` right after entering.

---

| Prev                                        | Next                                              |
|---------------------------------------------|---------------------------------------------------|
| [⬅ Fan-out (scatter/gather)](4_fan-out.md) | [Conditional MoveTo ➡](6_conditional-move-to.md) |
