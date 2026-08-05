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

report := messages.AddStage().SetLimit(3) // 3 concurrent reporting tasks

commit := c.AddStage()

c.Run(ctx, func(ctx context.Context) error {
    batch := read() // (1) read a batch of messages

    // (2) run the fetch+write and report tasks on each message 
    // and wait for all of them to finish before moving to the commit stage
    err := split.MoveTo(ctx, conveyor.Tasks{
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
            return reportDB(cctx, batch[i]) // one at a time, in batch order
        }),	
    })
    if err != nil {
        return err
    }

    // (3) waits for every child of this item to finish
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

## The lane's entrance is limit 1, like the conveyor's start

A `Lane` has **no `SetLimit`** — its entrance always admits one child at a time, exactly like the implicit start
stage that paces item creation on the conveyor. The next
child isn't created until the previous one has moved off the entrance.

The parallelism comes from the **interior stages**, not the entrance — which is why `enrich` above carries the
`SetLimit(6)`.

## Order: every child takes a ticket

When a piece of a lane's work is **pulled** off the lane's queue, it takes a numbered ticket. Interior stages admit
strictly by ticket number — a stage's limit says how many may be *inside* at once, never *who goes next*.

Tickets are handed out by three rules, in this priority:

1. **Older item first** — every piece of an item's work precedes the *first* piece of the next item's.
2. **Then the order you added the tasks** to the `Tasks` value.
3. **Then index order** within a `NewTasks` / generator / channel.

So for `Tasks{messages.NewTask(A), messages.NewTasks(2, B)}` from item 1 and `Tasks{messages.NewTasks(2, C)}` from
item 2, every interior stage is entered in the order `A, B0, B1, C0, C1`.

---

| Prev                                       | Next                                              |
|---------------------------------------------|------------------------------------------------------|
| [⬅ Fan-out (scatter/gather)](4_fan-out.md) | [Conditional MoveTo ➡](6_conditional-move-to.md) |
