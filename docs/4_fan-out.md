# Fan-out (scatter/gather)

_True fan-out without a join is not supported since we always describe the path of 1 item._

Imagine a **dbsWrite** stage where we want to perform parallel tasks (writing to 2 different DBs):
- Some items may need to write to both DBs
- Some may need to write to only 1
- DB pools may have different sizes and the backpressure should be propagated

Add a [FanOut](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Conveyor.AddFanOut) stage for that case. An item
**enters** it with `MoveTo`, **adds work** with `Schedule`, and the `MoveTo` that takes it out **joins** that work:

```go
c := conveyor.NewConveyor()
// SetLimit(2): 2 items can be inside the fan-out (have work outstanding on its branches) at a time
dbsWrite := c.AddFanOut().SetLimit(2)
// Pools are single-step branches with their own capacity limits (similar to connection pools)
db1Write := dbsWrite.AddPool().SetLimit(2) // 2 tasks (from 1 or more items) can occupy it at a time
db2Write := dbsWrite.AddPool().SetLimit(3) // 3 tasks (from 1 or more items) can occupy it at a time
commit := c.AddStage()

c.Run(ctx, func(ctx context.Context) error {
    // (1) read batch

    // (2) enter dbsWrite:
    // - waits for a free slot, then releases the previous stage
    // - the item is inside with an empty body; nothing runs yet
    if err := dbsWrite.MoveTo(ctx); err != nil {
        return err
    }

    // (3) schedule the tasks: one for db1Write, two for db2Write (i = 0 and 1)
    // - returns once the tasks are queued (not finished), so the code below runs alongside them
    // - never blocks
    err := dbsWrite.Schedule(ctx,
        db1Write.NewTask(func(ctx context.Context) error {
            // write to db1
            return nil
        }),
        db2Write.NewTasks(2, func(ctx context.Context, i int) error {
            // write to db2
            return nil
        }),
    )
    if err != nil {
        return err
    }

    // (4) leave: the tasks are this node's body, so the item can't move on before all of them have finished
    // - waits for the tasks and returns an error if any of them failed
    // - then waits until it can enter commit
    if err := commit.MoveTo(ctx); err != nil {
        return err
    }

    // (5) commit offsets / ack messages
    return nil
})
```
### [★ Interactive Demo](https://cardinalby.github.io/go-conveyor/#%7B%22v%22%3A2%2C%22startDelayMs%22%3A1000%2C%22nodes%22%3A%5B%7B%22kind%22%3A%22fanout%22%2C%22name%22%3A%22dbsWrite%22%2C%22limit%22%3A2%2C%22queueSize%22%3A0%2C%22branches%22%3A%5B%7B%22kind%22%3A%22pool%22%2C%22name%22%3A%22db1Write%22%2C%22limit%22%3A1%2C%22delayMs%22%3A1700%2C%22tasksPerItem%22%3A1%7D%2C%7B%22kind%22%3A%22pool%22%2C%22name%22%3A%22db2Write%22%2C%22limit%22%3A2%2C%22delayMs%22%3A3150%2C%22tasksPerItem%22%3A1%7D%5D%7D%2C%7B%22kind%22%3A%22stage%22%2C%22name%22%3A%22commit%22%2C%22limit%22%3A1%2C%22queueSize%22%3A0%2C%22delayMs%22%3A1000%7D%5D%2C%22mode%22%3A%22run%22%2C%22showCode%22%3Afalse%2C%22showLegend%22%3Afalse%7D)
![FanOut stage](./res/readme/fanout.svg)

It's similar to using a `sync.WaitGroup` inside the dbsWrite stage, but gives you:
- granular **backpressure** control: the fan-out's limit bounds the items with work outstanding, and
  [SetAdmission](#setadmission-backpressure-that-follows-the-pools) can make the pressure follow the pools' capacity instead
- **ordering** guarantees: an older item's work always has priority over a younger item's (in taking slots on
  the branches)
- **no deadlocks**: a running task holds one slot and never waits for another one

If:
- **db1Write** is fast and has free slots, but
- **db2Write** is slow and saturated
- AND **dbsWrite** has a free slot (limit > 1)

Then:
- The next item can also move to **dbsWrite** and schedule its tasks on **db1Write**, while
the slow **db2Write** pool is still processing tasks from the previous item.
- Even if the next item completes its tasks first, it cannot move to **commit** stage before the previous item does

Use [Pool.NewTask](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Pool.NewTask),
[Pool.NewTasks](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Pool.NewTasks),
[Pool.NewTasksGen](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Pool.NewTasksGen),
[Pool.NewTasksChan](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Pool.NewTasksChan)
to create tasks for a branch (a [Lane](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Lane) has the
same four constructors). A `Task` is single-use; pass any number of them to one `Schedule` call.

A fan-out fans out to **branches**, and there are two kinds of them:

| branch   | built with         | what it is                                      | capacity                    |
|----------|--------------------|-------------------------------------------------|-----------------------------|
| **pool** | `FanOut.AddPool()` | one step: a task runs there and is done         | `SetLimit(n)` — n at a time |
| **lane** | `FanOut.AddLane()` | a sub-pipeline: the task travels its own stages | its stages' own limits      |

Most fan-outs need only pools. Lanes are for the case where one branch's work is itself a multi-step
path — see [Lanes](5_lanes.md). Start with pools; you'll know when you need a lane.

Both kinds satisfy [Branch](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Branch) — the four task
constructors they share — so code that only builds work on a branch needn't care which kind it got.
`FanOut.Branches()` returns them all, in creation order.

## SetAdmission: backpressure that follows the pools

By default `MoveTo` lets an item into a fan-out when the fan-out has a free item slot (`SetLimit`) and it is the
item's turn, and the item leaves the previous stage at that moment. A full pool never blocks anything by itself:
items pile up inside the fan-out until its item limit is reached, and only then does the stage before feel it.

[FanOut.SetAdmission](https://pkg.go.dev/github.com/cardinalby/go-conveyor#FanOut.SetAdmission) makes the
backpressure follow the pools instead. Two variants:

| policy                   | `MoveTo` lets the item in when                    | item leaves the previous stage                                            | pressure appears when                                              |
|--------------------------|---------------------------------------------------|---------------------------------------------------------------------------|--------------------------------------------------------------------|
| `AdmitByLimit` (default) | free item slot, item's turn                       | on entering                                                               | the item limit is reached                                          |
| `AdmitByPools`           | plus some pool has a free slot and nothing queued | on entering                                                               | every pool is full, or the item limit is reached                   |
| `AdmitByPoolsStrict`     | same as above                                     | when its first `Schedule` has started one task on every branch it touched | as above, plus items stuck on a full pool block the previous stage |

```go
read := c.AddStage().SetLimit(2)
dbsWrite := c.AddFanOut().SetLimit(100).SetAdmission(conveyor.AdmitByPools)
db1Write := dbsWrite.AddPool().SetLimit(4) // fast
db2Write := dbsWrite.AddPool().SetLimit(1) // slow, saturated
commit := c.AddStage()
```

**`AdmitByPools`** keeps the pools saturated. Items keep entering while any pool can take work and stop as soon as
no pool can, whatever the item limit still allows. So the item limit only has to be large enough; the pressure
follows the pools, and the item limit bounds how many items have work queued on a full pool. This is the choice for
throughput: while
one long `db2Write` task runs, later items get their `db1Write` work done, and items that need only `db1Write` are
not held back. When the next `db2Write` tasks are quick, those items leave at once.

**`AdmitByPoolsStrict`** keeps fewer items in flight. An item whose work waits for a full pool also stays in the
previous stage until its first `Schedule` has started one task on every branch it touched (or until it leaves,
detaches, or its processor returns). Such items keep `read` busy, and once `read` is full of them the item behind
waits, even if `db1Write` is idle. A younger item whose work can start may still enter and run ahead of a stuck one
on the branches; downstream order is unchanged. Things to know:

- **The fan-out limit bounds how far younger items can get ahead**, so raise it deliberately. With the default
  limit 1 nobody passes anybody.
- **Only the first `Schedule` counts.** The previous stage is released at the first task start per touched branch,
  not when the whole batch is dispatched. Later rounds and follow-ups scheduled by tasks never hold it. A first
  `Schedule` with zero tasks releases at once.
- **A waiting room (`SetQueueSize`) weakens the pressure** by its size: an item in the waiting room has already left
  the previous stage, so it is the waiting-room slot that is kept instead.
- **Don't wait for the item behind.** A producer or background operation of item N must not wait for item N+1 to
  enter: item N may be the one holding it back.
- In `Stats` and `DebugUnitOccupants` the kept slot shows as occupancy of the previous stage for an item that is
  already inside the fan-out (see [Observability](7_observability.md)).

`SetAdmission` is live, like `SetLimit` and `SetQueueSize`: it applies to items entering after the call.

### [★ Interactive Demo](https://cardinalby.github.io/go-conveyor/#%7B%22v%22%3A2%2C%22startDelayMs%22%3A500%2C%22nodes%22%3A%5B%7B%22kind%22%3A%22stage%22%2C%22name%22%3A%22read%22%2C%22limit%22%3A2%2C%22queueSize%22%3A0%2C%22delayMs%22%3A500%7D%2C%7B%22kind%22%3A%22fanout%22%2C%22name%22%3A%22dbsWrite%22%2C%22admission%22%3A%22poolsStrict%22%2C%22limit%22%3A6%2C%22queueSize%22%3A0%2C%22branches%22%3A%5B%7B%22kind%22%3A%22pool%22%2C%22name%22%3A%22db1Write%22%2C%22limit%22%3A4%2C%22delayMs%22%3A1000%2C%22tasksPerItem%22%3A1%7D%2C%7B%22kind%22%3A%22pool%22%2C%22name%22%3A%22db2Write%22%2C%22limit%22%3A1%2C%22delayMs%22%3A3000%2C%22tasksPerItem%22%3A1%7D%5D%7D%2C%7B%22kind%22%3A%22stage%22%2C%22name%22%3A%22commit%22%2C%22limit%22%3A3%2C%22queueSize%22%3A0%2C%22delayMs%22%3A500%7D%5D%2C%22mode%22%3A%22run%22%2C%22showCode%22%3Afalse%2C%22showLegend%22%3Atrue%7D)

The demo runs `AdmitByPoolsStrict`. Every item schedules one task per pool, so the dimmed copy in `read` is an item
that is inside `dbsWrite` with its `db2Write` task queued. The fan-out's `Admit` field has all three values; switch
it while the demo runs to compare (items already inside keep behaving as they were let in): with `by pools` new items
no longer keep `read`, so `db1Write` stays busy; with `by limit` items pile up inside the fan-out up to its limit.

## FanOut.Detach()

If you don't need to wait for the fan-out's tasks when leaving it, detach the body and wait for the returned
[Wave](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Wave) later (similar to
[Retain](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Stage.Retain)). Where the item stands while it waits
is your choice: `Wave.Wait` after `commit.MoveTo` holds the **commit** slot, before it holds the **report** slot.

<details>
<summary>Example</summary>

```go
if err := crawl.MoveTo(ctx); err != nil {
    return err
}
if err := crawl.Schedule(ctx, roots...); err != nil {
    return err
}
wave := crawl.Detach(ctx) // the tree keeps growing in the background; the crawl slot follows it

if err := report.MoveTo(ctx); err != nil { // does NOT wait for the tasks
    return err
}
// work in another stage

if err := commit.MoveTo(ctx); err != nil {
    return err
}
if err := wave.Wait(ctx); err != nil { // the whole tree is done; an error reads "crawl work: <err>"
    return err
}
// commit
```
</details>

- Detaching moves the wait, not the ceiling: the **crawl** slot is held until the work finishes, so `SetLimit` keeps
  bounding how many items have work outstanding.
- The detached tasks may still `Schedule` follow-ups into the wave. After `Detach` the ItemProcessor may not
  `Schedule` or `Wait` at this fan-out again (that panics).
- Only the item that created the wave may `Wait` on it. An error on a wave nobody waits for is not lost: it fails the
  item when it completes.
- A failing task cancels the item, so a `Wave.Wait` in progress wakes at once. If the tree is still winding down,
  `Wait` returns the cancellation cause; wait for `Finished` and read `Err`, or call `Wait` again after `Finished`.
- `Wave.Started` closes once every task the *ItemProcessor* scheduled has been handed out (streaming sources
  drained). Follow-ups scheduled by tasks do not count. Use it to know when state read by your sources may change.

## Usage patterns

### Conditional body

Build the task list after entering; the tasks you don't need are never created. Zero tasks is legal.

<details>
<summary>Example</summary>

```go
if err := dbsWrite.MoveTo(ctx); err != nil {
    return err
}
var tasks []conveyor.Task
if len(batch.Db1Rows) > 0 {
    tasks = append(tasks, db1Write.NewTask(writeDb1))
}
if len(batch.Db2Rows) > 0 {
    tasks = append(tasks, db2Write.NewTask(writeDb2))
}
if err := dbsWrite.Schedule(ctx, tasks...); err != nil {
    return err
}
```
</details>

### Rounds: `Wait`, then schedule again

[FanOut.Wait](https://pkg.go.dev/github.com/cardinalby/go-conveyor#FanOut.Wait) blocks until everything scheduled
so far in this fan-out has finished (including follow-ups the tasks scheduled) and returns the first error. The item
keeps its slot and may `Schedule` again, so the results of one round can decide the next. `Wait` on an empty body
returns at once.

<details>
<summary>Example</summary>

```go
if err := dbsWrite.MoveTo(ctx); err != nil {
    return err
}
if err := dbsWrite.Schedule(ctx, db1Write.NewTask(writeIndex)); err != nil {
    return err
}
if err := dbsWrite.Wait(ctx); err != nil {
    return err
}
for _, rows := range planNextRound(batch) {
    if err := dbsWrite.Schedule(ctx, db2Write.NewTasks(len(rows), writeRows(rows))); err != nil {
        return err
    }
    if err := dbsWrite.Wait(ctx); err != nil {
        return err
    }
}
return commit.MoveTo(ctx)
```
</details>

### Trees: a task schedules follow-ups

Work is not always known up front: crawling, directory listing, paginated APIs. A task running on one of the
fan-out's pools may call `Schedule` **with its own context** to add follow-up work to the same body. Three rules:

1. **Schedule, never wait.** A task holds a slot, so it must not wait for other work: `Wait` with a task's context
   panics. Leaving the fan-out (or `Wait` in the ItemProcessor) is the join for the whole tree.
2. **Before the task returns.** A follow-up from a goroutine that outlives its task is refused with `ErrStaleContext`
   once the body has finished. Keep `Schedule` inside the callback.
3. **Results flow forward.** A task passes what it found to the follow-ups it schedules; nothing flows back to it.

A failing task cancels the item, so a failing tree terminates: queued work is dropped and new `Schedule` calls
return the cause.

<details>
<summary>Example</summary>

```go
var visit func(ctx context.Context, url string) error
visit = func(ctx context.Context, url string) error {
    links, err := fetchPage(ctx, url, &found)
    if err != nil {
        return err
    }
    // 0..N follow-ups, queued at this item's place before this task returns. Never blocks.
    return crawl.Schedule(ctx, fetch.NewTasks(len(links), func(ctx context.Context, i int) error {
        return visit(ctx, links[i])
    }))
}

if err := crawl.MoveTo(ctx); err != nil {
    return err
}
// the root task (several roots are fine too)
if err := crawl.Schedule(ctx, fetch.NewTask(func(ctx context.Context) error { return visit(ctx, seed) })); err != nil {
    return err
}
// leaving crawl waits for the whole tree, follow-ups included (or call crawl.Wait(ctx) explicitly)
if err := commit.MoveTo(ctx); err != nil {
    return err
}
```
</details>

### Join as a continuation

A task must not wait for the work it spawned. When some step needs *all* the siblings done, make that step a task of
its own, scheduled by the last sibling to finish. A lane's child item may `Schedule` at its fan-out the same way (see
[Lanes](5_lanes.md)).

<details>
<summary>Example</summary>

```go
remaining := int32(len(parts))
if err := f.Schedule(ctx, work.NewTasks(len(parts), func(ctx context.Context, i int) error {
    if err := processPart(ctx, parts[i]); err != nil {
        return err
    }
    if atomic.AddInt32(&remaining, -1) == 0 {
        // still holding a slot: the merge is queued before any slot frees
        return f.Schedule(ctx, merge.NewTask(func(ctx context.Context) error { return mergeAll(ctx, parts) }))
    }
    return nil
})); err != nil {
    return err
}
```
</details>

## Ordering

> On a branch, a free slot is never given to a younger item's work while an older item has work queued there.
> Running work is never taken back.

- **Work is queued at the owning item's place** on each branch: after that item's and older items' queued work,
  ahead of younger items' work. Within one item: `Schedule` order, then argument order, then task index order.
- **The door.** An item inside a fan-out keeps the item behind it out until its first `Schedule` call (or until it
  leaves or detaches). The item behind may step into the fan-out's waiting room, but not into the node. This is what
  keeps each branch in item order: the younger item cannot queue anything before the older one has. Keep the code
  between `MoveTo` and the first `Schedule` short. `Schedule` with zero tasks only opens the door; `Wait` does not.
- **Later rounds and follow-ups** are queued ahead of younger items' queued work, but a younger item's task that is
  already running is not preempted. Only an unbroken chain of follow-ups onto a limit-1 pool keeps strict item order
  on that pool.
- **Starvation.** Priority by age can starve a younger item: if an older item's task on a limit-1 pool always
  schedules one replacement onto the same pool before returning, the younger item's queued task never runs until the
  chain ends. Bound such chains, or give the pool more than one slot.

---

| Prev                                   | Next                   |
|----------------------------------------|------------------------|
| [⬅ Shared Stages](3_shared-stages.md) | [Lanes ➡](5_lanes.md) |
