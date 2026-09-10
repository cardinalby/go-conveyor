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
- granular **backpressure** control (all pools are fully utilized)
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

## The door: when the next item may enter

An item that is inside a fan-out holds a **door** closed for the item behind it until its first `Schedule` call,
or until it leaves or detaches — whichever comes first. Meanwhile the item behind may step into the fan-out's
waiting room (if it has one), but not into the node. This is what keeps each branch's work in item order: the
younger item cannot queue anything before the older item has queued its first tasks. The cost is the window
between `MoveTo` and the first `Schedule`, so keep the code there short. `Schedule` with zero tasks is legal and
does only that — it opens the door.

## Conditional body

Build the task list after entering; the tasks you don't need are never created:

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
// zero tasks is legal: it only opens the door for the next item
if err := dbsWrite.Schedule(ctx, tasks...); err != nil {
    return err
}
```

## Rounds: `Wait`, then schedule again

[FanOut.Wait](https://pkg.go.dev/github.com/cardinalby/go-conveyor#FanOut.Wait) blocks until everything scheduled
so far in this fan-out has finished (including work the tasks scheduled themselves) and returns the first error.
The item keeps its slot and may `Schedule` again, so the results of one round can decide the next:

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

`Wait` on an empty body returns at once. It does not open the door; only `Schedule`, leaving and `Detach` do.

## Trees: a task schedules follow-ups

Work is not always known up front: crawling, recursive directory listing, paginated APIs, graph traversal. A task
running on one of the fan-out's pools may call `Schedule` **with its own context** to add follow-up work to the same
body. Three rules:

1. **Schedule, never wait.** A task holds a slot, so it must not wait for other work: `Wait` with a task's context
   panics. Leaving the fan-out (or `Wait` in the ItemProcessor) is the join for the whole tree.
2. **Before the task returns.** A follow-up scheduled from a goroutine that outlives its task is refused with
   `ErrStaleContext` once the body has finished — and cannot be told from a legal call while sibling work keeps the
   body busy. Keep `Schedule` inside the callback.
3. **Results flow forward.** A task passes what it found to the follow-ups it schedules (closures, a shared
   structure); nothing flows back to the task, because it has already returned by the time they run.

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
initTask := fetch.NewTask(func(ctx context.Context) error { return visit(ctx, seed) })
// schedule the root task (you can have multiple roots if needed)
if err := crawl.Schedule(ctx, initTask); err != nil {
    return err
}

// leaving crawl stage (moving to commit) waits for the whole tree (all tasks, including follow-ups) to finish
// Or you can do explicit `crawl.Wait(ctx)`

if err := commit.MoveTo(ctx); err != nil {
    return err
}
```

A failing task cancels the item, so a failing tree terminates: running tasks see the cancellation, queued work is
dropped, and new `Schedule` calls return the cause.

### Join as a continuation

A task must not wait for the work it spawned. When some step needs *all* the siblings done, make that step a task of
its own, scheduled by the last sibling to finish:

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

A lane's child item may `Schedule` at the fan-out its lane belongs to in the same way, before its callback returns
(see [Lanes](5_lanes.md)).

## Ordering

> On a branch, a free slot is never given to a younger item's work while an older item has work queued there. Work
> that has started, or a slot already reserved for a streaming pull, is never taken back.

Every `Schedule` call queues its work **at the owning item's place** in each branch: after everything that item and
older items already have queued there, ahead of younger items' queued work. Within one item the order is: `Schedule`
order, then argument order within one call, then index / yield order within one task. What follows from this:

- **One `Schedule` right after entering** (the basic example): the door keeps the younger item out until the older
  one has scheduled, so *everything* the older item scheduled starts before *anything* the younger item scheduled
  on the same branch.
- **A follow-up scheduled from a running task onto the same pool** is queued while the task still holds its slot.
  When the task returns, the freed slot goes to the head of the queue — this item's work or an older item's. On
  that pool the item never has a gap in which it holds nothing while still having work to do.
- **A follow-up onto a different branch** waits for a slot to free there and then gets it by priority; a younger
  item's task may already be running on that branch.
- **A later round** (`Schedule` after the door opened): younger items' tasks may be running. The round's tasks are
  queued ahead of their queued work and take slots as they free, one finishing younger task per slot.
- **Pool limit 1**: an unbroken chain that spawns onto the same pool from the running task keeps strict item order
  on that pool. Rounds, cross-pool follow-ups and lane children interleave items.
- Running work is never preempted, on any branch.

## FanOut.Detach()

If you don't need to wait for the results of the fan-out's tasks at the next stage's `MoveTo()` call, you can detach
the body and wait for the tasks to finish later
(similar to [Retain](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Stage.Retain)):

```go
if err := crawl.MoveTo(ctx); err != nil {
    return err
}
if err := crawl.Schedule(ctx, roots...); err != nil {
    return err
}
wave := crawl.Detach(ctx) // the tree keeps growing in the background; the slot follows it

// - DON'T wait for the tasks to finish (Detach was called)
if err := report.MoveTo(ctx); err != nil {
    return err
}
// work in another stage

// - Wait for the whole tree to finish and return an error if any of it failed
if err := commit.MoveTo(ctx, wave); err != nil {
    return err
}
// commit
```

Detaching moves the wait, not the ceiling: the **crawl** slot is still held until the work finishes (it follows the
work instead of the item), so `SetLimit` keeps bounding how many items have work outstanding. The detached tasks may
still `Schedule` follow-ups into the wave while it runs; `Finished` closes when the whole tree is done. After
`Detach` the ItemProcessor may not `Schedule` or `Wait` at this fan-out again (that panics). And an error on a wave
that nobody ever joins is not lost — it fails the item when it completes.

**`Wave.Started`** closes once the wave is sealed (detached) and every task the *ItemProcessor* scheduled has been
handed out — its streaming sources (`NewTasksGen`, `NewTasksChan`) drained. Follow-ups scheduled by tasks do not
count and do not delay it: a spawned source is created by a task that is still running at that moment and owns the
decision. So state read only by the ItemProcessor's own sources may be mutated after `Started`; state a spawned
source reads is not covered.

---

| Prev                                   | Next                   |
|----------------------------------------|------------------------|
| [⬅ Shared Stages](3_shared-stages.md) | [Lanes ➡](5_lanes.md) |
