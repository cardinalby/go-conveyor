# Fan-out (scatter/gather)

_True fan-out without a join is not supported since we always describe the path of 1 item._

Imagine a **dbsWrite** stage where we want to perform parallel tasks (writing to 2 different DBs):
- Some items may need to write to both DBs
- Some may need to write to only 1
- DB pools may have different sizes and the backpressure should be propagated

Just using semaphores inside a stage never gives you the right backpressure and ordering guarantees.

Add a [FanOut](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Conveyor.AddFanOut) stage for that case. An item:
- **adds tasks** with `Schedule`
- **enters** the fan-out with `MoveTo`. Tasks start once possible:
  - if free slots are available in a branch
  - earlier items have no queued work on the same branch
- _optional: schedules more work once it's discovered_
- **moving to** next stage **joins** that work

```go
c := conveyor.NewConveyor()
// SetLimit(2): at most 2 item bodies can occupy the fan-out at a time
dbsWrite := c.AddFanOut().SetLimit(2)
// Pools are single-step branches with their own capacity limits (similar to connection pools)
db1Write := dbsWrite.AddPool().SetLimit(2) // 2 tasks (from 1 or more items) can occupy it at a time
db2Write := dbsWrite.AddPool().SetLimit(3) // 3 tasks (from 1 or more items) can occupy it at a time
commit := c.AddStage()

c.Run(ctx, func(ctx context.Context) error {
    // (1) read batch

    // (2) schedule the tasks, never blocks
    if err := dbsWrite.Schedule(ctx,
        // one task for db1Write
        db1Write.NewTask(func(ctx context.Context) error {
            // write to db1
            return nil
        }),
        // alternative syntax. 2 tasks (will be called with i=0 and i=1) for db2Write
        db2Write.NewTasks(2, func(ctx context.Context, i int) error {
            // write to db2
            return nil
        }),
    ); err != nil {
        return err
    }

    // (3) enter dbsWrite:
    // - waits for a free item slot (see `AddFanOut().SetLimit(2)`)
    // - the prepared tasks become eligible to start
    // - MoveTo returns once the item is inside, not when the tasks finish
    if err := dbsWrite.MoveTo(ctx); err != nil {
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


## Usage patterns

### Schedule before entry

Tasks can be scheduled after `MoveTo` if needed (if the work is discovered while inside the fan-out).

When the **initial** tasks **are known upfront**, schedule them **before** `MoveTo`.
This eliminates the gap between admission and publishing the initial batch, allowing work to start—and the
next item to enter—as soon as this item is admitted.

### Rounds: `Wait`, then schedule again

[FanOut.Wait](https://pkg.go.dev/github.com/cardinalby/go-conveyor#FanOut.Wait) blocks until everything scheduled
so far in this fan-out has finished (including follow-ups the tasks scheduled) and returns the first error.

The item keeps its slot and may `Schedule` again, so the results of one round can decide the next.
`Wait` on an empty body returns at once.

<details>
<summary>Simple example</summary>

```go
// ...

if err := dbsWrite.MoveTo(ctx); err != nil {
    return err
}

// planRounds returns iter.Seq[Round] and may inspect results after each yield.
for round := range planRounds(batch) {
    if err := dbsWrite.Schedule(ctx,
        db1Write.NewTasks(len(round.db1Rows), round.writeDB1),
        db2Write.NewTasks(len(round.db2Rows), round.writeDB2),
    ); err != nil {
        return err
    }
    if err := dbsWrite.Wait(ctx); err != nil {
        return err
    }
    // planRounds can discover the next round now
}
return commit.MoveTo(ctx)
```
</details>

<details>
<summary>Recommended way</summary>

```go
// ...
first := true
// planRounds returns iter.Seq[Round] and may inspect results after each yield.
for round := range planRounds(batch) {
    if err := dbsWrite.Schedule(ctx,
        db1Write.NewTasks(len(round.db1Rows), round.writeDB1),
        db2Write.NewTasks(len(round.db2Rows), round.writeDB2),
    ); err != nil {
        return err
    }

    if first {
        first = false
        // enter only after scheduling the first round;
        // skip the fan-out entirely if no rounds are planned
        if err := dbsWrite.MoveTo(ctx); err != nil {
            return err
        }
    }

    if err := dbsWrite.Wait(ctx); err != nil {
        return err
    }
    // planRounds can discover the next round now
}
return commit.MoveTo(ctx)
```
</details>

### Trees: a task schedules follow-ups

Work is not always known up front: crawling, directory listing, paginated APIs. A task running on one of the
fan-out's pools may call `Schedule` **with its own context** to add follow-up work to the same body.
It must schedule before returning and must not call `Wait`.

A failing task cancels the item, so a failing tree terminates: queued work is dropped and new `Schedule` calls
return the cause.

<details>
<summary>Example</summary>

```go
var visit func(ctx context.Context, url string) error
visit = func(ctx context.Context, url string) error {
    links, err := fetchPage(ctx, url)
    if err != nil {
        return err
    }
    // 0..N follow-ups, queued at this item's place before this task returns. Never blocks.
    return crawl.Schedule(ctx, fetch.NewTasks(len(links), func(ctx context.Context, i int) error {
        return visit(ctx, links[i])
    }))
}

// the root task (several roots are fine too)
if err := crawl.Schedule(ctx, fetch.NewTask(func(ctx context.Context) error { return visit(ctx, seed) })); err != nil {
    return err
}

if err := crawl.MoveTo(ctx); err != nil {
    return err
}

// leaving crawl waits for the whole tree, follow-ups included (or call crawl.Wait(ctx) explicitly)
if err := commit.MoveTo(ctx); err != nil {
    return err
}
```
</details>

### Join as a continuation

A task must not call `Wait`: it holds a branch slot, so waiting for work that needs the same branch could deadlock.
When several tasks need a final step after their results are ready, make that step another task instead. The task
that observes the completion condition schedules the continuation before returning; `Schedule` never blocks, and
the continuation remains part of the same fan-out body. Leaving the fan-out then waits for both the original work
and the continuation.

## Different kinds of branches

A fan-out fans out to [branches](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Branch),
and there are two kinds of them:

| branch   | built with         | what it is                                      | capacity                                  |
|----------|--------------------|-------------------------------------------------|-------------------------------------------|
| **pool** | `FanOut.AddPool()` | one step: a task runs there and is done         | `SetLimit(n)` — n at a time               |
| **lane** | `FanOut.AddLane()` | a sub-pipeline: the task travels its own stages | entrance limit 1, then its stages' limits |

- Most fan-outs need only pools
- Lanes are for the case where one branch's work is itself a multi-step
  path — see [Lanes](5_lanes.md).
- Start with pools; you'll know when you need a lane.
- Both kinds satisfy [Branch](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Branch).
  `FanOut.Branches()` returns them all, in creation order.

## FanOut.Retain()

If you don't need to wait for the fan-out's tasks when leaving it, retain the body and wait for the returned
[Wave](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Wave) later. It is the fan-out counterpart of
[Stage.Retain](https://pkg.go.dev/github.com/cardinalby/go-conveyor#Stage.Retain): a stage takes a callback because
its work runs inline; a fan-out already has its work scheduled. Where the item stands while it waits is your choice:
`Wave.Wait` after `commit.MoveTo` holds the **commit** slot, before it holds the **report** slot.

<details>
<summary>Example</summary>

```go
if err := crawl.Schedule(ctx, roots...); err != nil {
    return err
}
if err := crawl.MoveTo(ctx); err != nil {
    return err
}
wave := crawl.Retain(ctx) // the tree keeps growing in the background; the crawl slot follows it

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

- Retaining moves the wait, not the ceiling: the **crawl** slot is held until the work finishes and the item has
  moved on, so `SetLimit` keeps bounding how many items have work outstanding.
- Retaining does not release the previous stage early under `Balanced` or `Strict`
  (see [SetBackpressure](#setbackpressure-when-the-previous-stage-is-released)).
- The retained tasks may still `Schedule` follow-ups into the wave. After `Retain` the ItemProcessor may not
  `Schedule` or `Wait` at this fan-out again (that panics).
- Only the item that created the wave may `Wait` on it. An error on a wave nobody waits for is not lost: it fails the
  item when it completes.
- A failing task cancels the item, so a `Wave.Wait` in progress wakes at once. If the tree is still winding down,
  `Wait` returns the cancellation cause; wait for `Finished` and read `Err`, or call `Wait` again after `Finished`.
- After the ItemProcessor stops adding work (leaves, retains, or returns), `Wave.Started` closes once every task it
  scheduled has been handed out (streaming sources drained). Follow-ups scheduled by tasks do not count.

## SetBackpressure: when the previous stage is released

`MoveTo` lets an item into a fan-out when the fan-out has a free item slot (`SetLimit`).
But you can control **when the previous stage (or queue slot) is released** using
[`fanOut.SetBackpressure()`](https://pkg.go.dev/github.com/cardinalby/go-conveyor#FanOut.SetBackpressure):

```go
// ...
dbsWrite := c.AddFanOut().SetLimit(100).SetBackpressure(conveyor.BackpressureStrict)
// ...
```

Branch capacity never gates entry. The **initial batch** is all work scheduled before entry, or the first
`Schedule` after entry when nothing was prepared. Several pre-entry calls form one batch. A known-empty batch
releases `Balanced` and `Strict` immediately.

■ **`BackpressureBuffered`** gives the most lookahead:
- Props:
  - Less backpressure; may increase memory usage
  - Can improve pool utilization
- The previous stage is released when:
  - the item enters the fan-out
- Behavior:
  - items pile up inside the fan-out up to its limit
  - the stage before feels the pressure only when that limit is reached
  - Item N waiting for `db2Write` does not stop item N+1 from getting its `db1Write` work done.

■ **`BackpressureBalanced`** requires some progress before the item stops occupying `read`:
- Props: balanced between `Buffered` and `Strict`
- The previous stage is released when:
  - the first task of the item's initial batch has started (on any branch)
- Behavior:
  - an item whose entire initial batch waits for full pools keeps `read` busy
  - once any initial task starts, the item no longer keeps `read` busy.

■ **`BackpressureStrict`** keeps fewer items in flight:
- Props:
  - Higher backpressure; may reduce memory usage
  - Can reduce pool utilization
- The previous stage is released when:
  - every branch of the item's initial batch has started one task or proved empty
- Behavior:
  - an item stays in `read` while any branch of its initial batch is entirely waiting
  - once `read` is full of such items, the item behind waits even if `db1Write` is idle.
  - a younger item whose work can start may still enter and run ahead of a stuck one on the branches
  - downstream order is unchanged

`SetBackpressure` can be changed in runtime: it applies to items entering after the call. Items
already inside keep the mode they were let in with.

★ The demo page allows switching the backpressure mode while it runs

## Ordering details

> On a branch, a free slot is never given to a younger item's work while an older item has work queued there.
> Running work is never taken back.

- **Work is queued at the owning item's place** on each branch: after that item's and older items' queued work,
  ahead of younger items' work. Within one item: `Schedule` order, then argument order, then task index order.
- **The door.** An item inside a fan-out keeps the item behind it out until its initial batch is on the branches:
  at entry if it prepared work before, else at its first `Schedule` call (or until it leaves or retains). The item
  behind may step into the fan-out's waiting room, but not into the node. This is what keeps each branch in item
  order: the younger item cannot queue anything before the older one has. Keep the code between `MoveTo` and the
  first `Schedule` short, or schedule before entering. `Schedule` with zero tasks only opens the door; `Wait` does
  not.
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
