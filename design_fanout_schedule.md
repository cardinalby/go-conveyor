# FanOut body that grows: `Schedule` and `Wait`

Status: design proposal, not implemented. The change is breaking and is meant for a v0.x release before v1.

This document records the problem, the options that were declined and why, and the full specification of the
proposed interfaces and behavior, followed by the implementation plan (§11). Revision 2 incorporates an external
design review; revision 3 records the decisions taken afterwards; revision 4 adds the implementation plan (see the
change log at the end).

---

## 1. Problem

### 1.1 Original request

Today the tasks of a fan-out must be prepared ahead of time and passed to `FanOut.MoveTo(ctx, tasks)`. The move to
the next node waits for them to complete (join). Some ItemProcessors do not know all the tasks in advance. They want
to enter the fan-out, probably with a first set of tasks, wait until those are done, and then decide whether to run
more tasks on the same pools. This may repeat several times.

### 1.2 Extended during the discussion

- Work may not come in rounds (sets of tasks). It may be a **tree**: after each completed task, 0..N new tasks may
  appear or not. Examples: crawling, recursive directory listing, paginated APIs, graph traversal.
- The tree may have **several roots**, for example one per message in a batch.
- The **priority of the older item** on the pools must be kept as far as physically possible. With rounds there is a
  gap between "round k finished" and "round k+1 scheduled". In that gap a younger item's tasks take free slots, and
  the older item's next round then waits behind running work. Scheduling follow-ups from inside a task, while the
  task still holds its slot, closes that gap on that pool.

### 1.3 What must not change

The invariants from `impl_details.md` §13 stay in force:

1. No state without a token.
2. `maxRank` is non-increasing with item age within a scope (the O(1) ordering gate).
3. Admission is atomic: the lock is held from the admission check through the occupancy mutation.
4. One slot per piece of work, released on completion. Queued work holds nothing.
5. Per-branch queue in item order (the exact wording is restated, see §6.8).
6. User code never runs under `run.mu`.
7. An error is never lost.
8. No slot outlives its item, and no item sits somewhere holding nothing.

---

## 2. Baseline: relevant facts about today's runtime

- `FanOut.MoveTo(ctx, tasks, joins...)` waits for admission (the wait may release the lock), joins the listed waves
  (may release the lock), and then, in one uninterrupted lock hold, claims and groups the tasks into one collection per
  branch, enqueues them in item order, publishes the node's rank and starts work. It returns once the tasks are queued,
  not finished.
- A node is entered once per item. A second `MoveTo` to the same fan-out panics.
- The work scheduled at a fan-out is one **wave**, kept as `item.pending`. It is "the node's body". `Finished` closes
  when all its collections are exhausted and no task is running. The next `MoveTo` (`joinPending`) waits for it and
  returns its error. `Detach` hands the wave to the caller and lets the slot follow the work.
- A fan-out publishes its rank at enqueue, not at admission. That is what keeps a younger item from enqueueing onto
  the branches before an older item has. An item admitted directly (not through the waiting room) publishes nothing
  until enqueue.
- Per branch, work is pulled from the head collection only. All of an older item's work starts before any younger
  item's work on the same branch.
- A pool task receives a context marked non-movable. The marker names the pool. Every node method called with such a
  context panics. The context is built once per collection and shared by all of its callbacks.
- Streaming sources exist: `NewTasksGen`, `NewTasksChan`. A pull from them runs user code outside the lock, single
  flight per collection, with a slot reserved for the duration. While such a pull is in flight nothing else starts on
  that branch.
- Every node method judges cancellation by the **call context** only: `TryMoveTo` declines a canceled call context in
  its preamble, the blocking waits (`waitUntil`) watch the call context, and `Retain` decides from the call context
  whether to run its callback. The item's own context (`item.ctx`) is consulted only by the branch workers, which drop
  queued work of a canceled item. A context made with `context.WithoutCancel(ctx)` keeps the item handle but hides
  the item's cancellation, so today a canceled item can keep entering nodes if its processor passes such a context.
- Item completion (`completeItem`): a non-shutdown processor error poisons the item; the item waits for all its waves;
  the effective error is the processor's own error unless that is nil or a `ShutdownError`, in which case the first
  error of a wave nobody observed replaces it.
- A detached or retained slot is released by "whichever happens last": the wave finishing, or the item moving past the
  node. An undetached fan-out slot is released when the item steps into the next node **or its waiting room**.

---

## 3. Declined options

| # | Option | Why it was declined |
|---|--------|---------------------|
| A | **Use `NewTasksChan` as the producer.** A goroutine waits for results and pushes more callbacks. | The puller reserves a branch slot while the producer thinks. The whole branch stops starting new work while the head collection has a pull in flight, so younger items starve too. Result plumbing between tasks and the producer is manual, and cross-pool decisions are awkward. For a tree, a task that pushes children onto its own channel blocks on the send while holding a slot; when every slot is held by a pushing task the pool deadlocks. Nobody knows when to close the channel without a hand-written counter of outstanding work, which is exactly what the wave already keeps. |
| B | **Two or more fan-outs in a row**, one per round. | Works only for a fixed number of rounds known at build time. A pool belongs to one fan-out, so the second fan-out cannot share the first one's capacity. |
| C | **Rounds only**: `Schedule` from the ItemProcessor plus `Wait`, nothing from tasks. | Sufficient for rounds, but a tree becomes breadth-first search by levels. Each `Wait` is a barrier. A slow task at level k keeps every level k+1 task from starting even when its parent finished long ago. The pool cannot tell "thinking" from "done", so slots flow to younger items during the gap and cannot be taken back. Kept as a special case of the design, not as the mechanism. |
| D | **One wave per round**, `Schedule` returns a `Wave`, wait on a specific round. | More expressive, but `Detach` then needs a composite wave over all open rounds, and the composite must propagate acknowledgements to its parts or an already handled error fails the item at completion. "Wait for everything so far" covers the realistic cases, because the next round usually depends on all earlier results. Per-submission handles may be added later on top of this design (§10). |
| E | **Keep `MoveTo(ctx, tasks)` and add `Schedule`** (non-breaking). | Two ways to submit the first tasks that behave identically. The docs would have to explain a difference that does not exist. |
| F | **`Schedule` before `MoveTo` buffers tasks on the item**; `MoveTo` enqueues the buffer atomically (the "smart fan-out" variant). | Ordering does not come from the buffer. Entry to every node is already gated by item age: a younger item cannot enter the fan-out until the older one has published its rank, whether or not it announced tasks. A gate keyed on buffered tasks would be unsound, because intent is unknowable (the older item may schedule later or skip the node). What the buffer adds is hidden per-item per-fan-out state, a verb with two meanings (store outside, start inside), and a rule for buffers that are never submitted (skipped node, declined `TryMoveTo`). |
| G | **`Schedule` before `MoveTo` that enqueues for real.** | Work runs on the branches while the item still holds the previous node. The fan-out limit no longer bounds items with outstanding work, backpressure from the fan-out disappears, and with a shared previous stage a younger item can enqueue first and break per-branch order. |
| H | **Publish the fan-out's rank at `MoveTo`** (as today), not at the first `Schedule`. | Between `MoveTo` and `Schedule` a younger item could enter and its tasks could start on free slots. The older item's first tasks would then wait behind running work. This is the rounds gap at the very beginning of the body. |
| I | **Append later collections to the branch queue** (FIFO by submission time). | A freed slot would go to a younger item's queued work instead of the older item's follow-up. It loses the no-gap property of scheduling from inside a task. |
| J | **Let a task wait for the work it spawned** (fork-join inside a task). | The task holds a slot and waits for slots. That is hold-and-wait and deadlocks at any pool limit. Joins are expressed as continuations instead (§8.6). |
| K | **Allow `Schedule` / `Wait` from the ItemProcessor after `Detach`.** | Contrived: the item would need a second handle for a node whose slot it already handed over. Panic keeps the model "Detach ends the ItemProcessor's part of the body". |
| L | **Close `Wave.Started` together with `Finished`**, because a task may add a source after it closed. | The runtime cannot know at seal time whether a running task will spawn later, so the rule would have to apply to every fan-out wave and `Started` would become identical to `Finished`. Instead `Started` is redefined precisely over the sources the ItemProcessor scheduled (§6.7). |
| M | **Seal the body only after admission to the next node succeeds**, so a failed leave leaves the body open. | Between the idle check and admission the lock is released. A `Schedule` through the ItemProcessor path (for example from a `Retain` callback holding the item's context) could make the body busy again after it was judged idle; the item would enter the next node with a busy body, or the leave would need an idle-recheck loop. Sealing at the idle check keeps the open-check in `Schedule` sufficient. The price is a "closed" body after a failed leave (§6.4). |
| N | **Panic when work schedules into a body that has already finished.** | A context that outlives the work it belongs to is the same category as a context that outlives its item, which the package already treats as benign and answers with `ErrStaleContext`. One rule for both (§6.2). |
| O | **Keep `context.WithoutCancel` as an escape hatch for the blocking `MoveTo`**, checking the canonical context only in `Schedule` and `TryMoveTo`. | It leaves an asymmetry: for the same canceled item `TryMoveTo` answers "canceled" while `MoveTo` lets it in. Worse, it breaks the promise the conveyor exists for. If item 5 fails while writing and item 6 hides its cancellation to reach `commit`, item 6 commits cumulative Kafka offsets past item 5's messages, and those messages are lost. Code that must run after cancellation (logging, a compensating action) can still run in plain Go after `MoveTo` returns the cause; it just does not run inside a stage. Decided: every node method checks the canonical context (§6.9). |

---

## 4. Proposed design

### 4.1 Overview

- `FanOut.MoveTo(ctx, joins...)` only enters the fan-out. It has the same shape as `Stage.MoveTo`. The item enters
  with an **empty, open body**.
- `FanOut.Schedule(ctx, tasks...)` adds work to a body. It never blocks. It can be called by the ItemProcessor while
  the item is inside with an open body, by a task running on one of the fan-out's pools before it returns, and by a
  child item of one of the fan-out's lanes before its callback returns.
- `FanOut.Wait(ctx)` blocks until the body is idle: nothing queued, nothing running, including work spawned by work.
  The item keeps its slot and may schedule again.
- `FanOut.Detach(ctx)` keeps its shape. The detached work may still grow from its own tasks.
- Every item records, for each fan-out it has entered, a **body state**: `open`, `closed` or `detached`, plus the
  wave. The state drives what `Schedule`, `Wait` and `Detach` may do and gives stable diagnostics.
- The body wave is **sealed** when the item leaves, detaches, or its processor returns. A sealed wave can still grow
  from its own running work. `Finished` closes when the wave is sealed and idle.
- The fan-out's rank is published at the item's **first `Schedule`** through its own body, or when it leaves or
  detaches, whichever comes first. On admission only the waiting-room rank is published.
- Later collections are **inserted at the owning item's place** in each branch queue, ahead of younger items' queued
  work. Ordering is defined at the moment a slot is handed out, never by preemption.
- `Wave.Started` is redefined over **root sources** only: the sources the ItemProcessor scheduled into its own body.
- **Cancellation is judged by the item, not by the context you pass.** Every node method also checks the owning
  item's canonical context, so a context with cancellation stripped cannot move, schedule, wait or retain for a
  canceled item. A child context with a shorter deadline still works; it inherits both cancellations.
- The `Tasks` type is removed. `Schedule` is variadic.

### 4.2 Glossary

- **Body**: the work an item has outstanding at a fan-out, represented by one wave.
- **Body state**: per item and per fan-out: `none` (never entered), `open`, `closed`, `detached`. See §6.7.
- **Door**: the ordering gate of the fan-out node. It is closed to the next item until the current item publishes the
  node's rank.
- **Sealed** wave: no more additions through the ItemProcessor path; only the wave's own running work may add.
- **Busy / idle** wave: busy if any collection may still produce work or any task is running; idle otherwise.
- **Own-body path**: a `Schedule` whose calling context is an item acting in its own scope (the ItemProcessor, or a
  lane child at an interior fan-out of its lane).
- **Root**: a collection scheduled through the own-body path.
- **Spawn**: a collection scheduled from a running pool task, or from a lane child into the fan-out its lane belongs
  to. A spawn adds to the wave that owns the caller.
- **Round**: a `Schedule` through the own-body path after the body was already non-empty.
- **Canonical context**: the context the runtime created for the item that is charged for a body (`item.ctx`). Its
  cancellation cause is the item's true status, whatever the caller derived from it.

---

## 5. Interfaces

### 5.1 `FanOut`

```go
type FanOut interface {
    Unit

    // AddPool / AddLane: unchanged.
    AddPool(opts ...AnyUnitOption) Pool
    AddLane(opts ...AnyUnitOption) Lane

    // MoveTo advances the item into this fan-out, releasing the previous node, and joins the listed waves.
    // The item enters with an empty body; add work with Schedule. Until the item's first Schedule (or until it
    // leaves or detaches), the item behind it may step into this fan-out's waiting room but cannot enter the node.
    //
    // It returns ErrForeignContext, ErrStaleContext, or the item's cancellation cause, whether the cancellation
    // is visible on ctx or only on the item's own context. It panics on misuse: moving backward, re-entering this
    // node, a node outside the item's own series, or a wave from another item.
    MoveTo(ctx context.Context, joins ...Wave) error

    // TryMoveTo is MoveTo without waiting: it enters the fan-out only if it can do so right now, and reports
    // whether it did. entered == false means nothing happened. It bypasses the waiting room and never jumps an
    // item already waiting there. The joins are awaited only if the item entered. A canceled item returns
    // (false, its cancellation cause), whether the cancellation is visible on ctx or only on the item's own
    // context. It panics on the same misuse as MoveTo.
    TryMoveTo(ctx context.Context, joins ...Wave) (entered bool, err error)

    // Schedule adds tasks to a body of this fan-out on behalf of the calling context and returns once they are
    // queued. It never blocks. Three callers are allowed:
    //   - the ItemProcessor, while its item is inside this fan-out with an open body (not yet left, not detached);
    //   - a task running on one of this fan-out's pools, before the task returns;
    //   - a child item of one of this fan-out's lanes, before its callback returns.
    //
    // Work is queued per branch at the owning item's place: after everything that item and older items already
    // have queued there, ahead of younger items' queued work. Running work is never preempted. Several tasks for
    // one branch in one call start in argument order.
    //
    // The item's first Schedule opens this fan-out to the item behind it. Calling Schedule with no tasks is legal
    // and does only that.
    //
    // It returns ErrForeignContext; ErrStaleContext when the context belongs to a finished item, a finished child,
    // or work whose wave has finished; or the item's cancellation cause. In all these cases nothing is queued.
    // It panics on misuse: a task for another fan-out's branch, a Task submitted twice, the ItemProcessor calling
    // it outside this fan-out, after it left or tried to leave (the body is closed), or after Detach, or a task
    // calling it for a fan-out it does not run under.
    Schedule(ctx context.Context, tasks ...Task) error

    // Wait blocks until every task scheduled so far in this fan-out's body has finished, including work those
    // tasks scheduled themselves, and returns the first error, or the item's cancellation cause (visible on ctx
    // or only on the item's own context). The item keeps its slot and may Schedule again afterwards. Repeated
    // calls return the same error again.
    //
    // Only the ItemProcessor (or a child item at a fan-out of its own lane) may call it, while the body is open.
    // It panics when called with a task's context (a task must never wait for other work), outside this fan-out,
    // after the body was closed by a leave, or after Detach.
    Wait(ctx context.Context) error

    // Detach hands this fan-out's slot to the work already scheduled here, letting the item move on without
    // waiting for it. Join the returned Wave in a later MoveTo, or read its channels. The detached work may still
    // grow from its own running tasks; the wave finishes when all of it is done. After Detach the ItemProcessor
    // may not Schedule or Wait here again.
    //
    // It panics on misuse: a node the item does not currently occupy, or nothing to detach (never entered here,
    // already detached, or the body was already closed by a leave).
    Detach(ctx context.Context) Wave

    // SetLimit sets how many items may be inside this fan-out at once (default 1; a limit <= 0 means 1). An item
    // counts from the moment it enters until it steps into the next node or that node's waiting room. After
    // Detach it counts until both the detached work is done and the item has moved on, whichever is later. The
    // ordering gate is separate from the limit: the item behind cannot enter before the item ahead has scheduled,
    // left, or detached, whatever the limit.
    SetLimit(limit int) FanOut

    // SetQueueSize, Limit, QueueSize, Branches: unchanged.
    SetQueueSize(size int) FanOut
    Limit() int
    QueueSize() int
    Branches() []Branch
}
```

### 5.2 `Task`, `Tasks`, `Branch`

- `Task` is unchanged: a single-use bundle of work for one branch, produced by `NewTask`, `NewTasks`,
  `NewTasksGen`, `NewTasksChan`.
- `Tasks` and `Tasks.Add` are **removed**. Build a `[]Task` and pass it with `...`.
- `Branch`, `Pool`, `Lane` constructors are unchanged. Their godoc on ordering changes to the wording in §6.8, and
  the streaming constructors' note about `Started` changes to the wording in §6.7.

### 5.3 `TaskFunc` and the pool-work context

- Unchanged type. Its doc changes: on a Pool the context can be used with `Schedule` of the pool's fan-out, and with
  nothing else. `MoveTo`, `TryMoveTo`, `Wait`, `Retain`, `Detach` with it still panic.
- The pool-work context must let the runtime find the collection (and so the wave and the owning item) the work
  belongs to. Today it only names the pool. It stays one shared context per collection, so the runtime cannot tell
  individual callbacks of one collection apart; it can only tell whether the wave has finished.

### 5.4 `Wave`

Unchanged interface. Doc changes:

- `Started`: closes once the wave is sealed and every **root source** (a source the ItemProcessor scheduled into its
  own body) has been handed out. Sources added by tasks or lane children do not count and do not delay it. The
  guarantee is only about state read by root sources; spawned sources and running callbacks may read the same state
  and are not covered.
- `Finished`: closes when the wave is sealed and idle.

### 5.5 Sentinels

New unexported panic sentinels (same policy as today: misuse panics, runtime conditions return):

- `errBodyClosed`: the ItemProcessor called `Schedule` or `Wait` at a fan-out whose body it already closed by
  leaving, trying to leave, or returning.
- `errWorkDetached`: the ItemProcessor called `Schedule` or `Wait` at a fan-out where it detached its work.
- A wrong-target message under the existing `errInvalidUnit`: work running under fan-out X called `Schedule` on
  fan-out Y.

Returned: `ErrStaleContext` now also covers a finished lane child's context and a pool task's context whose wave has
finished.

Existing sentinels keep their roles: `errStageNotEntered` (Schedule/Wait/Detach on a fan-out the item does not
occupy), `errNothingToDetach`, `errTaskReused`, `errCannotMove` (Wait or a move with a task's context),
`errWrongScope`, `errWrongEnterOrder`, `errNodeAlreadyEntered`, `errForeignWave`, `errNilTaskFunc`.

---

## 6. Behavior specification

### 6.1 Entering

`MoveTo(ctx, joins...)`:

1. Resolve the acting item. A task's context panics (`errCannotMove`). A foreign or stale context returns
   `ErrForeignContext` / `ErrStaleContext`.
2. `checkEnterOrder`: backward move or re-entry panics.
3. If the item is inside another fan-out with an open body, leaving it first follows §6.4.
4. Wait for admission: a free slot and the ordering gate, or room in the waiting room (unchanged). The wait may
   release the lock. It ends early with the cancellation cause if the call context or the item's canonical context
   is canceled; the wait must wake on either.
5. On admission to the node, publish the **waiting-room rank** (`rank - 1`) if the item's `maxRank` is lower. Do not
   publish the node's rank. Consequence: the item behind may step into the waiting room and release its previous
   node, but cannot enter the node.
6. Create the body: a new wave, open, idle, `atNode` set to this fan-out. Record body state `open` for this fan-out.
7. Join the listed waves (unchanged; may release the lock). A join error is returned as today ("join at <node>: ...").
8. Return nil. The item is inside with an empty open body.

`TryMoveTo(ctx, joins...)`: the same, without waiting and bypassing the waiting room. Its preamble declines a
canceled item (§6.4 for the exact rule). On `entered == false` nothing happened. On success the body is created as
in step 6.

The `maxRank` invariant holds: an item admitted directly publishes `rank - 1`; an item in the waiting room behind it
also published `rank - 1`. Equal is allowed.

### 6.2 Schedule

`Schedule(ctx, tasks...)` performs these steps in this order:

1. **Static task validation**, before touching the context: every task's branch must belong to this fan-out
   (`errInvalidUnit`). Statically empty tasks (`NewTasks` with count <= 0, nil generator, nil channel) are skipped.
2. **Validate the fan-out handle** against this conveyor (`errInvalidUnit`), as every node method does.
3. **Resolve the caller** from the context:
   - a pool-work marker: the caller is that work; its collection gives the wave and the owning item;
   - otherwise an item handle: the caller is that item;
   - otherwise return `ErrForeignContext`.
4. **Validate the conveyor**: the owning item must belong to this conveyor (`errInvalidUnit`), before any array is
   indexed by a unit of this conveyor.
5. **Lock.**
6. **Stale checks** (return `ErrStaleContext`): the caller item is finished; or, for pool work, its wave is finished.
   For a lane child this check runs **before** any redirect to the parent's wave, so a child's context used after the
   child's callback returned is refused even if the parent wave is still busy. The runtime detects an expired wave,
   not an expired individual callback: a pool goroutine that outlives its task while sibling work keeps the wave
   busy is not detected (see the contract below).
7. **Resolve the body** and classify the addition:
   - pool work: the body is its wave. Its node must be this fan-out (`errInvalidUnit`, wrong target). The addition is
     a **spawn**.
   - an item whose `parentWave` exists and whose `parentWave.atNode` is this fan-out: the body is `parentWave` (a
     lane child scheduling into the fan-out its lane belongs to). The addition is a **spawn**. This check runs before
     the scope check, because the fan-out lives in the parent scope.
   - otherwise the item acts in its own scope: check the scope (`errWrongScope`). Look up the item's body state at
     this fan-out: `none` panics `errStageNotEntered`; `closed` panics `errBodyClosed`; `detached` panics
     `errWorkDetached`; `open` continues. The addition is a **root**.
8. **Cancellation**: if the cancellation cause of the **owning item's canonical context** or of the **call context**
   is set, return it. Nothing is claimed or queued. `Schedule` never blocks, so it must make this check itself. The
   canonical check is what stops a caller that stripped cancellation (`context.WithoutCancel`) from queueing work for
   a poisoned item, which the workers would only drop later.
9. **Claim** every task's source. A source already claimed panics (`errTaskReused`). This happens before the body is
   touched, so a panic leaves the wave consistent.
10. **Group** into one collection per branch, in argument order; several tasks for one branch become one collection
    consumed front to back. Each collection records whether it is a root or a spawn.
11. **Insert** each collection into its branch queue at the owning item's place (§6.8). Register it on the body:
    total unexhausted count, and the root count if it is a root. Count it in the branch's `Queued` gauge.
12. **Publish** this fan-out's rank on the owning item if its `maxRank` is lower. Idempotent. For a spawn this is
    normally a no-op, because the item already published at its first root.
13. **Pump** every touched branch and broadcast.
14. Return nil.

Properties:

- Steps 5 to 13 run under `run.mu` with no user code. `Schedule` is safe from any goroutine, and several tasks may call
  it concurrently with the ItemProcessor. Concurrent spawns of the same item are ordered by lock acquisition; no order
  is promised between siblings that spawn concurrently.
- Zero tasks after step 1 is legal. The effect is step 12 only: the door opens.
- A spawn happens while the spawner is still running, so the body cannot be idle at that moment. Idle can therefore
  never be observed with work still to come, except by a goroutine that violates the contract below.
- **Callback lifetime contract**: a pool task or lane child must call `Schedule` before it returns. A goroutine that
  outlives its callback gets `ErrStaleContext` once the wave (for pool work) or the child (for a lane child) has
  finished; while sibling work keeps the wave busy, its addition is accepted and cannot be told apart from a legal one.

### 6.3 Wait

`Wait(ctx)`:

1. Resolve the acting item. A task's context panics (`errCannotMove`): a running task holds a slot and must not wait.
   A lane child calling `Wait` on the fan-out its lane belongs to panics with `errWrongScope`, for the same reason.
2. The item's body state at this fan-out must be `open`: `none` panics `errStageNotEntered`, `closed` panics
   `errBodyClosed`, `detached` panics `errWorkDetached`.
3. Block until the body is idle, or the call context or the item's canonical context is canceled (`waitUntil`). The
   item keeps its fan-out slot.
4. Cancellation versus work error: a failing task poisons the item, so `waitUntil` answers with the cancellation
   first. After a cancellation wake-up, re-check idle. If the body is idle **and has an error**, report the body's
   error (step 5): the node's work failed, and that is the truer message. Otherwise return the cancellation cause,
   whether the body is idle and clean or still busy. `Wait` never returns nil for a canceled item.
5. On idle without cancellation: if the body has an error, mark it observed and return it wrapped with the node name
   ("<node> work: ..."). Otherwise return nil.
6. The body is **not** sealed and its state stays `open`, whatever `Wait` returned. The item may `Schedule` again; on
   a canceled item that returns the cause.

Properties:

- `Wait` on an empty body returns nil at once.
- `Wait` returns only when the whole tree scheduled so far is done, because a spawn can only happen while a task is
  running.
- A streaming source (`NewTasksGen`, `NewTasksChan`) must end before `Wait` can return. An unclosed channel blocks
  `Wait` forever, exactly as it blocks the next `MoveTo` today.
- `Wait` does not open the door.

### 6.4 Leaving to a later node

`Stage.MoveTo` / `FanOut.MoveTo` from inside a fan-out with an `open` body (`joinPending`):

1. Wait until the body is idle. On a cancellation wake-up: a busy body returns the cause and stays `open` (item
   completion accounts for it); an idle body with an error is sealed and closed (step 2) and its error is returned
   (step 3); an idle clean body is sealed and closed and the cancellation cause is returned. A leave never returns
   nil for a canceled item.
2. Seal the body and set the body state to `closed`. Because the wave is idle, `Started` and `Finished` close now.
   The outcome is marked observed. This happens **before** the admission wait for the target.
3. If the body has an error, return it wrapped with the fan-out's name, without entering the target. The body is
   `closed`.
4. Otherwise continue with admission to the target. The admission wait may release the lock and may fail when either
   context is canceled: a derived deadline on the call context, or poison or shutdown on the item's own context.
   **If it fails**, `MoveTo` returns the cause, the body stays `closed`, and the item is in one of two places:
   - it had not stepped into the target's waiting room: it is still inside the fan-out, holding its slot;
   - it had stepped into the waiting room (`takeQueue` already released the fan-out slot): it stands in the
     target's waiting room, holding a queued slot, and the fan-out is free.
   In both places a later `MoveTo` or `TryMoveTo` to a later node is legal and has nothing left to join. A repeated
   `MoveTo` to the same target from the waiting room **resumes waiting for the node and takes no second queued
   slot** (the guard is `queuedAt == target`); today's `enterUnit` has no such guard, and a retry can double-count
   the item in the waiting room, a latent bug this change fixes. `Schedule`, `Wait` and `Detach` at the fan-out
   panic (`errBodyClosed`, `errBodyClosed`, `errNothingToDetach`).
5. On admission the item publishes a rank above this fan-out's: at least the target's waiting-room rank, or the
   target's own rank if it is a stage entered directly. Either opens this fan-out's door if the first `Schedule` had
   not. The fan-out slot is released by the usual rank rule when the item steps into the target or its waiting room.

`Stage.TryMoveTo` / `FanOut.TryMoveTo` from inside a fan-out (`tryEnterUnit`), **changed rules**:

1. **Preamble (changed)**: the call declines with `(false, cause)` if the cancellation cause of the **call context**
   or of the **item's canonical context** is set. Today only the call context is checked, so a caller that passed
   `context.WithoutCancel(ctx)` could admit a poisoned item. Because every body error poisons the item, this rule also
   means a body with an error is never examined here: the node-qualified body error is produced only by `Wait` and by
   the blocking `MoveTo`.
2. If the body is busy: return `(false, nil)`. Nothing is touched. The body stays `open`.
3. If the body is idle but the target has no room or the ordering gate is closed: return `(false, nil)`. Nothing is
   touched. The body stays `open`. The item may keep scheduling.
4. Otherwise: seal the body, set its state to `closed`, take the target, publish, join. Return `(true, nil)` or a
   join error.

Today `tryEnterUnit` consumes the pending wave before it knows whether the item can enter. With a growable body that
would leave the item inside with no body to add to, so the order of checks changes as above. Admission stays atomic:
the idle check, the admission check and the occupancy mutation happen in one lock hold.

### 6.5 Detach

1. The item's body state at this fan-out must be `open` (`errStageNotEntered` if the item does not occupy the node;
   `errNothingToDetach` if the state is `closed` or `detached`).
2. Set the wave's `retainUnit` to this node. Seal the wave. Publish this fan-out's rank on the item (the door opens).
   Set the body state to `detached`.
3. If the wave is already idle it finishes now. Its slot is released by "whichever happens last": the next move of
   the item, or the wave's finish if the item has already moved on (unchanged).
4. Return the wave.

After `Detach`:

- The ItemProcessor may not `Schedule` or `Wait` at this fan-out (`errWorkDetached`). The diagnostic is stable
  whatever the wave's progress, because it comes from the recorded body state, not from occupancy.
- Tasks and lane children of the detached wave may still `Schedule` into it while it is not finished. The slot
  follows the whole tree. `Finished` closes when the tree is done.
- `Detach` twice panics (`errNothingToDetach`), as today.
- `Detach` with a canceled context returns the item's own wave, not an error, as today.

### 6.6 Item completion while inside a fan-out

When an item's processor returns (root item's ItemProcessor, or a lane child's callback) while its body at some
fan-out is `open`:

1. Poison the item if the processor returned a real error (not a `ShutdownError`), unchanged.
2. Seal the open body and set its state to `closed`. Without this the item would wait forever for a wave that never
   finishes. From here the ItemProcessor path is refused (`errBodyClosed`), which covers a `Retain` callback that
   holds the item's context and tries to schedule after the processor returned.
3. Wait for all the item's waves to finish (unchanged). A sealed body still grows from its running tasks until the
   tree is exhausted.
4. Compute the effective error as today: the processor's own error, unless it is nil or a `ShutdownError`, in which
   case the first error of a wave nobody observed replaces it.
5. Release slots, cancel the context, report to the parent wave for a child (unchanged).

A poisoned item's running tasks see cancellation, its queued work is dropped at the head, and new `Schedule` calls
return the cause. A failing tree therefore terminates.

### 6.7 Body state and wave lifecycle

Body state, recorded per item and per fan-out:

| state | meaning | entered by | `Schedule` (own-body path) | `Wait` | `Detach` |
|-------|---------|------------|----------------------------|--------|----------|
| `none` | never entered | initial | panic `errStageNotEntered` | panic `errStageNotEntered` | panic `errStageNotEntered` |
| `open` | inside, body may grow | `MoveTo` / `TryMoveTo` admission (§6.1) | allowed | allowed | allowed |
| `closed` | body joined or processor returned | leave idle check (§6.4), `TryMoveTo` admission (§6.4), completion (§6.6) | panic `errBodyClosed` | panic `errBodyClosed` | panic `errNothingToDetach` |
| `detached` | body handed to the caller | `Detach` (§6.5) | panic `errWorkDetached` | panic `errWorkDetached` | panic `errNothingToDetach` |

`closed` and `detached` are terminal for the ItemProcessor. The states are independent of occupancy: an item may
still occupy the node (a detached wave in flight, or a failed leave) or may have moved on.

Wave dimensions:

| dimension | values | transitions |
|-----------|--------|-------------|
| sealed | no: the own-body path may add. yes: only the wave's own running work may add. | Born unsealed by `MoveTo`. Sealed by the leave idle check, `TryMoveTo` admission, `Detach`, or processor return. Retain waves and finished/standalone waves are born sealed. |
| busy / idle | busy: any collection may still produce work, or any task is running. | Unsealed: idle to busy by a root `Schedule`; busy to idle when the last collection is exhausted and the last task finished. Sealed: a spawn happens only while busy, so a sealed wave goes busy to busy or busy to idle. Sealed and idle is terminal. |
| finished | `Finished` closed. | Sealed and idle, decided under the lock in the same step that made it idle or sealed. |

Channel semantics:

- While the wave is **unsealed**, no channel closes. The wave is not observable while unsealed, because `MoveTo`
  returns no handle and `Detach` seals.
- Once **sealed**: `Started` closes when the **root** unexhausted count is zero; `Finished` closes when the wave is
  idle. Sealing by a leave happens when the wave is idle, so both close at once; sealing by `Detach` lets them close
  later, in that order.
- **`Started` definition**: closes once the wave is sealed and every root source has been handed out. A streaming root
  counts until its source is exhausted, not until its first pull. Spawned sources do not count and do not delay it.
  What it guarantees: state read only by root sources may be mutated after `Started`. What it does not guarantee:
  anything about spawned sources or running callbacks, which may capture the same state. Rationale: `Started` exists
  so the ItemProcessor knows when its own streaming sources are done reading; a spawned source is created by a task
  that is still running at that moment and owns the decision.
- `Err` is unchanged: reading it after `Finished` marks the outcome observed.

Growth rules:

- Adding to an **unsealed** wave is allowed through the own-body path (a root) and from the wave's own work (a spawn).
- Adding to a **sealed, unfinished** wave is allowed only from the wave's own running work (a spawn).
- Adding to a **finished** wave is refused with `ErrStaleContext` (the work the context belongs to is over).

Observation and acknowledgement:

- `Wait` acknowledges an error it returns.
- Leaving acknowledges (the idle check in §6.4 step 2).
- A join of a detached wave acknowledges, as does `Err` after `Finished`.
- An unacknowledged error fails the item at completion (unchanged).

### 6.8 Ordering guarantees

Per-branch queue rules:

1. **Across items**: a collection is inserted before the first queued collection that belongs to a younger item, and
   after every queued collection of the same item or an older one. Age is a creation sequence per scope that works for
   root items and lane children alike; `item.no` is not usable, because a lane's children share their parent's number.
2. **Within one item on one branch**: collections in `Schedule` order (lock order for concurrent spawns); within one
   `Schedule` call, tasks for the same branch in argument order; within one task, index or yield order.
3. **Slot assignment**: a free slot is filled from the head of the queue, the oldest queued work. A slot **reserved
   for a streaming pull** counts as handed out at reservation time: the callback the pull later yields runs on that
   slot even if older work was queued while the pull was in flight. Running work is never preempted.
4. **Lanes**: a child takes its ticket when pulled and holds the lane's entrance only until its first move. Interior
   stages admit children by ticket. Ordering inside a lane is therefore by child creation order, which follows the
   queue order above, not by parent item age at the time a parent task spawns.
5. **Pools** remain first-come first-served among running tasks (unchanged).

User-facing statement (replaces "everything an older item scheduled here starts before anything a younger item
scheduled"):

> On a branch, a free slot is never given to a younger item's work while an older item has work queued there. Work
> that has started, or a slot already reserved for a streaming pull, is never taken back.

Consequences:

- **Single `Schedule` right after entering** (the old pattern): the door does not open until the older item's first
  `Schedule`, so the younger item cannot even queue before that. The old guarantee holds exactly.
- **Spawning onto the same pool from a running task**: the follow-up is queued while the spawner still holds its slot.
  When the task returns, the freed slot goes to the head of the queue, which is the spawner's item or an older one. On
  that pool there is no window in which the item had nothing queued and nothing running while still having work to
  do. This is the **no-gap property**, and it holds only for the pool the spawner runs on while it holds the slot.
- **Spawning onto a different branch**: the follow-up waits for a slot to free there, then gets it by priority. A
  younger item's work may already be running on that branch.
- **Spawning from a lane child** after its first move: the child no longer holds the entrance slot, so a younger
  item's child may be created meanwhile. The no-gap property does not apply.
- **Rounds through the own-body path** after the door opened: younger items' tasks may be running. The round's tasks
  are queued ahead of younger items' queued work and take slots as they free, one finishing younger task per slot.
- **Pool limit 1**: an uninterrupted chain that spawns onto the same pool from the running task keeps strict item
  order on that pool. Rounds, cross-pool spawns and lane children interleave items.

Queue mechanics required by rule 1:

- An inserted older collection may displace a collection whose asynchronous pull is in flight from index 0. That
  collection keeps its single-flight status and its reserved slot. Pulls continue head-only from the new head.
- Removing a collection from the queue, when it is exhausted or dropped, is **by identity**, not "remove the head".
  Each collection leaves the queue exactly once, with exactly one `Queued` decrement and exactly one
  source-exhaustion notification to its wave, whether it drained or was dropped.

### 6.9 Cancellation and errors

- A task error is recorded on its wave and poisons the item (fail-fast), unchanged.
- Queued work of a poisoned item is dropped when it reaches the head, with the cancellation cause recorded on the wave
  (`recordAbandoned`), unchanged. Async pulls treat cancellation as exhaustion, unchanged.
- **Cancellation is judged by both contexts everywhere.** Every node method reads the cancellation cause of the call
  context and of the owning item's canonical context, and answers with whichever is set: the `TryMoveTo` preamble,
  the admission waits of `MoveTo` and `TryMoveTo`, `Wait`, `Schedule`, and `Retain`'s decision not to run its
  callback. A context made with `context.WithoutCancel` therefore cannot move, schedule, wait or retain for a canceled
  item. A child context with a shorter deadline keeps working, because it inherits the item's cancellation and adds
  its own. `Detach` is unchanged: it hands back the item's own wave whatever the cancellation state.
- `Schedule` on a canceled item returns the cause and queues nothing (§6.2 step 8). Together with the previous points
  this guarantees a failing tree terminates: running tasks finish, nothing new is accepted, queued work is dropped.
- `Wait` and the blocking leave report a body error with the node name; a cancellation from elsewhere while the body
  is busy is reported as the context's cause and the body stays open (§6.3 step 4).
- A lane child's error escalates to its parent item, unchanged.
- Shutdown behavior is unchanged: the shutdown context bounds in-flight items; items blocked in `Wait` or `MoveTo`
  return promptly; tasks that ignore their context are not interrupted.

### 6.10 Deadlock freedom

The argument is made per kind of slot holder, with its assumptions stated.

- **Pool callbacks** hold one slot each and release it on return. The runtime never makes them wait: `Schedule` is a
  non-blocking enqueue, and `Wait` with a task's context panics. Assumption: the callback terminates, which is the
  same assumption the library makes today (it respects its context).
- **Streaming pulls** hold one reserved slot while user code runs (a generator step or a channel receive). The slot
  becomes the work's slot when the pull yields, or is given back when the source is exhausted or the item is canceled.
  Assumption, unchanged from today: the producer terminates or respects the item's context. A producer that blocks
  forever holds one slot forever; the design neither adds nor removes that hazard.
- **Lane children** move forward only through the lane's nodes and are admitted by ticket, the same argument as for the
  root series. A child never waits on its parent or on its siblings except through ordered admission. A child's
  completion waits for the child's own waves, which live in interior fan-outs of the same lane and follow this same
  argument recursively.
- **Queued work** holds nothing.
- **The item** holds its fan-out slot and waits, in `Wait` or when leaving, for the body to drain. The body drains
  through the holders above.
- **A younger item at the door** waits for the older item's first `Schedule`, leave, or `Detach`. The older item never
  waits on the younger one.
- **Ordered insertion** moves queue positions only; it creates no waits.
- **Growth** adds no new kind of wait: spawns are enqueues from a running holder, and a task that wants the result of
  spawned work must express the join as a continuation (§8.6), never as a wait.

### 6.11 Panic versus return

| situation | outcome |
|-----------|---------|
| `Schedule` / `Wait` / `Detach` at a fan-out the item never entered or does not occupy | panic `errStageNotEntered` |
| `Schedule` / `Wait` through the own-body path at a fan-out whose body is `closed` | panic `errBodyClosed` |
| `Schedule` / `Wait` through the own-body path at a fan-out whose body is `detached` | panic `errWorkDetached` |
| `Detach` at a fan-out whose body is `closed` or `detached` | panic `errNothingToDetach` |
| `Schedule` from a task for a fan-out it does not run under | panic `errInvalidUnit` (wrong target) |
| `Wait`, `MoveTo`, `TryMoveTo`, `Retain`, `Detach` with a task's context | panic `errCannotMove` |
| Lane child calls `Wait` on the fan-out its lane belongs to | panic `errWrongScope` |
| task for another fan-out's branch | panic `errInvalidUnit` |
| `Task` submitted twice | panic `errTaskReused` |
| backward move, re-entry, foreign wave, nil callback in eager constructors, topology change while running | unchanged panics |
| foreign context | `ErrForeignContext` |
| finished item's context; finished lane child's context; pool work whose wave has finished | `ErrStaleContext` |
| canceled item (call context or canonical context) | the cancellation cause (`ShutdownError` on shutdown) |
| body error observed by `Wait` or when leaving | "<node> work: <err>" |
| nil callback from a streaming source | fails the item (unchanged) |

### 6.12 Observability and dynamic limits

- `Stats` fields are unchanged. The fan-out's occupancy counts items inside, including empty-body visits and items that
  failed to leave. A branch's `Queued` counts collections not fully handed out, whatever call queued them. `InFlight`
  is unchanged.
- `SetLimit` and `SetQueueSize` stay admission-only and non-preemptive. A lowered pool limit stops handing out slots;
  a growing tree keeps its running tasks and drains through completions.

---

## 7. Edge cases

1. **Enter, schedule nothing, leave.** Legal. Empty body, sealed and finished at the leave's idle check. The door
   opens when the item steps into the next node or its waiting room. During the visit the fan-out behaved as if its
   limit were one.
2. **Enter, schedule nothing, `Detach`.** Returns a finished wave. The door opens at `Detach`.
3. **Enter, schedule nothing, processor returns.** The body is sealed and finished; the item completes normally.
4. **`Wait` on an empty body.** Returns nil at once. Does not open the door.
5. **`Schedule` with zero tasks, or only statically empty tasks.** Legal. Opens the door, nothing else.
6. **`Schedule` before `MoveTo`.** Panic `errStageNotEntered`.
7. **`Schedule` at a fan-out the item left normally.** Body state `closed`: panic `errBodyClosed`, whether or not the
   item still occupies the node.
8. **`Schedule` at a fan-out the item detached.** Body state `detached`: panic `errWorkDetached`, whether the detached
   wave is still running or already finished.
9. **Blocking leave fails after the body was joined** (a derived call context expires while waiting for the next
   node). `MoveTo` returns the cause and the body is `closed`. If the item had not reached the target's waiting room
   it is still inside the fan-out, holding its slot; if it had, it stands in that waiting room and the fan-out slot is
   already free. A later `MoveTo` or `TryMoveTo` to a later node is legal, and a retry from the waiting room takes no
   second queued slot; `Schedule` and `Wait` here panic `errBodyClosed`; `Detach` panics `errNothingToDetach`.
10. **Pool task calls `Schedule` after it returned** (a leaked goroutine). Wave finished: `ErrStaleContext`. Wave still
    busy with sibling work: accepted and undetectable; documented as a violation of the callback lifetime contract.
11. **Lane child calls `Schedule` after its callback returned.** The child is finished: `ErrStaleContext`, even if the
    parent wave is still busy. The child's own lifetime is checked before any redirect to the parent's wave.
12. **Task on fan-out X calls `Schedule` on fan-out Y.** Panic `errInvalidUnit`.
13. **Lane child calls `Schedule` on the fan-out its lane belongs to.** Allowed; the work joins the parent's body as a
    spawn. A lane child calling `Schedule` on an interior fan-out of its lane follows the own-body path in its own
    scope and creates roots there.
14. **Lane child calls `Wait` on the fan-out its lane belongs to.** Panic `errWrongScope`.
15. **Pool task calls `Wait`.** Panic `errCannotMove`.
16. **`Schedule` from a canceled item.** Returns the cause; nothing is claimed or queued. Also when the caller passed a
    context with cancellation stripped: the canonical context is checked too.
17. **A task fails.** `Wait` returns "<node> work: <err>"; `Schedule` returns the cause; the blocking leave returns the
    error; `TryMoveTo` returns `(false, cause)` from its preamble.
18. **`Wait` returned an error, the ItemProcessor calls `Wait` again.** Same error again.
19. **Shutdown while `Wait` is blocked and the body is busy.** `Wait` returns the shutdown cause; the body stays open;
    item completion seals it and waits for running tasks; queued work is dropped.
20. **`TryMoveTo` to the next node while the body is busy.** `(false, nil)`, body untouched, scheduling still
    allowed.
21. **`TryMoveTo` while the body is idle and clean, target has no room.** `(false, nil)`, body untouched and still
    open.
22. **`TryMoveTo` while the body has an error.** The item is poisoned, so the preamble returns `(false, cause)`. The
    body is untouched; `Wait` or `MoveTo` afterwards return the node-qualified error.
23. **Any node method with `context.WithoutCancel(ctx)` on a canceled item.** `MoveTo`, `TryMoveTo`, `Wait` and
    `Schedule` return the cancellation cause; `Retain` returns a finished wave carrying it and does not run the
    callback. Today `MoveTo` and `Retain` would proceed. Rationale: if item 5 fails while writing and item 6 hides its
    cancellation to reach `commit`, item 6 would commit cumulative offsets past item 5's messages.
24. **`TryMoveTo` while the body is idle and clean, target has room.** Seals, sets `closed`, enters.
25. **Rounds.** The door opens at the first root `Schedule`. Younger items' tasks may be running when a later round is
    scheduled. The round is queued ahead of their queued work and takes slots as they free.
26. **Ordered insertion in front of a collection with an async pull in flight.** Allowed. The displaced collection
    keeps single-flight and its reserved slot; its callback, when the pull yields, runs on that slot; pulls continue
    from the new head; removal is by identity.
27. **Two items spawn concurrently on the same branch.** The older item's spawned work is always ahead of the younger
    item's queued work.
28. **Long empty visit with `SetLimit(2)`.** The younger item waits at the door (or in the waiting room) until the
    older item schedules or leaves. Documented cost.
29. **Fan-out with a waiting room.** The younger item may enter the waiting room as soon as the older item is
    admitted, because the waiting-room rank is published on admission. It enters the node only after the older item's
    first root `Schedule`.
30. **Item skips the fan-out entirely.** The younger item enters when the older one publishes a later node's rank.
    Unchanged.
31. **Pool limit lowered while a tree grows.** Admission-only; running tasks keep their slots. Unchanged.
32. **Detached tree keeps spawning.** The slot follows the tree; `Finished` closes when idle; the item joins it later
    or an unobserved error fails the item at completion.
33. **Spawn after the owning item finished.** Impossible: an item completes only after all its waves finished, and a
    finished wave refuses additions with `ErrStaleContext`.
34. **Streaming source plus `Wait`.** The generator must end or the channel must be closed before `Wait` returns.
    Same hazard as leaving today.
35. **`Started` on a detached wave whose tasks spawn streaming sources.** `Started` closes when the root sources are
    handed out; the spawned sources are not covered. Documented in §6.7.
36. **Concurrent `Schedule` calls from tasks and from the ItemProcessor.** Serialized by the lock; sibling order is
    lock order.
37. **One `Schedule` with tasks for several branches.** One collection per branch, each inserted at the item's place
    in its own queue.
38. **`Schedule` from a `Retain` callback using the captured item context.** Treated as the own-body path. Allowed
    while the body is `open`; after the ItemProcessor left, detached, or returned it panics (`errBodyClosed` or
    `errWorkDetached`). Not a documented pattern.
39. **Interior fan-out of a lane.** Child items follow all the rules above within the lane's scope.
40. **Spawn with zero tasks.** No-op; the door is already open.
41. **`Wait` while spawns are in flight.** Returns only when the whole tree is done.
42. **Processor returns a real error while the tree still spawns.** Poison, `Schedule` returns the cause, queued work
    dropped, running tasks finish. Terminates.
43. **Processor returns nil while the body is busy.** Sealed and `closed` in completion; the tree finishes; an
    unobserved task error becomes the item's error.
44. **Processor returns a `ShutdownError` while a wave has an unobserved real error.** The wave error becomes the
    item's effective error, as today.
45. **`Wait` with a stripped context on a canceled item whose body is idle and clean.** Returns the cancellation
    cause, never nil. With an idle body that has an error, the body error wins.
46. **Item canceled while a stripped-context call is blocked.** `MoveTo` waiting for admission, a join of a listed
    wave, or a busy `Wait` called with `context.WithoutCancel(ctx)`; the item's own context is then canceled. The
    call wakes and returns the cause.

---

## 8. Usage patterns

### 8.1 One batch of tasks (replaces today's basic example)

```go
if err := dbsWrite.MoveTo(ctx); err != nil {
    return err
}
if err := dbsWrite.Schedule(ctx, db1Write.NewTask(writeDb1), db2Write.NewTasks(2, writeDb2)); err != nil {
    return err
}
// runs alongside the tasks; leaving joins them
if err := commit.MoveTo(ctx); err != nil {
    return err
}
```

### 8.2 Conditional body

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

### 8.3 Rounds

```go
if err := dbsWrite.MoveTo(ctx); err != nil {
    return err
}
if err := dbsWrite.Schedule(ctx, db1.NewTask(writeIndex)); err != nil {
    return err
}
if err := dbsWrite.Wait(ctx); err != nil {
    return err
}
for _, rows := range planNextRound(batch) {
    if err := dbsWrite.Schedule(ctx, db2.NewTasks(len(rows), writeRows(rows))); err != nil {
        return err
    }
    if err := dbsWrite.Wait(ctx); err != nil {
        return err
    }
}
return commit.MoveTo(ctx)
```

### 8.4 Tree: a task spawns follow-ups

```go
crawl := c.AddFanOut("crawl")
fetch := c.AddPool("fetch", 10)

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
if err := crawl.Schedule(ctx, oltp.NewTask(func(ctx context.Context) error { return visit(ctx, seed) })); err != nil {
    return err
}

crawl.Wait(ctx)

// leaving waits for the whole tree
if err := commit.MoveTo(ctx); err != nil {
    return err
}
```

### 8.5 Several roots, several pools

```go
var visit func(ctx context.Context, url string, depth int) error
visit = func(ctx context.Context, url string, depth int) error {
    page, links, err := fetchPage(ctx, url)
    if err != nil {
        return err
    }
    next := []conveyor.Task{
        store.NewTask(func(ctx context.Context) error { return savePage(ctx, page) }),
    }
    if depth < maxDepth {
        next = append(next, fetch.NewTasks(len(links), func(ctx context.Context, i int) error {
            return visit(ctx, links[i], depth+1)
        }))
    }
    return crawl.Schedule(ctx, next...)
}

if err := crawl.MoveTo(ctx); err != nil {
    return err
}
// one root per message, in batch order
if err := crawl.Schedule(ctx, fetch.NewTasks(len(batch), func(ctx context.Context, i int) error {
    return visit(ctx, batch[i].SeedURL, 0)
})); err != nil {
    return err
}
if err := commit.MoveTo(ctx); err != nil {
    return err
}
return ack(batch)
```

### 8.6 Join as a continuation

A task must not wait for the work it spawned. The join step is itself a task, scheduled by the last sibling to finish.

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

### 8.7 Pagination as a chain

```go
var page func(ctx context.Context, token string) error
page = func(ctx context.Context, token string) error {
    next, err := fetchPage(ctx, token)
    if err != nil || next == "" {
        return err
    }
    return f.Schedule(ctx, fetch.NewTask(func(ctx context.Context) error { return page(ctx, next) }))
}
```

### 8.8 Detach a growing tree

```go
if err := crawl.MoveTo(ctx); err != nil {
    return err
}
if err := crawl.Schedule(ctx, roots...); err != nil {
    return err
}
wave := crawl.Detach(ctx) // the tree keeps growing in the background; the slot follows it

if err := report.MoveTo(ctx); err != nil {
    return err
}
// ...
if err := commit.MoveTo(ctx, wave); err != nil { // joins the whole tree
    return err
}
```

### 8.9 Optional fan-out with `TryMoveTo`

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
// not entered: nothing was touched, no tasks were built for nothing
```

---

## 9. Breaking changes and affected surfaces

API:

- `FanOut.MoveTo(ctx, tasks, joins...)` becomes `MoveTo(ctx, joins...)`.
- `FanOut.TryMoveTo(ctx, tasks, joins...)` becomes `TryMoveTo(ctx, joins...)`.
- `Tasks` and `Tasks.Add` are removed.
- New: `FanOut.Schedule(ctx, tasks...)`, `FanOut.Wait(ctx)`.
- A pool task's context is accepted by `Schedule` of its own fan-out. Every other node method still panics with it.
- `ErrStaleContext` also covers a finished lane child's context and pool work whose wave has finished.

Behavior:

- The door opens at the item's first root `Schedule`, leave, or `Detach`, instead of at `MoveTo`. An item that enters
  and schedules nothing holds the next item at the door until it leaves. User code now runs between admission and the
  first submission, with the door closed during that interval.
- The waiting-room rank is published on admission to a fan-out, so the next item can wait in the queue meanwhile.
- The per-branch ordering guarantee is restated (§6.8). It is unchanged for code that schedules once right after
  entering.
- Every node method on both node kinds (`MoveTo`, `TryMoveTo`, `Schedule`, `Wait`, and `Retain`'s decision to run
  its callback) declines an item whose canonical context is canceled, even if the call context hides it (§6.9).
  `TryMoveTo` that does not admit the item leaves the fan-out body untouched (§6.4).
- A blocking leave that fails after joining the body leaves the body `closed` (§6.4, edge case 9).
- `Wave.Started` is redefined over root sources (§6.7).

Documentation and examples to revise:

- `README.md` features list; `docs/4_fan-out.md` (rewrite around `Schedule`, add rounds and trees, restate ordering,
  `Detach` section); `docs/5_lanes.md` (ticket rule wording, examples); `docs/6_conditional-move-to.md` (fan-out
  `TryMoveTo` section); `docs/9_design_faq.md` (a new entry: why `MoveTo` takes no tasks).
- `impl_details.md` §5 (publish rules, `TryMoveTo` preamble), §7 (scheduling, ordered insert, identity removal, root
  versus spawn), §8 (sealing, body state, `Started`), §10 (stale contexts), §13 (invariant 5 wording), §14 (the no-gap
  argument and its limits).
- Godoc: `FanOut`, `Pool`, `Lane`, `Branch`, `TaskFunc`, `Task`, `Wave`, `ErrStaleContext`.
- `demo/` and `bench/` call sites that pass tasks to `MoveTo`.

---

## 10. Open questions

1. **Names.** `Schedule` matches the existing vocabulary ("MoveTo schedules tasks", `scheduleWave`). Alternatives:
   `Add`, `Submit`, `Spawn`. `Wait` matches `sync.WaitGroup`; alternative: `Join`, which risks confusion with the
   `joins ...Wave` parameter.
2. **`Wait(ctx, joins ...Wave)`.** Mirrors `MoveTo` and costs little because `run.join` exists. Not included in this
   version.
3. **Opening the door without work.** This version uses `Schedule` with no tasks. A dedicated method would be more
   discoverable but adds surface.
4. **Depth-first spawning.** Inserting a spawn at the front of the item's own segment would give depth-first order
   within the item. Not planned; breadth-first by spawn time is the default.
5. **Per-submission handles.** A future `Schedule` variant returning a handle would restore fine-grained "sources
   drained" observation for one submission (declined option D). Not planned for this version.

## 11. Implementation plan

**Progress (2026-09-10):** phases 0 to 7 are done and committed on branch `fanout-schedule` (one commit each).
Next: phase 8 (§11.10). Process notes: from phase 3 on the phases are self-reviewed, not sent to Codex; nothing is
pushed. Each phase's record sits under its checklist.

### 11.1 Ground rules

- Work on a branch. One commit per phase below, so a regression can be bisected to a phase.
- Every phase ends with the root test suite green under the CI command. Phases 1 to 4 keep the old public API, so the
  existing tests keep compiling and act as the regression net while the runtime changes underneath them.
- Verification command for the root module (the same as `.github/workflows/test.yml`):

  ```
  go vet ./... && go test -race -cpu=1,4 ./...
  ```

- The demo and the bench are separate modules (`demo/go.mod`, `bench/go.mod`) that point at the library through a
  `replace` directive. CI does not test them. Run them by hand whenever the library API changes:

  ```
  (cd demo && go vet ./... && go test -race ./...)
  (cd bench && go vet ./... && go test -race ./...)
  (cd demo/web && npm ci && npm run lint && npm run build)
  ```

- Almost every test is black-box through the public API. The exceptions reach into units and occupancy, not into
  the queue helpers: `implOf` and `occupancyOf` in `helpers_test.go`, and the rank checks in `builder_test.go`. The
  internal renames in phases 1 to 3 should therefore need no test edits; confirm by compiling after phase 1.
- Phase 5 changes the public API, so the demo and bench modules stop compiling at that moment. Their mechanical
  migration is part of phase 5; phase 8 keeps only their wording, tests and rebuild.
- Repeated runs of the new tests use a package command with a name selector, never a list of files (a file list
  drops the package's helpers):

  ```
  go test -race -cpu=1,4 -count=10 -run 'Test(Schedule|Wait|Spawn|Door|Property|Completion|Stats)' .
  ```
- Panics inside task callbacks crash the test binary. A test that asserts a panic from a task or a lane child must
  recover inside the callback and hand the recovered value back through a channel.

### 11.2 Phase 0: baseline

- [x] Create the branch. Record the baseline: root, demo and bench suites green, `npm run build` green.
- [x] Note the current benchmark numbers of `BenchmarkFanOutSchedule` (`fanout_bench_test.go`) for a before/after
      comparison at the end.

Baseline record (2026-09-10, branch `fanout-schedule`, Go 1.26.3, Apple M1 Pro):

- Demo and bench suites green; `npm run lint` and `npm run build` green.
- Root suite: green except `TestFanOutWaitingRoomStartsNoWork` (`gather_test.go`), which was flaky on `master`
  (2 failures in 60 runs). The observer read the live-task counter as soon as the fan-out's occupancy reached 1,
  but the task runs on a worker goroutine spawned under the same lock hold, so the counter could still be 0. Fixed
  in this phase by waiting for the task to start; the peak assertion is what checks the waiting room adds no work.
- `BenchmarkFanOutSchedule` (`-count 5`, median of ns/op; B/op and allocs/op were stable):

  | sub-benchmark | ns/op | B/op | allocs/op |
  |---------------|-------|------|-----------|
  | lanes1        | ~5080 | 1557 | 26        |
  | lanes2        | ~7560 | 1827 | 32        |
  | lanes4        | ~10520 | 2324 | 44       |
  | lanes8        | ~17280 | 3370 | 68-69    |

### 11.3 Phase 1: internal groundwork, behavior-neutral

Goal: add the data the design needs without changing any observable behavior.

- [x] `item.go`: add `seq` (creation order within the run; assigned in `run.newItem`, used only for branch queue
      insertion). Document why `no` cannot be used (lane children share the parent's number).
- [x] `item.go`: add the per-fan-out body state. Suggested shape: a `bodyState []bodyState` slice indexed by unit
      index, like `occupied` and `entered`, with values `bodyNone`, `bodyOpen`, `bodyClosed`, `bodyDetached`. Keep
      `pending *wave` as the pointer to the open body (nil when the state is not `open`).
- [x] `state.go` (`taskCollection`): add `root bool` (scheduled through the own-body path) for the `Started` rule.
      `fanout.go` (`scheduleWave`) must set it to true on every collection it creates, so `rootUnexhausted` mirrors
      `unexhausted` under the old API and `Started` keeps closing exactly when it does today
      (`TestWaveStartedWaitsForStreamingSource`, `TestNewTasksChanArrivesAsSentAndStartedWaitsForClose`).
- [x] `context.go`: make the pool-work marker carry the `*taskCollection` instead of only the pool. `nonMovableCtx`
      still builds one context per collection. `resolveItem` keeps panicking for every method except `Schedule`,
      which gets its own resolver in phase 4.
- [x] `wave.go`: add `sealed bool`, `rootUnexhausted int`, `idle()`, `seal()`. `settle` closes nothing while
      unsealed; once sealed it closes `Started` when `rootUnexhausted == 0` and `Finished` when idle. `addSource(root)`
      and `sourceExhausted(root)` maintain both counters. `newWave` returns a sealed wave; only the fan-out body
      constructor (phase 3) creates an unsealed one. Verify that `Retain`, `finishedWave` and `standaloneWave` behave
      exactly as before.
- [x] `state.go`: replace `enqueueCollection` with `insertCollection(branchIdx, col)`: walk from the tail while the
      tail's item is younger (`seq` greater) than `col.it`, insert there. With the old API every insert lands at the
      tail, so this is behavior-neutral for now.
- [x] `state.go`: replace `dequeueHead` with `dequeue(branchIdx, col)` by identity, keeping the constant-time fast
      path when `col` is the head (clear the pointer, reslice, as today) and scanning only for a displaced collection,
      so draining a long spawned backlog stays linear under `run.mu`; rename `settleHead` to `settleCollection` and
      `dropHead` to `dropCollection`, both taking the collection. `pullableHead` and `grabNext` stay head-only.
      Check that each collection leaves the queue exactly once with one `Queued` decrement and one exhaustion
      notification, on both the drain path and the drop path.
- [x] `debug.go`: no change needed; confirm `DebugUnitOccupants` still walks `taskQueues` correctly after the rename.
- [x] Run the root suite. Nothing observable changed.

Phase 1 record: root suite green under `-race -cpu=1,4`; Codex review found no material issues.
`BenchmarkFanOutSchedule` allocs/op went from 26/32/44/68 to 27/33/45/70 (one allocation per item for the
`body` slice); ns/op within noise.

### 11.4 Phase 2: cancellation judged by the canonical context

Goal: §6.9, decided as option B. Behavior change, independent of the API change, with its own tests.

- [x] `state.go` (`waitUntil`): take the item, check `context.Cause` of both the call context and `it.ctx` before
      waiting and after every wake-up, and arrange the wake-up on both (an `AfterFunc` on `it.ctx` when it differs
      from the call context). Return whichever cause is set, the item's own first.
- [x] `context.go` (`actingItem`): when `checkCancel` is set, check both contexts.
- [x] `stage.go` (`Retain`): decide from both contexts whether to run the callback.
- [x] `errors.go` / godoc: document that a context with cancellation stripped cannot move, schedule, wait or retain for
      a canceled item.
- [x] Tests (`errors_test.go` or a new `cancel_test.go`): `MoveTo`, `TryMoveTo` and `Retain` called with
      `context.WithoutCancel(ctx)` on a poisoned item and on an item canceled by error-shutdown return or skip as
      specified; a child context with a shorter deadline still returns its own deadline error;
      `TestRetainOnCanceledItemSkipsBgOp` gets a stripped-context variant; `TestErrStaleContextFromFinishedItem`
      still passes (stale wins over canceled for a finished item).
- [x] Tests for cancellation **during** a wait, which exercise the second wake-up source: a stripped-context `MoveTo`
      blocked on admission, a stripped-context `MoveTo` blocked in a join of a listed wave (`wave.go` `join` calls
      `waitUntil` too), and later, in phase 4, a busy `Wait`; the item's own context is then canceled and the call
      must return the cause.
- [x] Run the root suite.

Phase 2 record: root suite green under `-race -cpu=1,4`. Tests live in `cancel_test.go`. The two wake-up tests are
white-box on purpose: they park the call inside the wait (observable only after `cond.Wait` released the lock), cancel
the item's own context directly with no broadcast, and fail after a bounded 5s if the call does not wake. A mutation
check (second `AfterFunc` disabled) makes both fail; Codex confirmed no other broadcast source remains in that state.
The `Wait` variant follows in phase 4.

### 11.5 Phase 3: body state, sealing, leave rules (old API still in place)

Goal: §6.4 to §6.7 without touching the public signatures. `MoveTo(ctx, tasks, joins...)` still creates and fills
the body in one step.

- [x] `fanout.go`: split `scheduleWave` into `newBody(it, f)` (unsealed wave, `atNode`, `pending`, state `open`) and
      `addToBody(w, f, tasks, root)` (claim, group, insert, register, publish, pump). `MoveTo` calls both; the
      publish stays where it is for now.
- [x] `state.go` (`joinPending`): wait for `idle` instead of `isFinished`; on idle, seal, set state `closed`, clear
      `pending`, acknowledge, return the node-qualified error if any. On a cancellation wake-up apply §6.4 step 1:
      busy body returns the cause and stays open; idle body is sealed and closed, then the body error wins over the
      cause if there is one. Today's code returns nil for an idle clean body after a cancellation; under option B it
      must return the cause.
- [x] `state.go` (`enterUnit`): if the item already stands in the target's waiting room (`it.queuedAt ==
      target.index`), skip the admission-or-queue wait and `takeQueue`, and resume waiting for the node. This closes
      the double-count on a retried move (§6.4 step 4) that exists today whenever a derived call context expires
      while the item waits in a queue.
- [x] `state.go` (`tryEnterUnit`): new order per §6.4: busy body returns false untouched; idle body with no room
      returns false untouched; otherwise seal, set `closed`, take, publish, join. Remove the consume-before-check.
- [x] `fanout.go` (`Detach`): require state `open`; seal; set state `detached`; clear `pending`. `errNothingToDetach`
      for `closed` and `detached`.
- [x] `run.go` (`completeItem`): before the `hasLiveWaves` loop, seal the open body if any and set it `closed`.
- [x] `errors.go`: add `errBodyClosed` and `errWorkDetached` (unused by the public API until phase 4, but wired into
      `Detach` diagnostics now).
- [x] Re-check the tests that observe these paths and adjust only where today's behavior was an artifact of
      consume-before-check: `TestTryMoveToReportsFinishedFanOutFailure` (`gather_test.go`), `TestDetachAfterWorkAlreadyFinished`,
      `TestDetachWithoutWorkPanics`, `TestDetachAfterMovingOnPanics`, `TestDetachOfAnEarlierFanOutPanics`
      (`detach_test.go`). Derive the expected panic from the state table in §6.7.
- [x] Run the root suite.

Phase 3 record: root suite green under `-race -cpu=1,4`. `scheduleWave` became `newBody` + `addToBody`;
`consumePending` was replaced by `item.sealBody` / `run.closeBody`. None of the five listed tests needed a change
(only a comment in `TestTryMoveToReportsFinishedFanOutFailure`): the body-state table gives the same panics they
already expected. Two tests were added to `queue_test.go` for the waiting-room fixes, each checked by mutation:
`TestRetryFromWaitingRoomTakesNoSecondQueuedSlot` (queue size 2, retry against a still-full stage; the stage-only
half of the phase 6 "failed leave, after the waiting room" case) and `TestWaitingRoomSlotReleasedWhenTheItemWaitsElsewhere`
for a second latent bug found on the way: `takeQueue` now gives back a queued slot the item still held in front of an
earlier node (a failed move there, then a move to a later node's waiting room) — before, that slot leaked for the
rest of the run. Both belong in the `impl_details.md` §5/§6 rewrite of phase 7.

### 11.6 Phase 4: `Schedule` and `Wait`, additive

Goal: §6.2 and §6.3. The old `MoveTo(ctx, tasks, joins...)` still exists and still publishes at entry, so the door
rule is not testable yet; everything else is.

- [x] `fanout.go`: add `Schedule(ctx, tasks ...Task) error` to the interface and implement the fourteen steps of §6.2:
      static task validation, handle validation, caller resolution (pool-work marker, item), conveyor validation,
      lock, stale checks (finished item; finished wave; child checked before the parent-wave redirect), body
      resolution and root/spawn classification, cancellation by both contexts, claim, group, insert, register,
      publish, pump.
- [x] `context.go`: add the `Schedule` resolver that accepts a pool-work context and returns its collection.
- [x] `fanout.go`: add `Wait(ctx) error` per §6.3: task context panics, body state must be `open`, `waitUntil(idle)`
      with the cancellation nuance, acknowledge and return the node-qualified error, never seal.
- [x] `errors.go`: `ErrStaleContext` doc now covers a finished lane child and pool work whose wave finished.
- [x] Godoc for `FanOut.Schedule`, `FanOut.Wait`, `TaskFunc` (a pool context may call `Schedule` of its own fan-out).
- [x] Tests, written already in the style that survives phase 5 (`MoveTo(ctx, nil)` then `Schedule`), in a new
      `schedule_test.go` and `wait_test.go`:
  - [x] rounds: `Schedule`, `Wait`, `Schedule`, `Wait`, leave; results of round 1 decide round 2.
  - [x] `Wait` on an empty body returns at once; repeated `Wait` after an error returns the same error.
  - [x] `Wait` returns only when the whole tree is done (spawn depth 3 on one pool).
  - [x] tree on one pool with several roots; tree across two pools; pagination chain.
  - [x] the join-as-continuation idiom (last sibling schedules the merge task).
  - [x] spawn from a lane child into the fan-out its lane belongs to; the parent's exit joins the siblings.
  - [x] spawn into a detached wave; `Finished` closes only when the tree is done; the fan-out slot is held meanwhile.
  - [x] `Schedule` from a canceled item returns the cause and queues nothing (`Queued` gauge stays 0).
  - [x] `Schedule` from a pool goroutine after its wave finished returns `ErrStaleContext`; from a lane child after
        its callback returned returns `ErrStaleContext` even while the parent wave is busy.
  - [x] panics: `Schedule` before entering (`errStageNotEntered`); after `Detach` (`errWorkDetached`); a task calling
        `Schedule` on another fan-out (`errInvalidUnit`); `Wait` from a pool task (`errCannotMove`); a lane child
        calling `Wait` on the parent's fan-out (`errWrongScope`); a `Retain` callback calling `Schedule` after the
        processor returned (`errBodyClosed`, recovered inside the callback).
  - [x] `TryMoveTo` leaving: busy body returns `(false, nil)` and scheduling continues afterwards; idle clean body with
        a full target returns `(false, nil)` and scheduling continues; poisoned item returns `(false, cause)`.
  - [x] ordering: with a pool limit of 1, an older item's same-pool spawn chain runs to the end before the younger
        item's queued task starts; with a limit above 1, the older item's spawn takes the next freed slot ahead of the
        younger item's queued work.
  - [x] displaced async pull, yield path: a younger item's generator is mid-pull when the older item spawns; the
        older work is pulled from the new head; the yielded younger callback still runs on its reserved slot.
  - [x] displaced async pull, exhaustion path (this is what identity removal protects): the displaced generator
        returns no callback while the older work still sits at the head; the displaced collection, not the head, is
        removed; its wave gets exactly one exhaustion notification; the `Queued` gauge returns to 0. Variant: the
        younger item is canceled mid-pull, so the pull ends as exhaustion and the wave records the abandonment.
  - [x] `Wait(context.WithoutCancel(ctx))` on a canceled item with an idle clean body returns the cause; with an idle
        failed body returns the body error.
  - [x] a spawn with zero tasks is a no-op; concurrent `Schedule` calls from the ItemProcessor and from tasks of the
        same item all land, in lock order, and every callback runs once.
  - [x] a channel source that is not closed blocks `Wait`; closing it releases `Wait` (documents §7.34).
  - [x] `Started` on a detached wave closes when the root generator is drained even while a spawned generator is still
        pulling.
  - [x] interior fan-out of a lane: a child runs rounds and spawns inside its lane.
- [x] Run the root suite.

Phase 4 record: root suite green under `-race -cpu=1,4`; the new tests also pass `-count=10`. `Schedule` resolves its
caller with `conveyor.resolveCaller` (pool-work marker or item), then `fanOut.bodyFor` classifies the addition (pool
work: its wave, wrong target panics; lane child at the fan-out of its lane: the parent's wave; otherwise the own-body
path through `fanOut.openBody`, shared with `Wait`). The stale check runs before `bodyFor`, so a finished child is
refused before any redirect. Mutation checks: insertion at the tail (instead of at the item's place) fails the two
ordering tests; head-only removal in `dequeue` fails the displaced-pull tests; dropping the finished-wave stale check
fails `TestScheduleFromFinishedWorkIsStale`. Two test assumptions were corrected on the way: a slot freed by a spawned
task goes to the next queued work, whichever item it belongs to, once the older item has nothing queued (so the
limit-above-1 ordering test asserts only "spawn starts before the younger work"); and a pull in flight reserves a pool
slot, so occupancy does not return to zero while a channel source is open. The phase 5 items for
`TestErrForeignContextFromFanOutMoveTo` and `TestErrWrongScopeIsCheckedOnEveryEntryPoint` were done here, plus
`Schedule`/`Wait` rows in `TestErrStaleContextFromFinishedItem`. The `Schedule` godoc leaves out the door sentence
("the item's first Schedule opens this fan-out to the item behind it") until phase 5 makes it true. The busy-`Wait`
stripped-context test (`TestStrippedContextWaitWakesOnItemCancellation`) pins the contract but cannot force the
wake-up path: `Wait` changes no runtime state before it parks, so nothing observable says the wait has begun; the
watcher itself is pinned by the phase 2 tests through the same `waitUntil`.

### 11.7 Phase 5: the API switch

Goal: §5 and §6.1. Breaking change, mechanical migration of every call site, then the semantic updates.

- [x] `fanout.go`: `MoveTo(ctx, joins ...Wave)` and `TryMoveTo(ctx, joins ...Wave)`. Entry creates the empty open
      body. Remove the tasks path from both.
- [x] `state.go` (`enterUnit` / `takeUnit`): on admission with `publish == false`, publish the waiting-room rank
      (`rank - 1`) if the item's `maxRank` is lower.
- [x] Move the node-rank publish to: the first root `Schedule` (idempotent max in `addToBody`), `Detach`, and the
      leave (already implied by entering the next node or its waiting room). Remove it from entry.
- [x] `task.go`: remove `Tasks` and `Tasks.Add`.
- [x] Mechanical migration of every test call site: `X.MoveTo(ctx, Tasks{...}, joins...)` becomes `X.MoveTo(ctx,
      joins...)` followed by `X.Schedule(ctx, ...)`; `X.MoveTo(ctx, nil)` becomes `X.MoveTo(ctx)`; `var tasks Tasks;
      tasks.Add(...)` becomes `var tasks []Task; tasks = append(tasks, ...)`. Files by weight: `task_sources_test.go`,
      `fanout_test.go`, `children_test.go`, `errors_test.go`, `shutdown_test.go`, `wave_test.go`, `detach_test.go`,
      `setlimit_test.go`, `gather_test.go`, `retain_test.go`, `branch_test.go`, `interaction_test.go`,
      `property_test.go`, `queue_test.go`, `trymoveto_test.go`, `worker_pool_test.go`, `conveyor_test.go`,
      `items_limit_test.go`, `stats_test.go`, `builder_test.go`, `debug_test.go`, `bound_test.go`,
      `fanout_bench_test.go`, `helpers_test.go`, and the phase-4 tests (`MoveTo(ctx, nil)` to `MoveTo(ctx)`).
- [x] Semantic updates where the old test encoded the old rule:
  - [x] `TestFanOutReleasesPreviousStageAtEnqueue` (`fanout_test.go`): the previous stage is now released at
        admission; rename and assert the door stays closed until the first `Schedule`.
  - [x] `TestFanOutEmptyMoveIsStillAMove` (`fanout_test.go`) and `TestEmptyFanOutIsAUsableNode`
        (`interaction_test.go`): an empty visit holds the next item at the door until the visitor leaves.
  - [x] `TestTryMoveToFanOutLeavesTasksUnclaimed` (`trymoveto_test.go`): replace with "declined entry leaves the item
        in the previous node, and the same `[]Task` can be scheduled later at its owning fan-out" (a task belongs to
        one branch, so it can never go to another fan-out).
  - [x] `TestTryMoveToFanOutJoinErrorLeavesTasksUnscheduled` (`trymoveto_test.go`): entered with a join error; the
        body is open and empty; a following `Schedule` returns the cause because the join error poisoned the item.
  - [x] `TestFanOutTasksFromForeignBranchPanics`, `TestTaskReusePanics` (`fanout_test.go`),
        `TestErrInvalidUnitTaskFromAnotherFanOut`, `TestErrTaskReusedOnSecondSubmission` (`errors_test.go`): the
        panic now comes from `Schedule`.
  - [x] `TestErrForeignContextFromFanOutMoveTo` (`errors_test.go`): add `Schedule` and `Wait` with a foreign context
        (done in phase 4, together with `TestErrStaleContextFromFinishedItem`).
  - [x] `TestErrWrongScopeIsCheckedOnEveryEntryPoint` (`errors_test.go`): add `Schedule` (own-body path) and `Wait`
        (done in phase 4).
  - [x] `TestErrCannotMoveFromNonTravellingWork` (`errors_test.go`) and `TestNonTravellingWorkCannotMove`
        (`children_test.go`): `Schedule` with a pool context is now allowed; `Wait` and the moves still panic.
  - [x] `TestErrStageNotEnteredAfterLeaving` (`errors_test.go`): add `Schedule` and `Wait` after leaving
        (`errBodyClosed`) and after `Detach` (`errWorkDetached`).
  - [x] `TestMixedSourcesAcrossPoolsInOneMove` (`task_sources_test.go`): rename to "in one Schedule".
  - [x] `TestSetLimitRaiseFanOutAdmitsMoreItems` (`setlimit_test.go`): a raise admits the next item only after the item
        ahead has scheduled; make the test schedule before asserting admission.
  - [x] `TestQueueOnFanOut`, `TestFanOutQueueCreatedAtRuntime` (`queue_test.go`): confirm they still hold; add the
        waiting-room-during-closed-door case here or in phase 6.
  - [x] `TestPoolFIFOAcrossItems`, `TestPoolTasksStartInSubmissionOrder`, `TestChildrenPreserveOrderAcrossItems`,
        `TestChildTicketOrderAcrossTasksAndItems`, `TestFanOutWorkPerPoolOrderSurvivesTheWaitingRoom`: schedule once
        right after entering; the old guarantee must still hold exactly.
- [x] Godoc for `FanOut`, `Pool`, `Lane`, `Branch`, `Task`, `Wave` per §5 and §6.8.
- [x] `debug.go` (`UnitOccupants.InQueue` doc): a branch's backlog is listed in queue order, which is item age and
      then submission order, not arrival order.
- [x] Mechanical migration of the dependent modules so they compile again in this phase:
      `demo/internal/topology/process.go` (`runNodes`, `KindFanOut` case: `fo.MoveTo(ctx)`, build a
      `[]conveyor.Task`, `fo.Schedule(ctx, tasks...)`; keep `MarkPending` before `MoveTo` and `MarkConfirmed` after
      `Schedule`; treat a `Schedule` error like a `MoveTo` error), `bench/internal/pipeline/conveyor.go` (`Run`:
      `nd.fanout.MoveTo(ic, pending...)`, `nd.fanout.Schedule(ic, tasks...)`, then `Detach`), and
      `demo/web/src/codegen/generateGoCode.ts` (`emitBody`: emit the two calls, no `Tasks{`). Run both module suites
      and `npm run build`.
- [x] Run the root suite.

Phase 5 record: root suite green under `-race -cpu=1,4`; demo and bench modules vet, build and test; `npm run lint`
and `npm run build` pass. Runtime: `occupy` publishes the waiting-room rank (`queueRank`) when `publish` is false, so a
fan-out admission lets the follower step aside without opening the node; the node rank is published only by
`addToBody` (first `Schedule`, also with zero tasks), `Detach`, and the leave. `MoveTo`/`TryMoveTo` create the body
with `newBody` before the join, so a failed join leaves an open empty body on a poisoned item (pinned by
`TestTryMoveToFanOutJoinErrorPoisonsBody`). The mechanical migration was done with a throwaway AST-driven rewriter
(`MoveTo(ctx, Tasks{...})` became `MoveTo(ctx)` plus `Schedule(ctx, ...)` with the same error handling; `MoveTo(ctx,
nil)` dropped the argument; one `Stage.MoveTo(ctx, nil)` in `TestErrForeignWaveFromNilWave` was restored by hand,
since there `nil` is a nil Wave). Door checks: `TestFanOutReleasesPreviousStageAtAdmission` and
`TestEmptyFanOutIsAUsableNode` hold the visitor inside for the check; both fail when `occupy` publishes the node
rank at entry (mutation check). The waiting-room-during-closed-door case and the other door tests are left to phase 6.
`TestSetLimitRaiseFanOutAdmitsMoreItems` already scheduled right after entering; only its comment changed.

### 11.8 Phase 6: tests that need the door rule, and the property suite

- [x] Door: the next item cannot enter the fan-out until the item ahead schedules; it can enter the waiting room
      meanwhile (publish of the waiting-room rank on admission); `Schedule` with zero tasks opens the door; leaving
      without scheduling opens it; `Detach` opens it; `Wait` does not.
- [x] Failed leave, before the waiting room: a derived call context with a short deadline, the next stage full and
      without a waiting room; `MoveTo` returns `context.DeadlineExceeded`; the item still occupies the fan-out with a
      `closed` body; `Schedule` and `Wait` panic `errBodyClosed`; `Detach` panics `errNothingToDetach`; a second
      `MoveTo` succeeds once the stage frees.
- [x] Failed leave, after the waiting room: the next stage full but its waiting room open; the deadline expires while
      the item waits there; the fan-out slot is already free (`occupancyOf`), the item holds one queued slot; a
      retried `MoveTo` takes no second queued slot (`Stats` `Queued` of the stage stays 1) and enters when the stage
      frees.
- [x] Completion: the processor returns nil while the body is busy; the tree finishes and an unobserved task error
      fails the item. The processor returns an error while the tree spawns; the run terminates with that error;
      `Schedule` from the still-running tasks returns the cause; queued spawns are dropped and the wave records the
      abandonment. Do not assert that no callback runs after cancellation: a callback assigned just before the
      cancellation still runs and sees a canceled context, by design.
- [x] Completion, error policy: the processor returns a `ShutdownError` while a wave holds an unobserved real error;
      the wave error becomes the item's error (§7.44). `TestShutdownErrorFromProcessorIsNotAFailure` does not cover
      this.
- [x] Shutdown while blocked in `Wait`: returns the shutdown cause; in-flight tasks finish; queued spawns are dropped
      and the wave records the abandonment.
- [x] Stats: the branch `Queued` gauge counts spawned collections and returns to 0; fan-out occupancy counts an empty
      visit and an item that failed to leave.
- [x] `property_test.go`: extend the random processor with rounds (`Schedule` then `Wait`, 0 to 2 times), spawn depth
      (0 to 2 follow-ups per task, bounded depth), `Detach` of growing waves, and `TryMoveTo` out of a fan-out. Extend
      the invariants: every scheduled callback runs exactly once unless its item was canceled; every wave finishes;
      no unit keeps occupancy after the run; `Run` leaves no goroutines; on every branch, at the moment a slot is
      handed out, the branch had no queued work of an older item than the one receiving it (measure at assignment
      inside `grabNext` through an in-package test hook, not at callback invocation).
- [x] `property_test.go` (`assertNoLeaks`): today it reads `Stats` after `Run`, which is the zero value once
      `currentRun` is nil, so it proves nothing about occupancy. Capture the `*run` through `implOf` while the run is
      active and inspect its `occupancy` and `queued` counters after `Run` returns; include fan-out occupancy.
- [x] `fanout_bench_test.go`: migrate `BenchmarkFanOutSchedule`; add a benchmark for spawning from a task and one for
      `Wait` between rounds; compare with the phase-0 numbers.
- [x] Run the root suite with `-count=20` on the new ordering tests to shake out races.

Phase 6 record: root suite green under `-race -cpu=1,4`; the new tests and the property suite also pass `-count=20`.
New files: `door_test.go` (door rule, waiting room during a closed door, failed leave before and after the waiting
room) and `completion_test.go` (processor returns with a busy body, clean and failing; processor error while the tree
spawns, open and detached; `ShutdownError` yielding to an unobserved wave error; shutdown while blocked in `Wait`);
two Stats tests in `stats_test.go`. One spec nuance: after a leave failed **from the waiting room** the item no longer
occupies the fan-out, so `Detach` there panics `errStageNotEntered` (§6.11 row 1), not `errNothingToDetach`; §6.4 step
4 and §7.9 describe the case where the item is still inside. Property suite: `process` draws a body shape per fan-out
(schedule once and detach, or 0 to 2 rounds of `Schedule` then `Wait` with the body left open for the next move or
for completion), tries some moves with `TryMoveTo` first (declined attempts fall back to `MoveTo`), and every piece
of branch work spawns 0 to 2 follow-ups down to depth 2 — pool tasks through their own context, lane children into the
parent's fan-out after their first move. New invariants: every detached wave is finished after the run; the captured
`*run` (through `implOf`, taken by the first item) holds no occupancy, no queued item, no queued collection and no
item in flight; and `conveyor.assignHook` (an in-package hook read by `grabNext` at the moment a slot is handed out,
for a sync pull or a streaming reservation) never sees an older item's collection still queued on the branch. Mutation
check: inserting collections at the tail instead of the item's place fails that invariant on almost every seed.
Test harness fix found on the way: `runUntil` and `runOnce` returned at once for the items beyond the ones under
test, which freed the start stage for the next such item, so one worker churned items in a tight loop that starved the
test's goroutines; under the race detector with `GOMAXPROCS=1` the jitter property test took 50 s on HEAD and timed
out with the new shapes. The extra items now park until the run is stopped; the whole root suite runs in 16 s under
the CI command. A test assertion that runs on an item's goroutine must use `Errorf` (see
`assertClosedBodyRefusesEverything`): a `Fatalf` there ends the worker, not the test, and the run hangs.
Benchmarks (`-count 5`, medians, same machine as phase 0): `BenchmarkFanOutSchedule` lanes1 ~4600 ns/op, 1622 B/op,
27 allocs/op; lanes2 ~6300 / 1909 / 33; lanes4 ~8800 / 2446 / 45; lanes8 ~19400 / 3568 / 70 (noisy). Against phase 0:
time equal or better, one more allocation and 4-6% more bytes per item for the body wave created at entry.
`BenchmarkFanOutSpawnChain` depth1 ~6000 ns/op / 1690 B / 28 allocs, depth4 ~10600 / 2587 / 50, depth16 ~23500 / 5932
/ 132 (about 1200 ns and 22 allocs per link). `BenchmarkFanOutRounds` rounds1 ~5500 / 1634 / 27, rounds2 ~8400 / 2040
/ 37, rounds4 ~14400 / 2815 / 57 (about 3000 ns and 10 allocs per round).

### 11.9 Phase 7: documentation

- [x] `docs/4_fan-out.md`: rewrite around `MoveTo` then `Schedule`. Sections: the basic example; the door rule in one
      paragraph; conditional body; rounds with `Wait`; trees (a task schedules follow-ups) with the three rules
      (schedule, never wait; before the task returns; results flow forward); several roots; join as a continuation;
      the ordering statement from §6.8 with its consequences; `Detach` with a growing tree; the `Started` note; a
      short "migrating from v0.9" table (old call, new calls). Keep the interactive demo link only if the demo's
      URL schema did not change (see phase 9).
- [x] `docs/5_lanes.md`: migrate the example; ticket rule wording (children order by pull, a spawn from a child after
      its first move does not hold the entrance); a lane child may `Schedule` at the fan-out its lane belongs to.
- [x] `docs/6_conditional-move-to.md`: rewrite the fan-out `TryMoveTo` section (nothing to leave unclaimed; declined
      entry needs no cleanup) and add a paragraph on `TryMoveTo` out of a fan-out (busy body means declined).
- [x] `docs/9_design_faq.md`: add "Why does `FanOut.MoveTo` take no tasks?" and "Why can a task not wait for the work it
      spawned?" and "Why does a stripped context not bypass cancellation?".
- [x] `docs/1_retain-previous-stage.md`, `docs/2_queues.md`, `docs/3_shared-stages.md`: check for
      `MoveTo(ctx, tasks` and ordering wording.
- [x] `docs/7_observability.md`: a branch's backlog is no longer bounded by "one collection per item inside the
      fan-out" (the sentence near line 61); with rounds and spawns an item may have several collections queued.
      Reword `Queued` for branches as "collections not fully handed out".
- [x] `debug.go` (`UnitOccupants.InQueue`) and `stats.go` (`UnitStat.Queued`) godoc: queue order and collection
      counting per the two points above.
- [x] `docs/README.md`: index line for 4 mentions `Schedule` and `Wait`.
- [x] `README.md`: the features list and the scatter-gather sentence.
- [x] `impl_details.md`: §2 or §4 (body state per item), §5 (waiting-room rank on admission; node rank at first root
      `Schedule`; `TryMoveTo` preamble and canonical cancellation), §7 (Schedule steps, ordered insert, identity
      removal, root versus spawn, displaced pull), §8 (sealed waves, `Started` over roots, body state table), §10
      (`ErrStaleContext` for finished work; canonical cancellation), §13 (invariant 5 wording), §14 (the no-gap
      argument and its limits), §15 if the `Started` caveat belongs there.
- [x] Godoc pass over `conveyor.go` (package doc mentions of `Tasks`), `errors.go`, `wave.go`, `branch.go`,
      `task.go`.
- [x] Release notes draft for the tag: breaking changes list from §9, migration table, new features.


Phase 7 record: `docs/4_fan-out.md` rewritten around `MoveTo` then `Schedule` (door, conditional body, rounds, trees
with the three rules, several roots, join as a continuation, the §6.8 ordering statement and its consequences,
`Detach` with a growing tree, the `Started` note, a v0.9 migration table); the demo links are kept, since phase 9 is
not planned and the URL schema did not change. `docs/5_lanes.md` migrated (the example's `report` branch is now a
pool of the fan-out — before it was a lane stage used with `NewTasks`, which does not compile), the ticket rules now
speak of pull order, and a section says a child may `Schedule` at its parent's fan-out but not `Wait` there.
`docs/6_conditional-move-to.md` got the fan-out sections "into" (nothing to clean up) and "out of" (busy body means
declined, body stays open). `docs/9_design_faq.md` Q4 to Q6. `docs/7_observability.md`, `debug.go` and `stats.go`
describe a branch's backlog as collections not fully handed out, several per item possible. `docs/1`, `docs/2`,
`docs/3` and `docs/8` needed no change (no `Tasks` or ordering wording). `impl_details.md` rewritten: §3 (`waitUntil`
on both contexts), §4 (`seq`, body state table), §5 (leave steps with the failed-leave positions and the `queuedAt`
retry guard, publishing rules and the door, `tryEnterUnit` order and preamble), §6 (the two waiting-room slot fixes
from phase 3), §7 (bodies, the `Schedule` steps, queue order with displacement and identity removal), §8 (sealed /
idle / finished, `Started` over roots, `Wait`), §10 (canonical cancellation, new sentinels, stale contexts), §12,
§13 (invariant 5 reworded, invariant 9 added), §14 (growth and the no-gap argument with its limits), §15 (door cost,
`Started` caveat). Godoc pass: `errors.go` header, `task.go` (`claim`), `item.go` (`maxRank`, `reachedRank`,
`waves`), `state.go` (`checkEnterOrder` comment); `conveyor.go`, `wave.go`, `branch.go` and `fanout.go` were already
current from phases 4 and 5. Release notes draft added as §12 below. No code changed; `go vet` and the root suite
pass.

### 11.10 Phase 8: demo and bench migration (required)

- [ ] `demo/internal/topology/process.go`: the mechanical change was made in phase 5; keep task construction between
      `MoveTo` and `Schedule` short, since the door is closed in that window, which is the "pending" state the UI
      already draws.
- [ ] `demo/internal/topology/process.go` (`FanOutEntry` doc): the pending window is now "admitted, `Schedule` not
      yet returned".
- [ ] `demo/internal/runtime/manager.go` (`NodeState.PendingEntry` doc): same wording change.
- [ ] `demo/web/src/codegen/generateGoCode.ts`: migrated in phase 5; review the emitted snippet once more against
      §8.1 for formatting.
- [ ] `demo/web/src/types/state.ts` (`pendingEntry` doc), `demo/web/src/pipeline/itemPositions.ts` (`ItemFill` doc and
      `classifyFanOutEntries` comment), `demo/web/src/components/nodes/FanOutBox.tsx` (comment): replace the
      `FanOut.MoveTo` wording with the two-call wording, and describe the fills by what they are derived from
      (some of the item's work still queued on a branch; some running; none left) instead of as equivalents of
      `Wave.Started` and `Wave.Finished`. The channels are not observable for an open body, and `Started` now
      counts roots only, so the equivalence would be false as soon as the demo spawns (phase 9).
- [ ] `demo/web/src/components/LegendPanel.tsx`: check the "Item (waiting at MoveTo)" label still reads right.
- [ ] Rebuild the WASM binary and the site: `cd demo/web && npm run build`; run `npm run lint`.
- [ ] `demo/internal/runtime/manager_test.go`: all tests pass; add one that asserts the `PendingEntry` marker is set
      before `MoveTo` and cleared after `Schedule` returns. Do not relate it to the next item's admission: the marker
      is cleared outside the library lock after `Schedule` returned, while the door opened inside `Schedule`, so the
      follower may be admitted before the marker clears.
- [ ] `bench/internal/pipeline/conveyor.go`: migrated in phase 5; `TestPipelinesEquivalent` passes. Re-run the
      benchmark scenarios and refresh `docs/8_benchmarks.md` numbers only if they moved outside noise.
- [ ] Check `bench/README.md` and `demo/web/README.md` for API snippets (none found at the time of writing).

### 11.11 Phase 9: demo visualization of a growing body (not planned)

Decided: the demo keeps its current behavior. Every item schedules a fixed number of tasks per branch
(`tasksPerItem`) in one `Schedule` call right after `MoveTo`, and no task spawns follow-ups. Phase 8 therefore only
rewrites the demo to the new syntax, including the Go snippet the code panel shows to the user.

For the record, showing spawning in the demo would be a feature across the schema (`SpawnPerTask` on `BranchSpec`
with no `HASH_VERSION` bump), the runtime (`TaskCounts`, follow-up paths in `LanePaths`), the WASM API and manager,
the UI wiring (`PoolBox.tsx`, `nodes/types.ts`, `App.tsx` `handleEditBranch`, defaults, factory, mutations, `toSpec`,
`resolve.ts`, live state), the fill derivation in `itemPositions.ts` (which cannot mirror roots-only `Started` from
the polled data), the badge animation in `TaskStrip.tsx` (origins keyed by item number break for same-item
follow-ups), and the code generator (a recursive `visit` closure). It can be picked up later without touching the
library.

### 11.12 Phase 10: final verification

- [ ] Root: `go vet ./... && go test -race -cpu=1,4 ./...`, plus the repeated package run from §11.1 with a name
      selector covering the new tests and the property suite.
- [ ] Demo and bench modules: vet and race tests as in §11.1; `npm run lint && npm run build`.
- [ ] Go version floor: CI runs 1.23 and stable; if `context.AfterFunc` or another API newer than 1.23 is touched,
      confirm the floor still builds.
- [ ] Read the diff of the public API once more against §5; every panic and return in §6.11 has a test.

## 12. Release notes draft (v0.10.0)

**Fan-out bodies that grow: `Schedule` and `Wait`.** An item now enters a fan-out with `MoveTo(ctx)` and adds work
with `Schedule(ctx, tasks...)`, as many times as it likes. A task running on one of the fan-out's pools (or a child of
one of its lanes) may `Schedule` follow-ups into the same body with its own context before it returns, so trees of
work (crawling, pagination, recursive listing) grow while the item stays inside. `Wait(ctx)` blocks until everything
scheduled so far is done and lets the item schedule again. See `docs/4_fan-out.md`.

### Breaking changes

- `FanOut.MoveTo(ctx, tasks, joins...)` is now `MoveTo(ctx, joins...)`; `FanOut.TryMoveTo` likewise. Add work with
  the new `FanOut.Schedule(ctx, tasks...)`.
- `Tasks` and `Tasks.Add` are removed. Build a `[]Task` and pass it with `...`.
- The item behind a fan-out can enter it only after the item ahead has called `Schedule` once (or left, or
  detached), not at the item ahead's `MoveTo`. It may wait in the fan-out's waiting room meanwhile. Keep the code
  between `MoveTo` and the first `Schedule` short.
- Every node method (`MoveTo`, `TryMoveTo`, `Schedule`, `Wait`, and `Retain`'s decision to run its callback) declines
  an item whose own context is canceled, even when called with a context that hides the cancellation
  (`context.WithoutCancel`). A derived context with a shorter deadline still works.
- `Wave.Started` closes once the wave is sealed and every task the *ItemProcessor* scheduled has been handed out.
  Work scheduled by tasks or lane children does not count and does not delay it.
- A pool task's context is accepted by `Schedule` of its own fan-out. Every other node method still panics with it.
- `ErrStaleContext` also covers a finished lane child's context and a pool task's context whose wave has finished.
- The per-branch ordering guarantee is restated: on a branch, a free slot is never given to a younger item's work
  while an older item has work queued there; started work and reserved streaming pulls are never taken back. For
  code that schedules once right after entering it is unchanged.
- A `TryMoveTo` out of a fan-out that does not admit the item leaves the body untouched and open. A blocking `MoveTo`
  out of a fan-out that fails after the body was joined leaves the body closed; `Schedule` and `Wait` there panic.

### Migration

| v0.9                                              | v0.10                                                                |
|---------------------------------------------------|----------------------------------------------------------------------|
| `f.MoveTo(ctx, conveyor.Tasks{a, b}, joins...)`   | `f.MoveTo(ctx, joins...)` then `f.Schedule(ctx, a, b)`               |
| `f.MoveTo(ctx, nil)`                              | `f.MoveTo(ctx)`                                                      |
| `f.TryMoveTo(ctx, tasks)`                         | `entered, err := f.TryMoveTo(ctx)`; `if entered { f.Schedule(...) }` |
| `var t conveyor.Tasks; t.Add(x)`                  | `var t []conveyor.Task; t = append(t, x)`                            |

### New

- `FanOut.Schedule(ctx, tasks...)`: non-blocking; callable by the ItemProcessor while inside with an open body, by a
  pool task before it returns, and by a lane child before its callback returns. Work is queued at the owning item's
  place on each branch. Zero tasks is legal and only opens the fan-out to the next item.
- `FanOut.Wait(ctx)`: blocks until the body (including spawned work) is idle, returns the first error with the node's
  name, keeps the slot; repeated calls return the same error.
- `FanOut.Detach` returns a wave that may still grow from its own running tasks; the slot follows the whole tree.
- Two fixes on the way: a `MoveTo` retried from a waiting room after a failed move no longer takes a second queued
  slot, and a queued slot left in front of an earlier node by a failed move is given back when the item waits
  elsewhere.

## Decisions taken

- **`Tasks` is removed.** The review suggested keeping it as a deprecated helper to reduce churn and agreed there is no
  functional harm in removal. Every fan-out call site changes anyway.
- **Cancellation is judged by the item's canonical context in every node method** (option B over declined option O).
  The earlier open question on generalizing the check beyond `Schedule` and `TryMoveTo` is closed by this decision.
- **The demo does not visualize spawning.** It keeps a fixed number of tasks per branch, scheduled in one `Schedule`
  call after `MoveTo`, and is rewritten to the new syntax only, including the generated Go snippet (§11.10, §11.11).

---

## Change log

Revision 6:

- Phase 9 (demo visualization of spawning) is not planned. The demo keeps its behavior and is migrated to the new
  syntax only. Recorded under "Decisions taken"; §11.11 keeps a short note of what the feature would involve.

Revision 5, after an external review of the implementation plan:

- Spec fixes surfaced by the review: `Wait` and the blocking leave never return nil for a canceled item; after a
  cancellation wake-up an idle body's error wins, otherwise the cause is returned (§6.3, §6.4). A failed leave has two
  end positions, inside the fan-out or in the target's waiting room, and a retried move must not take a second queued
  slot, which is a latent bug in today's `enterUnit` (§6.4 step 4, edge cases 9, 45, 46). Declined option F no longer
  claims tasks get bound to one fan-out; they always are.
- Plan fixes: phase 1 flags every old-API collection as a root so `Started` keeps its timing; identity removal keeps
  the constant-time head path; the demo, bench and codegen mechanical migration moves into phase 5 so the dependent
  modules compile again in the phase that breaks them; the `enterUnit` waiting-room guard and the `joinPending`
  cancellation rule are phase 3 steps; tests added for the displaced-pull exhaustion path, cancellation during a
  stripped-context wait, `Wait` with a stripped context on an idle clean body, zero-task spawns, concurrent
  submissions, streaming sources blocking `Wait`, the failed leave from the waiting room, and the completion error
  policy override; completion and property assertions measure at slot assignment, not callback invocation; the leak
  check inspects the captured run instead of the post-run `Stats`; observability docs and godoc for `debug.go`,
  `stats.go` and `docs/7_observability.md` added to phase 7; the demo test no longer relates the pending marker to
  the follower's admission; demo comments describe fills by their derivation, not as `Started`/`Finished`
  equivalents.
- Phase 9 rewritten: no hash version bump, the full UI wiring listed, the badge animation problem named, and the fill
  semantics corrected. Still optional and pending a decision.
- Ground rules corrected: a few helpers reach into internals; repeated runs use a package command with a name
  selector.

Revision 4:

- Added §11, the implementation plan: ground rules and verification commands (root, demo and bench modules, web
  build), then ten phases with checkboxes. Phases 1 to 4 change the runtime under the old API so the existing tests
  stay green; phase 5 switches the API and migrates every test call site; phases 6 to 8 cover the remaining tests,
  the documentation, and the required demo and bench migration; phase 9 is an optional demo visualization of a
  growing body (decision pending); phase 10 is the final verification and the release tag.

Revision 3:

- Generalized the canonical-context cancellation check from `Schedule` and `TryMoveTo` to every node method,
  including the admission waits of `MoveTo`, the wait in `Wait`, and `Retain`'s decision to run its callback
  (§2, §4.1, §5.1, §6.1, §6.3, §6.4, §6.9, §9, edge case 23). Declined option O records the alternative.
- Recorded the two decisions above and removed the corresponding open question.

Revision 2, after an external design review against the source:

- Added an explicit per-item per-fan-out **body state** (`none` / `open` / `closed` / `detached`) instead of inferring
  the situation from a missing pending wave. Diagnostics no longer depend on occupancy timing (§4, §6.7, §6.11).
- The own-body path of `Schedule` requires the body to be **open**, which closes a hole where a `Retain` callback could
  add work after the processor returned and the completion path had already judged the body finished (§6.2, §6.6).
- A **blocking leave seals and closes the body at the idle check**, before the admission wait. A leave that then fails
  leaves a `closed` body; `Schedule` and `Wait` there panic with the new `errBodyClosed` (§6.4, edge case 9).
- Work whose owner is over now returns **`ErrStaleContext`** instead of panicking: a finished lane child's context
  (checked before any redirect to the parent's wave) and pool work whose wave finished. The runtime detects an expired
  wave, not an expired individual callback; the callback lifetime contract is documented (§6.2).
- `Schedule` checks the **owning item's canonical context** as well as the call context, so a caller that stripped
  cancellation cannot queue work for a poisoned item (§6.2).
- `TryMoveTo` on both node kinds also checks the **canonical context** in its preamble. The earlier "idle body with an
  error" rule was removed as unreachable: a body error always poisons the item, so the preamble answers first (§6.4).
- The ordering guarantee is now defined at **slot assignment**, with a slot reserved for a streaming pull counting as
  handed out at reservation; the displaced-pull case is covered explicitly (§6.8).
- The **no-gap property is restricted** to spawning onto the same pool while the spawner holds its slot. Lanes are
  described by child tickets; the "pool limit 1 keeps strict order" claim is limited to same-pool chains (§6.8).
- `Wave.Started` is **redefined over root sources** (those the ItemProcessor scheduled into its own body), closing when
  the wave is sealed and every root source is exhausted. Spawned sources do not count and are not covered (§6.7).
- The deadlock argument is split by **kind of slot holder**, with the assumptions about pool callbacks, streaming pulls
  and lane children stated (§6.10).
- Precision fixes: the "one lock hold" claim in the baseline; the `SetLimit` occupancy wording (release on entering the
  next node's waiting room; "whichever happens last" after `Detach`); the rank published on leaving; the completion
  error policy exceptions; the wave lifecycle table no longer shows a sealed idle-to-busy transition; identity removal
  performs exactly one backlog decrement and one exhaustion notification per collection.
- Declined options M and N added.
