# Implementation details

Internal design notes for maintainers of the package. The public contract is in the package docs
(`conveyor.go`, `stage.go`, `fanout.go`, `branch.go`, `wave.go`) and the README; this document describes how the
runtime behind them is built and which invariants it rests on.

## 1. The core idea: one capacity primitive

Everything the user builds is made of **units**. A unit is a counted set of **slots**. Taking a slot is the only
way to be somewhere in the conveyor, and an item (or a piece of a branch's work) always holds at least one slot from the
moment it is created until it finishes. That is the invariant the whole design rests on: **no state without a
token**.

Moving is "take the next slot, then let go of the old one". Ordering, backpressure, deadlock-freedom and the bound
on how many items are alive all follow from that.

| node / thing built     | units it owns                        | what a slot means                          |
|------------------------|--------------------------------------|--------------------------------------------|
| Conveyor (root series) | 1 start unit                         | an item that exists but has not moved yet  |
| Stage                  | 1 work unit                          | an item running the stage's code           |
| FanOut                 | 1 node unit                          | an item inside the fan-out                 |
| Pool                   | 1 start unit (of an empty series)    | a task running                             |
| Lane                   | 1 start unit (of the lane's series)  | a child item that has not moved on yet     |

A **branch** is a `Pool` or a `Lane` — the two kinds of thing a fan-out fans out to. They share one implementation
(`branch`) and one unit apiece; the only difference is whether the series that unit gates ever gets nodes. `AddPool`
and `AddLane` are the *same* call, differing only in the interface handed back, and which one was used is not
recorded: what governs behaviour is `branch.travels()` — "does this branch have nodes?" A `Pool` has no way to add
any, so its answer is fixed at build time. A `Lane` left without nodes answers `false` too, and so behaves as the
pool it is (legal, and pinned at concurrency 1 since `Lane` has no `SetLimit`).

A unit has two capacities:

- `limit` — how many items may be *in* the node (running its code, or inside the fan-out);
- `queueSize` — how many more may wait in front of it, having already released the previous node.

The waiting room is **not** a unit of its own: it is queued slots counted separately (`run.queued`) and held at the
node's reserved lower rank. That is what makes `queueSize` a plain atomic dial no item's journey depends on the
existence of, and so changeable on a live conveyor.

## 2. Topology

The whole topology is one recursive shape:

```
series = a start gate (one unit) + an ordered list of nodes
node   = a Stage, or a FanOut (a group of branches)
branch = a series again — empty of nodes for a Pool, populated for a Lane
```

The conveyor itself is the root series; its start gate is the implicit start stage that paces item creation. A branch
is a series whose start gate is the branch's own unit. "Sub-conveyor" is not a separate concept — it is just a `Lane`,
i.e. a branch given nodes of its own. A lane's unit keeps the default limit of 1, so it paces child creation exactly
as the conveyor's start paces items; a pool's is the dial `SetLimit` turns.

Three numbers describe a node:

- **`index`** — unique across the conveyor, assigned in creation order (`Conveyor.newUnit`). Indexes the per-run
  arrays (`occupancy`, `queued`, `taskQueues`) and the per-item arrays (`occupied`, `entered`). The implicit start
  stage is always index 0.
- **`rank`** — position within its series (scope). Assigned at `finalize`, and only ever compared between units of
  the same scope. Drives ordering and release.
- **`ord`** — 1-based position among the nodes of its series, assigned at creation. Used only for the positional
  name of an unnamed node, so that name does not move when ranks do.

**Every node reserves two consecutive ranks:** its waiting room takes the lower one (`unit.queueRank() == rank-1`)
and the node itself the upper one. The pair is reserved whether or not a queue exists, so a queue added later slots
into a rank that was always its own and nothing is renumbered — which is what lets `SetQueueSize` run on a live
conveyor. The two ranks are also what keep a queue honest: an item waiting in front of a node publishes the lower
rank, so the ordering gate still holds the item behind it out of the node, and a `TryMoveTo` cannot take a slot
from under it.

`Conveyor.finalize` walks every series once, assigns scopes and ranks (start gate = rank 0, then `+2` per node),
caches `scopeUnits` for the release path, and freezes the topology. It runs under `runMu` from `tryRun` on the
first `Run`, and idempotently from `newRun`.

## 3. Per-run state

`Conveyor` is immutable topology shared across `Run` invocations — except the two atomic capacities. All mutable
state lives on `run`, allocated fresh per `Run`, so nothing leaks between invocations. `Conveyor.currentRun` is an
atomic pointer to the active run (nil outside a run); it backs `Stats` and lets `SetLimit` / `SetQueueSize` reach a
live run.

Everything below `run.mu` is guarded by it:

- `occupancy[]`, `queued[]` — per unit index, windowed for `Stats`;
- `taskQueues[]` — per-branch FIFO of `*taskCollection`, in item order;
- `scopes[]` — the in-flight item list of each scope (0 = the root series, one per branch);
- `inFlight`, `liveWorkers` — windowed gauges;
- the pool bookkeeping (`idle`, `spawning`, `stopCreating`, `nextItemNo`, `runErr`).

`run.cond` is **broadcast, never signaled**, on every mutation: waiters block on different conditions, so a
targeted wake is not possible and all of them must re-check. `waitUntil` is the single blocking helper — it checks
the cancellation cause first, then the predicate, and arranges a broadcast on ctx cancellation via
`context.AfterFunc` (a `sync.Cond` does not wake on cancellation). On a nil return the caller still holds the lock
and the predicate is true, so it may mutate before releasing.

## 4. Items

An `item` is one journey through a series. Both kinds are items:

- a **root item** — one `ItemProcessor` call, moving through the conveyor's own nodes (scope 0). It owns a context
  derived from `run.itemsCtx` and its own `cancel`.
- a **child item** — one piece of a `Lane`'s work, moving through its interior nodes (the lane's scope).
  It inherits its parent's item number and context, owns no `cancel` (cancellation is item-wide, so `poison`
  escalates to the parent), and reports its outcome to the wave that created it (`parentWave`).

Position is tracked by two ranks per item:

- **`reachedRank`** — the highest rank the item has actually occupied. It says where the item *is*.
- **`maxRank`** — the highest rank it has *published*. It says what the items behind it are allowed to see.

They differ only between a fan-out's admission and its enqueue: an item admitted to a fan-out has reached the node,
but must not open the gate for the next item until its work is on the branches (see §5).

Each scope keeps its in-flight items in a doubly-linked list in creation order (`scopeList`). That gives an O(1)
ordering gate — each item checks only `it.prev` — and ordered iteration for the shutdown cascade. Item numbers are
*not* used for any ordering decision.

## 5. Admission and ordering

Three checks, deliberately separate:

- **`checkEnterOrder`** — permanent misuse, panics: moving backward (`target.rank < it.reachedRank`) or re-entering
  a node (`it.entered[target.index]`). Compared against the node's own rank, never its waiting room's.
- **`canEnter`** — transient: a free slot (`unitHasFreeSlot`) **and** ordering
  (`it.prev == nil || it.prev.maxRank >= u.rank`).
- **`canEnterQueue`** — the same two questions asked of the waiting room: room in `queued[]` versus `queueSize`,
  and `it.prev.maxRank >= u.queueRank()`.

`enterUnit` is the blocking move:

1. `joinPending` — if the item is in a fan-out with work it has not detached, wait for that work first. This
   precedes even the step into the waiting room, since stepping aside would release the fan-out's slot while its
   work still runs.
2. Wait for `canEnter || canEnterQueue`. One wait with two ways forward, rather than a decision up front about
   which to wait for: an item walks straight into a free node and never touches the waiting room, and because both
   are re-tested on every wake-up, a waiting room created or enlarged while an item is already waiting takes effect
   for that item at once.
3. If only the waiting room is open, `takeQueue` (take a queued slot, publish the *queue* rank, release everything
   held behind), then wait for `canEnter`.
4. `takeUnit`: `occupy` + `releaseBelow(u.rank)` + broadcast.

`publish` controls whether taking the target raises `maxRank`. A stage publishes immediately; a fan-out passes
`false` and publishes in `scheduleWave`, once the tasks are enqueued — that is what keeps each branch's work in item
order. Stepping into a waiting room always publishes, whatever `publish` says: the item really is in front of the
node, and the rank it publishes there is the waiting room's, which still holds the follower out of the node.

`tryEnterUnit` is the non-waiting variant. It declines, mutating nothing, when the item's fan-out work is
unfinished or `canEnter` is false. It **bypasses the waiting room** on purpose: stepping into it would admit the
item — releasing the previous node, spending its once-per-node entry — and then leave it blocked with no way back,
which is the opposite of what a non-waiting entry promises. Bypassing does not let it jump an item already waiting
there: that item published only the lower rank, so the ordering gate refuses. If the pending wave has already
finished and failed, the error is returned instead, since there is no wait to avoid.

**Caller contract the O(1) gate relies on:** an admission caller must hold `mu` continuously from the `canEnter`
check through the `occupy` mutation, and must pass the ordering part before mutating. Letting a later item advance
past an earlier one would break the "`maxRank` is non-increasing with item age among the items of a scope"
invariant, and with it the correctness of checking only `it.prev`.

## 6. Release

`releaseBelow(it, beforeRank)` frees every slot the item holds in units **of its own scope** with rank strictly
below `beforeRank`, skipping a unit whose slot a live wave is holding (`item.isRetaining`). A queued slot is
released by the same rank rule, which is what makes the waiting room disappear the moment the item is admitted: the
node's rank is one above its waiting room's, so the ordinary `releaseBelow` inside `takeUnit` performs the swap.

`freeUnit` releases all of the item's slots in one unit and, for a branch's start gate, `pump`s the branch — the next
queued piece of work may start now.

`finishItem` sweeps **every** unit of the conveyor, not just its own scope's, because an item can hold a slot
outside its scope: a pool's work cannot travel, so its slot is charged to the scheduling
item, which lives in the parent scope. By then `completeItem` has already joined all the item's waves, so those
slots are gone — the wider sweep is a backstop that keeps "no slot outlives its item" true of the function itself
rather than of the order its callers run in.

## 7. Fan-out and the branch runtime

### Tasks and sources

A `Task` is `{branch, src}`. A `taskSource` is a **lazy, stateful, single-use** producer of callbacks; `claim()` is
what detects a `Task` submitted twice. Sources come in two kinds:

- **sync** (`singleSource`, `countSource`): `pull` runs no user code, so the scheduler calls it under `run.mu` —
  the fast path, preserving the atomic slot-reuse of the branch workers.
- **async** (`genSource` over `iter.Pull`, `chanSource`): `pull` runs user code and may block, so it is called
  **without** `run.mu`, single-flight per collection (`taskCollection.pulling`), with the slot reserved for the
  duration. Async sources treat item-ctx cancellation as exhaustion.

`release()` gives up whatever a source still holds, for work that will never run — it is what stops a suspended
generator. Like an async pull it must be called **outside** `run.mu`, since resuming a generator to unwind runs the
user's deferred code.

### Scheduling

`FanOut.MoveTo` → `run.scheduleWave` groups the tasks into one `taskCollection` per branch (in argument order),
enqueues them atomically in item order, publishes the node's rank, and starts the work. The grouping — which claims
the sources, and so is where the single-use panic fires — happens **before** the wave is created on purpose: a
panic must not leave the item owning a wave that nothing will ever settle, or the item could never complete.

Each branch's queue holds collections in item order, and a collection's sources are drained front to back — which is
what makes the order tasks were added to `Tasks` the order their work starts in. Work is pulled from the **head**
collection one freed slot at a time, so all of an older item's work starts before any younger item's on the same
branch. A collection is dequeued as
soon as its work has all been handed out, so the branch's `Queued` gauge counts items with work **not yet started**,
never work merely still running.

### Running

`pump` starts as many pieces as free slots and pullable work allow, spawning one `branchWorker` per piece. `grabNext`
fills one slot from the head collection and has three outcomes: sync work pulled inline and fully accounted; an
async pull reserved and handed to the caller to finish outside the lock (`finishAsyncPull`); or nothing to start.
If the head collection's item is canceled, `grabNext` drops the collection (`dropHead`) and retries with the next
one — every source kind then behaves the same way on cancellation, and the branch serves the next item at once.

`startWork` decides where the slot is charged:

- a **`Lane`** (`travels()` is true): a child item is created and holds the branch's start gate itself, giving it up on its
  first move;
- a **`Pool`** (`travels()` is false): the work cannot travel, so the slot is charged to the scheduling item for the duration,
  and the callback gets a cached non-movable context (`withPoolWork`) whose use in a `MoveTo` panics.

`branchWorker` runs its piece, then keeps the slot busy with the next queued piece — possibly another item's — and
exits only when nothing is pullable or the limit has been lowered below the current occupancy.
**Release-on-completion** is what makes an item that scheduled more work than a branch's capacity drain through its
own completions; the **limit re-check on reuse** is what makes a `SetLimit` decrease actually shrink the pool.

## 8. Waves

A `wave` is the handle for background work charged to one item: a `Stage.Retain` bgOp, or the work scheduled at a
fan-out. It tracks two counters, both under `run.mu`:

- `unexhausted` — collections that may still produce work. At zero, `Started` closes.
- `running` — tasks (or child items) started but not finished. At zero, with `unexhausted` zero, `Finished` closes
  and `releaseRetained` runs.

`retainUnit` is the unit whose slot the wave holds until its work is done — the stage of a `Retain`, or the node of
a `Detach`. It is nil while a fan-out's work is still the node's body: then the *item* holds the slot, because it
cannot leave until the work is done. `releaseRetained` frees the slot only if the item has already moved past that
node; if the item is still in it, the item's next move does the freeing (`releaseBelow` stops skipping the unit
once the wave has finished). Those two halves are the "whichever happens last" contract: an item never sits in a
node holding nothing, and a slot never outlives the work it was kept for.

`atNode` names the fan-out whose body the wave is. It supplies the node name in the error of an implicit join, and
it is what lets `Detach` tell this wave apart from one the item detached at an earlier fan-out and is still
carrying.

Two ways an error reaches a wave:

- `recordErr` — the wave's own work failed. First error wins, and it **poisons** the owning item (fail-fast) so
  siblings and the ItemProcessor abort promptly.
- `recordAbandoned` — work was dropped without running because the item was canceled. It records the cancellation
  cause but does **not** poison (the item is already canceled by definition). Without it a wave would settle clean
  whenever its work was skipped rather than failed, and a caller could not tell "all my work ran" from "most of it
  was thrown away" — which is the one thing a wave must never be ambiguous about, since it is what a pipeline uses
  to decide whether the item's effects are complete.

`acked` records that the outcome was observed — by a join, or by an `Err()` call after the wave finished. An
unacked error fails the item at completion, so a failure can be delayed but never lost.

## 9. Worker pools

There are **two** kinds of worker goroutine, and they must stay separate.

**Item pool** (`run.worker` / `acquireItem`): one worker per in-flight root item, running the ItemProcessor.
Workers self-propagate — creating an item spawns a replacement when nobody is standing by — and at most one worker
parks idle; extras retire, so the pool shrinks back after a burst instead of leaving a herd of idle waiters that
every broadcast wakes. "Standing by" is `idle + spawning`: `spawning` counts workers started but not yet arrived at
`acquireItem`, without which a fast ItemProcessor would spawn a fresh goroutine per item, unboundedly.

**Branch workers**: one per grabbed branch slot, described in §7.

Do not merge them. There is a producer/consumer dependency — an ItemProcessor worker blocks inside a `MoveTo`
waiting for branch work to finish. If both ran on one bounded pool, a full set of ItemProcessor workers all blocked
on tasks would leave no worker to run those tasks (pool-inversion deadlock). Making a shared pool unbounded avoids
the deadlock but saves nothing and would forbid ever capping in-flight items. They also share only a shape
(loop-pull-until-empty); the substance is tightly coupled to `run.mu` / `cond` / `occupancy`, and a branch's capacity
is shared *across items*, so slot accounting must live in `run` state — which also rules out an `errgroup` with
`SetLimit`, whose limit is per-group.

## 10. Errors, cancellation and shutdown

**Fail-fast within an item.** The first error from a task or a bgOp is recorded on its wave and poisons the item's
context, so siblings and the ItemProcessor abort promptly. A child item's failure escalates to its parent, since
cancellation is item-wide.

**`completeItem`** runs after any item's processor returns — an ItemProcessor for a root item, a `TaskFunc` for a
child. It poisons the item on a real (non-shutdown) processor error, waits for every outstanding wave, then
computes the effective error: the processor's own, else the first error from a wave nobody observed. Then:

- a **child** reports it to `parentWave.workDone`, which cancels the parent and surfaces there;
- a **root item's** real error triggers error-shutdown: record it as `runErr`, cancel every *later* item with a
  `ShutdownError` (earlier ones are left to finish), and `markShutdownLocked`.

Slots are released and the context canceled before a child's outcome is reported, so a parent joining the wave sees
the branch already released.

**Shutdown** has two triggers — the `Run` context being canceled, or an item error — and both close `shutdownCh`.
`watchShutdown` then asks the `ShutdownContextFactory` (`OptShutdownContext`) for the context that bounds the
in-flight items, passing the shutdown cause, and cancels them once that context is done — no factory, or a nil
context from it, leaves them to finish. The factory is asked at most once per run, and not at all if the run drains
just as the shutdown is noticed; its `CancelFunc` runs before `Run` returns, so a timer-backed context is released
promptly. Cancellation goes through the common
parent `itemsCtx`, so items blocked in a node method return promptly; code inside an ItemProcessor that ignores its
context is not forcibly interrupted. Items already canceled individually keep their more specific cause, since the
first cancellation of a context wins.

**`ShutdownError`** is a sealed interface, so an ItemProcessor cannot fabricate a value that `completeItem` would
mistake for a shutdown abort — `isShutdown` can therefore trust `errors.As`.

**Panics are not recovered.** A panicking callback crashes the process; callbacks own their panic safety.

### Misuse: panic vs. returned error

The conveyor is a synchronization primitive, so it follows the std-lib rule (`sync.Mutex.Unlock` of an unlocked
mutex, a negative `sync.WaitGroup`): **misuse panics; genuine runtime conditions return.** Whether the misuse is
static wiring or a dynamic per-item contract violation is irrelevant.

- **Panic**, with unexported sentinels (`errInvalidUnit`, `errWrongScope`, `errCannotMove`, `errConveyorRunning`,
  `errConveyorFinalized`, `errStageNotEntered`, `errNothingToDetach`, `errWrongEnterOrder`, `errNodeAlreadyEntered`,
  `errNilTaskFunc`, `errTaskReused`, `errForeignWave`): builder calls on a running or finalized conveyor; a handle
  from another conveyor, or one used with a context from another conveyor; a node outside the item's series; moving
  backward or re-entering a node; `Retain` / `Detach` on a node the item does not occupy; a resubmitted `Task`; a
  foreign wave; a move attempted from a pool's non-movable work. They are unexported because a caller must not branch
  on them — they exist so the package's own tests can assert via `errors.Is`. The checks that run under `run.mu`
  are safe: the deferred `Unlock` still fires during the panic unwind.
- **Return**: context cancellation and `ShutdownError`, fail-fast work errors surfacing at a join,
  `ErrForeignContext` / `ErrStaleContext` (both wrapping `ErrInvalidContext`), and `ErrConveyorAlreadyRunning`.

The context cases are the deliberate carve-out. A **stale** context (a finished item's, including one decoupled
from cancellation via `context.WithoutCancel`) is benign — a context outliving its item, like a closed-channel
receive. A **foreign** context is a mistake, but a cleanly declinable one, not a memory-unsafe wiring bug like a
foreign *handle*. A nil callback is the mirror case: the eager constructors panic, but by the time a streaming
source yields nil the misuse surfaces on an internal goroutine, so it fails the item instead.

## 11. Dynamic capacity

`SetLimit` and `SetQueueSize` write an `atomic.Int64` on the unit and broadcast the live run, if any. Both are safe
from any goroutine at any time, and both are **admission-only, never preemptive**:

- a **raise** wakes waiters so they re-check admission at once, and re-`pump`s a branch so queued work starts;
- a **lower** is picked up lazily by the admission gate, which stops handing out slots beyond the new value; work
  already running keeps its slot and finishes normally.

`unitHasFreeSlot` is the single place occupancy is compared against a limit, so a live change is observed uniformly
by `canEnter`, `pump` and the branch worker's pull-next. A limit is clamped to `>= 1`, a queue size to `>= 0`.

## 12. Observability

`windowedInt` is a value plus the min/max it has reached since the window was last reset. Every mutation goes
through `add`, so the window is always accurate; `Stats` copies and resets it under the lock. This makes the gauges
transient-accurate for a timer-driven reader, at the cost of assuming a **single consumer** — concurrent readers
would split the window between them.

There is deliberately **no push/observer model**: state mutates under `run.mu` on every enter/exit and task
start/finish, so invoking a user callback there would either hold the lock across user code or need an async
fan-out.

A branch's backlog reuses `run.queued`, the same counter a stage's waiting room uses. Nothing else is shared: the
admission side (`canEnterQueue` / `takeQueue` / `item.queuedAt`) is never reached for a branch's start gate, because
work is born there rather than moving to it.

## 13. Invariants

Break any of these and the model stops holding:

1. **No state without a token.** Every item and every running piece of work holds at least one slot, from creation
   to completion.
2. **`maxRank` is non-increasing with item age within a scope.** This is what makes the O(1) `it.prev` ordering
   gate correct. Publish only under `mu`, and never let a later item advance past an earlier one.
3. **Admission is atomic.** Hold `mu` continuously from the `canEnter` check through the `occupy` mutation.
4. **One slot per piece of work, released on completion.** A running task holds one slot and waits for nothing; a
   queued task holds nothing. This is what makes the design deadlock-free (see §14).
5. **Per-branch FIFO in item order.** Work is pulled from the head collection only, and a fan-out publishes its rank
   at enqueue rather than at admission.
6. **User code never runs under `run.mu`.** Task callbacks, generator pulls, channel receives, `Retain` bgOps and
   source releases all run outside the lock. `dropHead` therefore hands abandoned sources to their own goroutine.
7. **An error is never lost.** A wave whose error nobody acknowledged fails its item at completion; a root item's
   real error becomes `runErr`.
8. **No slot outlives its item, and no item sits somewhere holding nothing.** The two halves of the retain/detach
   handover.

## 14. Why one slot per task is deadlock-free

The classic trap: shared node `A` (limit 2) followed by exclusive `B`. Item 1 takes one slot of `A`, item 2 the
second; item 2 wants `B` (ordering-gated behind item 1), item 1 wants a *second* slot of `A` (full, held by item 2)
— both wait forever.

Modeling "I need K slots here" as K independent pieces of work, each grabbing **one** slot, running, and
**releasing on return**, splits hold-and-wait across two different entities: a running task holds one slot and
waits for nothing; a queued task holds nothing. No single entity ever holds-and-waits, so there is no cycle, even
with a branch's capacity fully shared across items. Claiming an item's tasks before submitting them — in
`scheduleWave`, under one lock hold — is the other half: the work reaches the branches atomically, in item order.

Alternatives that were considered and rejected, worth knowing before reintroducing one:

- *Atomic acquisition of a whole set of slots*: deadlock-free, but it idles capacity (an item needing 6 slots waits
  for 6 free even if 5 are idle) and gives no partial progress.
- *Age-priority on free slots*: the cycle above survives, because the holder is itself ordering-gated behind the
  waiter, so no slot ever frees to hand out.
- *Preemption / wound-wait*: only breaks the cycle by aborting in-flight work; acceptable solely for idempotent
  retriable reads, and it wastes work.

Ordering is deliberately *not* enforced *inside* a `Pool` — connections are fungible, queries independent. That is
the one place in the library where slots go first-come, first-served; many items use a pool concurrently, and the
downstream exclusive stage re-serializes the results.

## 15. Known trade-offs

- **Broadcast cost.** The single per-run `sync.Cond` is broadcast on every mutation, so wake-ups are O(waiters) per
  event. Under very high in-flight counts over very cheap nodes this shows up as spurious wakes. The mitigation in
  place is the item pool's: at most one idle worker parks, so a past burst does not leave a herd of waiters that
  every broadcast has to wake. (`releaseBelow` reports whether it actually freed anything, which would let a caller
  skip a pointless broadcast, but no caller uses that today.) Replacing the shared cond with per-item parking and
  per-node wait queues remains possible but is high risk — missed-wakeup liveness bugs — and note that "signal
  `it.next`" is not expressible with one `sync.Cond`, since `Signal` wakes an arbitrary waiter, so it collapses
  into per-item parking. Before attempting it, benchmark whether the single `run.mu` — which targeted wakes do not
  fix — is the actual bottleneck.
- **A `Stats` read resets the windows**, so there can be only one consumer.
- **Not all topologies are expressible**: windowing, feedback loops and splits without a join are outside the
  model, which always describes the path of one item.
