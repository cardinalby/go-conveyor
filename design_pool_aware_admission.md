# FanOut: pool-aware admission (`AdmitByPools`, `AdmitByPoolsStrict`)

Status: implemented, all steps done. Two policies shipped instead of one: the design below (gate plus retained
token) is `AdmitByPoolsStrict`; option F of §1.4 (the gate alone) is `AdmitByPools`. Where the text below says
`AdmitByPools`, read `AdmitByPoolsStrict`. Bench comparison against commit 129a650 (in-package fan-out benchmarks with
benchstat, and the `bench/` medium sweep): no time regression, allocs/op unchanged, +16 B/op per item (the `hold`
pointer on `item`).
Additive change, no breaking API.

Deviations from the plan below, made during implementation:

- `taskCollection` carries `hold *upstreamHold` instead of `entering bool`, so a collection of a detached body
  cannot touch a later hold of the same item (`branchStarted` checks identity).
- `takeUnit` still runs `releaseBelow` after creating the hold; `releaseBelow` skips the held token. The hold protects
  one token: the queued slot if the item is waiting anywhere, else the highest-rank occupied unit below, whether or
  not a `Retain` wave also holds it (the slot is freed only when both have ended; see 3.2).
- A fan-out with no branches admits by limit alone under `AdmitByPools` (nothing to saturate).
- `SetAdmission` with an unknown value stores `AdmitByLimit`.
- The strict variant was renamed `AdmitByPoolsStrict`, and the gate-only variant (option F) was added as
  `AdmitByPools` after review of the demo: with variable task durations the strict hold leaves the fast pool idle
  while stuck items fill the previous stage, and the gate alone keeps the pools saturated with the queue bounded by
  the item limit. `unit.needsPoolCapacity` (both pool policies) drives `canEnter`; `unit.holdsUpstream` (strict
  only) drives the hold in `takeUnit`.
- After the external review: `completeItem` broadcasts after sealing the body (the discharge frees a slot upstream
  waiters are parked on); the capacity dials (`SetLimit`, `SetQueueSize`, `SetAdmission`) are stored under `run.mu`
  while a run is live, and `enterUnit` re-checks `canEnterQueue` before stepping into the waiting room, so one
  admission decision never straddles two values of a dial (this also closes a pre-existing window for a lowered
  `SetLimit` / `SetQueueSize`). The setters re-read the current run after the store and wake a run that started
  meanwhile, so a dial change cannot leave freshly parked waiters asleep.

This document describes the problem, the options that were declined, the new option and its exact semantics, and
the implementation steps. It follows the conventions of `design_fanout_schedule.md` and `impl_details.md`.

---

## 1. Problem

### 1.1 How a fan-out is admitted today

A fan-out is entered with `MoveTo`, like every other node. Admission needs a free item slot (`SetLimit` on the
fan-out, default 1) and the ordering gate. On admission the previous node is released (`takeUnit` calls
`releaseBelow`). Then the item adds work with `Schedule`, which never blocks: tasks are queued per branch at the
item's place, and the branch queues are unbounded. The move that takes the item out joins its work.

Pool saturation never blocks anything by itself. A saturated pool only makes joins longer. Items then sit inside the
fan-out, the fan-out fills, and the next `MoveTo` blocks. So the fan-out item limit is the only source of upstream
backpressure, and it counts items, not pool capacity.

### 1.2 What is wanted

Maximum utilization of the pools with backpressure that follows pool capacity:

- If some pool has free capacity, admit the next item so it can use it.
- If no pool has free capacity, do not admit anything. The item waits in its previous node, which is what makes the
  upstream stage feel the pressure.
- An item whose tasks all land on a saturated pool must not release its previous node until its tasks have
  started. Otherwise stuck items accumulate inside the fan-out without any upstream effect.
- A younger item whose tasks land on a pool with free slots may pass a stuck older item on the branches. Downstream
  order is unchanged: the younger item still waits at the next node's ordering gate.

The runtime does not know an item's tasks at admission (the body is empty). So admission cannot be precise: an item
admitted because `db1Write` has a free slot may need only the saturated `db2Write`. The design accepts this. The
retained upstream token is what pays for a wrong guess.

### 1.3 Example

`read` (limit 2) -> `dbsWrite` fan-out (limit 100) with pools `db1Write` (limit 2), `db2Write` (limit 1) -> `commit`.

1. Item 1 enters, schedules one task per pool. Both start. `read` is released.
2. Item 2 enters, schedules one task per pool. The `db1Write` task starts; the `db2Write` task queues. Item 2 keeps
   its `read` slot.
3. Item 3 needs only `db1Write`. `db1Write` has a free slot, so item 3 is admitted. Its task starts, `read` is
   released. Item 3 will wait at `commit` for item 2 (ordering gate, unchanged).
4. Item 4 needs `db2Write`. It is admitted (`db1Write` still has room), its task queues behind item 2, it keeps
   `read`. `read` is full now.
5. Item 5 blocks in `read.MoveTo`. Upstream feels the pressure, caused only by items stuck on the saturated pool.
6. Item 1's `db2Write` task finishes. The slot goes to item 2's task (oldest queued). Item 2 releases `read`. Item
   5 is admitted to `read`.
7. If every pool is full and every queue is non-empty, the next `MoveTo` into `dbsWrite` blocks, even with free
   item slots. Entering would only grow the queues.

### 1.4 Options declined

| # | Option | Why declined |
|---|--------|--------------|
| A | Tune the fan-out limit: `min over pools of floor(poolLimit / tasksPerItem)`. | Exact only for a fixed task count per item. Conservative otherwise. No code change; stays as documentation advice. |
| B | First `Schedule` blocks until its tasks have slots; later calls do not. | Implicit call-count semantics. A streaming source fed by the processor after `Schedule` deadlocks. Cancellation after enqueue breaks "on error nothing is queued". |
| C | Door policy: the door opens when the first round has started. | Blocks every younger item, including those that need only the free pool. |
| D | Remove `MoveTo`; the first `Schedule` is the entry. | Concurrent processor-side `Schedule` calls (allowed today) can deadlock an item against its own fan-out slot. The implicit join comes after the arguments are evaluated, so `B.Schedule(ctx, p.NewTasks(len(results), f))` reads `results` before A's body is joined. Removes a synchronization point users rely on. |
| E | `MoveTo(ctx, tasks...)` optional first round. | Still releases the previous node at admission (this is the pre-refactoring behavior). Reintroduces two scheduling methods for the recursive case. |
| F | Admission gate alone (admit while any pool has room), no retained token. | Weaker pressure: a run of items that need only the saturated pool is admitted one by one while the other pool has a free slot, so the saturated pool's queue grows — up to the item limit, not without bound as first written. Shipped as `AdmitByPools`; F and G together are the strict variant. |
| G | Retained token alone, no admission gate. | Gives the bound and the bypass, but admits items when every pool is full. Such an item cannot start anything. "Inside the fan-out" stops meaning "has a chance to run" and the item limit stops being a useful cap. |

F and G together are the chosen design for `AdmitByPoolsStrict`; F alone ships as `AdmitByPools`.

---

## 2. The new option

```go
// FanOutAdmission selects how a fan-out admits items (see FanOut.SetAdmission).
type FanOutAdmission int

const (
    // AdmitByLimit is the default: admission needs a free item slot and the ordering gate, and the previous node is
    // released on admission, like every other node.
    AdmitByLimit FanOutAdmission = iota

    // AdmitByPools: admission also needs a branch with free capacity (a free slot and an empty queue); the previous
    // node is released on admission.
    AdmitByPools

    // AdmitByPoolsStrict: AdmitByPools plus the previous node's token is retained until the item's first Schedule
    // has started on every branch it touched, so an item whose work waits for a saturated pool keeps pushing back
    // upstream while younger items whose work can start pass it on the branches.
    AdmitByPoolsStrict
)
```

Added to `FanOut`:

```go
// SetAdmission selects the admission policy and returns the fan-out for chaining.
//
// Safe to call at any time, from any goroutine, including on a running conveyor. It applies to admissions after
// the call: items already inside keep the release or hold they were admitted with.
SetAdmission(a FanOutAdmission) FanOut

// Admission returns the admission policy.
Admission() FanOutAdmission
```

`SetAdmission` is live, like `SetLimit` and `SetQueueSize`, and admission-only in the same sense:

- ByLimit -> ByPools: items waiting at the door re-check `canEnter` on the broadcast and now also need free pool
  capacity. Items admitted from then on get an upstream hold. Items already inside were admitted with a release
  and are left alone; nothing is reacquired.
- ByPools -> ByLimit: the predicate relaxes at once. Existing holds are not touched; they end on their own when the
  item's first round starts or its body is sealed (the same way a lowered `SetQueueSize` leaves items already in the
  room alone).

This is safe because `takeUnit` reads the policy and either releases or creates the hold in one lock hold, and the
hold's life cycle (3.3, 3.4) depends only on item state, never on the current policy.

No other public API changes. `MoveTo`, `TryMoveTo`, `Schedule`, `Wait`, `Detach` keep their signatures and error
sets. The door stays as it is.

---

## 3. Semantics of `AdmitByPools`

### 3.1 Admission predicate

`canEnter` for the fan-out node requires all three:

1. a free item slot under the fan-out's `SetLimit` (unchanged; the item limit stays a hard cap on items inside);
2. the ordering gate (unchanged);
3. some branch `b` of the fan-out has `unitHasFreeSlot(b) && len(taskQueues[b]) == 0`.

Rationale for the empty-queue part: a freed slot is handed to queued work under the same lock, before any waiter
wakes. A pool that has a free slot and a non-empty queue therefore has an async pull in flight on its head, and its
capacity is spoken for. The predicate means "capacity that nothing queued will consume".

The same predicate applies to `TryMoveTo` and to admission from the waiting room. `canEnterQueue` is unchanged: an
item may still step into the waiting room when the pools are full.

Wake-ups: every place that frees a pool slot or empties a queue already broadcasts (`runWork`, `finishAsyncPull`,
`settleCollection` callers). No new broadcast is needed, but this must be verified in step 4.

### 3.2 The upstream hold

On admission under `AdmitByPools`, `takeUnit` does not run `releaseBelow`. Instead the item records an **upstream
hold**: the release that admission would have done is deferred. The hold protects whatever the item would have given
up at that moment:

- the slot of the node the item came from, when it was admitted directly;
- the waiting-room token, when it was admitted from the fan-out's waiting room (the previous node was already
  released by `takeQueue`; see 3.6);
- the start unit's slot for an item entering from the start stage, which is what throttles item creation.

The hold is a new kind of ownership, separate from `wave.retainUnit`:

- `releaseBelow` skips the held token, the same way it skips a unit that `isRetaining` reports. This is what protects
  the hold from the sweep an unrelated `Retain` completion triggers (`releaseRetained` -> `releaseBelow`).
- Discharging the hold clears it and then runs `releaseBelow(it, it.reachedRank)`. A unit that an explicit `Retain`
  wave still holds stays held: a token is released only when every hold on it has ended.
- The hold is discharged exactly once. Every discharge path goes through one function.

### 3.3 Discharge trigger: the entering submission has started

The **entering submission** is the item's first root `Schedule` at this fan-out (the call that opens the door). Its
collections are marked. The hold tracks the set of branches the submission touched.

A branch is removed from the set when:

- a task from the marked collection is assigned on it (`startWork` for that collection; for a lane this is child
  creation at the lane's start gate, not admission to an interior stage); or
- the marked collection leaves the queue without assigning anything (exhausted with zero yield, or dropped on
  cancellation).

When the set is empty, the hold is discharged. This is "one assignment per touched branch", not "every task handed
out": with `NewTasks(100)` on a limit-2 pool the hold ends at the first assignment, not after 98 completions. The
question the trigger answers is "can this item run", not "has this batch been dispatched".

Consequences to document:

- An entering submission with zero tasks (or only statically empty tasks) discharges at once.
- Only the first root submission counts. Later rounds and follow-ups scheduled by tasks never hold upstream.
- Packaging matters: an item that enters with one task and schedules the rest later releases upstream sooner than an
  item that enters with everything. This is by design and gives the processor control.

### 3.4 Terminal discharge paths

The hold is also discharged when the body is sealed, whichever comes first. `item.sealBody` is the single funnel
for that, so the discharge lives there. It covers:

- a successful leave (`joinPending` -> `closeBody`);
- `Detach`;
- processor return (`completeItem` seals the body before joining its waves).

A failed leave with running work keeps the body open (see `joinPending`) and therefore keeps the hold. This is
correct: the item is still stuck.

Item cancellation: the marked collections are dropped by `grabNext` (`dropCollection`), which removes their
branches from the set and discharges the hold. `finishItem` frees every slot anyway; the hold must be cleared there
too so no dangling state survives the item.

`Wave.Started` is not reused. It needs a sealed wave and covers every root round, so it can never fire while the
body is open.

### 3.5 Ordering and invariants

- Rank publication is unchanged: the fan-out's rank is published at the first `Schedule`, leave or `Detach`. An
  older item that still holds `read` while having published the fan-out's rank does not violate "`maxRank` is
  non-increasing with item age": ownership and published progress are separate state.
- Bypass is bounded. A younger item passes a stuck older item only while the fan-out has a free item slot and some
  pool has free capacity. Fast items that have finished but wait at the next node's ordering gate also hold fan-out
  slots. The fan-out limit therefore bounds the lookahead and must be raised deliberately. With the default limit 1
  there is no bypass at all.
- Per-branch item order is unchanged (`insertCollection`).
- No new wait cycle: the two-token holder waits only for pool slots. Pool slots are held by running tasks, which
  never wait, or by lane children, which drain through their own series. Younger items that passed wait at the next
  node's gate, which opens when the older item passes.
- Application dependencies are the caller's responsibility: a producer or a background operation of item N must not
  wait for item N+1 to be admitted. Under `AdmitByPools` item N may be the one holding N+1 back. This rule holds
  today for `Retain` and is only more visible here.

`impl_details.md` §13 needs the wording of the invariants that mention "the door" and "one slot" reviewed (see
step 8).

### 3.6 Waiting room

A waiting room in front of an `AdmitByPools` fan-out weakens the upstream pressure to the size of the room, because
`takeQueue` releases the previous node when the item steps aside. The hold then covers the queue token. This is
documented, not forbidden. A full waiting room still blocks admission to it, so the pressure is delayed, not lost.

### 3.7 Observability

A held token shows as occupancy in the previous node (or as a queued token) for an item that is already inside the
fan-out. `DebugUnitOccupants` and `UnitStat` describe a queued token as "waiting for admission"; the wording must
say that under `AdmitByPools` it may be a retained token of an admitted item.

### 3.8 Streaming sources

`NewTasksGen` and `NewTasksChan` stay. Under `AdmitByPools` a streaming source in the entering submission keeps the
hold until its branch gets its first assignment, which for a channel source means the first successful receive. The
processor is never blocked, so it can feed the channel. The application-dependency rule in 3.5 applies.

---

## 4. Implementation steps

### Step 1: option and builder

- [x] Add `FanOutAdmission` with `AdmitByLimit` (zero value) and `AdmitByPools` to `fanout.go`.
- [x] Add `SetAdmission` and `Admission` to the `FanOut` interface and `fanOut`; store the policy on `fanOut`.
- [x] Store the policy as an atomic on the node `unit` (next to `limit` and `queueSize`), so `canEnter` and
      `takeUnit` read it under `mu` at admission time.
- [x] `SetAdmission` stores the value and, if a run is active, broadcasts so waiters re-check admission (same shape
      as `setQueueSize`). No topology change, so it is safe on a live conveyor.

### Step 2: admission predicate

- [x] Add `run.branchHasFreeCapacity(f *fanOut) bool`: some branch with `unitHasFreeSlot(b) && len(taskQueues[b]) == 0`.
- [x] In `canEnter`, when `u` is the node unit of an `AdmitByPools` fan-out, also require `branchHasFreeCapacity`.
- [x] Confirm `tryEnterUnit` goes through `canEnter` (it does today) so `TryMoveTo` inherits the predicate.
- [x] Leave `canEnterQueue` unchanged.

### Step 3: the upstream hold

- [x] Add to `item`:
  - `hold *upstreamHold` (nil when none);
  - `upstreamHold{ queued bool; unit int; awaiting map[int]struct{} }` where `unit` is the previous node's index
    (or -1) and `awaiting` is the set of branch indices of the entering submission not yet started.
- [x] In `takeUnit`: if the target is an `AdmitByPools` node and the item has no hold, create the hold instead of
      calling `releaseBelow`. Record the token: `queued = (it.queuedAt == u.index)`, else the highest-rank occupied
      unit below `u.rank` (a unit a `Retain` wave holds too is recorded, so the slot is freed only when both have
      ended). Broadcast as before.
- [x] In `releaseBelow`: skip `leaveQueue` when `it.hold != nil && it.hold.queued`; skip a unit `j` when
      `it.hold != nil && it.hold.unit == j`.
- [x] Add `run.dischargeHold(it *item)`: no-op if nil; otherwise clear `it.hold`, then `releaseBelow(it, it.reachedRank)`,
      then broadcast (caller holds `mu`). Single funnel; every path below calls it.
- [x] In `finishItem`: clear `it.hold` before the sweep (the sweep frees everything anyway).

### Step 4: discharge trigger

- [x] Add `entering bool` to `taskCollection`.
- [x] In `addToBody`: when `root` is true, the body has had no root submission yet, and the item has a hold at this
      fan-out, mark every new collection `entering = true` and set `hold.awaiting` to the touched branch indices.
      With no touched branches, call `dischargeHold` at once.
- [x] Track "first root submission done" on the wave (a bool set in `addToBody`), so only the first call marks.
- [x] In `startWork`: if `col.entering`, delete `branchIdx` from `hold.awaiting`; if empty, `dischargeHold`.
- [x] In `dequeue`: if `col.entering` and the branch is still in `awaiting` (collection left without assigning),
      delete it; if empty, `dischargeHold`. This covers zero-yield exhaustion and `dropCollection`.
- [x] In `item.sealBody`: call `dischargeHold` (covers leave, `Detach`, processor return). `sealBody` needs access
      to `run`; it already receives the wave, which has `w.run`.
- [x] Verify the wake-ups: every path that frees a pool slot or empties a queue broadcasts, so an item waiting on
      `branchHasFreeCapacity` re-checks. Add a broadcast where missing.

### Step 5: stats and debug

- [x] Review `DebugUnitOccupants` and `UnitStat` doc comments; state that a queued token or a previous-node slot
      may belong to an item already admitted to an `AdmitByPools` fan-out.
- [x] Decide whether `Stats` should expose held tokens separately. Default: no new field; document only.

### Step 6: tests (`admission_test.go`)

Deterministic, using `assignHook` and the existing helpers where possible.

- [x] Admission blocked while every pool is full; admitted when a slot frees and its queue is empty.
- [x] Admission blocked while a pool has a free slot but a non-empty queue (async head in flight).
- [x] Wrong-pool item keeps its previous slot; right-pool younger item passes it on the branches and still waits at
      the next node's ordering gate.
- [x] Upstream fills with stuck items and the stage before it blocks (the §1.3 walk-through).
- [x] Hold released at first assignment per touched branch, not at full dispatch (`NewTasks(100)` on limit 2).
- [x] Entering submission with zero tasks discharges at once. Statically empty tasks too.
- [x] Later rounds and task follow-ups never create a hold.
- [x] Unrelated `Retain` completion during a hold does not free the held token.
- [x] Explicit `Retain` on the previous stage plus a hold: the token is freed only when both have ended.
- [x] Waiting room: the hold covers the queue token; discharge frees it.
- [x] Entering from the start stage: item creation is throttled while the hold lasts.
- [x] Cancellation with the entering collection still queued: `dropCollection` discharges the hold; exact occupancy
      cleanup after `finishItem`.
- [x] Processor return with an open body: `completeItem` seals and discharges.
- [x] `Detach` discharges.
- [x] Failed leave (canceled call context, work still running) keeps the hold; a retry that succeeds discharges.
- [x] `TryMoveTo` under `AdmitByPools` declines when no pool has capacity, without mutating anything.
- [x] `SetLimit` lowered on a pool while a hold waits: no eviction, admission resumes when capacity appears.
- [x] Lane branch: the hold ends at child creation, not at interior-stage admission.
- [x] `SetAdmission` on a live conveyor: ByLimit -> ByPools blocks the next admission while pools are full and
      gives it a hold; items already inside have none. ByPools -> ByLimit admits waiters at once and leaves existing
      holds to end naturally.
- [x] Property test (`property_test.go`): rank order and "no slot outlives its item" hold under `AdmitByPools` with
      random topologies.

### Step 7: docs

- [x] `docs/4_fan-out.md`: new section "Pool-aware admission" with the §1.3 walk-through, the bounded-bypass note,
      the packaging note, and the waiting-room note. Reword "all pools are fully utilized" in the benefits list.
- [x] `README.md`: one line in the features list.
- [x] `docs/4_fan-out.md` "Ordering" section: add the pre-existing starvation case (an older item's task that
      always schedules one replacement onto a limit-1 pool keeps a younger item's queued task from ever running).
- [x] Doc comments on `MoveTo`: "releases the previous node on admission, unless the fan-out uses `AdmitByPools`;
      see `SetAdmission`".

### Step 8: `impl_details.md`

- [x] Add a section on the upstream hold next to Retain/Detach ("a token that follows the work").
- [x] Review §13 invariants: 1 and 8 (ownership accounting) mention the hold; 4 ("one slot") is qualified for held
      tokens; the door wording stays valid because the door is unchanged.
- [x] Fix the stale header of `design_fanout_schedule.md` ("not implemented"), noticed during review.

### Step 9: verification

- [x] `go test -race ./...`
- [x] `go vet ./...`
- [x] Run `bench/` and compare against the current numbers for `AdmitByLimit` (no regression: the new checks are
      behind a policy flag on the node unit).

### Step 10: demo (`demo/`)

Add an **Admission** dropdown to the fan-out node box, next to the Limit and Queue fields, with the values
`by limit` (default) and `by pools`. It is live, like the other two fields.

- [x] Schema: add `Admission string` (`"limit"` | `"pools"`, default `"limit"`) to the fan-out node in
      `demo/internal/topology/schema.go`; apply it in `build.go` (`SetAdmission`) next to `SetLimit` / `SetQueueSize`.
- [x] Live change: `Manager.SetAdmission(id, value)` in `demo/internal/runtime/manager.go`, and a `setAdmission`
      case in `demo/internal/wasmapi/handler.go` (same shape as `setQueueSize`, string body instead of int).
- [x] Web state: add `admission: "limit" | "pools"` to the fan-out node type (`types/state.ts`), the factory default
      (`pipeline/factory.ts`), `toSpec.ts`, `resolve.ts` (equality and merge), and `App.tsx` `onEditNode` handling
      (`api.setAdmission`).
- [x] URL state (`state/urlState.ts`): add the field to the fan-out shape, omit it when default so existing links stay
      short; decoding a link without it yields `"limit"`. Bump the URL format version only if the decoder cannot
      default the missing field.
- [x] UI: a `<select>` in `components/nodes/FanOutBox.tsx` with the two values, wired to `onEditNode(id, "admission", v)`.
      Reuse the field styling of the Limit / Queue inputs.
- [x] Code panel (`codegen/generateGoCode.ts`): emit `.SetAdmission(conveyor.AdmitByPools)` when the value is not
      the default.
- [x] Visualization of the hold: an item admitted under `by pools` whose first round has not started still occupies a
      slot in the previous node. Show it there as a dimmed or outlined token (the item appears in two places) so the
      backpressure is visible. Check what the wasm snapshot reports for such an item (`DebugUnitOccupants`) and
      extend the snapshot if the previous-node slot is not reported.
- [x] Preset: add a scenario link to `docs/4_fan-out.md` (fast `db1Write`, saturated `db2Write`, `read` limit 2,
      fan-out limit high, `by pools`) so the walk-through of §1.3 can be watched.
- [x] Legend (`components/LegendPanel.tsx`): one entry for the held-token visual.

---

## 5. Out of scope

- `AdmitByPools` for lanes' interior capacity. The trigger sees child creation only.
- Bounding later rounds or recursive backlog. Branch queues stay unbounded; the fan-out limit and this policy bound
  items, not tasks.
- Batch-dispatch backpressure ("hold until every task of the entering submission has been handed out"). Fits the
  same mechanism; add as a separate policy value if a use case appears.
- Removing `NewTasksGen` / `NewTasksChan`. Not required by this change.
