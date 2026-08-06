package conveyor

import (
	"context"
	"fmt"
	"sync"
)

// This file holds the lock-protected state machine that backs a run: creating items, the admission gate
// (canEnter / checkEnterOrder / enterUnit), the position transitions (occupy / releaseBelow / finishItem), and
// the lane runtime (per-lane queues + a worker per slot).
//
// All functions here assume run.mu is held by the caller unless noted. The blocking Stage / FanOut methods wrap
// them.
//
// Caller contract that the O(1) ordering gate relies on: an admission caller MUST hold mu continuously from the
// canEnter check through the occupy mutation, and MUST pass the ordering part before mutating. Letting a later
// item advance past an earlier one would break the "maxRank is non-increasing with item age among the items of a
// scope" invariant, and with it the correctness of checking only it.prev in canEnter.

// taskCollection is one item's ordered, lazily-consumed set of task sources for one lane — everything one
// FanOut.MoveTo call scheduled there. A lane queue holds collections in item order; work is pulled from the head
// collection one freed slot at a time, so all of an older item's work starts before any younger item's on the
// same lane. Consumed sources are nil'd as the collection advances, and an exhausted collection is dequeued
// immediately. Guarded by run.mu; the one exception is an async pull, which owns the current source exclusively
// while pulling is set.
type taskCollection struct {
	it     *item // the item that scheduled this work
	wave   *wave // the wave the work is charged to
	branch *branch

	// sources in submission order; entries before srcIdx are consumed and nil'd.
	sources []taskSource
	srcIdx  int
	// pulling marks an async pull in flight (running user code outside mu). While set, the collection stays at
	// the queue head untouched and no other worker may pull from it — pulls are single-flight per source.
	pulling bool

	// workCtx is the context handed to this collection's work when the lane has no interior nodes: the scheduling
	// item's context, marked as non-movable. Built once and shared by every piece of work here, so non-travelling
	// work costs no allocation per task.
	workCtx context.Context
}

// nonMovableCtx returns (and caches) the context for work that cannot travel. Caller holds run.mu.
func (col *taskCollection) nonMovableCtx() context.Context {
	if col.workCtx == nil {
		col.workCtx = withPoolWork(col.it.ctx, col.branch)
	}
	return col.workCtx
}

// exhaustedNow reports whether every source has been consumed (nothing left to pull).
func (col *taskCollection) exhaustedNow() bool { return col.srcIdx >= len(col.sources) }

// curSource returns the source to pull from next. Caller ensures the collection is not exhausted.
func (col *taskCollection) curSource() taskSource { return col.sources[col.srcIdx] }

// detachSources takes the sources that have not been consumed away from the collection and returns them, for work that
// will never run. The caller must release them (see releaseSources) OUTSIDE run.mu.
//
// Only the current source can be holding anything — the ones before it are consumed and nil'd, the ones after it were
// never pulled from — but handing back the whole tail costs nothing and needs no reasoning about which is which.
// Caller holds run.mu.
func (col *taskCollection) detachSources() []taskSource {
	rest := col.sources[col.srcIdx:]
	col.sources = nil
	col.srcIdx = 0
	return rest
}

// releaseSources lets go of sources whose work will never run. It must NOT be called under run.mu: stopping a
// suspended generator resumes it so it can unwind, which runs the user's deferred code.
func releaseSources(sources []taskSource) {
	for _, s := range sources {
		if s != nil {
			s.release()
		}
	}
}

// trim drops leading sources known to be exhausted, releasing them for GC. Sync sources report exhaustion
// eagerly (right after their last pull); async ones only after a pull has returned nothing.
func (col *taskCollection) trim() {
	for col.srcIdx < len(col.sources) && col.sources[col.srcIdx].exhausted() {
		col.sources[col.srcIdx] = nil
		col.srcIdx++
	}
}

// grabbed is one piece of lane work that has been accounted for and is ready to run: the slot is taken, the wave
// knows about it, and (on a lane with interior nodes) the child item exists and is linked.
type grabbed struct {
	col *taskCollection
	fn  TaskFunc
	// child is the child item running this work on a lane that has interior nodes, or nil when the lane has none —
	// then the work cannot travel and its slot is charged to the scheduling item instead.
	child *item
}

// scopeList is the in-flight list of one scope, ordered by creation. It gives an O(1) ordering gate (each item
// checks only its predecessor) and ordered iteration for shutdown.
type scopeList struct {
	head, tail *item
}

// newRun allocates a fresh run for one Run invocation. It finalizes the topology first (assigning ranks) so state
// built directly in tests still sees ranks; on the real path finalize already ran under runMu in tryRun.
func (c *Conveyor) newRun() *run {
	c.finalize()
	n := len(c.units)
	r := &run{
		conveyor:   c,
		occupancy:  make([]windowedInt, n),
		queued:     make([]windowedInt, n),
		taskQueues: make([][]*taskCollection, n),
		scopes:     make([]scopeList, c.nextScope+1),
		shutdownCh: make(chan struct{}),
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// --- item creation ---

// newRootItem creates a root item (one ItemProcessor call) occupying the implicit start stage, and links it into
// the root scope. Caller holds mu.
func (r *run) newRootItem() *item {
	r.nextItemNo++
	it := r.newItem(r.nextItemNo, 0)
	itemCtx, cancel := context.WithCancelCause(r.itemsCtx)
	it.ctx = withItem(itemCtx, it)
	it.cancel = cancel
	r.inFlight.add(1)                       // InFlight counts the conveyor's own items, not the children of lane work
	r.occupy(it, r.conveyor.units[0], true) // the start stage: rank 0 of the root scope
	return it
}

// newChildItem creates a child item for work on a lane that has interior nodes, occupying the lane's start gate,
// and links it into the lane's scope. It inherits its parent's number and context — cancellation is item-wide, so
// a child owns no cancel of its own (see item.poison). Caller holds mu.
func (r *run) newChildItem(col *taskCollection) *item {
	l := col.branch
	it := r.newItem(col.it.no, l.series.id)
	it.parentWave = col.wave
	it.ctx = withItem(col.it.ctx, it)
	r.occupy(it, l.start, true) // the lane's start gate: rank 0 of the lane's scope
	return it
}

// newItem allocates an item of the given scope and appends it to that scope's in-flight list. Items are appended in
// creation order, which is what keeps each list ordered. Caller holds mu.
func (r *run) newItem(no int64, scope int) *item {
	n := len(r.occupancy)
	it := &item{
		no:       no,
		run:      r,
		scope:    scope,
		occupied: make([]int, n),
		entered:  make([]bool, n),
		queuedAt: -1,
	}
	list := &r.scopes[scope]
	it.prev = list.tail
	if list.tail != nil {
		list.tail.next = it
	} else {
		list.head = it
	}
	list.tail = it
	return it
}

// --- admission ---

// canEnter reports whether it may enter unit u right now: both capacity (a free slot) and ordering (its binding
// predecessor in the same scope has already published u's rank or beyond). It is the transient part of admission
// (callers wait and re-check); the permanent order/re-entry misuse is caught separately (as a panic) by
// checkEnterOrder.
func (r *run) canEnter(it *item, u *unit) bool {
	if !r.unitHasFreeSlot(u.index) {
		return false
	}
	if it.prev != nil && it.prev.maxRank < u.rank {
		return false
	}
	return true
}

// unitHasFreeSlot reports whether unit j has a free slot right now, reading the unit's current limit (which
// SetLimit may change concurrently). Every limit is always >= 1, so a plain occupancy-vs-limit compare suffices.
// It is the single point where occupancy is compared against a limit, so a live SetLimit is observed uniformly by
// every admission path (canEnter, pump, the lane worker's pull-next). Caller holds mu.
func (r *run) unitHasFreeSlot(j int) bool {
	return r.occupancy[j].val < int(r.conveyor.units[j].limit.Load())
}

// hasItemsRoom reports whether the conveyor's items-in-flight cap (SetItemsLimit) allows another root item to be
// created right now; a limit <= 0 means unlimited. It is the sole gate acquireItem adds on top of the start
// stage's own admission, so a live SetItemsLimit is observed the same way a live SetLimit is. Caller holds mu.
func (r *run) hasItemsRoom() bool {
	limit := r.conveyor.itemsLimit.Load()
	return limit <= 0 || int64(r.inFlight.val) < limit
}

// canEnterQueue reports whether it may step into the waiting room in front of unit u right now: room there, and
// the same ordering rule as any other move, applied to the waiting room's own (lower) rank. Its counterpart for
// the node itself is canEnter; the two differ only in which counter and which rank they read, which is what keeps
// a queued item from letting a follower past it. Caller holds mu.
func (r *run) canEnterQueue(it *item, u *unit) bool {
	if r.queued[u.index].val >= int(u.queueSize.Load()) {
		return false
	}
	if it.prev != nil && it.prev.maxRank < u.queueRank() {
		return false
	}
	return true
}

// checkEnterOrder enforces forward-only, once-per-node movement: a target behind the item's high-water rank is
// backward, and a node the item has ever entered cannot be re-entered. Both are misuse of the contract —
// programmer errors with no benign occurrence — so they panic rather than return, consistent with the package's
// other misuse checks. The caller holds mu; the deferred Unlock still runs during the panic unwind, so the run is
// not left locked.
func (r *run) checkEnterOrder(it *item, target *unit) {
	// The comparison is against the node's own rank, not its waiting room's: reaching the waiting room's rank means
	// being queued in front of *this* node, which is not a position this item can be in while calling MoveTo, and
	// treating it as one would report a re-entry as a backward move.
	switch {
	case target.rank < it.reachedRank:
		// reachedRank, not maxRank: an item that has been admitted to a fan-out but not yet enqueued its work is
		// already there, even though the items behind it cannot see that yet.
		panic(fmt.Errorf("cannot move to %s: item has already advanced to %s; %w",
			target, r.conveyor.describeRank(it.scope, it.reachedRank), errWrongEnterOrder))
	case it.entered[target.index]:
		panic(fmt.Errorf("cannot re-enter %s: %w", target, errNodeAlreadyEntered))
	}
}

// enterUnit performs the move: through the node's waiting room if it has one, then into the node itself, releasing
// everything the item held behind it at each step (so the item behind can move up as early as possible).
//
// publish controls whether taking the target raises the item's high-water rank, which is what opens the ordering
// gate for the item behind. A stage publishes immediately; a fan-out defers it until its work is enqueued (see
// FanOut.MoveTo), so lane work stays in item order. Stepping into the waiting room always publishes, whatever
// publish says: the item really is in front of the node, and the rank it publishes there is the waiting room's,
// which still holds the follower out of the node.
//
// There is one wait with two ways forward — room in the node, or room to step aside in front of it — rather than a
// decision up front about which to wait for. Two things follow from that. An item walks straight into a free node
// and never touches the waiting room, so a queue costs nothing when the stage is keeping up. And because both ways
// are re-tested on every wake-up, a waiting room that is created or enlarged while an item is already waiting takes
// effect for that item at once, which is the same admission-only promise SetLimit makes (see setQueueSize).
//
// Caller holds mu and has passed checkEnterOrder.
func (r *run) enterUnit(ctx context.Context, it *item, target *unit, publish bool) error {
	// Leaving a fan-out means seeing its work finish: that work is the node's body. This precedes even the step into
	// the target's waiting room, since stepping aside would release the fan-out's slot while its work still runs.
	if err := r.joinPending(ctx, it); err != nil {
		return err
	}
	if err := r.waitUntil(ctx, func() bool {
		return r.canEnter(it, target) || r.canEnterQueue(it, target)
	}); err != nil {
		return err
	}
	if !r.canEnter(it, target) {
		// Only the waiting room is open: step aside (releasing the previous node) and wait for the node from there.
		r.takeQueue(it, target)
		if err := r.waitUntil(ctx, func() bool { return r.canEnter(it, target) }); err != nil {
			return err
		}
	}
	r.takeUnit(it, target, publish)
	return nil
}

// takeQueue steps the item into the waiting room in front of u: it takes a queued slot, publishes the waiting
// room's rank and releases everything the item held behind it — which is the entire point of a waiting room, since
// what it gives up the previous node for is the right to stand here. It deliberately does not mark u entered: the
// item has not been in the node yet, so a move that fails while waiting here leaves the node still enterable.
// Caller holds mu.
func (r *run) takeQueue(it *item, u *unit) {
	r.queued[u.index].add(1)
	it.queuedAt = u.index
	if q := u.queueRank(); q > it.reachedRank {
		it.reachedRank = q
		if q > it.maxRank {
			it.maxRank = q
		}
	}
	r.releaseBelow(it, u.queueRank())
	r.cond.Broadcast()
}

// leaveQueue drops the queued slot the item holds in front of unit j, if it holds one. j may be -1 ("wherever it
// is queued, if anywhere"), which is a no-op for an item that is not waiting. Caller holds mu.
func (r *run) leaveQueue(it *item, j int) {
	if j < 0 || it.queuedAt != j {
		return
	}
	r.queued[j].add(-1)
	it.queuedAt = -1
}

// tryEnterUnit is the non-waiting variant of enterUnit: it moves the item in only if that can be done without
// blocking, and reports whether it did. On false nothing is mutated — the item stays exactly where it was and the
// unit is not marked entered, so a later attempt (a retry, or a plain MoveTo) is still legal.
//
// A waiting room in front of the node is deliberately bypassed, so the item needs room in the node itself. Stepping
// into it would admit the item (releasing the previous node, spending its once-per-node entry) and *then* leave it
// blocked for a node slot with no way back — which is the opposite of what a non-waiting entry promises. See
// Stage.TryMoveTo for the reasoning and how it differs from a buffered-channel send. Nothing else depends on an item
// having passed through the waiting room (releaseBelow looks at held slots, and the once-per-node flag belongs to
// the node), so entering directly is safe.
//
// Bypassing the waiting room does not let this item jump one that is already in it: canEnter tests the node's rank,
// and an item waiting in front of the node has published only the waiting room's lower rank, so the ordering gate
// still refuses. Declining here is exactly right — the item ahead is about to take that slot.
//
// An item that has not finished its fan-out work is declined too: it may not leave that node yet, and waiting for it
// is exactly what this variant promises not to do. So "no room" and "my own work is not done" are the same answer —
// nothing happened, try again later.
//
// Caller holds mu and has passed checkEnterOrder.
func (r *run) tryEnterUnit(it *item, target *unit, publish bool) (bool, error) {
	if it.pending != nil {
		if !it.pending.isFinished() {
			return false, nil
		}
		// It has finished, so leaving costs no wait: take the outcome now and report a failure instead of entering.
		if w := it.consumePending(); w.err != nil {
			return false, joinedErr(w)
		}
	}
	if !r.canEnter(it, target) {
		return false, nil
	}
	r.takeUnit(it, target, publish)
	return true, nil
}

// joinPending waits for the work the item scheduled at the fan-out it currently occupies, unless it detached that
// work. It is what makes a fan-out a node an item passes through rather than a place it drops work off: the tasks are
// the node's body, so the item cannot leave before they are done, and the fan-out's limit therefore bounds how many
// items have work outstanding.
//
// The item keeps its fan-out slot for the whole wait — that is the point — so the items behind it stay out, which is
// the backpressure. Caller holds mu; on return the lock is still held.
func (r *run) joinPending(ctx context.Context, it *item) error {
	if it.pending == nil {
		return nil
	}
	// Settled work is reported from the wave, not from the context. A failing task poisons its own item (fail-fast),
	// and a poisoned context is what waitUntil answers with first — so taking that answer would report the item's
	// cancellation where the truth is "this node's work failed", losing the node. Whether the wave settled is the
	// question that distinguishes the two, and workDone poisons and settles under one lock hold, so re-asking it after
	// the wake-up is decisive.
	if !it.pending.isFinished() {
		if err := r.waitUntil(ctx, it.pending.isFinished); err != nil {
			if !it.pending.isFinished() {
				// Canceled by something other than this work: leave the wave pending, so completeItem accounts for it.
				return err
			}
		}
	}
	if w := it.consumePending(); w.err != nil {
		return joinedErr(w)
	}
	return nil
}

// joinedErr names the fan-out whose work failed, so the error reads for the node the work belongs to rather than for
// the node the item was trying to enter when it heard about it.
func joinedErr(w *wave) error {
	if w.atNode == nil {
		return w.err
	}
	return fmt.Errorf("%s work: %w", w.atNode.owner, w.err)
}

// takeUnit is the mutation half of a move: occupy u, release everything the item held behind it, wake the waiters
// that may now proceed. Every entry path funnels through it once admissibility has been established (by waiting, in
// enterUnit, or by a single check in tryEnterUnit). Caller holds mu.
func (r *run) takeUnit(it *item, u *unit, publish bool) {
	r.occupy(it, u, publish)
	r.releaseBelow(it, u.rank)
	r.cond.Broadcast()
}

// occupy takes one slot of unit u for it, records that u has been entered, and (if publish) raises the item's
// high-water rank. Caller holds mu.
func (r *run) occupy(it *item, u *unit, publish bool) {
	r.addOcc(u.index, 1)
	it.occupied[u.index]++
	it.entered[u.index] = true
	if u.rank > it.reachedRank {
		it.reachedRank = u.rank
	}
	if publish && u.rank > it.maxRank {
		it.maxRank = u.rank
	}
}

// addOcc changes unit j's occupancy by delta, maintaining its per-read window (see windowedInt). All occupancy
// mutations funnel through here so the window is always accurate.
func (r *run) addOcc(j, delta int) { r.occupancy[j].add(delta) }

// occupySlot / releaseSlot take/return one slot of unit j for it without touching rank or entered — used for lane
// slots held on behalf of a running task (caller holds mu).
func (r *run) occupySlot(it *item, j int) {
	r.addOcc(j, 1)
	it.occupied[j]++
}

func (r *run) releaseSlot(it *item, j int) {
	r.addOcc(j, -1)
	it.occupied[j]--
}

// --- release ---

// releaseBelow frees every slot the item holds in units of its own scope with rank strictly below beforeRank,
// except a unit whose slot a live Retain wave is holding (that slot is freed when the bgOp returns, or when the
// item moves on — whichever is later, so the item never sits somewhere holding nothing). It reports whether any
// slot was actually freed. Caller holds mu and must broadcast.
//
// A queued slot is released by the same rank rule as everything else, which is what makes the waiting room
// disappear the moment the item is admitted to the node: the node's own rank is one above its waiting room's, so
// the ordinary releaseBelow inside takeUnit performs the swap. Nothing else needs to know about it.
func (r *run) releaseBelow(it *item, beforeRank int) bool {
	freed := false
	if j := it.queuedAt; j >= 0 && r.conveyor.units[j].queueRank() < beforeRank {
		r.leaveQueue(it, j)
		freed = true
	}
	for _, u := range r.conveyor.scopeUnits[it.scope] {
		if u.rank >= beforeRank || it.occupied[u.index] == 0 {
			continue
		}
		if it.isRetaining(u.index) {
			continue
		}
		freed = r.freeUnit(it, u) || freed
	}
	return freed
}

// freeUnit releases every slot the item holds in unit u and reports whether it held any. Freeing a lane's start
// gate pumps that lane: the next queued piece of work may start now. Caller holds mu and must broadcast.
func (r *run) freeUnit(it *item, u *unit) bool {
	n := it.occupied[u.index]
	if n == 0 {
		return false
	}
	r.addOcc(u.index, -n)
	it.occupied[u.index] = 0
	if u.kind == kindStart && u.branchSeries != nil {
		r.pump(u.index)
	}
	return true
}

// finishItem releases every slot the item still holds, marks it finished and unlinks it from its scope's list.
// Like the other mutations it can unblock waiters, so the caller must broadcast. Caller holds mu.
//
// It sweeps every unit of the conveyor, not just its own scope's (as releaseBelow does), because an item can hold a
// slot outside its scope: work on a lane without interior nodes cannot travel, so startWork charges its slot to the
// scheduling item, which lives in the parent scope. By the time an item gets here completeItem has already waited
// for all of its waves, so those slots are gone — the wider sweep is a backstop that keeps the "no slot outlives its
// item" invariant true of this function alone, rather than of the order its callers run in.
func (r *run) finishItem(it *item) {
	r.leaveQueue(it, it.queuedAt) // an item canceled while waiting in front of a node still holds a queued slot
	for _, u := range r.conveyor.units {
		r.freeUnit(it, u)
	}
	if it.parentWave == nil {
		r.inFlight.add(-1)
	}
	it.finished = true
	r.unlink(it)
}

// unlink removes it from its scope's in-flight list, patching its neighbours (and head/tail) so the remaining
// items stay ordered and each keeps a correct predecessor.
func (r *run) unlink(it *item) {
	list := &r.scopes[it.scope]
	if it.prev != nil {
		it.prev.next = it.next
	} else {
		list.head = it.next
	}
	if it.next != nil {
		it.next.prev = it.prev
	} else {
		list.tail = it.prev
	}
	it.prev, it.next = nil, nil
}

// --- lane runtime ---

// enqueueCollection appends col to the lane's queue and registers it on its wave (whose Started channel closes
// once every collection is exhausted). Caller holds mu.
//
// It also counts the collection in run.queued, the same counter a stage's waiting room uses — a lane's queue is its
// waiting room, and this is what Stats reports as the lane's backlog (see UnitStat.Queued). Nothing else is shared:
// the admission side (canEnterQueue / takeQueue / item.queuedAt) is never reached for a lane's start gate, because
// work is born there rather than moving to it, so the counter is the lane's alone.
func (r *run) enqueueCollection(branchIdx int, col *taskCollection) {
	r.taskQueues[branchIdx] = append(r.taskQueues[branchIdx], col)
	r.queued[branchIdx].add(1)
	col.wave.addSource()
}

// dequeueHead removes the lane's head collection and uncounts it. Both ways a collection leaves the queue (settled
// or dropped) funnel through here, so the backlog gauge cannot drift from the queue itself. The consumed slot is
// zeroed so the backing array does not pin the collection until reallocation. Caller holds mu.
func (r *run) dequeueHead(branchIdx int) {
	q := r.taskQueues[branchIdx]
	q[0] = nil
	r.taskQueues[branchIdx] = q[1:]
	r.queued[branchIdx].add(-1)
}

// pullableHead returns the lane's head collection if work may be pulled from it right now, or nil when the queue is
// empty or an async pull is already in flight on the head (pulls are single-flight per source, and per-lane item
// ordering forbids pulling past the head). Caller holds mu.
func (r *run) pullableHead(branchIdx int) *taskCollection {
	q := r.taskQueues[branchIdx]
	if len(q) == 0 || q[0].pulling {
		return nil
	}
	return q[0]
}

// settleHead trims the lane's head collection (col) and, once the whole collection is exhausted, dequeues it — from
// then on nothing references it — and tells its wave one source is done. Because a collection leaves the queue as
// soon as its work has all been handed out, the backlog counts items with work not yet started, never work merely
// still running. Caller holds mu and must broadcast.
func (r *run) settleHead(branchIdx int, col *taskCollection) {
	col.trim()
	if !col.exhaustedNow() {
		return
	}
	r.dequeueHead(branchIdx)
	col.wave.sourceExhausted()
}

// dropHead abandons the lane's head collection because its item is canceled: the work it has not handed out yet will
// never run, so the collection is dequeued and its wave is told the source is done (which is what lets the wave
// resolve — see Wave). The wave records why the work was abandoned, so it cannot report a clean finish for work that
// never ran (see wave.recordAbandoned). Caller holds mu, has checked that no async pull is in flight on it, and must
// broadcast.
//
// The abandoned sources are released on their own goroutine, because this is the one path that discards a source that
// may still be holding something — a generator suspended between pulls — and letting go of it runs user code, which
// must not happen under run.mu. Everywhere else a source is finished by the pull that exhausted it. Since no pull is in
// flight and the collection is now unreachable, nothing can race with the release.
func (r *run) dropHead(branchIdx int, col *taskCollection, cause error) {
	r.dequeueHead(branchIdx)
	if rest := col.detachSources(); len(rest) > 0 {
		go releaseSources(rest)
	}
	col.wave.recordAbandoned(cause)
	col.wave.sourceExhausted()
}

// grabNext tries to fill one free slot of the lane from the head collection (caller holds mu; the caller must
// broadcast, as a pull can exhaust the head and settle its wave). Three outcomes:
//   - sync source: the work is pulled inline (no user code) and fully accounted — run it (ok == true);
//   - async source: the slot is reserved and the pull marked in flight — the caller must finish it via
//     finishAsyncPull WITHOUT holding mu (asyncCol != nil);
//   - nothing to start (no free slot, empty queue, or busy head): both zero.
func (r *run) grabNext(branchIdx int) (g grabbed, asyncCol *taskCollection, ok bool) {
	for {
		if !r.unitHasFreeSlot(branchIdx) {
			return grabbed{}, nil, false
		}
		col := r.pullableHead(branchIdx)
		if col == nil {
			return grabbed{}, nil, false
		}
		if cause := context.Cause(col.it.ctx); cause != nil {
			// The item is canceled (shutdown, or its own failure): its remaining work will never run, so drop it
			// rather than starting callbacks that would only find a canceled context. This makes every source kind
			// behave the same way on cancellation, and lets the lane serve the next item at once. Whether an item is
			// allowed to keep working is decided per item, never per task — a task is part of one item's work in a
			// node, so an item still allowed to continue pulls and runs all of its tasks whatever shape they came in.
			r.dropHead(branchIdx, col, cause)
			continue
		}
		src := col.curSource()
		if !src.isSync() {
			// Reserve the slot for the duration of the user pull, so admission and SetLimit see the lane as busy
			// serving this item.
			r.occupySlot(col.it, branchIdx)
			col.pulling = true
			return grabbed{}, col, false
		}
		fn, pulled := src.pull(col.it.ctx)
		if !pulled {
			// Unreachable when eager trimming holds (a sync head is never exhausted); settle and retry.
			r.settleHead(branchIdx, col)
			continue
		}
		g := r.startWork(col, branchIdx, fn, false)
		r.settleHead(branchIdx, col)
		return g, nil, true
	}
}

// finishAsyncPull completes a pull reserved by grabNext: it runs the user code (generator body / channel receive;
// the source treats item-ctx cancellation as exhaustion) WITHOUT holding mu, then re-enters the lock to account
// the outcome. On success the reserved slot becomes the work's slot. On exhaustion the slot is given back, the
// head settled and the lane re-pumped; the caller must not touch the slot again.
func (r *run) finishAsyncPull(col *taskCollection, branchIdx int) (grabbed, bool) {
	fn, pulled := col.curSource().pull(col.it.ctx)

	r.mu.Lock()
	defer r.mu.Unlock()
	col.pulling = false
	if pulled {
		g := r.startWork(col, branchIdx, fn, true)
		r.pump(branchIdx) // the head is pullable again; fill any other free slots
		r.cond.Broadcast()
		return g, true
	}
	r.releaseSlot(col.it, branchIdx)
	// A streaming source treats cancellation as exhaustion, so a pull that was in flight when the item was canceled
	// lands here rather than in dropHead. The item is what decides, so report it the same way: this source stopped
	// because the item lost permission to work, not because it ran out.
	if cause := context.Cause(col.it.ctx); cause != nil {
		col.wave.recordAbandoned(cause)
	}
	r.settleHead(branchIdx, col) // the failed pull marked the source exhausted; drop it (and maybe the collection)
	r.pump(branchIdx)            // the freed slot may start the collection's next source, or the next collection
	r.cond.Broadcast()
	return grabbed{}, false
}

// startWork accounts one pulled piece of work on the lane and registers it on the wave. Where the slot is charged
// depends on the lane: work on a lane with interior nodes runs as a child item that holds the lane's start gate
// itself (and gives it up on its first move); work on a lane without them cannot travel, so its slot is charged to
// the scheduling item for the duration. reserved says the slot is already taken (an async pull held it for the
// scheduling item). Caller holds mu.
func (r *run) startWork(col *taskCollection, branchIdx int, fn TaskFunc, reserved bool) grabbed {
	g := grabbed{col: col, fn: fn}
	if col.branch.travels() {
		if reserved {
			r.releaseSlot(col.it, branchIdx) // hand the reserved slot over to the child (no window: still under mu)
		}
		g.child = r.newChildItem(col)
	} else if !reserved {
		r.occupySlot(col.it, branchIdx)
	}
	col.wave.workStarted()
	return g
}

// pump starts as many queued pieces of work on the lane as free slots and pullable work allow, spawning one worker
// per started piece (caller holds mu and must broadcast). When the head collection's current source is async, pump
// reserves the slot and hands the pull to the spawned worker, then stops: the head is single-flight, so nothing
// more can start on this lane until that pull completes (per-lane item ordering).
func (r *run) pump(branchIdx int) {
	for {
		g, asyncCol, ok := r.grabNext(branchIdx)
		if asyncCol != nil {
			go func() {
				if g, ok := r.finishAsyncPull(asyncCol, branchIdx); ok {
					r.branchWorker(branchIdx, g)
				}
			}()
			return
		}
		if !ok {
			return
		}
		go r.branchWorker(branchIdx, g)
	}
}

// branchWorker runs one piece of lane work, then keeps its slot busy with the next queued piece (possibly another
// item's): a sync source is pulled inline and the worker continues with it; an async head makes this worker the
// puller (slot reserved, user code run outside the lock). Otherwise — nothing pullable, or a limit lowered below
// the current occupancy — the slot is released and the worker exits.
//
// Release-on-completion is what makes an item that scheduled more work than a lane's capacity drain through its
// own completions; the limit re-check on reuse (grabNext) is what makes a SetLimit decrease shrink the lane.
func (r *run) branchWorker(branchIdx int, g grabbed) {
	for {
		r.runWork(branchIdx, g)

		r.mu.Lock()
		next, asyncCol, ok := r.grabNext(branchIdx)
		if !ok && asyncCol == nil {
			r.cond.Broadcast()
			r.mu.Unlock()
			return
		}
		r.cond.Broadcast()
		r.mu.Unlock()
		if asyncCol != nil {
			var pulled bool
			if next, pulled = r.finishAsyncPull(asyncCol, branchIdx); !pulled {
				return // the source was exhausted; the slot was given back and the lane re-pumped
			}
		}
		g = next
	}
}

// runWork runs one piece of work to completion and accounts its outcome: a child item goes through the full item
// completion (its own background work joined, its slots released, its outcome reported to the wave); work that
// cannot travel frees its slot and reports to the wave directly. Not holding mu.
func (r *run) runWork(branchIdx int, g grabbed) {
	if g.child != nil {
		r.completeItem(g.child, g.fn(g.child.ctx))
		return
	}
	r.mu.Lock()
	ctx := g.col.nonMovableCtx()
	r.mu.Unlock()

	err := g.fn(ctx)

	r.mu.Lock()
	r.releaseSlot(g.col.it, branchIdx)
	g.col.wave.workDone(err)
	r.cond.Broadcast()
	r.mu.Unlock()
}

// runRetain runs a Retain bgOp and settles its wave, which is also what gives back the stage slot the wave was holding
// (see wave.releaseRetained — the same path a detached fan-out's slot takes). Not holding mu while bgOp runs.
func (r *run) runRetain(w *wave, bgOp func() error) {
	err := bgOp()

	r.mu.Lock()
	w.workDone(err)
	r.cond.Broadcast()
	r.mu.Unlock()
}

// setLimit publishes a unit's new capacity and, if a run is active, wakes it so waiting items re-check admission
// (a raise) and a lane re-pumps so queued work starts at once. Lowering is picked up lazily by the admission gate
// (canEnter / pump / the lane worker's pull-next), which stops handing out slots beyond the new limit; work
// already running keeps its slot and finishes normally.
func (u *unit) setLimit(limit int) {
	if limit <= 0 {
		limit = 1
	}
	u.limit.Store(int64(limit))
	r := u.conveyor.currentRun.Load()
	if r == nil {
		return
	}
	r.mu.Lock()
	if u.kind == kindStart && u.branchSeries != nil {
		r.pump(u.index) // a raise may let queued work start now; a lower is a no-op here
	}
	r.cond.Broadcast() // wake items waiting in canEnter so a raise admits them immediately
	r.mu.Unlock()
}

// setQueueSize publishes a node's new waiting-room size and wakes any run so items waiting at its door re-check
// admission. Unlike a limit it may be 0, which is what "no waiting room" means; there is no topology to change
// either way, which is why it is safe on a live conveyor.
//
// Both directions are admission-only, exactly as for setLimit: a raise admits waiting items at once — including
// items already blocked at the node's door, which step aside into the new room as soon as this broadcast wakes them
// (see enterUnit) — while a lower (including to 0) leaves the items already in the waiting room alone and stops
// admitting new ones until occupancy has fallen below the new size.
func (u *unit) setQueueSize(size int) {
	if size < 0 {
		size = 0
	}
	u.queueSize.Store(int64(size))
	r := u.conveyor.currentRun.Load()
	if r == nil {
		return
	}
	r.mu.Lock()
	r.cond.Broadcast()
	r.mu.Unlock()
}

// waitUntil blocks until admissible() reports true or ctx is canceled, whichever comes first. The caller must
// hold r.mu; on a nil return the lock is still held and admissible() is true, so the caller may mutate before
// releasing the lock.
func (r *run) waitUntil(ctx context.Context, admissible func() bool) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if admissible() {
		return nil
	}
	// sync.Cond does not wake on context cancellation, so arrange a broadcast when ctx is done. AfterFunc runs
	// the callback in its own goroutine (immediately if ctx is already done); it blocks on r.mu until we release
	// it in cond.Wait, so the wake-up is never missed.
	stop := context.AfterFunc(ctx, func() {
		r.mu.Lock()
		r.cond.Broadcast()
		r.mu.Unlock()
	})
	defer stop()
	for {
		r.cond.Wait()
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if admissible() {
			return nil
		}
	}
}
