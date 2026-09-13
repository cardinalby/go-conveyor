package conveyor

import (
	"context"
	"fmt"
)

// FanOut is a node whose work runs in parallel on branches. The item enters with MoveTo, adds tasks with Schedule
// (before or after entering, as often as needed), and the move that takes it out of the node waits for all of them to
// finish. Use it for concurrent work such as writing to several databases, or, with a Lane, to turn one item into
// several child journeys.
//
// A branch is either a Pool (AddPool), a single step where a task runs and is done, or a Lane (AddLane), a pipeline
// whose steps a task travels as a child item. Most fan-outs need only pools.
type FanOut interface {
	Unit

	// AddPool adds a Pool branch: a single step where a scheduled task runs to completion. Chain SetLimit to run
	// several tasks at once, and pass OptName to name it.
	//
	// It panics if the conveyor is running or has already run.
	AddPool(opts ...AnyUnitOption) Pool

	// AddLane adds a Lane branch: a pipeline of its own interior nodes (AddStage, AddFanOut) through which each
	// piece of scheduled work travels as a child item. Pass OptName to name it.
	//
	// It panics if the conveyor is running or has already run.
	AddLane(opts ...AnyUnitOption) Lane

	// MoveTo advances the item into this fan-out. It blocks until it is the item's turn and an item slot (SetLimit) is
	// free. Pool capacity does not gate entry.
	//
	// Work prepared with Schedule before entering becomes the item's initial batch and may start before MoveTo
	// returns. Otherwise the item enters with an empty body, and the first Schedule after entry is the initial batch.
	// SetBackpressure decides whether entering releases the previous node at once or keeps its slot until the initial
	// batch makes progress.
	//
	// The item behind cannot enter this fan-out until this item's initial batch is known (it schedules, leaves, or
	// retains); meanwhile it may wait in the fan-out's waiting room.
	//
	// It returns ErrForeignContext, ErrStaleContext, or the item's cancellation cause, whether the cancellation is
	// visible on ctx or only on the item's own context (see ItemProcessor). It panics on misuse: moving backward,
	// re-entering this node, or a node outside the item's own series.
	MoveTo(ctx context.Context) error

	// TryMoveTo is MoveTo without waiting: it enters the fan-out only if it can do so right now, and reports whether
	// it did. It bypasses the waiting room and never jumps an item already waiting there. Busy pools do not make it
	// decline; entering may keep the previous node's slot instead (see SetBackpressure).
	//
	// When entered is false nothing happened: the item stays where it is, and work prepared with Schedule stays
	// prepared for a later attempt or a MoveTo. A canceled item returns (false, its cancellation cause). It panics on
	// the same misuse as MoveTo.
	TryMoveTo(ctx context.Context) (entered bool, err error)

	// Schedule adds tasks to this item's work at this fan-out. It never blocks. Allowed callers:
	//   - the ItemProcessor, inside this fan-out with an open body (not yet left, not retained), or before entering it
	//     while the fan-out is still ahead of the item;
	//   - a task running on one of this fan-out's pools, before the task returns;
	//   - a child item of one of this fan-out's lanes, before its callback returns.
	//
	// Inside the fan-out, tasks are queued at once. Before entry, tasks are prepared: the Tasks are consumed, but
	// nothing runs and no generator or channel is pulled until the item enters. All prepared calls together form the
	// initial batch (see FanOutBackpressure). Prepared work is discarded without running if the item passes the
	// fan-out without entering or returns, and once a canceled item calls a node method or returns; a declined
	// TryMoveTo keeps it. Prepared work does not appear in Stats.
	//
	// Preparing the work and then entering lets it start as part of the entry:
	//
	// 	if err := f.Schedule(ctx, db1.NewTask(writeDB1), db2.NewTask(writeDB2)); err != nil {
	// 		return err
	// 	}
	// 	if err := f.MoveTo(ctx); err != nil {
	// 		return err
	// 	}
	//
	// Work is queued per branch by item age, then by call order, then by argument order. Running work is never
	// preempted.
	//
	// Calling Schedule with no tasks is legal: it makes the initial batch known (and empty), which lets the item
	// behind enter.
	//
	// It returns ErrForeignContext; ErrStaleContext for the context of a finished item, a finished child, a finished
	// wave, or (before entry) a processor that has returned; or the item's cancellation cause. In these cases nothing is
	// queued or prepared. It panics on misuse: a task of another fan-out, a Task submitted twice, a fan-out the item
	// has already passed, a body closed by leaving (or trying to leave), Retain, or a task calling it for a fan-out it
	// does not run under.
	Schedule(ctx context.Context, tasks ...Task) error

	// Wait blocks until every task scheduled so far in this fan-out's body has finished, including work those tasks
	// scheduled themselves, and returns the first error or the item's cancellation cause. The item keeps its slot and
	// may Schedule again afterwards. Use it for rounds: wait, plan the next tasks from the results, schedule again.
	// Repeated calls return the same error again.
	//
	// Only the ItemProcessor (or a child item at a fan-out of its own lane) may call it, while the body is open. It
	// panics when called with a task's context (a task must never wait for other work), before entering, after leaving,
	// or after Retain.
	Wait(ctx context.Context) error

	// Retain hands the work scheduled here to the returned Wave and lets the item move on without waiting for it. This
	// fan-out's slot stays held until the work is done and the item has moved on. The work may still grow from its own
	// running tasks. It is the fan-out counterpart of Stage.Retain: use it to overlap this fan-out's work with later
	// stages, then call Wave.Wait before the step that needs the result.
	//
	// After Retain the ItemProcessor may not Schedule or Wait here again. Retain does not release the previous node's
	// slot earlier than SetBackpressure allows.
	//
	// It panics on misuse: a fan-out the item does not currently occupy, or nothing to retain (never entered, already
	// retained, or the body was closed by leaving).
	Retain(ctx context.Context) Wave

	// SetLimit sets how many items may be inside this fan-out at once (default 1; a limit <= 0 means 1), and returns
	// the fan-out for chaining. An item counts from entering until it moves into the next node or its waiting room;
	// after Retain it counts until the retained work is done and the item has moved on. The limit does not let the item
	// behind enter before this item's initial batch is known.
	//
	// Safe to call at any time, from any goroutine, including on a running conveyor.
	SetLimit(limit int) FanOut

	// SetQueueSize gives this fan-out a waiting room of size items in front of it (a size <= 0 means none), and
	// returns the fan-out for chaining.
	//
	// Safe to call at any time, from any goroutine, including on a running conveyor.
	SetQueueSize(size int) FanOut

	// Limit returns how many items may be inside this fan-out at once.
	Limit() int

	// QueueSize returns the size of this fan-out's waiting room, or 0 if it has none.
	QueueSize() int

	// SetBackpressure selects when an item entering this fan-out releases the slot it held before: the previous node's
	// slot, or its place in this fan-out's waiting room. The default is BackpressureBalanced. See FanOutBackpressure.
	//
	// Safe to call at any time, from any goroutine, including on a running conveyor. It applies to items entering after
	// the call; items already inside keep the mode they entered with.
	SetBackpressure(mode FanOutBackpressure) FanOut

	// Backpressure returns the backpressure mode.
	Backpressure() FanOutBackpressure

	// Branches returns this fan-out's branches, pools and lanes alike, in creation order.
	Branches() []Branch
}

// FanOutBackpressure selects when an item entering a fan-out releases the slot it held before: the previous node's
// slot, or its place in the fan-out's waiting room (see FanOut.SetBackpressure). Entry itself is the same in every
// mode: the item's turn and a free item slot (SetLimit). Pool capacity never gates entry.
//
// The modes differ in how much of the item's initial batch must start before the previous slot is released. The
// initial batch is the work prepared with Schedule before entry or, if none, the first Schedule after entry. Later
// Schedule calls and work scheduled by running tasks never delay the release.
//
// A task counts as started when its callback is dispatched; a lane task counts when its child item is created. A
// source that ends without producing a task (NewTasks with count 0, a closed channel, a finished generator) counts
// for its branch as well. Work waiting behind a full pool has not started.
type FanOutBackpressure int

const (
	// BackpressureBalanced is the default. The previous slot is released when the first task of the initial batch starts
	// on any branch, or when every branch of the batch ran out without a task. An item whose work is all waiting for
	// full pools keeps the previous node busy; an item with some work running does not. A good general-purpose choice.
	BackpressureBalanced FanOutBackpressure = iota

	// BackpressureBuffered releases the previous slot on entering, like a plain stage. More items wait inside the
	// fan-out (within its limit) and upstream feels the pressure later. Use it when item N+1 should reach an idle pool
	// while item N waits for a busy one.
	BackpressureBuffered

	// BackpressureStrict releases the previous slot when every branch of the initial batch has started one task or ran
	// out. An item with any initial branch fully waiting keeps the previous slot; a younger item whose work can start
	// may still enter and run ahead of it on the branches. Fewer items in flight, at the cost of throughput when task
	// durations differ. Use it when per-item memory or upstream resources are costly.
	BackpressureStrict
)

// String names the mode for messages and debugging.
func (m FanOutBackpressure) String() string {
	switch m {
	case BackpressureBalanced:
		return "BackpressureBalanced"
	case BackpressureBuffered:
		return "BackpressureBuffered"
	case BackpressureStrict:
		return "BackpressureStrict"
	default:
		return fmt.Sprintf("FanOutBackpressure(%d)", int(m))
	}
}

// fanOut is the FanOut implementation: one node of a series, owning one node unit plus its branches.
type fanOut struct {
	series   *series // the series this node belongs to
	name     string  // optional user-given name (OptName); empty -> positional in String
	ord      int     // 1-based position among its series' nodes, for the positional name
	node     *unit   // this node's capacity unit (items inside), plus node.queue if a queue was added
	branches []*branch
}

func (f *fanOut) AddPool(opts ...AnyUnitOption) Pool { return f.addBranch(opts) }

func (f *fanOut) AddLane(opts ...AnyUnitOption) Lane { return f.addBranch(opts) }

// addBranch builds a branch. It is the whole of both constructors: the kinds differ only in the interface handed back,
// which is what decides whether nodes can be added to the series every branch gets. That series is what a freed slot
// pumps, and what a child item travels if there is anything there to travel (see unit.branchSeries, run.startWork).
func (f *fanOut) addBranch(opts []AnyUnitOption) *branch {
	cfg := newAnyUnitConfig(opts)
	c := f.series.conveyor
	b := &branch{fanout: f, name: cfg.name, no: len(f.branches) + 1}
	u := c.newUnit(b, kindStart)
	b.series = c.newSeries(u)
	u.branchSeries = b.series
	f.branches = append(f.branches, b)
	return b
}

// MoveTo enters this fan-out (through its queue, if it has one) with an open body holding the work the item prepared,
// if any. See the FanOut interface for the full contract.
func (f *fanOut) MoveTo(ctx context.Context) error {
	it, r, err := f.series.conveyor.actingItem(ctx, "move to", f.node, false)
	if err != nil {
		return fmt.Errorf("move to %s: %w", f, err)
	}
	defer r.mu.Unlock()
	r.checkEnterOrder(it, f.node) // panics on backward / repeat entry (misuse)
	// Enter the node without publishing its rank (the door): the items behind must not enqueue onto these branches
	// until this item has — the activation of its prepared work (below, in the same lock hold), its first Schedule,
	// its leave or its Retain publishes — so their work stays in item order. Admission publishes the waiting room's
	// rank instead, so the item behind may step aside meanwhile.
	if err := r.enterUnit(ctx, it, f.node, false); err != nil {
		return fmt.Errorf("move to %s: %w", f, err)
	}
	r.activateDormant(it, r.newBody(it, f), f)
	return nil
}

// TryMoveTo enters this fan-out only if that needs no waiting, with an open body holding the prepared work if it did.
// See the FanOut interface for the full contract.
func (f *fanOut) TryMoveTo(ctx context.Context) (entered bool, err error) {
	it, r, err := f.series.conveyor.actingItem(ctx, "try move to", f.node, true)
	if err != nil {
		return false, fmt.Errorf("try move to %s: %w", f, err)
	}
	defer r.mu.Unlock()
	r.checkEnterOrder(it, f.node) // panics on backward / repeat entry (misuse)
	// publish == false for the same reason as in MoveTo: the door stays closed until this item schedules.
	entered, err = r.tryEnterUnit(it, f.node, false)
	if err != nil || !entered {
		return false, err // declined: prepared work, if any, stays dormant and untouched
	}
	r.activateDormant(it, r.newBody(it, f), f)
	return true, nil
}

// Schedule adds tasks to a body of this fan-out on behalf of whoever holds ctx. See the FanOut interface for the full
// contract.
func (f *fanOut) Schedule(ctx context.Context, tasks ...Task) error {
	f.validateTasks(tasks) // static wiring check first, before anything touches the context
	c := f.series.conveyor
	c.validateUnit(f.node)
	col, it, err := c.resolveCaller(ctx)
	if err != nil {
		return fmt.Errorf("schedule at %s: %w", f, err)
	}
	r := it.run
	r.mu.Lock()
	defer r.mu.Unlock()
	// A finished caller's context is stale: the item itself, or, for a pool's work, the wave it belongs to. A lane
	// child whose callback has returned is over for its own code even while its background work keeps it unfinished:
	// its lifetime is judged before its work would be charged to the parent's wave. (Its own-body path is answered by
	// the closed body instead.) The boundary is completeItem taking the lock; a Schedule racing with the return is
	// the callback lifetime contract's problem.
	if it.finished || (col != nil && col.wave.isFinished()) ||
		(col == nil && it.returned && it.parentWave != nil && it.parentWave.atNode == f.node) {
		return fmt.Errorf("schedule at %s: %w", f, ErrStaleContext)
	}
	w, root := f.bodyFor(col, it)
	if w == nil {
		// Not entered yet: the work is prepared for the entry. A processor that has returned is over for its own path,
		// and the discard completeItem did must be the last word.
		if it.returned {
			return fmt.Errorf("schedule at %s: %w", f, ErrStaleContext)
		}
		if err := it.cancelCause(ctx); err != nil {
			r.dropDormantIfCanceled(it)
			return fmt.Errorf("schedule at %s: %w", f, err)
		}
		r.prepare(it, f, tasks)
		return nil
	}
	if err := w.it.cancelCause(ctx); err != nil {
		return fmt.Errorf("schedule at %s: %w", f, err)
	}
	r.addToBody(w.it, w, f, tasks, root)
	return nil
}

// bodyFor resolves the body a Schedule adds to, and whether the addition is a root (through the item's own path) or
// a spawn (from the body's own work). A pool's work adds to its own wave, which must be this fan-out's; a lane child
// adds to its parent's wave when this is the fan-out its lane belongs to (checked before the scope, which is the
// parent's); otherwise the item acts in its own scope: its body here must be open, or — a nil wave — it has not
// entered this fan-out yet and the work is prepared for the entry (see run.prepare). A fan-out the item has already
// passed is misuse, like a MoveTo to it. Caller holds mu.
func (f *fanOut) bodyFor(col *taskCollection, it *item) (w *wave, root bool) {
	if col != nil {
		if col.branch.fanout != f {
			panic(fmt.Errorf("work on %s cannot schedule at %s: %w", col.branch, f, errInvalidUnit))
		}
		return col.wave, false
	}
	if pw := it.parentWave; pw != nil && pw.atNode == f.node {
		return pw, false
	}
	c := f.series.conveyor
	c.validateScope(it, "schedule at", f.node)
	if it.body[f.node.index] == bodyNone {
		if f.node.rank < it.reachedRank {
			panic(fmt.Errorf("cannot schedule at %s: item has already advanced to %s; %w",
				f, c.describeRank(it.scope, it.reachedRank), errWrongEnterOrder))
		}
		return nil, true
	}
	return f.openBody(it, "schedule"), true
}

// Wait blocks until the item's body at this fan-out is idle and reports its outcome. See the FanOut interface for the
// full contract.
func (f *fanOut) Wait(ctx context.Context) error {
	it, r, err := f.series.conveyor.actingItem(ctx, "wait at", f.node, false)
	if err != nil {
		return fmt.Errorf("wait at %s: %w", f, err)
	}
	defer r.mu.Unlock()
	w := f.openBody(it, "wait")
	// A canceled wait with an idle failed body falls through: a failing task poisons its item, so the body's own error
	// is the truer message (see joinPending). A canceled item never gets nil.
	if err := r.waitUntil(ctx, it, w.idle); err != nil && (!w.idle() || w.err == nil) {
		return fmt.Errorf("wait at %s: %w", f, err)
	}
	if w.err != nil {
		w.acked = true
		return joinedErr(w)
	}
	return nil // the body stays open: the item may schedule again
}

// openBody returns the item's open body at this fan-out, or panics with the sentinel of its body state (see
// bodyState). verb names the refused call. Caller holds mu.
func (f *fanOut) openBody(it *item, verb string) *wave {
	switch it.body[f.node.index] {
	case bodyOpen:
		return it.pending
	case bodyClosed:
		panic(fmt.Errorf("cannot %s at %s: %w", verb, f, errBodyClosed))
	case bodyRetained:
		panic(fmt.Errorf("cannot %s at %s: %w", verb, f, errWorkRetained))
	default:
		panic(fmt.Errorf("cannot %s at %s: %w", verb, f, errStageNotEntered))
	}
}

// Retain hands this fan-out's slot to the work the item scheduled here. See the FanOut interface for the full
// contract.
func (f *fanOut) Retain(ctx context.Context) Wave {
	// checkCancel is false: a canceled item is handed its own wave (the work is already scheduled and will settle with
	// the cancellation cause), not an error — the same choice Stage.Retain makes.
	it, r, err := f.series.conveyor.actingItem(ctx, "retain", f.node, false)
	if err != nil {
		// No item to charge: hand back a standalone finished wave carrying the reason.
		return standaloneWave(fmt.Errorf("retain %s: %w", f, err))
	}
	defer r.mu.Unlock()
	// The body state, not occupancy, says whether there is work to hand over, so the diagnostic is the same whether
	// the item still stands here (a retained wave holding the slot, a failed leave) or has moved on.
	switch it.body[f.node.index] {
	case bodyOpen:
	case bodyRetained:
		panic(fmt.Errorf("cannot retain %s: already retained: %w", f, errNothingToRetain))
	case bodyClosed:
		panic(fmt.Errorf("cannot retain %s: the body was closed when the item left: %w", f, errNothingToRetain))
	default:
		panic(fmt.Errorf("cannot retain %s: %w", f, errStageNotEntered))
	}
	if it.occupied[f.node.index] == 0 {
		panic(fmt.Errorf("cannot retain %s: %w", f, errStageNotEntered)) // an open body is always occupied
	}
	w := it.pending
	// From here the slot follows the work, not the item: releaseBelow leaves it alone (see item.isRetaining) and the
	// branch workers free it once the last task is done — or the item's next move, if the work is already done.
	w.retainUnit = f.node
	it.sealBody(w, bodyRetained)
	// The door opens (a no-op after the item's first Schedule): its work here is on the branches for good.
	if f.node.rank > it.maxRank {
		it.maxRank = f.node.rank
	}
	r.cond.Broadcast()
	return w
}

// newBody opens the item's body at f: an unsealed idle wave, recorded as the item's pending work with body state
// open. It is sealed when the item leaves, retains or completes (see item.sealBody), so an empty body still
// finishes. Caller holds run.mu.
func (r *run) newBody(it *item, f *fanOut) *wave {
	w := newWave(r, it)
	w.sealed = false
	w.atNode = f.node
	for _, h := range it.holds {
		if h.at == f.node { // the hold this admission opened (takeUnit ran just before, under the same lock hold)
			w.hold = h
		}
	}
	it.pending = w
	it.body[f.node.index] = bodyOpen
	return w
}

// addToBody adds tasks to the item's body w at f: it claims the sources (where a resubmitted Task panics, before
// anything is mutated) and submits them. root says the tasks come through the item's own path (see
// taskCollection.root). Caller holds run.mu.
func (r *run) addToBody(it *item, w *wave, f *fanOut, tasks []Task, root bool) {
	claimTasks(tasks)
	r.submitClaimed(it, w, f, tasks, root)
}

// claimTasks claims the sources of tasks: from here they belong to the caller's item, whether they are queued now or
// kept dormant. A resubmitted Task (or one listed twice) panics here, before any task of the call is claimed, so a
// recovered misuse leaves the other tasks usable. A statically-empty task (nil source, e.g. NewTasks with count <= 0)
// has nothing to claim.
func claimTasks(tasks []Task) {
	seen := make(map[taskSource]struct{}, len(tasks))
	for _, t := range tasks {
		if t.src == nil {
			continue
		}
		if _, dup := seen[t.src]; dup || t.src.isClaimed() {
			panic(fmt.Errorf("task for %s was already submitted: %w", t.branch, errTaskReused))
		}
		seen[t.src] = struct{}{}
	}
	for _, t := range tasks {
		if t.src != nil {
			t.src.claim()
		}
	}
}

// submitClaimed adds already-claimed tasks to the item's body w at f: it groups them into one collection per branch
// in argument order, inserts each collection into its branch queue at the item's place, publishes the node's rank so
// the next item may enter (the door; a no-op after the first time), and starts the work. Caller holds run.mu.
//
// Several tasks for the same branch become one collection, whose sources are consumed front to back — which is what
// makes the order the caller listed them the order their work starts in.
func (r *run) submitClaimed(it *item, w *wave, f *fanOut, tasks []Task, root bool) {
	byBranch := make(map[int]*taskCollection, len(f.branches))
	touched := make([]int, 0, len(f.branches))
	for _, t := range tasks {
		if t.src == nil {
			continue // statically-empty task (e.g. NewTasks with count <= 0)
		}
		branchIdx := t.branch.start.index
		col := byBranch[branchIdx]
		if col == nil {
			col = &taskCollection{it: it, wave: w, branch: t.branch, root: root}
			byBranch[branchIdx] = col
			touched = append(touched, branchIdx)
		}
		col.sources = append(col.sources, t.src)
	}
	for _, branchIdx := range touched {
		r.insertCollection(branchIdx, byBranch[branchIdx])
	}
	if root && !w.rootSubmitted {
		w.rootSubmitted = true
		if w.hold != nil {
			// The entering submission of a Balanced/Strict admission: its first starts per branch end the hold.
			cols := make([]*taskCollection, 0, len(touched))
			for _, branchIdx := range touched {
				cols = append(cols, byBranch[branchIdx])
			}
			r.markEntering(w, cols)
		}
	}
	// Publishing the rank is what opens the gate for the next item and keeps the "older item's maxRank >=
	// younger item's" invariant. With no tasks this is the whole effect of the call.
	if f.node.rank > it.maxRank {
		it.maxRank = f.node.rank
	}
	for _, branchIdx := range touched {
		r.pump(branchIdx)
	}
	r.cond.Broadcast()
}

// assignRank reserves rank r for the waiting room and gives the node unit the next one, whether or not a queue is
// configured (see the rank discussion in builder.go). The branches' interior series are ranked independently (each is
// its own scope; see conveyor.finalize).
func (f *fanOut) assignRank(scope, r int) int {
	f.node.scope, f.node.rank = scope, r+1
	return r + 2
}

// owns reports whether b is one of this fan-out's branches (nil-safe, so a zero Task fails the ownership check with
// the wiring-bug panic rather than a nil dereference).
func (f *fanOut) owns(b *branch) bool { return b != nil && b.fanout == f }

// validateTasks panics if any task belongs to another fan-out — a static wiring mistake, so Schedule checks it
// before it touches the context.
func (f *fanOut) validateTasks(tasks []Task) {
	for _, t := range tasks {
		if !f.owns(t.branch) {
			panic(fmt.Errorf("task for %s does not belong to %s: %w", t.branchName(), f, errInvalidUnit))
		}
	}
}

// String returns the fan-out's name, or its positional name ("fan-out N", prefixed by the lane it was built in).
func (f *fanOut) String() string {
	if f.name != "" {
		return f.name
	}
	return f.series.positionalName("fan-out", f.ord)
}

func (f *fanOut) unit() *unit { return f.node }

func (f *fanOut) SetLimit(limit int) FanOut {
	f.node.setLimit(limit)
	return f
}

func (f *fanOut) SetQueueSize(size int) FanOut {
	f.node.setQueueSize(size)
	return f
}

func (f *fanOut) Limit() int { return int(f.node.limit.Load()) }

func (f *fanOut) QueueSize() int { return int(f.node.queueSize.Load()) }

func (f *fanOut) SetBackpressure(mode FanOutBackpressure) FanOut {
	f.node.setBackpressure(mode)
	return f
}

func (f *fanOut) Backpressure() FanOutBackpressure { return f.node.backpressureMode() }

func (f *fanOut) Branches() []Branch {
	bs := make([]Branch, 0, len(f.branches))
	for _, b := range f.branches {
		bs = append(bs, b)
	}
	return bs
}
