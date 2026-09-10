package conveyor

import (
	"context"
	"fmt"
)

// FanOut is a node whose work runs in parallel on branches: the item enters with MoveTo, adds tasks with Schedule
// (as many times as it likes, from the ItemProcessor or from the tasks themselves), and the move that takes it out
// of the node waits for all of them to finish. Use it for concurrent work such as writing to several databases, or,
// with a Lane, to turn one item into several child journeys.
//
// A branch is either a Pool (AddPool) — a single step where a task runs and is done — or a Lane (AddLane) — a
// pipeline whose steps a task travels as a child item. Most fan-outs need only pools.
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

	// MoveTo advances the item into this fan-out, releasing the previous node, and joins the listed waves. The item
	// enters with an empty body; add work with Schedule. Until the item's first Schedule (or until it leaves or
	// detaches), the item behind it may step into this fan-out's waiting room but cannot enter the node.
	//
	// It returns ErrForeignContext, ErrStaleContext, or the item's cancellation cause — whether the cancellation is
	// visible on ctx or only on the item's own context (see ItemProcessor). It panics on misuse: moving backward,
	// re-entering this node, a node outside the item's own series, or a wave from another item.
	MoveTo(ctx context.Context, joins ...Wave) error

	// TryMoveTo is MoveTo without waiting: it enters the fan-out only if it can do so right now, and reports
	// whether it did. It bypasses the waiting room and never jumps an item already waiting there.
	//
	// When entered is false nothing happened. The joins are awaited only if the item entered. A canceled item
	// returns (false, its cancellation cause), whether the cancellation is visible on ctx or only on the item's own
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

	// SetLimit sets how many items may be inside this fan-out at once (default 1; a limit <= 0 means 1), and
	// returns the fan-out for chaining. An item counts from the moment it enters until it steps into the next node
	// or that node's waiting room. After Detach it counts until both the detached work is done and the item has
	// moved on, whichever is later. The ordering gate is separate from the limit: the item behind cannot enter
	// before the item ahead has scheduled, left, or detached, whatever the limit.
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

	// Branches returns this fan-out's branches — pools and lanes alike — in creation order.
	Branches() []Branch
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

// MoveTo enters this fan-out (through its queue, if it has one) with an empty open body and joins the listed waves.
// See the FanOut interface for the full contract.
func (f *fanOut) MoveTo(ctx context.Context, joins ...Wave) error {
	it, r, err := f.series.conveyor.actingItem(ctx, f.node, false)
	if err != nil {
		return fmt.Errorf("move to %s: %w", f, err)
	}
	defer r.mu.Unlock()
	r.checkEnterOrder(it, f.node) // panics on backward / repeat entry (misuse)
	// Enter the node without publishing its rank (the door): the items behind must not enqueue onto these branches
	// until this item has — its first Schedule, its leave or its Detach publishes — so their work stays in item
	// order. Admission publishes the waiting room's rank instead, so the item behind may step aside meanwhile.
	if err := r.enterUnit(ctx, it, f.node, false); err != nil {
		return fmt.Errorf("move to %s: %w", f, err)
	}
	r.newBody(it, f)
	if err := r.join(ctx, it, joins); err != nil {
		return fmt.Errorf("join at %s: %w", f, err)
	}
	return nil
}

// TryMoveTo enters this fan-out only if that needs no waiting, with an empty open body if it did. See the FanOut
// interface for the full contract.
func (f *fanOut) TryMoveTo(ctx context.Context, joins ...Wave) (entered bool, err error) {
	it, r, err := f.series.conveyor.actingItem(ctx, f.node, true)
	if err != nil {
		return false, fmt.Errorf("try move to %s: %w", f, err)
	}
	defer r.mu.Unlock()
	r.checkEnterOrder(it, f.node) // panics on backward / repeat entry (misuse)
	// publish == false for the same reason as in MoveTo: the door stays closed until this item schedules.
	entered, err = r.tryEnterUnit(it, f.node, false)
	if err != nil || !entered {
		return false, err
	}
	r.newBody(it, f)
	if err := r.join(ctx, it, joins); err != nil {
		return true, fmt.Errorf("join at %s: %w", f, err)
	}
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
	if err := w.it.cancelCause(ctx); err != nil {
		return fmt.Errorf("schedule at %s: %w", f, err)
	}
	r.addToBody(w.it, w, f, tasks, root)
	return nil
}

// bodyFor resolves the body a Schedule adds to, and whether the addition is a root (through the item's own path) or
// a spawn (from the body's own work). A pool's work adds to its own wave, which must be this fan-out's; a lane child
// adds to its parent's wave when this is the fan-out its lane belongs to (checked before the scope, which is the
// parent's); otherwise the item acts in its own scope and its body here must be open. Caller holds mu.
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
	f.series.conveyor.validateScope(it, f.node)
	return f.openBody(it, "schedule"), true
}

// Wait blocks until the item's body at this fan-out is idle and reports its outcome. See the FanOut interface for the
// full contract.
func (f *fanOut) Wait(ctx context.Context) error {
	it, r, err := f.series.conveyor.actingItem(ctx, f.node, false)
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
	case bodyDetached:
		panic(fmt.Errorf("cannot %s at %s: %w", verb, f, errWorkDetached))
	default:
		panic(fmt.Errorf("cannot %s at %s: %w", verb, f, errStageNotEntered))
	}
}

// Detach hands this fan-out's slot to the work the item scheduled here. See the FanOut interface for the full
// contract.
func (f *fanOut) Detach(ctx context.Context) Wave {
	// checkCancel is false: a canceled item is handed its own wave (the work is already scheduled and will settle with
	// the cancellation cause), not an error — the same choice Stage.Retain makes.
	it, r, err := f.series.conveyor.actingItem(ctx, f.node, false)
	if err != nil {
		// No item to charge: hand back a standalone finished wave carrying the reason.
		return standaloneWave(fmt.Errorf("detach %s: %w", f, err))
	}
	defer r.mu.Unlock()
	// The body state, not occupancy, says whether there is work to hand over, so the diagnostic is the same whether
	// the item still stands here (a detached wave holding the slot, a failed leave) or has moved on.
	switch it.body[f.node.index] {
	case bodyOpen:
	case bodyDetached:
		panic(fmt.Errorf("cannot detach %s: already detached: %w", f, errNothingToDetach))
	case bodyClosed:
		panic(fmt.Errorf("cannot detach %s: the body was closed when the item left: %w", f, errNothingToDetach))
	default:
		panic(fmt.Errorf("cannot detach %s: %w", f, errStageNotEntered))
	}
	if it.occupied[f.node.index] == 0 {
		panic(fmt.Errorf("cannot detach %s: %w", f, errStageNotEntered)) // an open body is always occupied
	}
	w := it.pending
	// From here the slot follows the work, not the item: releaseBelow leaves it alone (see item.isRetaining) and the
	// branch workers free it once the last task is done — or the item's next move, if the work is already done.
	w.retainUnit = f.node
	it.sealBody(w, bodyDetached)
	// The door opens (a no-op after the item's first Schedule): its work here is on the branches for good.
	if f.node.rank > it.maxRank {
		it.maxRank = f.node.rank
	}
	r.cond.Broadcast()
	return w
}

// newBody opens the item's body at f: an unsealed idle wave, recorded as the item's pending work with body state
// open. It is sealed when the item leaves, detaches or completes (see item.sealBody), so an empty body still
// finishes. Caller holds run.mu.
func (r *run) newBody(it *item, f *fanOut) *wave {
	w := newWave(r, it)
	w.sealed = false
	w.atNode = f.node
	it.pending = w
	it.body[f.node.index] = bodyOpen
	return w
}

// addToBody adds tasks to the item's body w at f: it claims the sources and groups them into one collection per
// branch in argument order (where a resubmitted Task panics, before anything is mutated), inserts each collection
// into its branch queue at the item's place, publishes the node's rank so the next item may enter (the door; a
// no-op after the first time), and starts the work. root says the tasks come through the item's own path (see
// taskCollection.root). Caller holds run.mu.
//
// Several tasks for the same branch become one collection, whose sources are consumed front to back — which is what
// makes the order the caller listed them the order their work starts in.
func (r *run) addToBody(it *item, w *wave, f *fanOut, tasks []Task, root bool) {
	byBranch := make(map[int]*taskCollection, len(f.branches))
	touched := make([]int, 0, len(f.branches))
	for _, t := range tasks {
		if t.src == nil {
			continue // statically-empty task (e.g. NewTasks with count <= 0)
		}
		if !t.src.claim() {
			panic(fmt.Errorf("task for %s was already submitted: %w", t.branch, errTaskReused))
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

func (f *fanOut) Branches() []Branch {
	bs := make([]Branch, 0, len(f.branches))
	for _, b := range f.branches {
		bs = append(bs, b)
	}
	return bs
}
