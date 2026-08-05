package conveyor

import (
	"context"
	"fmt"
)

// FanOut is a node whose work runs in parallel on branches: the item schedules tasks with MoveTo, and the move
// that takes it out of the node waits for them to finish. Use it for concurrent work such as writing to several
// databases, or, with a Lane, to turn one item into several child journeys.
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

	// MoveTo advances the item into this fan-out, releasing the previous node, joins the listed waves, and
	// schedules tasks onto the fan-out's branches. It returns once the tasks are scheduled, not finished: the work
	// runs in the background, and the item's next move waits for it unless it is handed off with Detach.
	//
	// Build tasks with the branches' constructors (Pool.NewTask / Lane.NewTask and their siblings) and pass them
	// as Tasks. Passing no tasks is legal.
	//
	// It returns ErrForeignContext, ErrStaleContext, or the item's cancellation cause on shutdown. It panics on
	// misuse: a task from another fan-out, resubmitting a Task, moving backward, re-entering this node, a node
	// outside the item's own series, or a wave from another item.
	MoveTo(ctx context.Context, tasks Tasks, joins ...Wave) error

	// TryMoveTo is MoveTo without waiting: it enters the node and schedules the tasks only if it can do so right
	// now, and reports whether it did.
	//
	// When entered is false nothing happened: the tasks are left unclaimed, so the same Tasks value may be
	// submitted later. It panics on the same misuse as MoveTo.
	TryMoveTo(ctx context.Context, tasks Tasks, joins ...Wave) (entered bool, err error)

	// Detach hands this fan-out's slot to the work already scheduled here, letting the item move on without
	// waiting for it to finish. Join the returned Wave in a later MoveTo to wait for it.
	//
	// It panics on misuse: a node the item does not currently occupy, or no outstanding work here to detach.
	Detach(ctx context.Context) Wave

	// SetLimit sets how many items may have work outstanding in this fan-out at once (default 1; a limit <= 0
	// means 1), and returns the fan-out for chaining. An item counts against it from the moment its work is
	// scheduled until it enters the next node.
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

// MoveTo enters this fan-out (through its queue, if it has one), joins the listed waves and schedules the tasks. See
// the FanOut interface for the full contract.
func (f *fanOut) MoveTo(ctx context.Context, tasks Tasks, joins ...Wave) error {
	f.validateTasks(tasks) // static wiring check first, before anything touches the context
	it, r, err := f.series.conveyor.actingItem(ctx, f.node, false)
	if err != nil {
		return fmt.Errorf("move to %s: %w", f, err)
	}
	defer r.mu.Unlock()
	r.checkEnterOrder(it, f.node) // panics on backward / repeat entry (misuse)
	// Enter the node without publishing its rank: the items behind must not enqueue onto these branches until this
	// item has (step 4), so their work stays in item order.
	if err := r.enterUnit(ctx, it, f.node, false); err != nil {
		return fmt.Errorf("move to %s: %w", f, err)
	}
	if err := r.join(ctx, it, joins); err != nil {
		return fmt.Errorf("join at %s: %w", f, err)
	}
	r.scheduleWave(it, f, tasks)
	return nil
}

// TryMoveTo enters this fan-out only if that needs no waiting, and schedules the tasks if it did. See the FanOut
// interface for the full contract.
func (f *fanOut) TryMoveTo(ctx context.Context, tasks Tasks, joins ...Wave) (entered bool, err error) {
	f.validateTasks(tasks)
	it, r, err := f.series.conveyor.actingItem(ctx, f.node, true)
	if err != nil {
		return false, fmt.Errorf("try move to %s: %w", f, err)
	}
	defer r.mu.Unlock()
	r.checkEnterOrder(it, f.node) // panics on backward / repeat entry (misuse)
	// publish == false for the same reason as in MoveTo: the items behind must not enqueue onto these branches before
	// this item has (scheduleWave publishes once the work is on the lanes).
	entered, err = r.tryEnterUnit(it, f.node, false)
	if err != nil || !entered {
		return false, err
	}
	if err := r.join(ctx, it, joins); err != nil {
		return true, fmt.Errorf("join at %s: %w", f, err)
	}
	r.scheduleWave(it, f, tasks)
	return true, nil
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
	if it.occupied[f.node.index] == 0 {
		panic(fmt.Errorf("cannot detach %s: %w", f, errStageNotEntered))
	}
	// The pending wave must be this node's: an item that detached at an earlier fan-out still occupies that node
	// (its slot is held by the wave), so occupancy alone would not tell the two apart.
	if it.pending == nil || it.pending.atNode != f.node {
		panic(fmt.Errorf("cannot detach %s: %w", f, errNothingToDetach))
	}
	w := it.pending
	it.pending = nil
	// From here the slot follows the work, not the item: releaseBelow leaves it alone (see item.isRetaining) and the
	// branch workers free it once the last task is done.
	w.retainUnit = f.node
	return w
}

// scheduleWave groups the tasks into one collection per branch (in argument order), enqueues them atomically — in
// item order, per the gate in MoveTo — publishes the node's rank so the next item may enqueue, and starts the
// work. The wave becomes the item's pending work: the node's body, which it must see finish before it may leave.
// Caller holds run.mu.
//
// Several tasks for the same branch become one collection, whose sources are consumed front to back — which is what
// makes the order the caller added them to Tasks the order their work starts in.
//
// The grouping (which claims the sources, and so is where the single-use misuse panic fires) happens before the
// wave is created on purpose: a panic must not leave the item owning a wave that nothing will ever settle, or the
// item could never complete.
func (r *run) scheduleWave(it *item, f *fanOut, tasks Tasks) *wave {
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
			col = &taskCollection{it: it, branch: t.branch}
			byBranch[branchIdx] = col
			touched = append(touched, branchIdx)
		}
		col.sources = append(col.sources, t.src)
	}
	w := newWave(r, it)
	w.atNode = f.node
	it.pending = w
	for _, branchIdx := range touched {
		byBranch[branchIdx].wave = w
		r.enqueueCollection(branchIdx, byBranch[branchIdx])
	}
	// Publishing the rank is what opens the gate for the next item and keeps the "older item's maxRank >=
	// younger item's" invariant.
	if f.node.rank > it.maxRank {
		it.maxRank = f.node.rank
	}
	w.settle() // an empty submission is born finished
	for _, branchIdx := range touched {
		r.pump(branchIdx)
	}
	r.cond.Broadcast()
	return w
}

// assignRank reserves rank r for the waiting room and gives the node unit the next one, whether or not a queue is
// configured (see the rank discussion in builder.go). The branches' interior series are ranked independently (each is
// its own scope; see Conveyor.finalize).
func (f *fanOut) assignRank(scope, r int) int {
	f.node.scope, f.node.rank = scope, r+1
	return r + 2
}

// owns reports whether b is one of this fan-out's branches (nil-safe, so a zero Task fails the ownership check with
// the wiring-bug panic rather than a nil dereference).
func (f *fanOut) owns(b *branch) bool { return b != nil && b.fanout == f }

// validateTasks panics if any task belongs to another fan-out — a static wiring mistake, so both MoveTo variants
// check it before they touch the context.
func (f *fanOut) validateTasks(tasks Tasks) {
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
