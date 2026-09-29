// Package conveyor moves items through an ordered series of nodes, keeping their relative order while managing
// capacity, backpressure, and completion. The path of one item is written as a single function, the ItemProcessor.
//
// Everyday vocabulary:
//   - MoveTo advances the item into a node.
//   - Schedule registers parallel work for a fan-out, before or after entering it.
//   - FanOut.Wait joins the work scheduled so far without leaving the fan-out.
//   - Retain (on a Stage or a FanOut) lets unfinished work keep the node while the item moves on.
//   - Wave.Wait waits for retained work later in the item's path.
//
// Most processors need only MoveTo and Schedule. Adaptive rounds add FanOut.Wait; overlapping work across stages
// adds Retain.
package conveyor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Conveyor moves items through an ordered series of nodes, preserving their relative order. Create one with
// NewConveyor, build the nodes, then call Run with an ItemProcessor. The conveyor runs one ItemProcessor per item,
// each on its own goroutine, and handles ordering, capacity, and backpressure between nodes.
//
// A node is either a Stage (AddStage), whose code runs inline in the ItemProcessor, or a FanOut (AddFanOut), where
// the item schedules work onto branches (Pool or Lane) that run it in parallel. An item advances between nodes with
// MoveTo.
//
// Background work started with Stage.Retain or FanOut.Retain is represented by a Wave: wait for it with Wave.Wait,
// or read its Finished and Err.
type Conveyor interface {
	// AddStage adds a Stage to the end of the conveyor, admitting one item at a time by default. Chain SetLimit and
	// SetQueueSize to adjust its capacity, and pass OptName to name it.
	//
	// It panics if the conveyor is running or has already run.
	AddStage(opts ...AnyUnitOption) Stage

	// AddFanOut adds a FanOut to the end of the conveyor: a node whose work runs in parallel on branches added with
	// AddPool or AddLane. It admits one item at a time by default; chain SetLimit and SetQueueSize to adjust its
	// capacity, and pass OptName to name it.
	//
	// It panics if the conveyor is running or has already run.
	AddFanOut(opts ...AnyUnitOption) FanOut

	// Run starts the conveyor, creating one item per itemProcessor invocation until ctx is canceled or an item
	// fails. It blocks until every in-flight item has finished, then returns the first item error, or else ctx's
	// cancellation cause.
	//
	// Build all nodes before calling Run; the topology is frozen from the first Run on. Run may be called again
	// after it returns, but a concurrent second call returns ErrConveyorAlreadyRunning.
	Run(ctx context.Context, itemProcessor ItemProcessor) error

	// SetItemsLimit caps how many items may be in flight across the whole conveyor at once. One item is one
	// ItemProcessor call, from creation to completion, so this also bounds the number of worker goroutines. A limit <= 0
	// means unlimited, the default. It returns the conveyor for chaining.
	//
	// It is a global bound on top of the nodes' own limits and does not change any of them.
	//
	// Safe to call at any time, from any goroutine, including on a running conveyor. Lowering it never evicts an item in
	// flight; it only stops new items from being created until the count has fallen below the new limit.
	SetItemsLimit(n int) Conveyor

	// ItemsLimit returns the current cap on items in flight across the whole conveyor, or 0 if unlimited (the
	// default).
	ItemsLimit() int

	// SetNoAbortPoint marks node as the place where side effects start. When shutdown begins (the Run context is
	// canceled, or an item fails), every item that has not entered node or a later node yet is aborted: canceled at
	// once with a ShutdownError. Items that have entered it are not aborted, even if the point is moved later: they
	// finish as usual, bounded by OptShutdownContext. An item waiting in node's waiting room, or blocked in
	// MoveTo(node), has not entered it; an item that skipped node and entered a later node, or its waiting room, has
	// passed it. It returns the conveyor for chaining.
	//
	//	write := c.AddStage() // side effects start here
	//	commit := c.AddStage()
	//	c.SetNoAbortPoint(write)
	//
	// The default is the starting stage: every item enters it first, so no item is aborted. Pass c.StartingStage()
	// to restore it. Otherwise node must be a Stage or a FanOut added with c.AddStage or c.AddFanOut. It panics for
	// nil, a pool, a lane, a node inside a lane, or a node of another conveyor.
	//
	// Safe to call at any time, from any goroutine, including on a running conveyor. Items that have already entered
	// node or a later node are protected at once. Items created while the point was the starting stage have entered
	// it, so they are never aborted. Once shutdown has begun, a call changes nothing for items in flight. Code that
	// ignores its ctx is not interrupted.
	SetNoAbortPoint(node Unit) Conveyor

	// NoAbortPoint returns the node set with SetNoAbortPoint; the starting stage by default.
	NoAbortPoint() Unit

	// StartingStage returns the implicit stage every item starts in. The ItemProcessor code before the item's first
	// MoveTo runs in it, one item at a time, and the next item is created only when it is free. It has no MoveTo
	// and its limit is always 1.
	//
	// Use it to match the stage in Stats, or call Retain to hand its slot to background work: the item moves on,
	// but the next item is not created until that work returns.
	StartingStage() RetainableStage

	// Stats returns a snapshot of the active run's state and resets the gauge windows. Safe to call at any time,
	// from any goroutine; outside a run it reports the zero Stats.
	Stats() Stats

	// DebugUnitOccupants reports, for every unit, exactly which items occupy its body and its waiting room right
	// now. It exists for debugging and visualization; use Stats for production observability.
	//
	// Safe to call at any time, from any goroutine; outside a run it reports nil.
	DebugUnitOccupants() []UnitOccupants
}

// conveyor is the Conveyor implementation. It is the root series (see builder.go) plus the whole-conveyor state:
// the flat unit list a run is sized from, the scope bookkeeping finalize walks, and the run currently using them.
type conveyor struct {
	// series is the root series; its AddStage / AddFanOut are promoted, satisfying that part of Conveyor.
	*series

	// shutdownCtxFactory is asked for the context that bounds a shutdown, once one begins (see
	// OptShutdownContext). Nil means in-flight items are left to finish on their own.
	shutdownCtxFactory ShutdownContextFactory

	// units is every capacity unit, in creation order; index 0 is the implicit start stage. Immutable after the
	// first Run.
	units []*unit
	// allSeries is every series (the root plus one per branch), used by finalize to assign ranks.
	allSeries []*series
	// scopeUnits[scope] lists the units of that scope, cached at finalize for the release path.
	scopeUnits [][]*unit
	// nextScope is the last scope id handed out (the root series is 0).
	nextScope int

	runMu     sync.Mutex // guards isRunning, finalized and topology mutation
	isRunning bool
	finalized bool // set once ranks are assigned (finalize); the topology is frozen thereafter

	// currentRun points at the active run for the duration of a Run call (nil otherwise). It backs Stats and
	// lets a unit reach the live run (e.g. to wake waiters after a SetLimit). It is an atomic pointer so
	// concurrent readers never see a torn value.
	currentRun atomic.Pointer[run]

	// itemsLimit caps how many items may be in flight across the whole conveyor at once — the root items a
	// worker drives from creation to completion (see run.worker), so this is also a cap on live workers. 0 means
	// unlimited, the default. Atomic because SetItemsLimit may change it from any goroutine while a run is
	// active; every read otherwise happens under run.mu (see run.hasItemsRoom).
	itemsLimit atomic.Int64

	// noAbortPoint is the unit of the node set with SetNoAbortPoint, or nil for the starting stage (the default). A
	// root item that enters it or a later node is marked noAbort; when shutdown begins, the unmarked items are
	// aborted (see run.abortUnprotectedLocked). Atomic for the lock-free getter and the store before any run exists;
	// while a run is active it is stored and read under run.mu.
	noAbortPoint atomic.Pointer[unit]

	// assignHook, when set by an in-package test before Run, observes every hand-out of a branch slot (see
	// run.grabNext). Nil in production; read without a lock, so it must be set before Run and never changed during
	// one.
	assignHook func(branchIdx int, col *taskCollection, queue []*taskCollection)

	// noAbortStoreHook, when set by an in-package test before SetNoAbortPoint, runs in it between the load of
	// currentRun and the store when no run was loaded: a test can start a run there. Nil in production.
	noAbortStoreHook func()
}

// Option configures a Conveyor at creation. See NewConveyor and OptShutdownContext.
type Option func(c *conveyor)

// ShutdownContextFactory produces the context that bounds a shutdown. cause is the first item error, or the Run
// context's cancellation cause. See OptShutdownContext.
type ShutdownContextFactory func(cause error) (context.Context, context.CancelFunc)

// OptShutdownContext bounds how long items may keep running after a shutdown begins — triggered by the Run
// context being canceled, or by an ItemProcessor error. Once shutdown starts, the factory is asked for a context;
// when that context is done, every in-flight item's context is canceled.
//
//	return context.WithTimeout(context.Background(), 30*time.Second) // a grace period, then cancel
//	return alreadyDoneCtx, nil                                       // cancel in-flight items at once
//	return nil, nil                                                  // no limit: leave them to finish
//
// Without this option, items are left to finish on their own.
//
// Items before the no-abort point (SetNoAbortPoint) are aborted when shutdown begins, without waiting for this
// context.
func OptShutdownContext(factory ShutdownContextFactory) Option {
	return func(c *conveyor) {
		c.shutdownCtxFactory = factory
	}
}

// NewConveyor creates a conveyor with no nodes: add them with AddStage and AddFanOut, then call Run.
func NewConveyor(options ...Option) Conveyor {
	c := &conveyor{}
	root := &series{conveyor: c, id: 0}
	c.series = root
	c.allSeries = []*series{root}
	// The implicit start stage: rank 0 of the root series, limit 1, so one item at a time is created.
	start := &unit{conveyor: c, owner: startOwner{}, kind: kindStart, index: 0}
	start.limit.Store(1)
	c.units = []*unit{start}
	root.start = start
	for _, opt := range options {
		opt(c)
	}
	return c
}

// SetItemsLimit caps the items in flight across the whole conveyor (see the Conveyor interface).
func (c *conveyor) SetItemsLimit(n int) Conveyor {
	if n < 0 {
		n = 0
	}
	c.itemsLimit.Store(int64(n))
	if r := c.currentRun.Load(); r != nil {
		r.mu.Lock()
		r.cond.Broadcast() // wake the standby worker so a raise is picked up at once
		r.mu.Unlock()
	}
	return c
}

func (c *conveyor) ItemsLimit() int { return int(c.itemsLimit.Load()) }

// SetNoAbortPoint sets the node before which items are aborted at shutdown (see the Conveyor interface).
func (c *conveyor) SetNoAbortPoint(node Unit) Conveyor {
	u := c.noAbortPointUnit(node)
	// As in unit.storeDial: under the run's mu, so the store and the marking are one step for the run's items. A run
	// that starts between the load and the store is updated too. Nothing is aborted here: once shutdown has begun,
	// every live item is either marked or already aborted, and a move never clears a mark.
	apply := func(r *run) {
		r.mu.Lock()
		c.noAbortPoint.Store(u)
		r.protectReachedLocked()
		r.mu.Unlock()
	}
	r := c.currentRun.Load()
	if r == nil {
		if c.noAbortStoreHook != nil {
			c.noAbortStoreHook()
		}
		c.noAbortPoint.Store(u)
	} else {
		apply(r)
	}
	if r2 := c.currentRun.Load(); r2 != nil && r2 != r {
		apply(r2)
	}
	return c
}

// noAbortPointUnit returns the unit to store for n: nil for the starting stage, else the unit of a Stage or FanOut
// of the root series. It panics for anything else. The series is read from the owner, not from unit.scope, which is
// set only at finalize.
func (c *conveyor) noAbortPointUnit(n Unit) *unit {
	if n == nil {
		panic(fmt.Errorf("nil no-abort point: pass the starting stage to restore the default: %w", errInvalidUnit))
	}
	u := n.unit()
	c.validateUnit(u)
	var s *series
	switch o := u.owner.(type) {
	case startOwner:
		return nil
	case *stage:
		s = o.series
	case *fanOut:
		s = o.series
	default:
		panic(fmt.Errorf("%s cannot be the no-abort point: only a stage or a fan-out can: %w", u, errInvalidUnit))
	}
	if s != c.series {
		panic(fmt.Errorf("%s cannot be the no-abort point: it belongs to %s, not to the conveyor: %w",
			u, s.start, errWrongScope))
	}
	return u
}

func (c *conveyor) NoAbortPoint() Unit {
	u := c.noAbortPoint.Load()
	if u == nil {
		return c.StartingStage()
	}
	return u.owner.(Unit)
}

// startOwner names the implicit start stage in Stats and error messages.
type startOwner struct{}

func (startOwner) String() string { return "start" }

// StartingStage hands out the handle of the implicit start stage (see the Conveyor interface).
func (c *conveyor) StartingStage() RetainableStage { return startHandle{c.units[0]} }

// startHandle is the public handle of the implicit start stage. It is a value, so handles of one conveyor compare
// equal (Stats matching).
type startHandle struct{ u *unit }

func (h startHandle) String() string { return h.u.String() }
func (h startHandle) unit() *unit    { return h.u }

// Retain hands the start stage's slot to a background operation (see the RetainableStage interface).
func (h startHandle) Retain(ctx context.Context, bgOp func() error) Wave {
	return h.u.conveyor.retain(ctx, h.u, bgOp)
}

// validateUnit panics if u is not a unit of this conveyor — a handle from another conveyor, or a zero handle.
func (c *conveyor) validateUnit(u *unit) {
	if u == nil {
		panic(fmt.Errorf("nil node handle: %w", errInvalidUnit))
	}
	if u.index < 0 || u.index >= len(c.units) || c.units[u.index] != u {
		panic(fmt.Errorf("%s does not belong to this conveyor: %w", u, errInvalidUnit))
	}
}

// validateItemConveyor panics if the item does not belong to this conveyor — a node handle used with an
// ItemProcessor context from a different conveyor. It must be called (before indexing the item by a unit's
// index) whenever a handle from this conveyor is combined with an item resolved from a context, since
// validateUnit only proves the handle belongs to its own conveyor, not that it matches the acting item.
func (c *conveyor) validateItemConveyor(it *item) {
	if it.run.conveyor != c {
		panic(fmt.Errorf("context of an item from a different conveyor used with a node of this one: %w", errInvalidUnit))
	}
}

// validateScope panics if the acting item cannot use u because u lives in a different part of the topology: a child
// item running inside a lane may only use that lane's own nodes, and an item of the root series may not reach into a
// lane. Both are wiring mistakes with no benign occurrence. verb names the refused call ("move to", "wait at", ...).
func (c *conveyor) validateScope(it *item, verb string, u *unit) {
	if it.scope != u.scope {
		panic(fmt.Errorf("cannot %s %s: it belongs to %s, but this item runs in %s: %w",
			verb, u, c.scopeName(u.scope), c.scopeName(it.scope), errWrongScope))
	}
}

// scopeName describes a scope for error messages: the root series or the lane that owns it.
func (c *conveyor) scopeName(scope int) string {
	if scope == 0 {
		return "the conveyor"
	}
	for _, s := range c.allSeries {
		if s.id == scope {
			return s.start.String()
		}
	}
	return fmt.Sprintf("scope %d", scope)
}

// describeRank names whatever occupies rank r in scope, for panic messages. Every node owns two ranks, so r may
// also be a node's reserved waiting-room rank (see unit.queueRank).
func (c *conveyor) describeRank(scope, r int) string {
	for _, u := range c.units {
		if u.scope != scope {
			continue
		}
		switch r {
		case u.rank:
			return u.String()
		case u.queueRank():
			return u.queueName()
		}
	}
	return fmt.Sprintf("rank %d", r)
}
