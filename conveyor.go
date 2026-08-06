package conveyor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Conveyor moves items through an ordered series of nodes, preserving their relative order. Build the nodes, then
// call Run with an ItemProcessor: the conveyor runs one ItemProcessor per item, each on its own goroutine, and
// handles ordering, capacity and backpressure between nodes.
//
// A node is either a Stage (AddStage), whose code runs inline in the ItemProcessor, or a FanOut (AddFanOut), which
// schedules work onto branches (Pool or Lane) that run it in parallel. An item advances between nodes with MoveTo.
//
// Background work started with Stage.Retain or FanOut.Detach is represented by a Wave, joined with a later MoveTo.
type Conveyor struct {
	// series is the root series; it provides AddStage / AddFanOut on the conveyor itself (see builder.go).
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
}

// Option configures a Conveyor at creation. See NewConveyor.
type Option func(c *Conveyor)

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
func OptShutdownContext(factory ShutdownContextFactory) Option {
	return func(c *Conveyor) {
		c.shutdownCtxFactory = factory
	}
}

// NewConveyor creates a conveyor with no nodes: add them with AddStage and AddFanOut, then call Run.
func NewConveyor(options ...Option) *Conveyor {
	c := &Conveyor{}
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

// SetItemsLimit caps how many items may be in flight across the whole conveyor at once, which in effect bounds how
// many workers run concurrently: one worker drives one root item for its whole journey, from creation to
// completion (see Run). A limit <= 0 means unlimited, the default.
//
// Unlike a node's SetLimit, this bounds the conveyor globally, on top of whatever capacity the nodes themselves
// admit — it does not replace per-node limits, and setting it does not change any of them.
//
// Safe to call at any time, from any goroutine, including on a running conveyor: raising it wakes the standby
// worker so waiting to create the next item is picked up at once; lowering it never evicts an item already in
// flight — it only stops new items from being created until the in-flight count has fallen below the new limit.
func (c *Conveyor) SetItemsLimit(n int) *Conveyor {
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

// ItemsLimit returns the current cap on items in flight across the whole conveyor, or 0 if unlimited (the
// default).
func (c *Conveyor) ItemsLimit() int { return int(c.itemsLimit.Load()) }

// startOwner names the implicit start stage in Stats and error messages.
type startOwner struct{}

func (startOwner) String() string { return "start" }

// StartUnit returns the handle of the implicit start stage that paces item creation, for matching it in Stats. It
// is not a Stage; there is nothing to move to.
func (c *Conveyor) StartUnit() Unit { return startHandle{c.units[0]} }

type startHandle struct{ u *unit }

func (h startHandle) String() string { return h.u.String() }
func (h startHandle) unit() *unit    { return h.u }

// validateUnit panics if u is not a unit of this conveyor — a handle from another conveyor, or a zero handle.
func (c *Conveyor) validateUnit(u *unit) {
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
func (c *Conveyor) validateItemConveyor(it *item) {
	if it.run.conveyor != c {
		panic(fmt.Errorf("node used with a context from a different conveyor: %w", errInvalidUnit))
	}
}

// validateScope panics if the acting item cannot move to u because u lives in a different part of the topology:
// a child item running inside a lane may only move through that lane's own nodes, and an item of the root series
// may not reach into a lane. Both are wiring mistakes with no benign occurrence.
func (c *Conveyor) validateScope(it *item, u *unit) {
	if it.scope != u.scope {
		panic(fmt.Errorf("cannot move to %s: it belongs to %s, but this item runs in %s: %w",
			u, c.scopeName(u.scope), c.scopeName(it.scope), errWrongScope))
	}
}

// scopeName describes a scope for error messages: the root series or the lane that owns it.
func (c *Conveyor) scopeName(scope int) string {
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
func (c *Conveyor) describeRank(scope, r int) string {
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
