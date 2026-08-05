package conveyor

import (
	"fmt"
	"sync/atomic"
)

// This file holds the single capacity primitive of the package — the unit — plus the naming and build-time
// options shared by every node.
//
// Everything the user builds is made of units. A unit is a counted set of slots; taking a slot is the only way
// to be somewhere in the conveyor, and an item (or a task) always holds at least one slot from the moment it is
// created until it finishes. That is the invariant the whole design rests on: no state without a token.
//
//	node / thing built      units it owns                          what a slot means
//	---------------------   ------------------------------------   -----------------------------------------
//	Conveyor (root series)  1 start unit                           an item that exists but has not moved yet
//	Stage                   1 work unit                            an item running the stage's code
//	FanOut                  1 node unit                            an item inside the fan-out
//	Pool                    1 start unit (of an empty series)      a task running
//	Lane                    1 start unit (of the lane's series)    a child item that has not moved on yet
//
// A branch's unit is the start unit of the branch's own series, which is why neither kind needs separate concepts: a
// pool's task never moves off it (so the limit is "tasks in parallel"), while a lane's child may move on into the
// interior stages (so the same unit, always limit 1, paces child creation exactly as the conveyor's start paces
// items).
//
// A unit has two capacities, and the difference between them is the whole of the waiting-room feature:
//
//	limit     — how many items may be *in* the node (running its code, or inside the fan-out)
//	queueSize — how many more may wait in front of it, having already released the previous node
//
// The waiting room is not a unit of its own: it is these queued slots, counted separately (run.queued) and held
// at the node's reserved lower rank (see rank / queueRank). That is what makes queueSize a plain atomic dial an
// item's journey never depends on the existence of — so unlike a topology change, it can be turned up, down, or
// off at any time, on a running conveyor.

// unitKind classifies what a unit's slots hold. It drives naming and the few places the runtime must treat a
// unit specially (pumping a branch when its start gate frees).
type unitKind int

const (
	// kindStart is the start gate of a series: the conveyor's implicit start stage, or a branch's own unit.
	kindStart unitKind = iota
	// kindStage is a stage's work unit — items running the stage's code inline.
	kindStage
	// kindFanOut is a fan-out's node unit — items positionally inside the fan-out.
	kindFanOut
)

// unitOwner is the user-facing thing a unit belongs to (a Stage, a FanOut, a Branch, or the conveyor itself). It
// supplies the name used in Stats and in error messages.
type unitOwner interface {
	fmt.Stringer
}

// unit is the one capacity primitive (see the file comment). It is immutable topology shared across Run
// invocations — all mutable, per-run state (occupancy, sync) lives on run — except its two capacities, which
// SetLimit and SetQueueSize may change at any time.
type unit struct {
	conveyor *Conveyor
	owner    unitOwner
	kind     unitKind

	// limit is the max number of slots of the node itself in use at once (always >= 1). It is atomic because
	// SetLimit may change it from any goroutine while the run reads it; every read otherwise happens under run.mu.
	limit atomic.Int64

	// queueSize is how many items may wait in front of the node, on top of limit (0 = no waiting room). Atomic for
	// the same reason as limit: SetQueueSize may change it from any goroutine at any time, including while items
	// are waiting.
	queueSize atomic.Int64

	// index is unique per unit across the whole conveyor. It indexes the per-run occupancy array and the
	// per-item occupied/entered arrays. The conveyor's start unit is always index 0.
	index int

	// scope is the id of the series this unit's rank lives in (0 = the root series; each branch gets its own).
	// Ranks are only ever compared between units of the same scope.
	scope int

	// rank is the position in the flow within scope, assigned at finalize. It drives ordering (the gate that
	// keeps items in order) and release (leaving everything behind you). Every node owns two consecutive ranks —
	// its waiting room takes rank-1 (see queueRank), the node itself takes rank — so an item merely waiting in
	// front of a node cannot let a follower into the node ahead of it. The pair is reserved whether or not a
	// queue exists, so adding one never renumbers anything (see builder.go).
	rank int

	// branchSeries is set on a branch's start unit and points at the branch's series, so freeing a slot here can
	// pump the branch (start the next queued task / child). Empty of nodes for a pool.
	branchSeries *series
}

// String returns the unit's name: its owner's name (see Stage.String / FanOut.String / branch.String).
func (u *unit) String() string { return u.owner.String() }

// queueName is how this node's waiting room is identified in error messages.
func (u *unit) queueName() string { return u.owner.String() + ".queue" }

// queueRank is the rank of this node's waiting room: the one reserved just below the node itself. It is defined
// whether or not a queue exists, and no other unit of the scope ever holds it.
func (u *unit) queueRank() int { return u.rank - 1 }

// Unit is a handle to something with capacity: a Stage, a FanOut, a Branch (Pool or Lane), or Conveyor.StartUnit.
// It names the node in Stats and in error messages.
type Unit interface {
	fmt.Stringer
	// unit returns the backing capacity unit. It seals the interface: only this package can implement Unit.
	unit() *unit
}

// AnyUnitOption configures a node at creation (AddStage, AddFanOut, AddPool, AddLane). OptName is the only one;
// capacity is set separately with SetLimit and SetQueueSize.
type AnyUnitOption interface {
	applyToAnyUnitCfg(*anyUnitConfig)
}

type anyUnitConfig struct {
	name string
}

func newAnyUnitConfig(opts []AnyUnitOption) anyUnitConfig {
	var cfg anyUnitConfig
	for _, opt := range opts {
		opt.applyToAnyUnitCfg(&cfg)
	}
	return cfg
}

// OptName names a node, so it shows up by that name in Stats and in error messages. An unnamed node falls back to
// a positional name that shifts when the topology changes around it.
type OptName string

func (o OptName) applyToAnyUnitCfg(c *anyUnitConfig) { c.name = string(o) }
