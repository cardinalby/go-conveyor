package conveyor

import "fmt"

// This file holds the topology builder.
//
// The whole topology is built from one recursive shape:
//
//	series = a start gate (one unit) + an ordered list of nodes
//	node   = a Stage, or a FanOut (a group of branches)
//	branch = a series again — empty of nodes for a Pool, populated for a Lane
//
// The conveyor itself is the root series; its start gate is the implicit start stage that paces item creation.
// A branch is a series whose start gate is the branch's own unit. That is the whole model — "sub-conveyor" is not a
// separate concept, it is just a Lane, i.e. a branch given nodes of its own.
//
// Three numbers describe a node:
//   - index — unique across the conveyor; indexes the per-run occupancy array and the per-item arrays.
//   - rank  — position within its series (scope). Ranks are assigned once, at finalize, and only ever compared
//     between units of the same scope.
//   - ord   — 1-based position among the nodes of its series, assigned at creation. It is only used for the
//     positional name of an unnamed node, so that name does not move when ranks do.
//
// Every node reserves *two* consecutive ranks: its waiting room takes the first (see unit.queueRank) and the node
// itself the second. The pair is reserved whether or not a queue exists, so a queue added later slots into a rank
// that was always its own and nothing is renumbered — which is what lets SetQueueSize run on a live conveyor.
// The two ranks are what keep a queue honest: an item waiting in front of a node publishes the lower rank, so the
// ordering gate still holds the item behind it back, and a TryMoveTo cannot take a slot from under it.
//
// index is assigned in creation order as units are built (Conveyor.newUnit); rank is assigned by walking each
// series in finalize.

// node is a series element that can have ranks assigned at finalize. assignRank sets the ranks of the units the
// node owns, starting at r, and returns the first rank that follows it.
type node interface {
	assignRank(scope, r int) int
}

// series is one scope of the flow: a start gate plus the nodes built on it. The root series is the conveyor
// (start gate = the implicit start stage); every branch is a series too (start gate = the branch's unit).
type series struct {
	conveyor *Conveyor
	id       int    // scope id; 0 is the root series
	start    *unit  // the series' start gate, always rank 0 of this scope
	nodes    []node // stages and fan-outs, in creation order
}

// AddStage adds a new Stage to the topology, admitting one item at a time by default. Chain SetLimit and
// SetQueueSize to adjust its capacity, and pass OptName to name it.
//
// It panics if the conveyor is running or has already run.
func (s *series) AddStage(opts ...AnyUnitOption) Stage {
	cfg := newAnyUnitConfig(opts)
	st := &stage{series: s, name: cfg.name, ord: len(s.nodes) + 1}
	st.work = s.conveyor.newUnit(st, kindStage)
	s.nodes = append(s.nodes, st)
	return st
}

// AddFanOut adds a new FanOut to the topology: a node whose work runs in parallel on branches added with AddPool
// or AddLane. It admits one item at a time by default; chain SetLimit and SetQueueSize to adjust its capacity, and
// pass OptName to name it.
//
// It panics if the conveyor is running or has already run.
func (s *series) AddFanOut(opts ...AnyUnitOption) FanOut {
	cfg := newAnyUnitConfig(opts)
	f := &fanOut{series: s, name: cfg.name, ord: len(s.nodes) + 1}
	f.node = s.conveyor.newUnit(f, kindFanOut)
	s.nodes = append(s.nodes, f)
	return f
}

// assignRanks lays out this series: the start gate takes rank 0, then each node in creation order takes the
// ranks that follow. It recurses into the branches of every fan-out (each branch is a series of its own).
func (s *series) assignRanks() {
	s.start.scope = s.id
	s.start.rank = 0
	r := 1
	for _, n := range s.nodes {
		r = n.assignRank(s.id, r)
	}
}

// positionalName builds the fallback name of a node in this series from its ordinal: "stage 3" is the third node
// of the root series, "exports.1 / stage 1" the first node of a lane (the lane's own name is resolved when asked,
// so it is correct even for an unnamed lane). Nodes of both kinds share one sequence, so the ordinal identifies a
// node within its series regardless of kind.
func (s *series) positionalName(kind string, ord int) string {
	if s.id == 0 {
		return fmt.Sprintf("%s %d", kind, ord)
	}
	return fmt.Sprintf("%s / %s %d", s.start.owner, kind, ord)
}

// newUnit creates a capacity unit owned by owner, assigns it the next index in creation order and appends it to
// the flat, index-ordered units slice the per-run occupancy array is sized from. Its scope and rank are filled
// in later, at finalize. It panics if the topology is no longer mutable.
func (c *Conveyor) newUnit(owner unitOwner, kind unitKind) *unit {
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if c.isRunning {
		panic(errConveyorRunning)
	}
	if c.finalized {
		panic(errConveyorFinalized)
	}
	u := &unit{
		conveyor: c,
		owner:    owner,
		kind:     kind,
		index:    len(c.units),
	}
	u.limit.Store(1) // limit 1 by default; SetLimit relaxes it
	c.units = append(c.units, u)
	return u
}

// newSeries creates a branch's series (a new scope) with the branch's unit as its start gate. Like newUnit it holds
// runMu for the whole mutation — both the scope counter and the allSeries list it appends to are topology that
// finalize walks — and refuses to extend a conveyor that is running or has run. Its caller (fanOut.addBranch) reaches
// newUnit first, so in practice that check has already fired; keeping it here means the guarantee belongs to the
// function rather than to the order its callers happen to use.
func (c *Conveyor) newSeries(start *unit) *series {
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if c.isRunning {
		panic(errConveyorRunning)
	}
	if c.finalized {
		panic(errConveyorFinalized)
	}
	c.nextScope++
	s := &series{conveyor: c, id: c.nextScope, start: start}
	c.allSeries = append(c.allSeries, s)
	return s
}

// finalize assigns scopes and ranks over every series exactly once; after it, the topology is frozen. It also
// caches the per-scope unit lists the release path walks. It is called under runMu from tryRun on the first
// Run, and (idempotently) from newRun so tests that build a run directly still see ranks assigned.
func (c *Conveyor) finalize() {
	if c.finalized {
		return
	}
	for _, s := range c.allSeries {
		s.assignRanks()
	}
	c.scopeUnits = make([][]*unit, c.nextScope+1)
	for _, u := range c.units {
		c.scopeUnits[u.scope] = append(c.scopeUnits[u.scope], u)
	}
	c.finalized = true
}
