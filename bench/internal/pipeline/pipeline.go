// Package pipeline defines a single, implementation-agnostic model of a
// staged processing pipeline plus a progress Observer, so a benchmark can drive
// the conveyor and a hand-rolled channels/goroutines pipeline through the exact
// same harness and compare only their coordination machinery.
//
// The pieces:
//
//   - Spec — a declarative topology (a list of StageSpec nodes). One Spec is
//     interpreted by every Pipeline implementation, so both sides run the
//     identical shape of work.
//   - Pipeline — the thing under test. NewConveyor and NewTraditional both build
//     one from the same Spec and Observer; only their internals differ.
//   - Observer — collects per-item completion times and per-stage occupancy. Both
//     implementations feed it through the same calls (via the shared stageRuntime),
//     so the harness derives its metrics from identical data regardless of what
//     runs underneath.
//   - stageRuntime — the actual per-node "work" (mark occupancy, sleep for the
//     node's delay, unmark). Shared by both implementations, so the work and its
//     observation are literally the same code on both sides.
package pipeline

import (
	"context"
	"slices"
	"sync/atomic"
	"time"
)

// Kind classifies a node in a pipeline topology.
type Kind int

const (
	// Exclusive admits one item at a time and preserves order (a conveyor Stage,
	// or a single goroutine in the traditional pipeline).
	Exclusive Kind = iota
	// Shared admits up to Limit items concurrently (a shared conveyor Stage, or a
	// pool of Limit goroutines). Order is restored downstream.
	Shared
	// FanOut runs each item's per-branch tasks in parallel, joined before the item
	// proceeds (a conveyor FanOutStage, or one worker pool per branch plus a join).
	FanOut
)

// BranchSpec declares one branch of a FanOut node.
type BranchSpec struct {
	Name  string
	Limit int           // concurrent tasks on this branch
	Work  time.Duration // per-item task delay on this branch
}

// StageSpec declares one node of the pipeline.
type StageSpec struct {
	Name     string
	Kind     Kind
	Limit    int           // Shared: concurrent slots (ignored for Exclusive)
	Work     time.Duration // Exclusive/Shared: per-item delay
	Branches []BranchSpec  // FanOut only
}

// Spec is a full pipeline topology, interpreted identically by every Pipeline
// implementation so that only the execution machinery differs between them.
type Spec struct {
	Name   string
	Stages []StageSpec
}

// Pipeline drives items through a Spec's topology, reporting progress to the
// Observer it was built with. It is single-use: build it, then call Run once.
type Pipeline interface {
	// Run drives items numbered 1..n through the pipeline and blocks until all n
	// have finished their last stage, or ctx is canceled. Output order matches
	// input order (1, 2, 3, ...), matching the conveyor's ordering guarantee.
	Run(ctx context.Context, n int) error
}

// sleep parks for d, returning early if ctx is canceled (so shutdown is prompt).
// It models an I/O-bound stage: the goroutine is parked, not spinning, so many
// concurrent stages saturate slots without saturating CPU.
func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// Gauge tracks the concurrent occupancy of one stage/branch and its high-water
// mark. It is lock-free (atomics only) so its cost is the same negligible amount
// on both sides of the comparison.
type Gauge struct {
	cur  atomic.Int64
	peak atomic.Int64
}

func (g *Gauge) enter() {
	c := g.cur.Add(1)
	for {
		p := g.peak.Load()
		if c <= p || g.peak.CompareAndSwap(p, c) {
			return
		}
	}
}

func (g *Gauge) leave() { g.cur.Add(-1) }

// Peak returns the high-water occupancy observed so far.
func (g *Gauge) Peak() int { return int(g.peak.Load()) }

// stageRuntime is one node's shared work: mark occupancy, sleep for the node's
// delay, unmark. Both pipeline implementations call it, so the work itself and
// its observation are the same code path regardless of the surrounding machinery.
type stageRuntime struct {
	work  time.Duration
	gauge *Gauge
}

func (r *stageRuntime) process(ctx context.Context, _ int) {
	r.gauge.enter()
	sleep(ctx, r.work)
	r.gauge.leave()
}

// Observer collects per-item timing and per-stage occupancy for one run. One
// instance is shared by every goroutine of the run and is safe for concurrent
// use: each item index is written exactly once by a single goroutine, and the
// gauges are atomic. Both pipeline implementations report through the same calls.
type Observer struct {
	base   time.Time
	finish []int64 // monotonic ns since base, indexed by item-1; single writer each
	gauges map[string]*Gauge
}

// NewObserver allocates an Observer sized for n items, with one gauge per stage
// and per fan-out branch named in spec.
func NewObserver(n int, spec Spec) *Observer {
	o := &Observer{
		base:   time.Now(),
		finish: make([]int64, n),
		gauges: make(map[string]*Gauge),
	}
	for _, s := range spec.Stages {
		if s.Kind == FanOut {
			for _, b := range s.Branches {
				o.gauges[b.Name] = &Gauge{}
			}
			continue
		}
		o.gauges[s.Name] = &Gauge{}
	}
	return o
}

// itemFinished records the completion time of item no (1-based). Called once per
// item by the goroutine that observes it leave the last stage.
func (o *Observer) itemFinished(no int) {
	if no >= 1 && no <= len(o.finish) {
		o.finish[no-1] = int64(time.Since(o.base))
	}
}

// runtime builds a stageRuntime bound to the gauge for the named node.
func (o *Observer) runtime(name string, work time.Duration) *stageRuntime {
	return &stageRuntime{work: work, gauge: o.gauges[name]}
}

// SteadyInterval returns the average time between consecutive item completions
// over the steady-state window — the first and last `drop` items are excluded to
// remove pipeline fill and drain. This is the per-item throughput cost (T/N) the
// benchmark plots. ok is false when the window is too small or nothing finished.
//
// Output is ordered, so finish times are nondecreasing by index and the window is
// a clean contiguous slice.
func (o *Observer) SteadyInterval(drop int) (time.Duration, bool) {
	lo, hi := drop, len(o.finish)-drop
	if hi-lo < 2 {
		return 0, false
	}
	span := o.finish[hi-1] - o.finish[lo]
	steps := int64(hi - 1 - lo)
	if span <= 0 || steps <= 0 {
		return 0, false
	}
	return time.Duration(span / steps), true
}

// FinishedAll reports whether every item recorded a completion time.
func (o *Observer) FinishedAll() bool {
	return !slices.Contains(o.finish, 0)
}

// OrderedOutput reports whether items finished in ascending item order (finish
// times nondecreasing by index) — the ordering guarantee both implementations
// must uphold.
func (o *Observer) OrderedOutput() bool {
	for i := 1; i < len(o.finish); i++ {
		if o.finish[i] < o.finish[i-1] {
			return false
		}
	}
	return true
}

// PeakOccupancy returns the high-water occupancy recorded for a stage/branch, or
// 0 if no such node exists. Used to verify a shared stage/branch actually
// saturated (reached its limit).
func (o *Observer) PeakOccupancy(name string) int {
	if g, ok := o.gauges[name]; ok {
		return g.Peak()
	}
	return 0
}
