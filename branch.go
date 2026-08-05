package conveyor

import (
	"context"
	"fmt"
	"iter"
)

// This file holds the two kinds of branch a FanOut fans out to, and the one implementation behind both.
//
// A branch is where a fan-out's work runs. Both kinds own exactly one unit — the branch's own start gate — and both
// are built with the same four task constructors (the Branch interface). They differ in one thing: whether the work
// can travel.
//
//	kind                    interior nodes   what a slot means           capacity
//	---------------------   --------------   -------------------------   -------------------------------------
//	Pool (FanOut.AddPool)   never            a task running              SetLimit(n): n tasks at once
//	Lane (FanOut.AddLane)   yes              a child not yet moved on    always 1 (like the conveyor's start)
//
// The runtime needs to know nothing about the two kinds: it asks travels() — "does this branch have nodes?" — and
// charges the slot to a fresh child item or to the scheduling item accordingly (see run.startWork). A Pool has no way
// to add nodes, so its answer is fixed at build time. AddPool and AddLane are therefore the *same* call, differing
// only in what they hand back; which one you used is not recorded anywhere, and a Lane left without nodes simply
// behaves as the pool it is.
//
// The four constructors are spelled out in each interface rather than only inherited from Branch, so that every
// method carries the wording of its own kind and gets its own godoc anchor.

// Branch is what a FanOut's work is scheduled on: a Pool or a Lane. It is the task-construction surface the two
// share, for code that builds work without caring which kind it got.
//
// FanOut.Branches returns them in creation order; type-assert to reach a kind's own methods (Pool.SetLimit,
// Lane.AddStage).
type Branch interface {
	Unit

	// NewTask creates a task of one callback — one slot's worth of work on this branch. A nil fn panics.
	NewTask(fn TaskFunc) Task

	// NewTasks creates a task of count independent callbacks fn(ctx, 0)..fn(ctx, count-1), each one slot's worth
	// of work on this branch. A count <= 0 yields a no-op task; a nil fn with count > 0 panics.
	NewTasks(count int, fn func(ctx context.Context, index int) error) Task

	// NewTasksGen creates a task whose callbacks are produced by gen, pulled one by one as slots free up.
	NewTasksGen(gen iter.Seq[TaskFunc]) Task

	// NewTasksChan creates a task whose callbacks are received from ch, pulled one by one as slots free up.
	NewTasksChan(ch <-chan TaskFunc) Task
}

// Pool is a single-step branch of a FanOut's work: a task runs on the slot it was given and is done. SetLimit(n)
// bounds how many of its tasks run at once.
//
// A pool's work has nowhere to travel, so calling MoveTo with a pool task's context panics — use a Lane for a
// branch whose work is a multi-step path.
//
// Work is queued per branch in item order: everything an older item scheduled here starts before anything a
// younger item scheduled.
type Pool interface {
	Branch

	// SetLimit sets how many of this pool's tasks may run at once (default 1; a limit <= 0 means 1), and returns
	// the pool for chaining.
	//
	// Safe to call at any time, from any goroutine, including on a running conveyor.
	SetLimit(limit int) Pool

	// Limit returns how many of this pool's tasks may run at once.
	Limit() int

	// NewTask creates a task of one callback — one slot's worth of work on this pool. A nil fn panics.
	NewTask(fn TaskFunc) Task

	// NewTasks creates a task of count independent callbacks fn(ctx, 0)..fn(ctx, count-1), each one slot's worth
	// of work on this pool, built lazily as slots free up. A count <= 0 yields a no-op task; a nil fn with
	// count > 0 panics.
	NewTasks(count int, fn func(ctx context.Context, index int) error) Task

	// NewTasksGen creates a task whose callbacks are produced by gen, pulled one by one as pool slots free up. Use
	// it to stream work without materializing it upfront; gen should respect the ItemProcessor's ctx. State it
	// reads must not be mutated until the wave's Started channel closes. A nil gen is a no-op.
	NewTasksGen(gen iter.Seq[TaskFunc]) Task

	// NewTasksChan creates a task whose callbacks are received from ch, pulled one by one as pool slots free up.
	// The producer must run in its own goroutine, respect the ItemProcessor's ctx, and close the channel when
	// done. A nil channel is a no-op.
	NewTasksChan(ch <-chan TaskFunc) Task
}

// Lane is a branch of a FanOut's work that is a pipeline in its own right: it has interior nodes (AddStage,
// AddFanOut), and each piece of scheduled work runs as a child item travelling them, using the same MoveTo an
// ItemProcessor uses on the conveyor.
//
// A lane's entrance always admits one child at a time — there is no SetLimit — so its parallelism comes from the
// interior stages' own limits. Children are admitted to the interior nodes in the order their work was scheduled.
//
// A lane with no interior nodes is legal but behaves as a Pool pinned at concurrency 1.
type Lane interface {
	Branch

	// AddStage adds an interior Stage to this lane. Chain SetLimit to let several children run its code at once.
	AddStage(opts ...AnyUnitOption) Stage

	// AddFanOut adds an interior FanOut to this lane, so a child item may itself scatter work onto its own
	// branches.
	AddFanOut(opts ...AnyUnitOption) FanOut

	// NewTask creates a task of one callback — one child journey through this lane. A nil fn panics.
	NewTask(fn TaskFunc) Task

	// NewTasks creates a task of count independent callbacks fn(ctx, 0)..fn(ctx, count-1), each one child journey
	// through this lane, admitted in index order. A count <= 0 yields a no-op task; a nil fn with count > 0
	// panics.
	//
	// The ctx each callback receives is the child's own — pass it to the lane's stages, not the item's.
	NewTasks(count int, fn func(ctx context.Context, index int) error) Task

	// NewTasksGen creates a task whose callbacks are produced by gen, pulled one by one as the lane's entrance
	// frees up. Use it to stream work without materializing it upfront; gen should respect the ItemProcessor's
	// ctx. State it reads must not be mutated until the wave's Started channel closes. A nil gen is a no-op.
	NewTasksGen(gen iter.Seq[TaskFunc]) Task

	// NewTasksChan creates a task whose callbacks are received from ch, pulled one by one as the lane's entrance
	// frees up. The producer must run in its own goroutine, respect the ItemProcessor's ctx, and close the channel
	// when done. A nil channel is a no-op.
	NewTasksChan(ch <-chan TaskFunc) Task
}

// branch is the implementation behind both Pool and Lane: a series whose start gate is the branch's own unit. Which
// interface it was handed out as is deliberately not recorded — nothing needs it, since what governs behaviour is
// whether the series ended up with nodes (travels()), not which constructor was called.
type branch struct {
	*series // the branch's own interior series; series.start is the branch's unit. Always empty for a pool.

	fanout *fanOut
	name   string // optional user-given name (OptName); empty -> positional in String
	no     int    // 1-based position among its fan-out's branches, for the positional name
}

// String returns the branch's name, or its positional name after its fan-out ("exports.2", "fan-out 3.1"). Pools and
// lanes share one numbering sequence, since a fan-out's branches are one list.
func (b *branch) String() string {
	if b.name != "" {
		return b.name
	}
	return fmt.Sprintf("%s.%d", b.fanout, b.no)
}

func (b *branch) unit() *unit { return b.start }

// travels reports whether this branch's work runs as child items that may move — i.e. whether it has anywhere to go.
// It is a property of the topology, which is frozen from the first Run on, so it is stable for the lifetime of a run.
// A pool can never have nodes; a lane normally does, and one that was left without any answers false and so behaves
// as a pool.
func (b *branch) travels() bool { return len(b.nodes) > 0 }

func (b *branch) SetLimit(limit int) Pool {
	b.start.setLimit(limit)
	return b
}

func (b *branch) Limit() int { return int(b.start.limit.Load()) }

func (b *branch) NewTask(fn TaskFunc) Task {
	if fn == nil {
		panic(errNilTaskFunc)
	}
	return Task{branch: b, src: &singleSource{fn: fn}}
}

func (b *branch) NewTasks(count int, fn func(ctx context.Context, index int) error) Task {
	if count <= 0 {
		return Task{branch: b}
	}
	if fn == nil {
		panic(errNilTaskFunc)
	}
	return Task{branch: b, src: &countSource{count: count, fn: fn}}
}

func (b *branch) NewTasksGen(gen iter.Seq[TaskFunc]) Task {
	if gen == nil {
		return Task{branch: b}
	}
	return Task{branch: b, src: &genSource{seq: gen}}
}

func (b *branch) NewTasksChan(ch <-chan TaskFunc) Task {
	if ch == nil {
		return Task{branch: b}
	}
	return Task{branch: b, src: &chanSource{ch: ch}}
}
