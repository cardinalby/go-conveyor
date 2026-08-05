package conveyor

import (
	"context"
	"fmt"
)

// Stage is a node whose work runs inline: the item enters it with MoveTo, runs the stage's code in the
// ItemProcessor body while holding the slot, then moves on. A stage is entered at most once per item.
type Stage interface {
	Unit

	// MoveTo advances the item into this stage, releasing the previous node, and joins the listed waves before the
	// stage's code runs. It blocks until the stage (or its waiting room) has room and it is the item's turn.
	//
	// It returns ErrForeignContext, ErrStaleContext, or the item's cancellation cause on shutdown. It panics on
	// misuse: moving backward, re-entering an already-entered stage, a handle from another conveyor or series, or
	// a wave from another item.
	MoveTo(ctx context.Context, joins ...Wave) error

	// TryMoveTo is MoveTo without waiting: it enters the stage only if a slot is free right now, and reports
	// whether it did. Use it to make a stage optional under load — skip it, or take a different path — instead of
	// queueing.
	//
	// entered == false means nothing happened: the item stays where it is and the stage remains unentered, so it
	// may be tried again or entered later with a blocking MoveTo. It bypasses the stage's waiting room
	// (SetQueueSize) and never jumps an item already waiting there.
	//
	// The joins are awaited only if the item entered. A canceled item returns (false, its cancellation cause). It
	// panics on the same misuse as MoveTo.
	TryMoveTo(ctx context.Context, joins ...Wave) (entered bool, err error)

	// Retain runs bgOp in the background while keeping this stage's slot held, letting the item move on without
	// releasing the stage. The slot is freed once bgOp returns and the item has moved on. Join the returned Wave
	// in a later MoveTo to wait for bgOp; an error from bgOp cancels the item.
	//
	// It panics on misuse: a handle from another conveyor, or a stage the item does not currently occupy.
	Retain(ctx context.Context, bgOp func() error) Wave

	// SetLimit sets how many items may run this stage's code at once (default 1; a limit <= 0 means 1), and
	// returns the stage for chaining. Item order through the stage is only guaranteed at limit 1.
	//
	// Safe to call at any time, from any goroutine, including on a running conveyor; it never evicts items already
	// admitted.
	SetLimit(limit int) Stage

	// SetQueueSize gives this stage a waiting room of size items in front of it (a size <= 0 means none), and
	// returns the stage for chaining. An item that cannot enter the stage directly waits here instead, freeing the
	// previous node. Only MoveTo uses the waiting room; TryMoveTo never does.
	//
	// Safe to call at any time, from any goroutine, including on a running conveyor; it never evicts items already
	// waiting.
	SetQueueSize(size int) Stage

	// Limit returns how many items may run this stage's code at once.
	Limit() int

	// QueueSize returns the size of this stage's waiting room, or 0 if it has none.
	QueueSize() int
}

// stage is the Stage implementation: one node of a series, owning one work unit. It is immutable topology shared
// across Run invocations (its waiting room is a capacity dial on that unit, not a node of its own).
type stage struct {
	series *series
	name   string // optional user-given name (OptName); empty -> positional in String
	ord    int    // 1-based position among its series' nodes, for the positional name
	work   *unit
}

// assignRank is the stage's node implementation: rank r is reserved for the waiting room and the work unit takes
// the next one, whether or not a queue is configured (see the rank discussion in builder.go).
func (s *stage) assignRank(scope, r int) int {
	s.work.scope, s.work.rank = scope, r+1
	return r + 2
}

// String returns the stage's name, or its positional name ("stage N", prefixed by the lane it was built in).
func (s *stage) String() string {
	if s.name != "" {
		return s.name
	}
	return s.series.positionalName("stage", s.ord)
}

func (s *stage) unit() *unit { return s.work }

// MoveTo enters this stage (through its queue, if it has one) and joins the listed waves. See the Stage
// interface for the full contract.
func (s *stage) MoveTo(ctx context.Context, joins ...Wave) error {
	it, r, err := s.series.conveyor.actingItem(ctx, s.work, false)
	if err != nil {
		return fmt.Errorf("move to %s: %w", s, err)
	}
	defer r.mu.Unlock()
	r.checkEnterOrder(it, s.work) // panics on backward / repeat entry (misuse)
	if err := r.enterUnit(ctx, it, s.work, true); err != nil {
		return fmt.Errorf("move to %s: %w", s, err)
	}
	if err := r.join(ctx, it, joins); err != nil {
		return fmt.Errorf("join at %s: %w", s, err)
	}
	return nil
}

// TryMoveTo enters this stage only if that needs no waiting, and reports whether it did. See the Stage interface
// for the full contract.
func (s *stage) TryMoveTo(ctx context.Context, joins ...Wave) (bool, error) {
	it, r, err := s.series.conveyor.actingItem(ctx, s.work, true)
	if err != nil {
		return false, fmt.Errorf("try move to %s: %w", s, err)
	}
	defer r.mu.Unlock()
	r.checkEnterOrder(it, s.work) // panics on backward / repeat entry (misuse)
	entered, err := r.tryEnterUnit(it, s.work, true)
	if err != nil || !entered {
		return false, err
	}
	if err := r.join(ctx, it, joins); err != nil {
		return true, fmt.Errorf("join at %s: %w", s, err)
	}
	return true, nil
}

func (s *stage) SetLimit(limit int) Stage {
	s.work.setLimit(limit)
	return s
}

func (s *stage) SetQueueSize(size int) Stage {
	s.work.setQueueSize(size)
	return s
}

func (s *stage) Limit() int { return int(s.work.limit.Load()) }

func (s *stage) QueueSize() int { return int(s.work.queueSize.Load()) }

// Retain hands this stage's slot to a background operation (see the Stage interface).
func (s *stage) Retain(ctx context.Context, bgOp func() error) Wave {
	// checkCancel is false: a canceled item is answered with a wave of this item's own (below), not an error, which
	// needs the lock this call takes.
	it, r, err := s.series.conveyor.actingItem(ctx, s.work, false)
	if err != nil {
		// No item to charge: hand back a standalone finished wave carrying the reason.
		return standaloneWave(fmt.Errorf("retain %s: %w", s, err))
	}
	defer r.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return finishedWave(r, it, err) // shutting down: do not run bgOp
	}
	if it.occupied[s.work.index] == 0 {
		panic(fmt.Errorf("cannot retain %s: %w", s, errStageNotEntered))
	}
	w := newWave(r, it)
	w.retainUnit = s.work
	w.workStarted()
	go r.runRetain(w, bgOp)
	return w
}
