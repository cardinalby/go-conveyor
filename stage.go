package conveyor

import (
	"context"
	"errors"
	"fmt"
)

// RetainableStage is a stage whose slot the item can keep after moving on: a Stage, or the conveyor's starting
// stage (Conveyor.StartingStage).
type RetainableStage interface {
	Unit

	// Retain keeps this stage's slot held until the returned release is called, letting the item move on without
	// releasing the stage. The slot is freed once release is called and the item has moved on, so a release called
	// while the item is still in the stage changes nothing. Use it to keep the next item out of the stage while this
	// item's later work (in the ItemProcessor or in its fan-out tasks) still uses the same resource.
	//
	// release may be called from any goroutine, more than once. Each Retain call holds the slot until its own
	// release is called. A hold not released by then ends when the item completes, after all its tasks finished:
	// do not call release from a goroutine you started yourself and expect it to keep the stage after the item
	// returns — use RetainFor for background work.
	//
	// On a finished item the returned release does nothing.
	//
	// It panics on misuse: a context that does not belong to an item, a handle from another conveyor, or a stage the
	// item is not in now.
	Retain(ctx context.Context) (release func())

	// RetainFor runs bgOp in the background while keeping this stage's slot held, letting the item move on without
	// releasing the stage. The slot is freed once bgOp returns and the item has moved on. Wait for bgOp with the
	// returned Wave (before or after a later MoveTo); an error from bgOp cancels the item. Several calls run their
	// bgOps in parallel and the slot is held until all of them return.
	//
	// On a canceled item — canceled on ctx or on its own context — bgOp does not run and the returned wave is
	// already finished, carrying the cancellation cause.
	//
	// It panics on misuse: a handle from another conveyor, or a stage the item is not in now.
	RetainFor(ctx context.Context, bgOp func() error) Wave
}

// Stage is a node whose work runs inline: the item enters it with MoveTo, runs the stage's code in the
// ItemProcessor body while holding the slot, then moves on. A stage is entered at most once per item.
type Stage interface {
	RetainableStage

	// MoveTo advances the item into this stage, releasing the previous node. It blocks until the stage (or its
	// waiting room) has room and it is the item's turn.
	//
	// It returns ErrForeignContext, ErrStaleContext, or the item's cancellation cause — whether the cancellation is
	// visible on ctx or only on the item's own context (see ItemProcessor). It panics on misuse: moving backward,
	// re-entering an already-entered stage, or a handle from another conveyor or series.
	MoveTo(ctx context.Context) error

	// TryMoveTo is MoveTo without waiting: it enters the stage only if a slot is free right now, and reports
	// whether it did. Use it to make a stage optional under load — skip it, or take a different path — instead of
	// queueing.
	//
	// entered == false means nothing happened: the item stays where it is and the stage remains unentered, so it
	// may be tried again or entered later with a blocking MoveTo. It bypasses the stage's waiting room
	// (SetQueueSize) and never jumps an item already waiting there.
	//
	// A canceled item returns (false, its cancellation cause), whether the cancellation is visible on ctx or only on
	// the item's own context. It panics on the same misuse as MoveTo.
	TryMoveTo(ctx context.Context) (entered bool, err error)

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

// MoveTo enters this stage (through its queue, if it has one). See the Stage interface for the full contract.
func (s *stage) MoveTo(ctx context.Context) error {
	it, r, err := s.series.conveyor.actingItem(ctx, "move to", s.work, false)
	if err != nil {
		return fmt.Errorf("move to %s: %w", s, err)
	}
	defer r.mu.Unlock()
	r.checkEnterOrder(it, s.work) // panics on backward / repeat entry (misuse)
	if err := r.enterUnit(ctx, it, s.work, true); err != nil {
		return fmt.Errorf("move to %s: %w", s, err)
	}
	return nil
}

// TryMoveTo enters this stage only if that needs no waiting, and reports whether it did. See the Stage interface
// for the full contract.
func (s *stage) TryMoveTo(ctx context.Context) (bool, error) {
	it, r, err := s.series.conveyor.actingItem(ctx, "try move to", s.work, true)
	if err != nil {
		return false, fmt.Errorf("try move to %s: %w", s, err)
	}
	defer r.mu.Unlock()
	r.checkEnterOrder(it, s.work) // panics on backward / repeat entry (misuse)
	return r.tryEnterUnit(it, s.work, true)
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

// Retain keeps this stage's slot until the returned release is called (see the RetainableStage interface).
func (s *stage) Retain(ctx context.Context) func() {
	return s.series.conveyor.retain(ctx, s.work)
}

// RetainFor hands this stage's slot to a background operation (see the RetainableStage interface).
func (s *stage) RetainFor(ctx context.Context, bgOp func() error) Wave {
	return s.series.conveyor.retainFor(ctx, s.work, bgOp)
}

// retain keeps the slot of stage unit u for the acting item until the returned release is called: the body of
// Retain on a stage, the starting stage and a lane.
func (c *conveyor) retain(ctx context.Context, u *unit) func() {
	it, r, err := c.actingItem(ctx, "retain", u, false)
	if err != nil {
		if errors.Is(err, ErrStaleContext) {
			return func() {} // the item is over and holds nothing
		}
		// No item to hold the slot for, and no error to return: a silent no-op would lose the exclusivity asked for.
		panic(fmt.Errorf("retain %s: %w", u, err))
	}
	defer r.mu.Unlock()
	// No cancellation check: a canceled call ctx does not stop the item from moving on with its own context, so the
	// hold is kept in any case. On a canceled item it is harmless: finishItem drops it.
	checkInStage(it, u)
	h := &stageHold{unit: u.index}
	it.stageHolds = append(it.stageHolds, h)
	return func() { r.releaseStageHold(it, h) }
}

// retainFor hands the slot of stage unit u to bgOp: the body of RetainFor on a stage, the starting stage and a lane.
func (c *conveyor) retainFor(ctx context.Context, u *unit, bgOp func() error) Wave {
	// checkCancel is false: a canceled item is answered with a wave of this item's own (below), not an error, which
	// needs the lock this call takes.
	it, r, err := c.actingItem(ctx, "retain", u, false)
	if err != nil {
		// No item to charge: hand back a standalone finished wave carrying the reason.
		return standaloneWave(fmt.Errorf("retain %s: %w", u, err))
	}
	defer r.mu.Unlock()
	if err := it.cancelCause(ctx); err != nil {
		return finishedWave(r, it, err) // the item is canceled (on ctx or on its own context): do not run bgOp
	}
	checkInStage(it, u)
	w := newWave(r, it)
	w.retainUnit = u
	w.workStarted()
	go r.runRetain(w, bgOp)
	return w
}

// checkInStage panics unless the item is in stage unit u now. Occupancy alone is not enough: a slot kept by an
// earlier Retain stays occupied after the item has moved on. Caller holds run.mu.
func checkInStage(it *item, u *unit) {
	if it.occupied[u.index] == 0 || it.reachedRank != u.rank {
		panic(fmt.Errorf("cannot retain %s: %w", u, errStageNotEntered))
	}
}
