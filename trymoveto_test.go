package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// TestTryMoveToEntersWhenFree covers the plain success path: an uncontended stage is entered, with the same
// consequences as MoveTo (so a second attempt is the once-per-node misuse).
func TestTryMoveToEntersWhenFree(t *testing.T) {
	t.Parallel()
	c := NewConveyor()
	st := c.AddStage()

	err := runOnce(t, c, func(ctx context.Context) error {
		entered, err := st.TryMoveTo(ctx)
		if err != nil {
			return err
		}
		if !entered {
			t.Error("expected to enter an empty stage")
		}
		if occ := occupancyOf(c, st); occ != 1 {
			t.Errorf("stage occupancy = %d, want 1", occ)
		}
		assertPanics(t, errNodeAlreadyEntered, func() { _, _ = st.TryMoveTo(ctx) })
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestTryMoveToDeclinesWhenFullAndRetries is the point of the primitive: a full stage is declined instead of waited
// for, the item is left exactly where it was, and the declined stage stays enterable by a later blocking MoveTo.
func TestTryMoveToDeclinesWhenFullAndRetries(t *testing.T) {
	t.Parallel()
	c := NewConveyor()
	busy := c.AddStage() // limit 1

	tried := make(chan struct{}) // item 2 has had its try declined
	var enteredLater bool        // item 2 got in with a blocking MoveTo afterwards

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if no == 1 {
			if err := busy.MoveTo(ctx); err != nil {
				return err
			}
			<-tried // hold the only slot until item 2 has been declined
			return nil
		}
		ok, err := busy.TryMoveTo(ctx)
		if err != nil {
			return err
		}
		if ok {
			t.Error("expected to be declined by a full stage")
		}
		if occ := occupancyOf(c, busy); occ != 1 {
			t.Errorf("stage occupancy = %d, want 1 (the declined item must not have taken a slot)", occ)
		}
		close(tried)
		// The declined stage was left unentered, so a blocking move is still legal and succeeds once item 1 leaves.
		if err := busy.MoveTo(ctx); err != nil {
			return err
		}
		enteredLater = true
		return nil
	})
	// Run has returned, so item 2's write is visible here.
	if !enteredLater {
		t.Fatal("a declined stage must stay enterable by a later blocking MoveTo")
	}
}

// TestTryMoveToFanOutDeclinedLeavesItemInPlace covers the fan-out side of the promise: a declined entry leaves the
// item in the previous node, and tasks built ahead of the attempt are still unclaimed, so the same []Task can be
// scheduled once the item really enters (tasks are otherwise single-use).
func TestTryMoveToFanOutDeclinedLeavesItemInPlace(t *testing.T) {
	t.Parallel()
	c := NewConveyor()
	fan := c.AddFanOut() // limit 1: one item inside at a time
	pool := fan.AddPool()
	after := c.AddStage()

	inside := make(chan struct{}) // item 1 is inside the fan-out, with its rank published
	tried := make(chan struct{})
	var ran int64

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		tasks := []Task{pool.NewTask(func(context.Context) error { return nil })}
		if no == 1 {
			err := fan.MoveTo(ctx)
			if err == nil {
				err = fan.Schedule(ctx, tasks...)
			}
			if err != nil {
				return err
			}
			close(inside)
			<-tried // stay inside the fan-out until item 2 has been declined
			return after.MoveTo(ctx)
		}
		// Wait for item 1's rank to be published, so the decline below can only be for lack of capacity — not
		// because item 2's turn had not come yet.
		<-inside
		ok, err := fan.TryMoveTo(ctx)
		if err != nil {
			return err
		}
		if ok {
			t.Error("expected to be declined by a full fan-out")
		}
		if occ := occupancyOf(c, c.StartUnit()); occ != 1 {
			t.Errorf("start occupancy = %d, want 1 — the declined item stays in the previous node", occ)
		}
		close(tried)
		// The tasks, built before the declined attempt, are scheduled for real now.
		err = fan.MoveTo(ctx)
		if err == nil {
			err = fan.Schedule(ctx, tasks...)
		}
		if err != nil {
			return err
		}
		if err := after.MoveTo(ctx); err != nil {
			return err
		}
		ran++
		return nil
	})
	if ran != 1 {
		t.Fatalf("resubmitted work ran %d times, want 1", ran)
	}
}

// TestTryMoveToJoinsWavesOnEntry: the joins are the one part of a TryMoveTo that may still block, and they are
// awaited exactly as in MoveTo once the item is in — so by the time it returns true, the named work is done.
func TestTryMoveToJoinsWavesOnEntry(t *testing.T) {
	t.Parallel()
	c := NewConveyor()
	first := c.AddStage(OptName("first"))
	second := c.AddStage(OptName("second"))

	var bgDone atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := first.MoveTo(ctx); err != nil {
			return err
		}
		w := first.Retain(ctx, func() error {
			bgDone.Store(true)
			return nil
		})
		entered, err := second.TryMoveTo(ctx, w)
		if err != nil {
			return err
		}
		if !entered {
			t.Error("expected to enter an empty stage")
		}
		if !bgDone.Load() {
			t.Error("TryMoveTo returned before the joined wave had finished")
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestTryMoveToReportsEnteredWithJoinError pins the one return shape a non-blocking entry can produce that MoveTo
// cannot describe: the item DID enter (so the previous node is released and the stage is spent), and the error comes
// from the join that followed. A caller that reads only the error would otherwise assume nothing happened.
func TestTryMoveToReportsEnteredWithJoinError(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	c := NewConveyor()
	first := c.AddStage(OptName("first"))
	second := c.AddStage(OptName("second"))

	inSecond := make(chan struct{})
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := first.MoveTo(ctx); err != nil {
			return err
		}
		// The bgOp fails only once the item is inside `second`, so the entry is decided before the item is poisoned:
		// had it failed earlier, the cancellation check would decline the move instead and entered would be false.
		w := first.Retain(ctx, func() error {
			<-inSecond
			return boom
		})
		go func() {
			waitFor(t, "the item to enter second", func() bool { return occupancyOf(c, second) == 1 })
			close(inSecond)
		}()

		entered, err := second.TryMoveTo(ctx, w)
		if !entered {
			t.Error("expected entered == true: the stage was free, and only the join failed afterwards")
		}
		if !errors.Is(err, boom) {
			t.Errorf("error = %v, want the joined wave's %v", err, boom)
		}
		if occ := occupancyOf(c, first); occ != 0 {
			t.Errorf("first occupancy = %d, want 0 — entering second released it", occ)
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, boom) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestTryMoveToFanOutJoinErrorPoisonsBody: a fan-out entered with a failing join leaves the item inside the node with
// an open, empty body — a state no other path produces, since the node is entered but its rank is never published.
// The join error poisons the item, so a following Schedule returns the cause and the tasks never run.
func TestTryMoveToFanOutJoinErrorPoisonsBody(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	c := NewConveyor()
	first := c.AddStage(OptName("first"))
	fan := c.AddFanOut(OptName("fan"))
	pool := fan.AddPool(OptName("pool"))

	var ran atomic.Int64
	inFan := make(chan struct{})
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := first.MoveTo(ctx); err != nil {
			return err
		}
		w := first.Retain(ctx, func() error {
			<-inFan
			return boom
		})
		go func() {
			waitFor(t, "the item to enter the fan-out", func() bool { return occupancyOf(c, fan) == 1 })
			close(inFan)
		}()

		tasks := []Task{pool.NewTask(func(context.Context) error {
			ran.Add(1)
			return nil
		})}
		entered, err := fan.TryMoveTo(ctx, w)
		if !entered {
			t.Error("expected entered == true: the fan-out was free, and only the join failed afterwards")
		}
		if !errors.Is(err, boom) {
			t.Errorf("error = %v, want the joined wave's %v", err, boom)
		}
		if err := fan.Schedule(ctx, tasks...); !errors.Is(err, boom) {
			t.Errorf("Schedule after the failed join = %v, want the item's cause %v", err, boom)
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, boom) {
		t.Fatalf("run failed: %v", err)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("%d tasks ran, want 0 — the failed join poisoned the item before anything was scheduled", got)
	}
}
