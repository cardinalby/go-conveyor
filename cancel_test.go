package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// wakeTimeout bounds how long a test waits for a blocked call to notice a cancellation it must wake on. It is far
// above any real wake-up latency and far below testTimeout, so a missing wake-up fails fast instead of hanging.
const wakeTimeout = 5 * time.Second

// This file pins the rule that cancellation is judged by the item, not by the context a node method is called with:
// a context with the cancellation stripped (context.WithoutCancel) cannot move, retain, or otherwise act for a
// canceled item (see ItemProcessor). A context that adds a deadline of its own keeps working.

// TestStrippedContextCannotActForCanceledItem: once the drain timeout has canceled the item, MoveTo, TryMoveTo and
// RetainFor called with context.WithoutCancel(ctx) all answer with the item's ShutdownError, and nothing is entered or
// run.
func TestStrippedContextCannotActForCanceledItem(t *testing.T) {
	c := NewConveyor(OptDrainTimeout(0)) // cancel in-flight items as soon as shutdown starts
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	var bgRan atomic.Bool
	var checked atomic.Bool
	stop, done := runAsync(c, func(ctx context.Context) error {
		if itemNo(ctx) != 1 {
			<-ctx.Done()
			return nil
		}
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		<-ctx.Done() // the drain timeout canceled the item
		var se ShutdownError
		if !errors.As(context.Cause(ctx), &se) {
			t.Errorf("item cause = %v, want a ShutdownError", context.Cause(ctx))
		}
		stripped := context.WithoutCancel(ctx)

		if err := b.MoveTo(stripped); !errors.As(err, &se) {
			t.Errorf("MoveTo with a stripped context on a canceled item = %v, want a ShutdownError", err)
		}
		if got := occupancyOf(c, b); got != 0 {
			t.Errorf("b occupancy = %d after the refused move, want 0", got)
		}
		entered, err := b.TryMoveTo(stripped)
		if entered || !errors.As(err, &se) {
			t.Errorf("TryMoveTo with a stripped context on a canceled item = (%v, %v), want (false, a "+
				"ShutdownError)", entered, err)
		}
		w := a.RetainFor(stripped, func(context.Context) error {
			bgRan.Store(true)
			return nil
		})
		select {
		case <-w.Finished():
		default:
			t.Errorf("RetainFor with a stripped context on a canceled item must hand back a finished task group")
		}
		if got := groupErr(w); !errors.As(got, &se) {
			t.Errorf("task group error = %v, want a ShutdownError", got)
		}
		checked.Store(true)
		return nil
	})
	waitFor(t, "item 1 to enter a", func() bool { return occupancyOf(c, a) == 1 })
	stop(errors.New("stop"))
	runErr := recvErr(t, "Run", done)
	if bgRan.Load() {
		t.Fatalf("the task ran although the item was canceled")
	}
	if !checked.Load() {
		t.Fatalf("the checks did not run")
	}
	var re RunError
	if !errors.As(runErr, &re) || re.DrainError() == nil {
		t.Fatalf("Run = %v, want a RunError with the drain timeout as DrainError", runErr)
	}
}

// TestStrippedContextCannotMoveForShutdownCanceledItem: an item canceled by another item's failure is refused with
// its ShutdownError even when it hides the cancellation from MoveTo and TryMoveTo.
func TestStrippedContextCannotMoveForShutdownCanceledItem(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	commit := c.AddStage(OptName("commit"))

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	secondReady := make(chan struct{})
	var checked atomic.Bool

	runErr := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		switch no {
		case 1:
			if err := s.MoveTo(ic); err != nil { // leave the start gate, so item 2 is created
				return err
			}
			<-secondReady
			return boom
		case 2:
			close(secondReady)
			<-ic.Done() // item 1's failure canceled every later item
			stripped := context.WithoutCancel(ic)

			var se ShutdownError
			err := commit.MoveTo(stripped)
			if !errors.As(err, &se) || !errors.Is(err, boom) {
				t.Errorf("MoveTo with a stripped context on a shutdown-canceled item = %v, want a ShutdownError "+
					"caused by the item error", err)
			}
			if got := occupancyOf(c, commit); got != 0 {
				t.Errorf("commit occupancy = %d after the refused move, want 0", got)
			}
			entered, err := commit.TryMoveTo(stripped)
			if entered || !errors.As(err, &se) {
				t.Errorf("TryMoveTo with a stripped context on a shutdown-canceled item = (%v, %v), want (false, a "+
					"ShutdownError)", entered, err)
			}
			checked.Store(true)
			return nil
		default:
			return nil
		}
	})
	if !errors.Is(runErr, boom) {
		t.Fatalf("Run = %v, want the item error", runErr)
	}
	if !checked.Load() {
		t.Fatalf("the checks did not run")
	}
}

// TestDerivedDeadlineStillCancelsTheCall: a context derived from the item's with a shorter deadline keeps working as
// before — the call fails with the deadline error — and it does not cancel the item, which may go on.
func TestDerivedDeadlineStillCancelsTheCall(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		expired, cancel := context.WithTimeout(ctx, 0)
		defer cancel()
		<-expired.Done()
		if err := s.MoveTo(expired); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("MoveTo with an expired derived context = %v, want DeadlineExceeded", err)
		}
		if entered, err := s.TryMoveTo(expired); entered || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("TryMoveTo with an expired derived context = (%v, %v), want (false, DeadlineExceeded)",
				entered, err)
		}
		if err := context.Cause(ctx); err != nil {
			t.Errorf("the item's own context was canceled (%v) by a derived deadline; it must not be", err)
		}
		if err := s.MoveTo(ctx); err != nil { // the refused calls left the stage unentered
			t.Errorf("MoveTo with the item's own context after the refused calls = %v, want nil", err)
		}
		checked.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the checks did not run")
	}
}

// TestStrippedContextAdmissionWaitWakesOnItemCancellation: a MoveTo blocked on admission with a stripped context
// wakes when the item's own context is canceled, even though nothing else wakes the run. The item is parked in the
// stage's waiting room (observable through the waiting-room counter, which is only readable once the wait has
// released the lock), then its own context is canceled directly, with no broadcast — the wake-up must come from the
// watcher waitUntil placed on the item's context.
func TestStrippedContextAdmissionWaitWakesOnItemCancellation(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetQueueSize(1) // item 1 inside, item 2 waits in front of it

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	releaseFirst := make(chan struct{})
	second := make(chan *item, 1)
	returned := make(chan error, 1)

	go func() {
		defer cancel()
		it := <-second
		waitFor(t, "item 2 to wait in the stage's waiting room", func() bool { return queueOccupancy(c, s) == 1 })
		it.cancel(boom) // the item's own context only; the call context hides it, and nothing broadcasts
		select {
		case err := <-returned:
			if !errors.Is(err, boom) {
				t.Errorf("blocked MoveTo with a stripped context = %v, want the item's cancellation cause", err)
			}
			if got := occupancyOf(c, s); got != 1 {
				t.Errorf("s occupancy = %d after the refused move, want 1 (item 1 alone; item 2 never entered)", got)
			}
		case <-time.After(wakeTimeout):
			t.Errorf("MoveTo did not wake within %v after the item's own context was canceled", wakeTimeout)
		}
		close(releaseFirst)
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		switch no {
		case 1:
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			<-releaseFirst
			return nil
		case 2:
			second <- itemOf(ic)
			returned <- s.MoveTo(context.WithoutCancel(ic))
			return nil
		default:
			// Later items park at the start gate: an item that completed would broadcast and wake item 2 whatever
			// the watcher does, and so hide a missing wake-up.
			<-releaseFirst
			return nil
		}
	})
}

// TestStrippedContextJoinWaitWakesOnItemCancellation: a TaskGroup.Wait with a stripped context wakes when the item's own
// context is canceled, with nothing else waking the run. The item is inside the target stage when it starts the
// wait; once it is parked there, the item's own context is canceled directly, with no broadcast.
func TestStrippedContextJoinWaitWakesOnItemCancellation(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	park := make(chan struct{})
	itemCh := make(chan *item, 1)
	returned := make(chan error, 1)
	var checked atomic.Bool

	go func() {
		it := <-itemCh
		waitFor(t, "the item to take b and block in Wait", func() bool {
			return occupancyOf(c, b) == 1 && parkedOf(c) == 1
		})
		it.cancel(boom) // the item's own context only; the call context hides it, and nothing broadcasts
		select {
		case err := <-returned:
			if !errors.Is(err, boom) {
				t.Errorf("Wait on a parked task group with a stripped context = %v, want the item's cancellation cause", err)
			}
		case <-time.After(wakeTimeout):
			t.Errorf("Wait did not wake within %v after the item's own context was canceled", wakeTimeout)
		}
		checked.Store(true)
		close(park)
	}()

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		w := a.RetainFor(ctx, func(context.Context) error {
			<-park
			return nil
		})
		itemCh <- itemOf(ctx)
		stripped := context.WithoutCancel(ctx)
		if err := b.MoveTo(stripped); err != nil {
			return err
		}
		returned <- w.Wait(stripped)
		<-w.Finished()
		return groupErr(w)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the checks did not run")
	}
}
