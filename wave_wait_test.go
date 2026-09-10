package conveyor

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins Wave.Wait: who may call it, what it answers when the item is poisoned before, during and after the
// wave finishes, and that the processor chooses which slot the item holds while it waits.

// TestWaitAfterMoveBlockedByBusySlotAcknowledges is the case the join argument could not handle: the wave's task fails
// while the item waits for a slot in commit that another item holds. MoveTo returns the cause without entering; the
// Wait that follows reports the wave's own error and acknowledges it, so the processor may end the item clean. The
// outcome is the same as with a free slot. The task fails only once the item is blocked at commit's door (item 1 is
// parked on a channel, not in the runtime), so the cancellation lands in the admission wait and not before it.
func TestWaitAfterMoveBlockedByBusySlotAcknowledges(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	holderIn := make(chan struct{})
	waiterDone := make(chan struct{})
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		switch no {
		case 1: // holds commit for as long as item 2 runs, so item 2 can never enter it
			if err := commit.MoveTo(ctx); err != nil {
				return err
			}
			close(holderIn)
			<-waiterDone
			return nil
		default:
			defer close(waiterDone)
			<-holderIn
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
				waitFor(t, "item 2 to block at commit's door", func() bool { return parkedOf(c) == 1 })
				return boom
			})); err != nil {
				return err
			}
			w := fo.Detach(ctx)
			if err := commit.MoveTo(ctx); !errors.Is(err, boom) {
				t.Errorf("MoveTo = %v, want the poison %v", err, boom)
			}
			<-w.Finished()
			if err := w.Wait(ctx); !errors.Is(err, boom) {
				t.Errorf("Wait = %v, want the wave's %v", err, boom)
			} else if !strings.HasPrefix(err.Error(), "fo work: ") {
				t.Errorf("Wait = %q, want it named after the fan-out", err)
			}
			return nil // the error was reported to us and we handled it
		}
	})
}

// TestWaitOnRetainWaveAfterBlockedMoveAcknowledges: the same for a Retain wave, whose error is named after the stage.
func TestWaitOnRetainWaveAfterBlockedMoveAcknowledges(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))

	holderIn := make(chan struct{})
	waiterDone := make(chan struct{})
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		switch no {
		case 1:
			if err := commit.MoveTo(ctx); err != nil {
				return err
			}
			close(holderIn)
			<-waiterDone
			return nil
		default:
			defer close(waiterDone)
			<-holderIn
			if err := write.MoveTo(ctx); err != nil {
				return err
			}
			w := write.Retain(ctx, func() error {
				waitFor(t, "item 2 to block at commit's door", func() bool { return parkedOf(c) == 1 })
				return boom
			})
			if err := commit.MoveTo(ctx); !errors.Is(err, boom) {
				t.Errorf("MoveTo = %v, want the poison %v", err, boom)
			}
			<-w.Finished()
			if err := w.Wait(ctx); !errors.Is(err, boom) {
				t.Errorf("Wait = %v, want the wave's %v", err, boom)
			} else if !strings.HasPrefix(err.Error(), "write work: ") {
				t.Errorf("Wait = %q, want it named after the retained stage", err)
			}
			return nil
		}
	})
}

// TestWaitOnUnfinishedFailedWaveReturnsCause: a task has failed but a sibling is still running, so the wave is not
// finished. Wait returns the item's cancellation cause and acknowledges nothing — the outcome is not final. After
// Finished closes, Wait reports the wave's error and acknowledges it.
func TestWaitOnUnfinishedFailedWaveReturnsCause(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")).SetLimit(2)
	commit := c.AddStage(OptName("commit"))

	release := make(chan struct{})
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.Schedule(ctx,
			pool.NewTask(func(context.Context) error { return boom }),
			pool.NewTask(func(context.Context) error { <-release; return nil }), // ignores the cancellation on purpose
		)
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		waitFor(t, "the item to be poisoned", func() bool { return ctx.Err() != nil })
		if err := commit.MoveTo(ctx); !errors.Is(err, boom) {
			t.Errorf("MoveTo = %v, want the poison %v", err, boom)
		}

		if err := w.Wait(ctx); !errors.Is(err, boom) {
			t.Errorf("Wait on the unfinished wave = %v, want the cause %v", err, boom)
		}
		select {
		case <-w.Finished():
			t.Fatalf("the wave finished although a task is still blocked")
		default:
		}
		if ackedOf(ctx, w) {
			t.Errorf("an unfinished wave was acknowledged")
		}

		close(release)
		<-w.Finished()
		if err := w.Wait(ctx); !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), "fo work: ") {
			t.Errorf("Wait after Finished = %v, want the wave's error named after the fan-out", err)
		}
		if !ackedOf(ctx, w) {
			t.Errorf("the finished wave was not acknowledged")
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v; the wave error was acknowledged, completion must not raise it", err)
	}
}

// TestUnwaitedFailedWaveStillFailsCompletion: acknowledging one wave says nothing about the others. The processor
// waits for the first failed wave and returns nil; the second, never waited for, fails the item at completion.
func TestUnwaitedFailedWaveStillFailsCompletion(t *testing.T) {
	errA := errors.New("a boom")
	errB := errors.New("b boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		// Both bgOps fail only once both waves exist: a Retain on an already poisoned item does not run its bgOp and
		// hands back a finished wave carrying the poison instead.
		release := make(chan struct{})
		wa := write.Retain(ctx, func() error { <-release; return errA })
		wb := write.Retain(ctx, func() error { <-release; return errB })
		close(release)
		<-wa.Finished()
		<-wb.Finished()
		if err := wa.Wait(ctx); !errors.Is(err, errA) {
			t.Errorf("wa.Wait = %v, want %v", err, errA)
		}
		return nil
	})
	if !errors.Is(err, errB) {
		t.Fatalf("Run = %v, want the unacknowledged %v", err, errB)
	}
}

// TestWaitBeforeMoveHoldsPreviousSlot: waiting before the move keeps the item in the node it stands in. The item
// behind cannot enter that node meanwhile, and the target is still free.
func TestWaitBeforeMoveHoldsPreviousSlot(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(2)
	pool := fo.AddPool(OptName("pool"))
	mid := c.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	release := make(chan struct{})
	waiting := make(chan struct{})
	var secondInMid atomic.Bool
	go func() {
		<-waiting
		// Item 1's detached work holds one fan-out slot; item 2 inside the fan-out is the second. From there its only
		// way on is mid, which item 1 holds.
		waitFor(t, "item 2 to be inside the fan-out", func() bool { return occupancyOf(c, fo) == 2 })
		if occ := occupancyOf(c, mid); occ != 1 {
			t.Errorf("mid occupancy = %d while item 1 waits there, want 1", occ)
		}
		if occ := occupancyOf(c, commit); occ != 0 {
			t.Errorf("commit occupancy = %d while item 1 waits at mid, want 0", occ)
		}
		if secondInMid.Load() {
			t.Errorf("item 2 entered mid while item 1 was waiting there")
		}
		close(release)
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if no != 1 {
			if err := mid.MoveTo(ctx); err != nil {
				return err
			}
			secondInMid.Store(true)
			return commit.MoveTo(ctx)
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { <-release; return nil })); err != nil {
			return err
		}
		w := fo.Detach(ctx)
		if err := mid.MoveTo(ctx); err != nil {
			return err
		}
		close(waiting)
		if err := w.Wait(ctx); err != nil { // holds mid while the detached work runs
			return err
		}
		return commit.MoveTo(ctx)
	})
}

// TestWaitCallerRules: a standalone wave answers anyone with its stored error; an owned wave needs its item's context.
func TestWaitCallerRules(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	// Standalone: Retain with a context that carries no item hands back a finished wave carrying the reason.
	sw := write.Retain(context.Background(), func() error { return nil })
	if err := sw.Wait(context.Background()); !errors.Is(err, ErrForeignContext) {
		t.Fatalf("standalone Wait = %v, want ErrForeignContext", err)
	}

	fromTask := make(chan error, 1)
	var saved context.Context
	var w Wave
	err := runOnce(t, c, func(ctx context.Context) error {
		saved = ctx
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w = write.Retain(ctx, func() error { return nil })
		if err := w.Wait(context.Background()); !errors.Is(err, ErrForeignContext) {
			t.Errorf("Wait with a context without an item = %v, want ErrForeignContext", err)
		}
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(tctx context.Context) error {
			fromTask <- recoveredErr(func() { _ = w.Wait(tctx) })
			return nil
		})); err != nil {
			return err
		}
		return fo.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if perr := <-fromTask; !errors.Is(perr, errCannotMove) {
		t.Fatalf("Wait from a pool task panicked with %v, want errCannotMove", perr)
	}
	// Stale: the item has finished; a context decoupled from its cancellation is refused.
	if err := w.Wait(context.WithoutCancel(saved)); !errors.Is(err, ErrStaleContext) {
		t.Fatalf("Wait after the item finished = %v, want ErrStaleContext", err)
	}
}

// TestWaitByLaneChildOnParentsWavePanics: a lane child is an item of its own. It may wait on its own waves (see
// TestRetainByChildHoldsLaneInteriorStage) but not on its parent's — a wave is only meaningful to the item that
// created it. Recovered inside the child's callback.
func TestWaitByLaneChildOnParentsWavePanics(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	inner := lane.AddStage(OptName("inner"))

	got := make(chan error, 1)
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		pw := write.Retain(ctx, func() error { return nil })
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
			if err := inner.MoveTo(cctx); err != nil {
				return err
			}
			got <- recoveredErr(func() { _ = pw.Wait(cctx) })
			return nil
		})); err != nil {
			return err
		}
		if err := fo.Wait(ctx); err != nil {
			return err
		}
		return pw.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if perr := <-got; !errors.Is(perr, errForeignWave) {
		t.Fatalf("Wait by a lane child on the parent's wave panicked with %v, want errForeignWave", perr)
	}
}

// TestWaitCleanWaveOnCanceledItemReturnsCause: a canceled item never gets nil, even for a wave that finished clean,
// and the cancellation branch acknowledges nothing — there is no error on the wave to acknowledge.
func TestWaitCleanWaveOnCanceledItemReturnsCause(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.Retain(ctx, func() error { return nil })
		<-w.Finished()
		itemOf(ctx).cancel(boom)
		if err := w.Wait(ctx); !errors.Is(err, boom) {
			t.Errorf("Wait on a clean wave of a canceled item = %v, want the cause %v", err, boom)
		}
		if ackedOf(ctx, w) {
			t.Errorf("a clean wave was acknowledged by a Wait that returned the cancellation cause")
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, boom) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestWaitHonorsCallContextDeadline: a deadline the caller adds ends the wait with that deadline; the wave is not
// acknowledged, and a later Wait with the item's context still works.
func TestWaitHonorsCallContextDeadline(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))

	release := make(chan struct{})
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.Retain(ctx, func() error { <-release; return nil })
		dctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		if err := w.Wait(dctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Wait with an expired deadline = %v, want DeadlineExceeded", err)
		}
		if ackedOf(ctx, w) {
			t.Errorf("a running wave was acknowledged by a Wait that hit its deadline")
		}
		close(release)
		if err := w.Wait(ctx); err != nil {
			return err
		}
		if !ackedOf(ctx, w) {
			t.Errorf("a Wait that returned nil on a finished wave did not acknowledge it")
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestWaitItemCauseWinsOverCallContextCause: both contexts are canceled with different causes; the item's own cause
// is the answer, as everywhere in the runtime (item.cancelCause). The wave stays unacknowledged: it is still running.
func TestWaitItemCauseWinsOverCallContextCause(t *testing.T) {
	itemCause := errors.New("item cause")
	callCause := errors.New("call cause")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))

	release := make(chan struct{})
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.Retain(ctx, func() error { <-release; return nil })
		cctx, cancel := context.WithCancelCause(ctx)
		cancel(callCause)
		itemOf(ctx).cancel(itemCause)
		if err := w.Wait(cctx); !errors.Is(err, itemCause) {
			t.Errorf("Wait with both contexts canceled = %v, want the item's cause %v", err, itemCause)
		}
		if ackedOf(ctx, w) {
			t.Errorf("a running wave was acknowledged")
		}
		// Only the call context canceled: its cause is the answer.
		w2 := write.Retain(ctx, func() error { <-release; return nil }) // canceled item: a finished wave carrying the poison
		if err := w2.Wait(cctx); !errors.Is(err, itemCause) {
			t.Errorf("Wait on the wave of a canceled Retain = %v, want the poison %v", err, itemCause)
		}
		close(release)
		<-w.Finished()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, itemCause) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestWaitOnCanceledRetainWaveReturnsRawError: a Retain on an already canceled item runs no callback and hands back a
// finished wave carrying the cause, with no node behind it. Wait returns that error raw — no "<node> work:" prefix —
// and acknowledges it, exactly as Err after Finished would.
func TestWaitOnCanceledRetainWaveReturnsRawError(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		itemOf(ctx).cancel(boom)
		w := write.Retain(ctx, func() error {
			t.Errorf("the callback of a Retain on a canceled item ran")
			return nil
		})
		select {
		case <-w.Finished():
		default:
			t.Fatalf("the wave of a canceled Retain is not finished")
		}
		if err := w.Wait(ctx); err != boom { //nolint:errorlint // identity: the raw stored error, not a wrapping
			t.Errorf("Wait = %v (%q), want the raw %v", err, err, boom)
		}
		if !ackedOf(ctx, w) {
			t.Errorf("the finished wave was not acknowledged")
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, boom) {
		t.Fatalf("run failed: %v; the wave was acknowledged, completion must not raise it", err)
	}
}

// TestWaitConcurrentWaiters: several goroutines of the item wait on one wave at the same time. On a clean wave all get
// nil; on a failed wave all get the wave's error and it is acknowledged once; a waiter whose own call context is
// canceled gets that cancellation while the others are unaffected; a concurrent Err reads the same outcome. The
// processor stays alive until every waiter has returned.
func TestWaitConcurrentWaiters(t *testing.T) {
	const n = 8
	boom := errors.New("boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	waitAll := func(ctx context.Context, w Wave, park <-chan struct{}) []error {
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-park
				errs[i] = w.Wait(ctx)
			}()
		}
		wg.Wait()
		return errs
	}

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		// Clean wave: the waiters park until the callback is about to finish, so they overlap with each other.
		release := make(chan struct{})
		clean := write.Retain(ctx, func() error { <-release; return nil })
		park := make(chan struct{})
		go func() {
			waitFor(t, "the waiters to be about to block", func() bool { return parkedOf(c) == 0 }) // nothing else waits
			close(park)
			waitFor(t, "every waiter to be parked", func() bool { return parkedOf(c) == n })
			close(release)
		}()
		for i, err := range waitAll(ctx, clean, park) {
			if err != nil {
				t.Errorf("waiter %d on the clean wave = %v, want nil", i, err)
			}
		}

		// One waiter with its own canceled call context: only it is affected. The wave is still running.
		release2 := make(chan struct{})
		running := write.Retain(ctx, func() error { <-release2; return nil })
		cctx, cancel := context.WithCancelCause(ctx)
		mine := errors.New("mine")
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := running.Wait(cctx); !errors.Is(err, mine) {
				t.Errorf("Wait with a canceled call context = %v, want %v", err, mine)
			}
		}()
		other := make(chan error, 1)
		go func() { other <- running.Wait(ctx) }()
		waitFor(t, "both waiters to be parked", func() bool { return parkedOf(c) == 2 })
		cancel(mine)
		wg.Wait()
		if ackedOf(ctx, running) {
			t.Errorf("a running wave was acknowledged by a canceled call")
		}
		close(release2)
		if err := <-other; err != nil {
			t.Errorf("the other waiter = %v, want nil", err)
		}

		// Failed wave: every waiter gets the wave's error, named after the fan-out; Err agrees.
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		fail := make(chan struct{})
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { <-fail; return boom })); err != nil {
			return err
		}
		failed := fo.Detach(ctx)
		park2 := make(chan struct{})
		go func() {
			close(park2)
			waitFor(t, "every waiter to be parked on the failed wave", func() bool { return parkedOf(c) == n })
			close(fail)
		}()
		errs := waitAll(ctx, failed, park2)
		errFromErr := failed.Err()
		for i, err := range errs {
			if !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), "fo work: ") {
				t.Errorf("waiter %d on the failed wave = %v, want the wave's error named after the fan-out", i, err)
			}
		}
		if !errors.Is(errFromErr, boom) {
			t.Errorf("Err after the waiters = %v, want %v", errFromErr, boom)
		}
		if !ackedOf(ctx, failed) {
			t.Errorf("the failed wave was not acknowledged")
		}
		return nil // every waiter reported the error; completion must not raise it again
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want a clean completion", err)
	}
}
