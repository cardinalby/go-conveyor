package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins FanOut.Wait: it returns once the body is idle — the whole tree scheduled so far — reports the body's
// error with the node's name, never seals the body, and judges cancellation by the item.

// TestWaitOnEmptyBodyReturnsAtOnce: nothing scheduled, nothing to wait for. The body stays open: work may still be
// added and waited for afterwards.
func TestWaitOnEmptyBodyReturnsAtOnce(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	var ran atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Wait(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
			ran.Store(true)
			return nil
		})); err != nil {
			return err
		}
		if err := fo.Wait(ctx); err != nil {
			return err
		}
		if !ran.Load() {
			t.Error("Wait returned before the scheduled task ran")
		}
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestWaitReturnsWhenTheWholeTreeIsDone: a spawn happens while its spawner runs, so the body is never idle with work
// still to come; Wait returns only after the last level of the chain.
func TestWaitReturnsWhenTheWholeTreeIsDone(t *testing.T) {
	const depth = 3
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	var levels atomic.Int64
	var level func(ctx context.Context, d int) error
	level = func(ctx context.Context, d int) error {
		time.Sleep(time.Millisecond) // give an early Wait a chance to return
		levels.Add(1)
		if d == depth {
			return nil
		}
		return fo.Schedule(ctx, pool.NewTask(func(ctx context.Context) error { return level(ctx, d+1) }))
	}
	var atWait int64
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(ctx context.Context) error { return level(ctx, 0) })); err != nil {
			return err
		}
		if err := fo.Wait(ctx); err != nil {
			return err
		}
		atWait = levels.Load()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if atWait != depth+1 {
		t.Fatalf("%d levels had run when Wait returned, want %d", atWait, depth+1)
	}
}

// TestWaitReportsTheBodyErrorAndKeepsIt: a failed task surfaces from Wait with the fan-out's name, again on a repeated
// Wait, and Schedule afterwards returns the poison. Wait observed the error, so the item completes without failing.
func TestWaitReportsTheBodyErrorAndKeepsIt(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return boom })); err != nil {
			return err
		}
		first := fo.Wait(ctx)
		if !errors.Is(first, boom) || first.Error() != "fo work: boom" {
			t.Errorf("Wait after a failed task = %q, want %q", first, "fo work: boom")
		}
		if second := fo.Wait(ctx); second == nil || second.Error() != first.Error() {
			t.Errorf("repeated Wait = %v, want the same error again (%v)", second, first)
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil })); !errors.Is(err, boom) {
			t.Errorf("Schedule after the failure = %v, want the poison", err)
		}
		checked.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait observed the failure, so the run should not fail, got %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the checks did not run")
	}
}

// TestWaitWithStrippedContextOnCanceledItem: a context with the cancellation stripped cannot get nil out of Wait for
// a canceled item. With an idle clean body the cancellation cause is returned; with an idle failed body the body's
// own error wins, as the truer message.
func TestWaitWithStrippedContextOnCanceledItem(t *testing.T) {
	boom := errors.New("boom")
	t.Run("idle clean body returns the cause", func(t *testing.T) {
		c := NewConveyor()
		s := c.AddStage(OptName("s"))
		fo := c.AddFanOut(OptName("fo"))
		_ = fo.AddPool(OptName("pool"))

		trigger := make(chan struct{})
		var checked atomic.Bool
		err := runOnce(t, c, func(ctx context.Context) error {
			if err := s.MoveTo(ctx); err != nil {
				return err
			}
			poison := s.Retain(ctx, func() error {
				<-trigger
				return boom
			})
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			close(trigger)
			<-poison.Finished()
			_ = poison.Err()
			if err := fo.Wait(context.WithoutCancel(ctx)); !errors.Is(err, boom) {
				t.Errorf("Wait with a stripped context on a poisoned item with an empty body = %v, want the poison", err)
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
	})
	t.Run("idle failed body returns the body error", func(t *testing.T) {
		c := NewConveyor()
		fo := c.AddFanOut(OptName("fo"))
		pool := fo.AddPool(OptName("pool"))

		var checked atomic.Bool
		err := runOnce(t, c, func(ctx context.Context) error {
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return boom })); err != nil {
				return err
			}
			<-ctx.Done() // the failure poisoned the item
			err := fo.Wait(context.WithoutCancel(ctx))
			if !errors.Is(err, boom) || err.Error() != "fo work: boom" {
				t.Errorf("Wait with a stripped context on a failed body = %q, want %q", err, "fo work: boom")
			}
			checked.Store(true)
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait observed the failure, so the run should not fail, got %v", err)
		}
		if !checked.Load() {
			t.Fatalf("the checks did not run")
		}
	})
}

// TestStrippedContextWaitWakesOnItemCancellation: a Wait blocked on a busy body with a stripped context returns the
// item's cancellation cause once the item's own context is canceled, and the body stays open. The cancellation is
// applied directly to the item's context with no broadcast, so the wake-up can only come from waitUntil's watcher on
// it — the same one the phase-2 admission and join tests pin; here the wait may also be entered after the
// cancellation, which the same rule answers at once.
func TestStrippedContextWaitWakesOnItemCancellation(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	waiting := make(chan *item, 1)
	release := make(chan struct{})
	returned := make(chan error, 1)
	var checked atomic.Bool

	go func() {
		it := <-waiting
		it.cancel(boom) // the item's own context only; the call context hides it, and nothing broadcasts
		select {
		case err := <-returned:
			if !errors.Is(err, boom) {
				t.Errorf("blocked Wait with a stripped context = %v, want the item's cancellation cause", err)
			}
		case <-time.After(wakeTimeout):
			t.Errorf("Wait did not wake within %v after the item's own context was canceled", wakeTimeout)
		}
		checked.Store(true)
		close(release)
	}()

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
			<-release
			return nil
		})); err != nil {
			return err
		}
		waiting <- itemOf(ctx)
		returned <- fo.Wait(context.WithoutCancel(ctx))
		<-release
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil })); !errors.Is(err, boom) {
			t.Errorf("Schedule after the canceled Wait = %v, want the cause (the body is open, the item canceled)", err)
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the checks did not run")
	}
}

// TestUnclosedChannelSourceBlocksWait: a channel source counts as work to come until it is closed, so Wait blocks on
// it exactly as leaving does; closing the channel releases Wait.
func TestUnclosedChannelSourceBlocksWait(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	ch := make(chan TaskFunc)
	ran := make(chan struct{})
	waited := make(chan error, 1)
	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTasksChan(ch)); err != nil {
			return err
		}
		go func() { waited <- fo.Wait(ctx) }()
		ch <- func(context.Context) error {
			close(ran)
			return nil
		}
		<-ran // the next pull is now in flight (reserving a slot), waiting on the open channel
		select {
		case err := <-waited:
			t.Errorf("Wait returned (%v) while the channel source was still open", err)
		default:
		}
		close(ch)
		if err := <-waited; err != nil {
			t.Errorf("Wait after the channel was closed = %v, want nil", err)
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

// --- misuse ---

// TestWaitOutsideAnOpenBodyPanics: Wait needs an open body at this fan-out: none before entering, a closed one after
// leaving, a detached one after Detach.
func TestWaitOutsideAnOpenBodyPanics(t *testing.T) {
	t.Run("before entering", func(t *testing.T) {
		c := NewConveyor()
		fo := c.AddFanOut(OptName("fo"))
		_ = fo.AddPool(OptName("pool"))
		panicsInItem(t, c, errStageNotEntered, func(ctx context.Context) { _ = fo.Wait(ctx) })
	})
	t.Run("after leaving", func(t *testing.T) {
		c := NewConveyor()
		fo := c.AddFanOut(OptName("fo"))
		_ = fo.AddPool(OptName("pool"))
		commit := c.AddStage(OptName("commit"))
		panicsInItem(t, c, errBodyClosed, func(ctx context.Context) {
			if err := fo.MoveTo(ctx); err != nil {
				t.Fatalf("move failed: %v", err)
			}
			if err := commit.MoveTo(ctx); err != nil {
				t.Fatalf("move failed: %v", err)
			}
			_ = fo.Wait(ctx)
		})
	})
	t.Run("after Detach", func(t *testing.T) {
		c := NewConveyor()
		fo := c.AddFanOut(OptName("fo"))
		_ = fo.AddPool(OptName("pool"))
		panicsInItem(t, c, errWorkDetached, func(ctx context.Context) {
			if err := fo.MoveTo(ctx); err != nil {
				t.Fatalf("move failed: %v", err)
			}
			_ = fo.Detach(ctx)
			_ = fo.Wait(ctx)
		})
	})
}

// TestWaitFromPoolTaskPanics: a running task holds a slot and must not wait for other work. Recovered inside the task,
// which runs on a runtime goroutine.
func TestWaitFromPoolTaskPanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	got := make(chan error, 1)
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(tctx context.Context) error {
			got <- recoveredErr(func() { _ = fo.Wait(tctx) })
			return nil
		})); err != nil {
			return err
		}
		return fo.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if perr := <-got; !errors.Is(perr, errCannotMove) {
		t.Fatalf("Wait from a pool task panicked with %v, want errCannotMove", perr)
	}
}

// TestWaitFromLaneChildOnParentsFanOutPanics: a child may Schedule at the fan-out its lane belongs to, but not wait
// there — it holds a slot of that body, and the fan-out is outside its scope. Recovered inside the child's callback.
func TestWaitFromLaneChildOnParentsFanOutPanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	inner := lane.AddStage(OptName("inner"))

	got := make(chan error, 1)
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
			if err := inner.MoveTo(cctx); err != nil {
				return err
			}
			got <- recoveredErr(func() { _ = fo.Wait(cctx) })
			return nil
		})); err != nil {
			return err
		}
		return fo.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if perr := <-got; !errors.Is(perr, errWrongScope) {
		t.Fatalf("Wait from a lane child on the parent's fan-out panicked with %v, want errWrongScope", perr)
	}
}
