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

// This file pins FanOut.Wait: it returns once the body is idle — the whole tree scheduled so far — reports the
// body's error as a TaskError named after the branch, never seals the body, and judges cancellation by the item.

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

// TestWaitReportsTheBodyErrorAndKeepsIt: a failed task surfaces from Wait as a TaskError named after its pool, again
// on a repeated Wait, and Schedule afterwards is refused with it: the failure canceled the body, not the item. Wait
// joined the error and the processor returns nil, so the item completes without failing.
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
		var te TaskError
		if !errors.As(first, &te) || te.Unwrap() != boom || te.Unit() != Unit(pool) || first.Error() != "pool task: boom" {
			t.Errorf("Wait after a failed task = %v, want a TaskError %q", first, "pool task: boom")
		}
		if second := fo.Wait(ctx); second == nil || second.Error() != first.Error() {
			t.Errorf("repeated Wait = %v, want the same error again (%v)", second, first)
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil })); !errors.As(err, &te) ||
			te.Unwrap() != boom {
			t.Errorf("Schedule after the failure = %v, want the body's TaskError", err)
		}
		if cause := context.Cause(ctx); cause != nil {
			t.Errorf("item context cause = %v, want nil: a task error cancels only its body", cause)
		}
		checked.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait joined the failure and the processor dropped it, so the run should not fail, got %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the checks did not run")
	}
}

// runFirstItemAsync runs c with runAsync: the first item runs proc, later items wait until their context is done and
// return its cause (an abort). Used with OptDrainTimeout(0) to get an item canceled by a shutdown.
func runFirstItemAsync(c Conveyor, proc ItemProcessor) (context.CancelCauseFunc, <-chan error) {
	var once sync.Once
	return runAsync(c, func(ctx context.Context) error {
		first := false
		once.Do(func() { first = true })
		if !first {
			<-ctx.Done()
			return context.Cause(ctx)
		}
		return proc(ctx)
	})
}

// TestWaitWithStrippedContextOnCanceledItem: a context with the cancellation stripped cannot get nil out of Wait for
// a canceled item (here canceled by a shutdown with no drain time). With an idle clean body the cancellation cause is
// returned; with an idle failed body the body's own error wins, as the truer message.
func TestWaitWithStrippedContextOnCanceledItem(t *testing.T) {
	boom := errors.New("boom")
	cause := errors.New("stop")
	t.Run("idle clean body returns the cause", func(t *testing.T) {
		c := NewConveyor(OptDrainTimeout(0)) // cancel in-flight items as soon as shutdown starts
		fo := c.AddFanOut(OptName("fo"))
		_ = fo.AddPool(OptName("pool"))

		inFo := make(chan struct{})
		var waitErr error
		cancel, done := runFirstItemAsync(c, func(ctx context.Context) error {
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			signal(inFo)
			<-ctx.Done()
			waitErr = fo.Wait(context.WithoutCancel(ctx))
			return nil
		})
		<-inFo
		cancel(cause)
		err := recvErr(t, "Run", done)
		var se ShutdownError
		if !errors.As(waitErr, &se) || !errors.Is(waitErr, cause) {
			t.Fatalf("Wait with a stripped context on a canceled item with an empty body = %v, want its ShutdownError",
				waitErr)
		}
		var ie ItemError
		if !errors.As(err, &se) || !errors.Is(err, cause) || errors.As(err.(RunError).DrainError(), &ie) {
			t.Fatalf("Run error = %v, want a ShutdownError with %v and no item failure", err, cause)
		}
	})
	t.Run("idle failed body returns the body error", func(t *testing.T) {
		c := NewConveyor(OptDrainTimeout(0)) // cancel in-flight items as soon as shutdown starts
		fo := c.AddFanOut(OptName("fo"))
		pool := fo.AddPool(OptName("pool"))

		failed := make(chan struct{})
		var waitErr error
		cancel, done := runFirstItemAsync(c, func(ctx context.Context) error {
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return boom })); err != nil {
				return err
			}
			it := itemOf(ctx)
			waitFor(t, "the body to fail and become idle", func() bool {
				it.run.mu.Lock()
				defer it.run.mu.Unlock()
				return it.pending.idle() && it.pending.err != nil
			})
			signal(failed)
			<-ctx.Done() // the shutdown cancels the item; the failed body is not joined yet
			waitErr = fo.Wait(context.WithoutCancel(ctx))
			return nil
		})
		<-failed
		cancel(cause)
		err := recvErr(t, "Run", done)
		var te TaskError
		if !errors.As(waitErr, &te) || te.Unwrap() != boom || waitErr.Error() != "pool task: boom" {
			t.Fatalf("Wait with a stripped context on a failed body = %v, want %q", waitErr, "pool task: boom")
		}
		var se ShutdownError
		var ie ItemError
		if !errors.As(err, &se) || !errors.Is(err, cause) || errors.As(err.(RunError).DrainError(), &ie) {
			t.Fatalf("Run error = %v, want a ShutdownError with %v and no item failure: Wait joined the body", err, cause)
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

// --- misuse ---

// TestWaitOutsideAnOpenBodyPanics: Wait needs an open body at this fan-out: none before entering, a closed one after
// leaving, a retained one after Retain.
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
	t.Run("after Retain", func(t *testing.T) {
		c := NewConveyor()
		fo := c.AddFanOut(OptName("fo"))
		_ = fo.AddPool(OptName("pool"))
		panicsInItem(t, c, errWorkRetained, func(ctx context.Context) {
			if err := fo.MoveTo(ctx); err != nil {
				t.Fatalf("move failed: %v", err)
			}
			_ = fo.Retain(ctx)
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
	perr := <-got
	if !errors.Is(perr, errCannotMove) {
		t.Fatalf("Wait from a pool task panicked with %v, want errCannotMove", perr)
	}
	// The hint must fit a wait, not a move: the generic AddLane advice would point the user the wrong way.
	if !strings.Contains(perr.Error(), "must not wait for other work") {
		t.Fatalf("Wait from a pool task panicked with %q, want the hold-and-wait hint", perr)
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
