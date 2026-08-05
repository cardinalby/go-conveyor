package conveyor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// This file pins the package's error contract: which misuses panic with a sentinel, and which runtime conditions
// are returned (or carried by a Wave).

// panicsInItem runs one item through c and asserts that misuse, called with the item's context, panics with want.
// It is the shape most panic tests need: no setup moves, one misused call.
func panicsInItem(t *testing.T, c *Conveyor, want error, misuse func(ctx context.Context)) {
	t.Helper()
	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		assertPanics(t, want, func() { misuse(ctx) })
		checked.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the panic check did not run")
	}
}

// --- returned errors ---

// TestErrConveyorAlreadyRunningIsReturned: Run may not be called concurrently with itself; the second call is
// refused with ErrConveyorAlreadyRunning instead of joining the live run.
func TestErrConveyorAlreadyRunningIsReturned(t *testing.T) {
	c := NewConveyor()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inside := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(done)
		_ = c.Run(ctx, func(context.Context) error {
			once.Do(func() { close(inside) })
			<-release
			return nil
		})
	}()

	<-inside
	if err := c.Run(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrConveyorAlreadyRunning) {
		t.Fatalf("second Run = %v, want ErrConveyorAlreadyRunning", err)
	}
	cancel()
	close(release)
	<-done
}

// TestErrForeignContextFromStageMoveTo: a context that never came from an item cannot say who is acting, so the
// move is declined with ErrForeignContext (a variant of ErrInvalidContext) rather than panicking.
func TestErrForeignContextFromStageMoveTo(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	err := s.MoveTo(context.Background())
	if !errors.Is(err, ErrForeignContext) || !errors.Is(err, ErrInvalidContext) {
		t.Fatalf("MoveTo with a foreign context = %v, want ErrForeignContext", err)
	}
}

// TestErrForeignContextFromFanOutMoveTo: the declined move reports ErrForeignContext and schedules nothing, so the
// tasks it was given never run.
func TestErrForeignContextFromFanOutMoveTo(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	var ran atomic.Bool
	err := fo.MoveTo(context.Background(), Tasks{pool.NewTask(func(context.Context) error {
		ran.Store(true)
		return nil
	})})
	if !errors.Is(err, ErrForeignContext) || !errors.Is(err, ErrInvalidContext) {
		t.Fatalf("MoveTo with a foreign context = %v, want ErrForeignContext", err)
	}
	if ran.Load() {
		t.Fatalf("the task ran although the move was declined")
	}
}

// TestErrForeignContextFromRetain: Retain has no error to return, so it hands back a wave that is born finished
// carrying ErrForeignContext, and it does not run the bgOp.
func TestErrForeignContextFromRetain(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	var ran atomic.Bool
	w := s.Retain(context.Background(), func() error {
		ran.Store(true)
		return nil
	})
	if w == nil {
		t.Fatalf("Retain returned a nil Wave")
	}
	<-w.Started()
	<-w.Finished()
	if !errors.Is(w.Err(), ErrForeignContext) || !errors.Is(w.Err(), ErrInvalidContext) {
		t.Fatalf("wave error = %v, want ErrForeignContext", w.Err())
	}
	if ran.Load() {
		t.Fatalf("bgOp ran although the context was foreign")
	}
}

// TestErrStaleContextFromFinishedItem: a context of an item that already finished cannot drive transitions any
// more; decoupled from cancellation it is caught explicitly and every node method reports ErrStaleContext.
func TestErrStaleContextFromFinishedItem(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	captured := make(chan context.Context, 1)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	var checked atomic.Bool

	runErr := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		switch no {
		case 1:
			captured <- ic // this item finishes right away, without moving anywhere
			return nil
		case 2:
			defer cancel()
			old := <-captured
			<-old.Done() // item 1 has finished: finishing marks it and then cancels its context
			stale := context.WithoutCancel(old)

			err := s.MoveTo(stale)
			if !errors.Is(err, ErrStaleContext) || !errors.Is(err, ErrInvalidContext) {
				t.Errorf("Stage.MoveTo with a stale context = %v, want ErrStaleContext", err)
			}
			err = fo.MoveTo(stale, Tasks{pool.NewTask(func(context.Context) error { return nil })})
			if !errors.Is(err, ErrStaleContext) {
				t.Errorf("FanOut.MoveTo with a stale context = %v, want ErrStaleContext", err)
			}
			rw := s.Retain(stale, func() error { return nil })
			<-rw.Finished()
			if !errors.Is(rw.Err(), ErrStaleContext) {
				t.Errorf("Retain wave error = %v, want ErrStaleContext", rw.Err())
			}
			checked.Store(true)
			return nil
		default:
			return nil
		}
	})
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("run failed: %v", runErr)
	}
	if !checked.Load() {
		t.Fatalf("the stale-context checks did not run")
	}
}

// TestShutdownErrorFromRunContextCancellation: an item canceled by shutdown sees a ShutdownError whose cause is the
// Run context's cause, while Run itself returns that raw cause.
func TestShutdownErrorFromRunContextCancellation(t *testing.T) {
	c := NewConveyor(optCancelItemsOnShutdown()) // cancel in-flight items at once on shutdown
	s := c.AddStage(OptName("s"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inside := make(chan struct{})
	var once sync.Once
	var checked atomic.Bool

	go func() {
		<-inside
		cancel()
	}()

	runErr := c.Run(ctx, func(ic context.Context) error {
		once.Do(func() {
			close(inside)
			<-ic.Done() // the shutdown canceled this item's context

			err := s.MoveTo(ic)
			var se ShutdownError
			if !errors.As(err, &se) {
				t.Errorf("MoveTo after shutdown = %v, want a ShutdownError", err)
				return
			}
			if !errors.Is(se.Cause(), context.Canceled) {
				t.Errorf("ShutdownError.Cause() = %v, want context.Canceled", se.Cause())
			}
			if !errors.Is(err, context.Canceled) {
				t.Errorf("errors.Is(%v, context.Canceled) = false, want true (the cause must be reachable)", err)
			}
			if cause := context.Cause(ic); !errors.As(cause, &se) {
				t.Errorf("context cause = %v, want a ShutdownError", cause)
			}
			checked.Store(true)
		})
		return nil
	})
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", runErr)
	}
	var se ShutdownError
	if errors.As(runErr, &se) {
		t.Fatalf("Run returned a ShutdownError (%v); it must return the raw cause", runErr)
	}
	if !checked.Load() {
		t.Fatalf("the shutdown checks did not run")
	}
}

// TestShutdownErrorFromItemError: when one item fails, the later items are canceled with a ShutdownError carrying
// that error as its cause; Run returns the error itself.
func TestShutdownErrorFromItemError(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	boom := errors.New("boom")

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

			cause := context.Cause(ic)
			var se ShutdownError
			if !errors.As(cause, &se) {
				t.Errorf("context cause = %v, want a ShutdownError", cause)
				return nil
			}
			if !errors.Is(se.Cause(), boom) {
				t.Errorf("ShutdownError.Cause() = %v, want the item error", se.Cause())
			}
			if !errors.Is(cause, boom) {
				t.Errorf("errors.Is(%v, boom) = false, want true (the cause must be reachable)", cause)
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
	var se ShutdownError
	if errors.As(runErr, &se) {
		t.Fatalf("Run returned a ShutdownError (%v); it must return the raw cause", runErr)
	}
	if !checked.Load() {
		t.Fatalf("the shutdown checks did not run")
	}
}

// TestWaveErrorSurfacesFromJoinAndFromErr: a wave's error reaches the item through the MoveTo that joins it and
// through Wave.Err, and from there it fails the run.
func TestWaveErrorSurfacesFromJoinAndFromErr(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))
	boom := errors.New("boom")

	runErr := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error { return boom })})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		joinErr := commit.MoveTo(ctx, w)
		if !errors.Is(joinErr, boom) {
			t.Errorf("join error = %v, want the task error", joinErr)
		}
		<-w.Finished()
		if !errors.Is(w.Err(), boom) {
			t.Errorf("Wave.Err() = %v, want the task error", w.Err())
		}
		return joinErr
	})
	if !errors.Is(runErr, boom) {
		t.Fatalf("Run = %v, want the task error", runErr)
	}
}

// --- panic sentinels ---

// TestErrWrongEnterOrderMovingBackwards: items move forward only, so a stage behind the item's progress is misuse.
func TestErrWrongEnterOrderMovingBackwards(t *testing.T) {
	c := NewConveyor()
	first := c.AddStage(OptName("first"))
	second := c.AddStage(OptName("second"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := second.MoveTo(ctx); err != nil {
			return err
		}
		assertPanics(t, errWrongEnterOrder, func() { _ = first.MoveTo(ctx) })
		checked.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the enter-order check did not run")
	}
}

// TestErrWrongEnterOrderFanOutBehindItem: the same rule holds for a fan-out node the item has already passed.
func TestErrWrongEnterOrderFanOutBehindItem(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	after := c.AddStage(OptName("after"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := after.MoveTo(ctx); err != nil { // skip the fan-out and go past it
			return err
		}
		assertPanics(t, errWrongEnterOrder, func() {
			_ = fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error { return nil })})
		})
		checked.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the enter-order check did not run")
	}
}

// TestErrNodeAlreadyEnteredStage: a stage is entered once per item, so re-entering the one the item occupies is
// misuse.
func TestErrNodeAlreadyEnteredStage(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		assertPanics(t, errNodeAlreadyEntered, func() { _ = s.MoveTo(ctx) })
		checked.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the re-entry check did not run")
	}
}

// TestErrNodeAlreadyEnteredFanOut: a fan-out is entered once per item too — a second submission to the same node is
// misuse, even with fresh tasks.
func TestErrNodeAlreadyEnteredFanOut(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error { return nil })})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		assertPanics(t, errNodeAlreadyEntered, func() {
			_ = fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error { return nil })})
		})
		checked.Store(true)
		<-w.Finished()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the re-entry check did not run")
	}
}

// TestErrInvalidUnitStageFromAnotherConveyor: a node handle only works with items of its own conveyor.
func TestErrInvalidUnitStageFromAnotherConveyor(t *testing.T) {
	c := NewConveyor()
	_ = c.AddStage(OptName("mine"))
	other := NewConveyor()
	foreign := other.AddStage(OptName("foreign"))

	panicsInItem(t, c, errInvalidUnit, func(ctx context.Context) {
		_ = foreign.MoveTo(ctx)
	})
}

// TestErrInvalidUnitItemContextFromAnotherConveyor: the mirror image — this conveyor's stage used with an item
// context borrowed from another running conveyor.
func TestErrInvalidUnitItemContextFromAnotherConveyor(t *testing.T) {
	c := NewConveyor()
	mine := c.AddStage(OptName("mine"))
	other := NewConveyor()
	_ = other.AddStage(OptName("theirs"))

	otherCtxs := make(chan context.Context, 1)
	release := make(chan struct{})
	otherDone := make(chan struct{})
	otherRunCtx, cancelOther := context.WithTimeout(context.Background(), testTimeout)
	defer cancelOther()
	var once sync.Once
	go func() {
		defer close(otherDone)
		_ = other.Run(otherRunCtx, func(ic context.Context) error {
			once.Do(func() {
				otherCtxs <- ic
				<-release // keep the foreign item (and its conveyor) alive for the assertion
			})
			return nil
		})
	}()

	foreignCtx := <-otherCtxs
	panicsInItem(t, c, errInvalidUnit, func(context.Context) {
		_ = mine.MoveTo(foreignCtx)
	})
	close(release)
	cancelOther()
	<-otherDone
}

// TestErrInvalidUnitTaskFromAnotherFanOut: a task may only be submitted to the fan-out that owns its lane.
func TestErrInvalidUnitTaskFromAnotherFanOut(t *testing.T) {
	c := NewConveyor()
	fo1 := c.AddFanOut(OptName("fo1"))
	pool1 := fo1.AddPool(OptName("pool1"))
	fo2 := c.AddFanOut(OptName("fo2"))
	_ = fo2.AddPool(OptName("lane2"))

	panicsInItem(t, c, errInvalidUnit, func(ctx context.Context) {
		_ = fo2.MoveTo(ctx, Tasks{pool1.NewTask(func(context.Context) error { return nil })})
	})
}

// TestErrTaskReusedOnSecondSubmission: a Task is lazy, stateful and single-use, so submitting one twice is misuse.
func TestErrTaskReusedOnSecondSubmission(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	panicsInItem(t, c, errTaskReused, func(ctx context.Context) {
		task := pool.NewTask(func(context.Context) error { return nil })
		_ = fo.MoveTo(ctx, Tasks{task, task})
	})
}

// TestErrNilTaskFuncEagerConstructors: the eager constructors see the nil callback themselves, so they panic at
// construction time, before any item is involved.
func TestErrNilTaskFuncEagerConstructors(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	assertPanics(t, errNilTaskFunc, func() { _ = pool.NewTask(nil) })
	assertPanics(t, errNilTaskFunc, func() { _ = pool.NewTasks(3, nil) })
	// A non-positive count yields an empty task, so there is no callback to complain about.
	_ = pool.NewTasks(0, nil)
}

// TestErrNilTaskFuncFromGeneratorFailsItem: a generator that yields a nil callback misuses the API on an internal
// goroutine, so it fails the item (fail-fast) instead of panicking there.
func TestErrNilTaskFuncFromGeneratorFailsItem(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	runErr := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTasksGen(func(yield func(TaskFunc) bool) {
			yield(nil)
		})})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		<-w.Finished()
		return w.Err()
	})
	if !errors.Is(runErr, errNilTaskFunc) {
		t.Fatalf("Run = %v, want an error matching the nil-callback sentinel", runErr)
	}
}

// TestErrNilTaskFuncFromChannelFailsItem: the same for a nil callback sent on a task channel.
func TestErrNilTaskFuncFromChannelFailsItem(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	ch := make(chan TaskFunc, 1)
	ch <- nil
	close(ch)

	runErr := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTasksChan(ch)})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		<-w.Finished()
		return w.Err()
	})
	if !errors.Is(runErr, errNilTaskFunc) {
		t.Fatalf("Run = %v, want an error matching the nil-callback sentinel", runErr)
	}
}

// TestErrStageNotEnteredOnUnenteredStage: Retain hands over the slot the item holds in the stage, so retaining a
// stage the item never entered is meaningless.
func TestErrStageNotEnteredOnUnenteredStage(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	panicsInItem(t, c, errStageNotEntered, func(ctx context.Context) {
		_ = s.Retain(ctx, func() error { return nil })
	})
}

// TestErrStageNotEnteredAfterLeaving: the same once the item has moved on — the stage was entered, but is no longer
// occupied.
func TestErrStageNotEnteredAfterLeaving(t *testing.T) {
	c := NewConveyor()
	first := c.AddStage(OptName("first"))
	second := c.AddStage(OptName("second"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := first.MoveTo(ctx); err != nil {
			return err
		}
		if err := second.MoveTo(ctx); err != nil { // releases the first stage
			return err
		}
		assertPanics(t, errStageNotEntered, func() {
			_ = first.Retain(ctx, func() error { return nil })
		})
		checked.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the occupancy check did not run")
	}
}

// TestErrWrongScopeChildMovingToConveyorNode: a child item travels its lane's interior only; a node of the
// conveyor belongs to another series.
func TestErrWrongScopeChildMovingToConveyorNode(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	inner := lane.AddStage(OptName("inner")) // interior nodes: the lane's work runs as child items
	commit := c.AddStage(OptName("commit"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTask(func(cctx context.Context) error {
			if err := inner.MoveTo(cctx); err != nil {
				return err
			}
			assertPanics(t, errWrongScope, func() { _ = commit.MoveTo(cctx) })
			checked.Store(true)
			return nil
		})})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		<-w.Finished()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the scope check did not run")
	}
}

// TestErrWrongScopeItemMovingIntoLane: and the conveyor's own item may not reach into a lane's interior.
func TestErrWrongScopeItemMovingIntoLane(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	inner := lane.AddStage(OptName("inner"))

	panicsInItem(t, c, errWrongScope, func(ctx context.Context) {
		_ = inner.MoveTo(ctx)
	})
}

// TestErrWrongScopeIsCheckedOnEveryEntryPoint: the scope guard is not a per-method check that a new entry point could
// forget — every one of them resolves its acting item through Conveyor.actingItem, which validates the scope before it
// even takes the run lock. An item of the conveyor therefore cannot reach a lane's interior by any route: not by
// moving, not by trying, not by retaining, and not through an interior fan-out.
func TestErrWrongScopeIsCheckedOnEveryEntryPoint(t *testing.T) {
	for _, tc := range []struct {
		name   string
		misuse func(Stage, FanOut, Branch, context.Context)
	}{
		{"Stage.MoveTo", func(s Stage, _ FanOut, _ Branch, ctx context.Context) { _ = s.MoveTo(ctx) }},
		{"Stage.TryMoveTo", func(s Stage, _ FanOut, _ Branch, ctx context.Context) { _, _ = s.TryMoveTo(ctx) }},
		{"Stage.Retain", func(s Stage, _ FanOut, _ Branch, ctx context.Context) {
			_ = s.Retain(ctx, func() error { return nil })
		}},
		{"FanOut.MoveTo", func(_ Stage, f FanOut, b Branch, ctx context.Context) {
			_ = f.MoveTo(ctx, Tasks{b.NewTask(func(context.Context) error { return nil })})
		}},
		{"FanOut.TryMoveTo", func(_ Stage, f FanOut, b Branch, ctx context.Context) {
			_, _ = f.TryMoveTo(ctx, Tasks{b.NewTask(func(context.Context) error { return nil })})
		}},
		{"FanOut.Detach", func(_ Stage, f FanOut, _ Branch, ctx context.Context) { _ = f.Detach(ctx) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewConveyor()
			outer := c.AddFanOut(OptName("outer"))
			lane := outer.AddLane(OptName("lane"))
			innerStage := lane.AddStage(OptName("inner"))     // a stage of the lane's scope
			innerFanOut := lane.AddFanOut(OptName("innerFo")) // and a fan-out of it, with a branch to build tasks on
			innerPool := innerFanOut.AddPool(OptName("innerPool"))

			panicsInItem(t, c, errWrongScope, func(ctx context.Context) {
				tc.misuse(innerStage, innerFanOut, innerPool, ctx)
			})
		})
	}
}

// TestErrCannotMoveFromNonTravellingWork: work on a lane without interior nodes has nowhere to go, and its context
// carries the item that scheduled it — moving with it would move that item from a lane goroutine.
func TestErrCannotMoveFromNonTravellingWork(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // a pool: its work has nowhere to travel
	commit := c.AddStage(OptName("commit"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTask(func(cctx context.Context) error {
			assertPanics(t, errCannotMove, func() { _ = commit.MoveTo(cctx) })
			assertPanics(t, errCannotMove, func() { _ = commit.Retain(cctx, func() error { return nil }) })
			checked.Store(true)
			return nil
		})})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		<-w.Finished()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the cannot-move check did not run")
	}
}

// TestErrForeignWaveFromAnotherItem: a wave is only meaningful to the item that created it, so joining another
// item's wave is misuse.
func TestErrForeignWaveFromAnotherItem(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	waves := make(chan Wave, 1)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	var checked atomic.Bool

	runErr := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		switch no {
		case 1:
			err := fo.MoveTo(ic, Tasks{pool.NewTask(func(context.Context) error { return nil })})
			if err != nil {
				return err
			}
			w := fo.Detach(ic)
			waves <- w
			return commit.MoveTo(ic)
		case 2:
			defer cancel()
			foreign := <-waves
			assertPanics(t, errForeignWave, func() { _ = commit.MoveTo(ic, foreign) })
			checked.Store(true)
			return nil
		default:
			return nil
		}
	})
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatalf("run failed: %v", runErr)
	}
	if !checked.Load() {
		t.Fatalf("the foreign-wave check did not run")
	}
}

// TestErrForeignWaveFromNilWave: a nil (or otherwise foreign) Wave implementation is caught by the same check.
func TestErrForeignWaveFromNilWave(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	panicsInItem(t, c, errForeignWave, func(ctx context.Context) {
		_ = s.MoveTo(ctx, nil)
	})
}

// TestErrConveyorRunningOnTopologyChange: the topology may not be extended while the conveyor is running.
func TestErrConveyorRunningOnTopologyChange(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	fo := c.AddFanOut(OptName("fo"))
	_ = fo.AddPool(OptName("lane"))

	var checked atomic.Bool
	err := runOnce(t, c, func(context.Context) error {
		assertPanics(t, errConveyorRunning, func() { _ = c.AddStage(OptName("late")) })
		assertPanics(t, errConveyorRunning, func() { _ = c.AddFanOut(OptName("lateFanOut")) })
		assertPanics(t, errConveyorRunning, func() { _ = fo.AddPool(OptName("lateLane")) })
		_ = s.SetQueueSize(2) // capacity, not topology: legal on a live conveyor
		checked.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the topology checks did not run")
	}
}

// TestErrConveyorFinalizedOnTopologyChange: the topology is frozen from the first Run on, so extending it after a
// run has returned is misuse too. Capacity changes stay legal.
func TestErrConveyorFinalizedOnTopologyChange(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	fo := c.AddFanOut(OptName("fo"))
	_ = fo.AddPool(OptName("lane"))

	runNOK(t, c, 2, func(ctx context.Context, _ int64) error { return s.MoveTo(ctx) })

	assertPanics(t, errConveyorFinalized, func() { _ = c.AddStage(OptName("late")) })
	assertPanics(t, errConveyorFinalized, func() { _ = c.AddFanOut(OptName("lateFanOut")) })
	assertPanics(t, errConveyorFinalized, func() { _ = fo.AddPool(OptName("lateLane")) })
	s.SetLimit(3) // capacity is not topology
	s.SetQueueSize(2)
	if s.Limit() != 3 || s.QueueSize() != 2 {
		t.Fatalf("Limit()=%d QueueSize()=%d, want 3 and 2", s.Limit(), s.QueueSize())
	}
}
