package conveyor

import (
	"context"
	"errors"
	"testing"
)

// runFirstFails runs c with proc as the first item's processor and lets the run end by itself: later items wait for
// their cancellation, so the first item's failure is what shuts the run down (unlike runOnce, which cancels the Run
// context as soon as proc returns nil).
func runFirstFails(c Conveyor, proc ItemProcessor) error {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	return c.Run(ctx, func(ic context.Context) error {
		if itemNo(ic) == 1 {
			return proc(ic)
		}
		<-ic.Done()
		return context.Cause(ic)
	})
}

// TestFailedBodyCancelsSiblingsNotItem: a failed fan-out task cancels the other tasks of its body, not the item. The
// leave reports the failure as a TaskError naming the pool, the item stays where it is with its context alive, and
// it may move on and finish cleanly: returning nil skips the failure (e.g. after dead-lettering the message).
func TestFailedBodyCancelsSiblingsNotItem(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	failing := fo.AddPool(OptName("failing"))
	waiting := fo.AddPool(OptName("waiting"))
	commit := c.AddStage(OptName("commit"))

	var siblingCause, leaveErr, itemCause error
	var committed bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx,
			failing.NewTask(func(context.Context) error { return boom }),
			waiting.NewTask(func(tctx context.Context) error {
				<-tctx.Done() // released by the body's cancellation
				siblingCause = context.Cause(tctx)
				return nil
			}),
		); err != nil {
			return err
		}
		leaveErr = commit.MoveTo(ctx)
		itemCause = context.Cause(ctx)
		if err := commit.MoveTo(ctx); err != nil { // the body is joined now: the item moves on
			return err
		}
		committed = true
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want no failure (the item skipped the task error)", err)
	}
	var te TaskError
	if !errors.As(leaveErr, &te) || !errors.Is(leaveErr, boom) {
		t.Fatalf("leave error = %v, want a TaskError wrapping %v", leaveErr, boom)
	}
	if te.Unit() != failing {
		t.Errorf("TaskError.Unit = %v, want the failing pool", te.Unit())
	}
	if te.Unwrap() != boom {
		t.Errorf("TaskError.Unwrap = %v, want the task's own error", te.Unwrap())
	}
	if itemCause != nil {
		t.Errorf("item context canceled with %v, want it alive", itemCause)
	}
	if !errors.As(siblingCause, &te) || !errors.Is(siblingCause, boom) {
		t.Errorf("sibling task's context cause = %v, want the body's TaskError", siblingCause)
	}
	if !committed {
		t.Error("the item did not move on after the failed body")
	}
}

// TestFailedBodyRefusesMoreWork: after a failed FanOut.Wait the body takes no more tasks: Schedule returns the same
// TaskError and the task never runs. An item that joined the failure and returns nil does not fail.
func TestFailedBodyRefusesMoreWork(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	var waitErr, againErr, schedErr error
	var ran bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return boom })); err != nil {
			return err
		}
		waitErr = fo.Wait(ctx)
		againErr = fo.Wait(ctx)
		schedErr = fo.Schedule(ctx, pool.NewTask(func(context.Context) error { ran = true; return nil }))
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want no failure (the failure was joined and skipped)", err)
	}
	var te TaskError
	if !errors.As(waitErr, &te) || !errors.Is(waitErr, boom) {
		t.Fatalf("FanOut.Wait = %v, want a TaskError wrapping %v", waitErr, boom)
	}
	if !errors.Is(againErr, boom) {
		t.Errorf("second FanOut.Wait = %v, want the same failure", againErr)
	}
	if !errors.As(schedErr, &te) || !errors.Is(schedErr, boom) {
		t.Errorf("Schedule into the failed body = %v, want its TaskError", schedErr)
	}
	if ran {
		t.Error("a task scheduled into the failed body ran")
	}
}

// TestNewRoundAfterCleanWaitMustBeJoinedAgain: a clean FanOut.Wait joins only the work scheduled so far. A failure
// of a later round the processor never joins fails the item.
func TestNewRoundAfterCleanWaitMustBeJoinedAgain(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	err := runFirstFails(c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil })); err != nil {
			return err
		}
		if err := fo.Wait(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return boom })); err != nil {
			return err
		}
		return nil // returns without joining the second round
	})
	var ie ItemError
	var te TaskError
	if !errors.As(err, &ie) || !errors.As(err, &te) || !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want an ItemError wrapping a TaskError of %v", err, boom)
	}
}

// TestUnjoinedRetainForFailsItem: a RetainFor task nobody waits for still fails its item; the ItemError carries the
// TaskError naming the stage.
func TestUnjoinedRetainForFailsItem(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	err := runFirstFails(c, func(ctx context.Context) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		s.RetainFor(ctx, func(context.Context) error { return boom })
		return nil
	})
	var te TaskError
	if !errors.As(err, &te) || !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want an error wrapping a TaskError of %v", err, boom)
	}
	if te.Unit() != s {
		t.Errorf("TaskError.Unit = %v, want stage s", te.Unit())
	}
}

// TestUnjoinedFailureDuringJoinStartsShutdownAtOnce: a processor returned without joining its RetainFor tasks, and
// one of them fails while another still runs. The failure is final at once — nobody can join it any more — so the
// shutdown begins and later items are canceled before the item's background work is over.
func TestUnjoinedFailureDuringJoinStartsShutdownAtOnce(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	reached2 := make(chan struct{})
	returned1 := make(chan struct{})
	failNow := make(chan struct{})
	release := make(chan struct{})
	var bgDone bool
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			s.RetainFor(ic, func(context.Context) error { <-failNow; return boom })
			s.RetainFor(ic, func(context.Context) error { <-release; bgDone = true; return nil })
			<-reached2
			close(returned1)
			return nil // nothing joined
		case 2:
			close(reached2)
			<-returned1
			close(failNow)
			waitFor(t, "item 2 to be canceled while item 1's other task runs", func() bool { return ic.Err() != nil })
			if !shutdownBegun(c) {
				t.Error("the shutdown has not begun")
			}
			if bgDone {
				t.Error("item 1's background work ended before the checks")
			}
			close(release)
			return context.Cause(ic)
		}
		<-ic.Done()
		return context.Cause(ic)
	})
	var ie ItemError
	var te TaskError
	if !errors.As(err, &ie) || !errors.As(err, &te) || !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want an ItemError wrapping a TaskError of %v", err, boom)
	}
	if de := ie.DrainError(); de != nil {
		t.Errorf("DrainError = %v, want nil (item 2 was aborted)", de)
	}
}

// TestDrainFailureDuringJoinBeatsLaterDrainTimeout: during a drain, an older item's unjoined task fails while its
// other task still runs. The failure is the first problem of the drain and stays DrainError, even though the drain
// context ends while the item is still being joined.
func TestDrainFailureDuringJoinBeatsLaterDrainTimeout(t *testing.T) {
	boom := errors.New("boom")
	drainCtx, drainCancel := context.WithCancel(context.Background())
	defer drainCancel()
	c := NewConveyor(OptDrainContextFunc(func(error) (context.Context, context.CancelFunc) {
		return drainCtx, nil
	}))
	s := c.AddStage(OptName("s"))

	returned1 := make(chan struct{})
	failNow := make(chan struct{})
	release := make(chan struct{})
	cancelRun, done := runAsync(c, func(ic context.Context) error {
		if itemNo(ic) == 1 {
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			s.RetainFor(ic, func(context.Context) error { <-failNow; return boom })
			s.RetainFor(ic, func(context.Context) error { <-release; return nil })
			close(returned1)
			return nil // nothing joined
		}
		<-ic.Done()
		return context.Cause(ic)
	})
	<-returned1
	cancelRun(nil) // the shutdown begins; item 1 is in flight and drains
	waitFor(t, "the shutdown to begin", func() bool { return shutdownBegun(c) })
	close(failNow)
	waitFor(t, "item 1's failure to be the drain error", func() bool { return drainErrOf(c) != nil })
	drainCancel() // the drain context ends while item 1 is still joined
	close(release)

	err := recvErr(t, "Run", done)
	var se ShutdownError
	if !errors.As(err, &se) {
		t.Fatalf("Run = %v, want a ShutdownError", err)
	}
	var ie ItemError
	var te TaskError
	de := se.DrainError()
	if !errors.As(de, &ie) || !errors.As(de, &te) || !errors.Is(de, boom) {
		t.Fatalf("DrainError = %v, want an ItemError wrapping a TaskError of %v", de, boom)
	}
}

// TestFailedLaneChildCancelsItsBody: a lane child's failure fails its body: the other child sees its context canceled
// with the body's TaskError, and the leave reports the TaskError naming the lane.
func TestFailedLaneChildCancelsItsBody(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid")).SetLimit(2)
	commit := c.AddStage(OptName("commit"))

	inMid := make(chan struct{})
	var siblingCause, leaveErr error
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx,
			lane.NewTask(func(cctx context.Context) error {
				if err := mid.MoveTo(cctx); err != nil {
					return err
				}
				<-inMid // the other child exists and is inside the lane
				return boom
			}),
			lane.NewTask(func(cctx context.Context) error {
				if err := mid.MoveTo(cctx); err != nil {
					return err
				}
				close(inMid)
				<-cctx.Done()
				siblingCause = context.Cause(cctx)
				return siblingCause
			}),
		); err != nil {
			return err
		}
		leaveErr = commit.MoveTo(ctx)
		return nil // the failure is joined and skipped
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want no failure", err)
	}
	var te TaskError
	if !errors.As(leaveErr, &te) || !errors.Is(leaveErr, boom) {
		t.Fatalf("leave error = %v, want a TaskError wrapping %v", leaveErr, boom)
	}
	if te.Unit() != lane {
		t.Errorf("TaskError.Unit = %v, want the lane", te.Unit())
	}
	if !errors.Is(siblingCause, boom) {
		t.Errorf("sibling child's context cause = %v, want the body's TaskError", siblingCause)
	}
}

// TestTasksStoppedByShutdownAreAnAbort: when the conveyor cancels an item, its tasks return because of that. The leave
// reports the ShutdownError, not a TaskError, and the item that returns it is an abort, not a failure of the drain:
// DrainError is the drain timeout itself.
func TestTasksStoppedByShutdownAreAnAbort(t *testing.T) {
	c := NewConveyor(OptDrainTimeout(0))
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	running := make(chan struct{})
	var leaveErr error
	cancelRun, done := runAsync(c, func(ic context.Context) error {
		if itemNo(ic) != 1 {
			<-ic.Done()
			return context.Cause(ic)
		}
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		if err := fo.Schedule(ic, pool.NewTask(func(tctx context.Context) error {
			close(running)
			<-tctx.Done()
			return tctx.Err() // a plain error, caused by the cancellation
		})); err != nil {
			return err
		}
		leaveErr = commit.MoveTo(ic)
		return leaveErr
	})
	<-running
	cancelRun(nil)
	err := recvErr(t, "Run", done)

	var se ShutdownError
	var te TaskError
	if !errors.As(leaveErr, &se) || errors.As(leaveErr, &te) {
		t.Fatalf("leave error = %v, want a ShutdownError and no TaskError", leaveErr)
	}
	if !errors.As(err, &se) {
		t.Fatalf("Run = %v, want a ShutdownError", err)
	}
	var ie ItemError
	if de := se.DrainError(); !errors.Is(de, context.DeadlineExceeded) || errors.As(de, &ie) {
		t.Fatalf("DrainError = %v, want the drain timeout, not an item failure", de)
	}
}
