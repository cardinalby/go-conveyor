package conveyor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// runErrorKinds reports which kinds of RunError err matches.
func runErrorKinds(err error) (shutdown, item bool) {
	var se ShutdownError
	var ie ItemError
	return errors.As(err, &se), errors.As(err, &ie)
}

// TestRunErrorItemFailure: a failed item makes Run return an ItemError (and not a ShutdownError) that unwraps to
// the item's error and names the node the item was in.
func TestRunErrorItemFailure(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	boom := errors.New("boom")

	err := runOnce(t, c, func(ic context.Context) error {
		if err := s.MoveTo(ic); err != nil {
			return err
		}
		return boom
	})

	shutdown, item := runErrorKinds(err)
	if shutdown || !item {
		t.Fatalf("Run = %v, want an ItemError only", err)
	}
	var ie ItemError
	errors.As(err, &ie)
	if ie.Unwrap() != boom || !errors.Is(err, boom) {
		t.Fatalf("Unwrap = %v, want the item error", ie.Unwrap())
	}
	if ie.Unit() != Unit(s) {
		t.Fatalf("Unit = %v, want %v", ie.Unit(), s)
	}
	if ie.DrainError() != nil {
		t.Fatalf("DrainError = %v, want nil", ie.DrainError())
	}
}

// TestRunErrorContextCancellation: cancelling the Run context makes Run return a ShutdownError that unwraps to the
// context's cause.
func TestRunErrorContextCancellation(t *testing.T) {
	c := NewConveyor()
	cause := errors.New("stop")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)

	err := c.Run(ctx, func(context.Context) error { return nil })

	shutdown, item := runErrorKinds(err)
	if !shutdown || item {
		t.Fatalf("Run = %v, want a ShutdownError only", err)
	}
	var se ShutdownError
	errors.As(err, &se)
	if se.Unwrap() != cause {
		t.Fatalf("Unwrap = %v, want the context cause", se.Unwrap())
	}
}

// TestRunErrorAlreadyRunningIsPlain: ErrConveyorAlreadyRunning is returned as is, not as a RunError.
func TestRunErrorAlreadyRunningIsPlain(t *testing.T) {
	c := NewConveyor()
	started := make(chan struct{})
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	var once sync.Once
	go func() {
		done <- c.Run(ctx, func(ic context.Context) error {
			once.Do(func() { close(started) })
			<-release
			return nil
		})
	}()
	<-started
	err := c.Run(ctx, func(context.Context) error { return nil })
	if !errors.Is(err, ErrConveyorAlreadyRunning) {
		t.Fatalf("second Run = %v, want ErrConveyorAlreadyRunning", err)
	}
	if shutdown, item := runErrorKinds(err); shutdown || item {
		t.Fatalf("second Run returned a RunError: %v", err)
	}
	cancel()
	close(release)
	<-done
}

// TestRunErrorFirstEventWins: when the Run context is canceled first, a later failure of an item in flight does not
// replace the ShutdownError; it is the DrainError, an ItemError with the item's unit and error.
func TestRunErrorFirstEventWins(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	boom := errors.New("boom")
	cause := errors.New("stop")

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	err := c.Run(ctx, func(ic context.Context) error {
		if err := s.MoveTo(ic); err != nil {
			return err
		}
		cancel(cause)
		// wait until Run has seen the cancellation: the shutdown begins, item creation stops
		for c.(*conveyor).currentRun.Load() != nil && !runStopped(c) {
			time.Sleep(time.Millisecond)
		}
		return boom
	})

	shutdown, item := runErrorKinds(err)
	if !shutdown || item {
		t.Fatalf("Run = %v, want a ShutdownError only", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("Run = %v, want it to unwrap to the context cause", err)
	}
	if errors.Is(err, boom) {
		t.Fatalf("Unwrap chain reaches the later failure: %v", err)
	}
	var re RunError
	errors.As(err, &re)
	var ie ItemError
	if !errors.As(re.DrainError(), &ie) || ie.Unwrap() != boom || ie.Unit() != Unit(s) {
		t.Fatalf("DrainError = %v, want an ItemError with %v in %v", re.DrainError(), boom, s)
	}
	if want := "conveyor is shutting down: stop (drain: boom)"; err.Error() != want {
		t.Fatalf("Error() = %q, want %q", err.Error(), want)
	}
}

// runStopped reports whether the active run has begun shutting down.
func runStopped(c Conveyor) bool {
	r := c.(*conveyor).currentRun.Load()
	if r == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopCreating
}

// TestRunErrorItemsSeeTheTrigger: after an item failure, aborted items get a ShutdownError that unwraps to the same
// ItemError Run returns.
func TestRunErrorItemsSeeTheTrigger(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	boom := errors.New("boom")
	secondReady := make(chan struct{})
	seen := make(chan error, 1)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		switch no {
		case 1:
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			<-secondReady
			return boom
		case 2:
			close(secondReady)
			<-ic.Done()
			seen <- context.Cause(ic)
		}
		return nil
	})

	cause := <-seen
	var se ShutdownError
	if !errors.As(cause, &se) {
		t.Fatalf("item cause = %v, want a ShutdownError", cause)
	}
	var ie ItemError
	if !errors.As(se.Unwrap(), &ie) {
		t.Fatalf("ShutdownError.Unwrap() = %v, want an ItemError", se.Unwrap())
	}
	var runIe ItemError
	if !errors.As(err, &runIe) || runIe.Unwrap() != ie.Unwrap() || runIe.Unit() != ie.Unit() {
		t.Fatalf("Run = %v, want the ItemError the item saw (%v)", err, ie)
	}
	if se.DrainError() != nil {
		t.Fatalf("item-side ShutdownError has run-level data: %v", se.DrainError())
	}
}

// TestRunErrorLateFailureDoesNotReplaceTriggerForLaterItems: when the Run context is canceled first, a later item
// failure does not change the cause of the items after it. They still see the Run context's cause.
func TestRunErrorLateFailureDoesNotReplaceTriggerForLaterItems(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	w := c.AddStage(OptName("w"))
	boom := errors.New("boom")
	stop := errors.New("stop")
	firstInW := make(chan struct{})
	secondInS := make(chan struct{})
	kept := make(chan error, 1)

	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			if err := w.MoveTo(ic); err != nil {
				return err
			}
			signal(firstInW)
			for !shutdownBegun(c) {
				time.Sleep(time.Millisecond)
			}
			return boom // fails after the Run context was canceled
		case 2:
			<-firstInW
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			signal(secondInS) // only the boom cascade or the drain timeout can cancel it
			<-ic.Done()
			kept <- context.Cause(ic)
		}
		return nil
	})
	<-secondInS
	cancel(stop)
	err := recvErr(t, "Run", done)

	if shutdown, item := runErrorKinds(err); !shutdown || item {
		t.Fatalf("Run = %v, want a ShutdownError only", err)
	}
	cause := <-kept
	if !errors.Is(cause, stop) || errors.Is(cause, boom) {
		t.Fatalf("later item cause = %v, want the Run context's cause %v, not the late failure", cause, stop)
	}
}

// TestRunErrorItemSideCauseIsNotFilledByRun: the ItemError an aborted item keeps as its cancellation cause stays free
// of run-level data (DrainError) after Run fills it in the error it returns.
func TestRunErrorItemSideCauseIsNotFilledByRun(t *testing.T) {
	boom := errors.New("boom")
	late := errors.New("late")
	drainCause := errors.New("drain timed out")
	c := NewConveyor(OptDrainContextFunc(func(error) (context.Context, context.CancelFunc) {
		return context.WithTimeoutCause(context.Background(), 20*time.Millisecond, drainCause)
	}))
	s := c.AddStage(OptName("s"))
	secondReady := make(chan struct{})
	kept := make(chan error, 1)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		switch no {
		case 1:
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			<-secondReady
			return boom
		case 2:
			close(secondReady)
			<-ic.Done() // canceled by item 1's failure
			kept <- context.Cause(ic)
			// Outlive the drain timeout so the drain fails. late is an abort: the conveyor canceled the item.
			time.Sleep(50 * time.Millisecond)
			return late
		}
		return nil
	})

	var runIe ItemError
	if !errors.As(err, &runIe) {
		t.Fatalf("Run = %v, want an ItemError", err)
	}
	if !errors.Is(runIe.DrainError(), drainCause) {
		t.Fatalf("Run error: DrainError = %v, want the drain cause", runIe.DrainError())
	}
	var ie ItemError
	if !errors.As(<-kept, &ie) {
		t.Fatal("kept cause has no ItemError")
	}
	if ie.DrainError() != nil {
		t.Fatalf("kept ItemError changed after Run: DrainError = %v", ie.DrainError())
	}
}

// TestRunErrorDrainTimeout: when the drain context expires before the items finish, DrainError reports its cause.
func TestRunErrorDrainTimeout(t *testing.T) {
	drainCause := errors.New("drain timed out")
	c := NewConveyor(OptDrainContextFunc(func(error) (context.Context, context.CancelFunc) {
		return context.WithTimeoutCause(context.Background(), 20*time.Millisecond, drainCause)
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.Run(ctx, func(ic context.Context) error {
		cancel()
		<-ic.Done() // only the expiring drain context cancels this item
		return context.Cause(ic)
	})

	var re RunError
	if !errors.As(err, &re) {
		t.Fatalf("Run = %v, want a RunError", err)
	}
	if !errors.Is(re.DrainError(), drainCause) {
		t.Fatalf("DrainError = %v, want %v", re.DrainError(), drainCause)
	}
}

// TestRunErrorNoDrainErrorWhenItemsFinish: items that finish inside the drain timeout leave DrainError nil.
func TestRunErrorNoDrainErrorWhenItemsFinish(t *testing.T) {
	c := NewConveyor(OptDrainTimeout(testTimeout))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := c.Run(ctx, func(context.Context) error {
		cancel()
		return nil
	})

	var re RunError
	if !errors.As(err, &re) {
		t.Fatalf("Run = %v, want a RunError", err)
	}
	if re.DrainError() != nil {
		t.Fatalf("DrainError = %v, want nil", re.DrainError())
	}
}

// TestAbortedItemReturningAnyErrorIsNotAFailure: an item the conveyor canceled is aborted, not failed, whatever it
// returns: a bare context.Canceled (e.g. from an interrupted driver call) or an error of its own.
func TestAbortedItemReturningAnyErrorIsNotAFailure(t *testing.T) {
	for _, ret := range []error{context.Canceled, errors.New("driver: connection closed")} {
		t.Run(ret.Error(), func(t *testing.T) {
			boom := errors.New("boom")
			c := NewConveyor()
			s := c.AddStage(OptName("s")).SetLimit(2)
			inS, waiting := make(chan struct{}), make(chan struct{})
			returned := make(chan error, 1)

			_, done := runAsync(c, func(ic context.Context) error {
				switch itemNo(ic) {
				case 1:
					if err := s.MoveTo(ic); err != nil {
						return err
					}
					signal(inS)
					<-waiting
					return boom // cancels item 2, the younger one
				case 2:
					<-inS
					if err := s.MoveTo(ic); err != nil {
						return err
					}
					signal(waiting)
					<-ic.Done()
					returned <- context.Cause(ic)
					return ret
				}
				<-ic.Done()
				return context.Cause(ic)
			})
			err := recvErr(t, "Run", done)

			assertShutdownCause(t, "item 2", recvErr(t, "item 2", returned), boom)
			if ie := itemFailure(t, err); ie.Unwrap() != boom || ie.DrainError() != nil {
				t.Fatalf("Run = %v, DrainError = %v, want %v and no drain error", err, ie.DrainError(), boom)
			}
		})
	}
}

// TestPlainCancelAfterTaskFailureStaysAFailure: when the item's context was canceled by its own failed RetainFor, a
// returned context.Canceled does not turn into an abort: the item fails with what the processor returned.
func TestPlainCancelAfterTaskFailureStaysAFailure(t *testing.T) {
	c := NewConveyor()
	boom := errors.New("boom")

	err := runOnce(t, c, func(ic context.Context) error {
		c.StartingStage().RetainFor(ic, func() error { return boom })
		<-ic.Done()
		return context.Canceled
	})

	if shutdown, item := runErrorKinds(err); shutdown || !item {
		t.Fatalf("Run = %v, want an ItemError", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want the processor's context.Canceled", err)
	}
}

// itemFailure returns the ItemError of a Run that failed with one.
func itemFailure(t *testing.T, err error) ItemError {
	t.Helper()
	var ie ItemError
	if !errors.As(err, &ie) {
		t.Fatalf("Run = %v, want an ItemError", err)
	}
	return ie
}

// runFirstItem runs proc for the first item only and returns Run's result. Unlike runOnce it does not cancel the Run
// context itself, so the item's outcome (a failure found while it joins its background work) is the first event.
func runFirstItem(t *testing.T, c Conveyor, proc ItemProcessor) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	return c.Run(ctx, func(ic context.Context) error {
		if itemNo(ic) != 1 {
			<-ic.Done()
			return nil
		}
		return proc(ic)
	})
}

// TestItemErrorUnitLastStage: an item failing in its last stage is reported in that stage.
func TestItemErrorUnitLastStage(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))
	boom := errors.New("boom")

	err := runOnce(t, c, func(ic context.Context) error {
		if err := a.MoveTo(ic); err != nil {
			return err
		}
		if err := b.MoveTo(ic); err != nil {
			return err
		}
		return boom
	})

	if u := itemFailure(t, err).Unit(); u != Unit(b) {
		t.Fatalf("Unit = %v, want %v", u, b)
	}
}

// TestItemErrorUnitBeforeAnyMove: an item failing before its first MoveTo is in the starting stage.
func TestItemErrorUnitBeforeAnyMove(t *testing.T) {
	c := NewConveyor()
	c.AddStage(OptName("a"))
	boom := errors.New("boom")

	err := runOnce(t, c, func(context.Context) error { return boom })

	if u := itemFailure(t, err).Unit(); u != Unit(c.StartingStage()) {
		t.Fatalf("Unit = %v, want the starting stage", u)
	}
}

// TestItemErrorUnitBlockedWithoutQueue: an item whose MoveTo fails while it waits with no waiting room stays in
// the node it was in.
func TestItemErrorUnitBlockedWithoutQueue(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))
	holding := make(chan struct{})
	release := make(chan struct{})
	waitErr := errors.New("gave up waiting")

	cancel, done := runAsync(c, func(ic context.Context) error {
		if itemNo(ic) > 2 {
			return nil
		}
		if err := a.MoveTo(ic); err != nil {
			return err
		}
		if itemNo(ic) == 1 {
			if err := b.MoveTo(ic); err != nil {
				return err
			}
			signal(holding)
			<-release
			return nil
		}
		<-holding
		sub, subCancel := context.WithCancelCause(ic)
		defer subCancel(nil)
		go func() {
			waitFor(t, "item 2 blocked", func() bool { return parkedOf(c) > 0 })
			subCancel(waitErr)
		}()
		return b.MoveTo(sub)
	})
	defer cancel(nil)

	waitFor(t, "failure", func() bool { return shutdownBegun(c) })
	close(release)
	err := recvErr(t, "Run", done)

	if !errors.Is(err, waitErr) {
		t.Fatalf("Run = %v, want the wait error", err)
	}
	if u := itemFailure(t, err).Unit(); u != Unit(a) {
		t.Fatalf("Unit = %v, want %v (the item never left it)", u, a)
	}
}

// TestItemErrorUnitWaitingInQueue: an item that failed while waiting in the queue of b is reported in b, the node it
// waits in front of.
func TestItemErrorUnitWaitingInQueue(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b")).SetQueueSize(1)
	holding := make(chan struct{})
	release := make(chan struct{})
	waitErr := errors.New("gave up waiting")

	cancel, done := runAsync(c, func(ic context.Context) error {
		if itemNo(ic) > 2 {
			return nil
		}
		if err := a.MoveTo(ic); err != nil {
			return err
		}
		if itemNo(ic) == 1 {
			if err := b.MoveTo(ic); err != nil {
				return err
			}
			signal(holding)
			<-release
			return nil
		}
		<-holding
		sub, subCancel := context.WithCancelCause(ic)
		defer subCancel(nil)
		go func() {
			waitFor(t, "item 2 queued", func() bool { return queueOccupancy(c, b) == 1 })
			subCancel(waitErr)
		}()
		return b.MoveTo(sub)
	})
	defer cancel(nil)

	waitFor(t, "failure", func() bool { return shutdownBegun(c) })
	close(release)
	err := recvErr(t, "Run", done)

	if !errors.Is(err, waitErr) {
		t.Fatalf("Run = %v, want the wait error", err)
	}
	if u := itemFailure(t, err).Unit(); u != Unit(b) {
		t.Fatalf("Unit = %v, want %v (the node the item waits in front of)", u, b)
	}
}

// TestItemErrorUnitTaskFailsAfterMovingOn: a RetainFor task of a fails after the item moved on to b.
func TestItemErrorUnitTaskFailsAfterMovingOn(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))
	gate := make(chan struct{})
	boom := errors.New("boom")

	err := runFirstItem(t, c, func(ic context.Context) error {
		if err := a.MoveTo(ic); err != nil {
			return err
		}
		a.RetainFor(ic, func() error {
			<-gate
			return boom
		})
		if err := b.MoveTo(ic); err != nil {
			return err
		}
		close(gate)
		return nil
	})

	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the task error", err)
	}
	// The item had moved on to b when its task failed; it is where the item was, not where the work began.
	if u := itemFailure(t, err).Unit(); u != Unit(b) {
		t.Fatalf("Unit = %v, want %v", u, b)
	}
}

// TestItemErrorUnitFanOutTaskFailure: a task of a fan-out failing while the item is in the fan-out is reported in
// the fan-out, not in one of its branches.
func TestItemErrorUnitFanOutTaskFailure(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	boom := errors.New("boom")

	err := runOnce(t, c, func(ic context.Context) error {
		if err := a.MoveTo(ic); err != nil {
			return err
		}
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		if err := fo.Schedule(ic, pool.NewTask(func(context.Context) error { return boom })); err != nil {
			return err
		}
		return fo.Wait(ic)
	})

	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the task error", err)
	}
	if u := itemFailure(t, err).Unit(); u != Unit(fo) {
		t.Fatalf("Unit = %v, want %v", u, fo)
	}
}

// TestItemErrorUnitFanOutRetainedTaskFailsAfterMovingOn: same as the stage case, for FanOut.Retain: the item is in
// b when the task it left running in fo fails.
func TestItemErrorUnitFanOutRetainedTaskFailsAfterMovingOn(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	b := c.AddStage(OptName("b"))
	gate := make(chan struct{})
	boom := errors.New("boom")

	err := runFirstItem(t, c, func(ic context.Context) error {
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		if err := fo.Schedule(ic, pool.NewTask(func(context.Context) error {
			<-gate
			return boom
		})); err != nil {
			return err
		}
		fo.Retain(ic)
		if err := b.MoveTo(ic); err != nil {
			return err
		}
		close(gate)
		return nil
	})

	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the task error", err)
	}
	if u := itemFailure(t, err).Unit(); u != Unit(b) {
		t.Fatalf("Unit = %v, want %v", u, b)
	}
}

// drainErrOf reports the drain error the live run has recorded so far.
func drainErrOf(c Conveyor) error {
	r := implOf(c).currentRun.Load()
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.drainErr
}

// drainByTest returns a drain context option that is done when the test calls the returned func, with that cause.
func drainByTest(t *testing.T) (Option, context.CancelCauseFunc) {
	ctx, end := context.WithCancelCause(context.Background())
	t.Cleanup(func() { end(nil) })
	return OptDrainContextFunc(func(error) (context.Context, context.CancelFunc) { return ctx, nil }), end
}

// TestRunErrorDrainFailureBeforeDrainTimeout: the first event of the drain wins: an item failure before the drain times
// out stays the DrainError after the drain times out and cancels the remaining item.
func TestRunErrorDrainFailureBeforeDrainTimeout(t *testing.T) {
	boom := errors.New("boom")
	stop := errors.New("stop")
	errDrain := errors.New("drain timed out")
	opt, endDrain := drainByTest(t)
	c := NewConveyor(opt)
	s := c.AddStage(OptName("s")).SetLimit(2)
	inS, secondInS, fail := make(chan struct{}), make(chan struct{}), make(chan struct{})
	olderCause := make(chan error, 1)

	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			signal(inS)
			<-ic.Done() // only the drain timeout cancels it: it is older than the failed item
			olderCause <- context.Cause(ic)
			return context.Cause(ic)
		case 2:
			<-inS
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			signal(secondInS)
			<-fail
			return boom
		}
		<-ic.Done()
		return context.Cause(ic)
	})
	<-secondInS
	cancel(stop)
	waitFor(t, "shutdown to begin", func() bool { return shutdownBegun(c) })
	close(fail)
	waitFor(t, "the failure to be recorded", func() bool { return drainErrOf(c) != nil })
	endDrain(errDrain)
	assertShutdownCause(t, "older item", recvErr(t, "older item", olderCause), stop)
	err := recvErr(t, "Run", done)

	assertShutdownCause(t, "Run", err, stop)
	var ie ItemError
	if de := err.(RunError).DrainError(); !errors.As(de, &ie) || ie.Unwrap() != boom || ie.Unit() != Unit(s) {
		t.Fatalf("DrainError = %v, want an ItemError with %v in %v", de, boom, s)
	}
}

// TestRunErrorDrainTimeoutBeforeDrainFailure: the first event of the drain wins: once the drain timed out with an
// item in flight, DrainError is its cause even if that item then fails for real (its own RetainFor canceled it
// earlier, so the conveyor did not abort it).
func TestRunErrorDrainTimeoutBeforeDrainFailure(t *testing.T) {
	boom := errors.New("boom")
	stop := errors.New("stop")
	errDrain := errors.New("drain timed out")
	opt, endDrain := drainByTest(t)
	c := NewConveyor(opt)
	ready, failRetain, canceled, proceed := make(chan struct{}), make(chan struct{}), make(chan struct{}),
		make(chan struct{})

	cancel, done := runAsync(c, func(ic context.Context) error {
		if itemNo(ic) != 1 {
			<-ic.Done()
			return context.Cause(ic)
		}
		c.StartingStage().RetainFor(ic, func() error {
			<-failRetain
			return boom
		})
		signal(ready)
		<-ic.Done()
		signal(canceled)
		<-proceed
		return context.Cause(ic) // boom: a real failure
	})
	<-ready
	cancel(stop)
	waitFor(t, "shutdown to begin", func() bool { return shutdownBegun(c) })
	close(failRetain)
	<-canceled
	endDrain(errDrain)
	waitFor(t, "the drain timeout to be recorded", func() bool { return drainErrOf(c) != nil })
	close(proceed)
	err := recvErr(t, "Run", done)

	assertShutdownCause(t, "Run", err, stop)
	if de := err.(RunError).DrainError(); de != errDrain {
		t.Fatalf("DrainError = %v, want %v", de, errDrain)
	}
}

// TestRunErrorDrainFailureAfterItemTrigger: after an item failure began the shutdown, the failure of an older item
// still in flight is the DrainError; the trigger is not repeated, and a younger item canceled by the conveyor is
// aborted whatever it returns. Error() shows the drain.
func TestRunErrorDrainFailureAfterItemTrigger(t *testing.T) {
	boom := errors.New("boom")
	second := errors.New("second")
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetLimit(3)
	firstInS, secondInS, thirdInS := make(chan struct{}), make(chan struct{}), make(chan struct{})

	_, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			signal(firstInS)
			// Fail once items 2 and 3 have left s: the trigger and the abort are recorded first.
			<-thirdInS
			if !awaitTrue(func() bool { return occupancyOf(c, s) == 1 }) {
				return errors.New("items 2 and 3 never left s")
			}
			return second
		case 2:
			<-firstInS
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			signal(secondInS)
			<-thirdInS
			return boom
		case 3:
			<-secondInS
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			signal(thirdInS)
			<-ic.Done()                         // canceled by item 2's failure
			return errors.New("third: aborted") // an abort all the same
		}
		<-ic.Done()
		return context.Cause(ic)
	})
	err := recvErr(t, "Run", done)

	ie := itemFailure(t, err)
	if ie.Unwrap() != boom {
		t.Fatalf("Run = %v, want the trigger %v", err, boom)
	}
	var de ItemError
	if !errors.As(ie.DrainError(), &de) || de.Unwrap() != second || de.Unit() != Unit(s) {
		t.Fatalf("DrainError = %v, want an ItemError with %v in %v", ie.DrainError(), second, s)
	}
	if want := "boom (drain: second)"; err.Error() != want {
		t.Fatalf("Error() = %q, want %q", err.Error(), want)
	}
}

// TestRunErrorDrainFailureBeforeJoin: an item failure after the trigger is the DrainError as soon as the item returns,
// even if its own background work is still running and the drain times out while completion waits for it.
func TestRunErrorDrainFailureBeforeJoin(t *testing.T) {
	boom := errors.New("boom")
	stop := errors.New("stop")
	opt, endDrain := drainByTest(t)
	c := NewConveyor(opt)
	ready, release := make(chan struct{}), make(chan struct{})
	var first atomic.Pointer[item]

	cancel, done := runAsync(c, func(ic context.Context) error {
		if itemNo(ic) != 1 {
			<-ic.Done()
			return context.Cause(ic)
		}
		first.Store(itemOf(ic))
		c.StartingStage().RetainFor(ic, func() error {
			<-release // ignores the cancellation: the completion of the item waits for it
			return nil
		})
		signal(ready)
		<-UntilShutdown(ic).Done()
		return boom
	})
	<-ready
	cancel(stop)
	r := first.Load().run
	waitFor(t, "the item to return", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return first.Load().returned
	})
	endDrain(errors.New("drain timed out"))
	waitFor(t, "the drain timeout to be seen", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.drainErr != nil && r.itemsCtx.Err() != nil
	})
	close(release)
	err := recvErr(t, "Run", done)

	assertShutdownCause(t, "Run", err, stop)
	var ie ItemError
	if de := err.(RunError).DrainError(); !errors.As(de, &ie) || ie.Unwrap() != boom {
		t.Fatalf("DrainError = %v, want an ItemError with %v", de, boom)
	}
}
