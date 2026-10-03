package conveyor

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// awaitTrue polls cond until it holds and reports whether it did within testTimeout. Unlike waitFor it never
// touches *testing.T, so it is safe to call from a worker goroutine or an observer goroutine, which must report
// their failure through a value instead.
func awaitTrue(cond func() bool) bool {
	deadline := time.Now().Add(testTimeout)
	for {
		if cond() {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// assertShutdownCause fails unless err is (or wraps) a ShutdownError whose Cause is want.
func assertShutdownCause(t *testing.T, what string, err error, want error) {
	t.Helper()
	var se ShutdownError
	if !errors.As(err, &se) {
		t.Fatalf("%s: error %v is not a ShutdownError", what, err)
	}
	if !errors.Is(se.Unwrap(), want) {
		t.Fatalf("%s: shutdown cause = %v, want %v", what, se.Unwrap(), want)
	}
}

// TestGracefulShutdownFinishesInFlightItems: with no OptDrainTimeout, cancelling the Run context only stops
// item creation — every item already in flight finishes its whole journey, background work included.
func TestGracefulShutdownFinishesInFlightItems(t *testing.T) {
	const perItem = 2
	cause := errors.New("time to stop")
	c := NewConveyor() // nothing bounds the in-flight items
	gate := c.AddStage(OptName("gate")).SetLimit(4)
	fo := c.AddFanOut(OptName("fo")).SetLimit(4)
	pool := fo.AddPool(OptName("pool")).SetLimit(2)
	commit := c.AddStage(OptName("commit"))

	release := make(chan struct{})
	var created, bg atomic.Int64
	var triggered atomic.Bool
	var committed numbers

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		created.Add(1)
		if err := gate.MoveTo(ic); err != nil {
			return err
		}
		ferr := fo.MoveTo(ic)
		if ferr == nil {
			ferr = fo.Schedule(ic, pool.NewTasks(perItem, func(cctx context.Context, i int) error {
				<-release // deliberately ignores cctx: the unbounded default must let this work finish
				bg.Add(1)
				return nil
			}))
		}
		if ferr != nil {
			return ferr
		}
		// Decided by the item number, not by arrival order at this line: the fan-out admits items in order, so only
		// item 4 is sure to get in while items 1-3 hold their slots until the work is released.
		if no == 4 && triggered.CompareAndSwap(false, true) {
			cancel(cause) // shut down with four items past the gate and all of their work outstanding
			close(release)
		}
		if err := commit.MoveTo(ic); err != nil {
			return err
		}
		committed.add(no)
		return nil
	})

	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	got := committed.all()
	if int64(len(got)) != created.Load() {
		t.Fatalf("%d of %d created items committed: %v", len(got), created.Load(), got)
	}
	if len(got) < 4 {
		t.Fatalf("expected at least the four in-flight items to commit, got %v", got)
	}
	assertStrictlyIncreasing(t, got, "commit order across a graceful shutdown")
	if want := created.Load() * perItem; bg.Load() != want {
		t.Fatalf("%d pieces of background work finished, want %d", bg.Load(), want)
	}
}

// TestDrainContextAlreadyDoneCancelsInFlight: a drain context that is already done cancels every in-flight
// item at once, so blocked node calls and ctx-respecting pool work return promptly with the run cause as shutdown
// cause.
func TestDrainContextAlreadyDoneCancelsInFlight(t *testing.T) {
	cause := errors.New("stop now")
	c := NewConveyor(OptDrainTimeout(0))
	fo := c.AddFanOut(OptName("fo")).SetLimit(4)
	pool := fo.AddPool(OptName("pool")).SetLimit(2)
	commit := c.AddStage(OptName("commit")) // exclusive: the items behind block in MoveTo

	var laneStarted, laneUnwound atomic.Int64
	var mu sync.Mutex
	var observed []error
	record := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		observed = append(observed, err)
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		ferr := fo.MoveTo(ic)
		if ferr == nil {
			ferr = fo.Schedule(ic, pool.NewTasks(2, func(cctx context.Context, i int) error {
				laneStarted.Add(1)
				<-cctx.Done() // work that respects its context stops as soon as the item is canceled
				laneUnwound.Add(1)
				return context.Cause(cctx)
			}))
		}
		if ferr != nil {
			record(ferr)
			return ferr
		}
		if no == 1 {
			ok := awaitTrue(func() bool { return laneStarted.Load() >= 2 })
			cancel(cause)
			if !ok {
				return errors.New("timed out waiting for the pool work to start")
			}
		}
		if err := commit.MoveTo(ic); err != nil {
			record(err)
			return err
		}
		return nil
	})

	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	if laneStarted.Load() < 2 {
		t.Fatalf("pool work never started (%d pieces)", laneStarted.Load())
	}
	if got, want := laneUnwound.Load(), laneStarted.Load(); got != want {
		t.Fatalf("only %d of %d started pieces of pool work unwound", got, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(observed) == 0 {
		t.Fatal("no item observed the shutdown")
	}
	for _, e := range observed {
		assertShutdownCause(t, "canceled item", e, cause)
	}
}

// TestShutdownDrainTimeoutLetsItemsDrain: with a timer-backed drain context, items that drain inside the
// timeout are never canceled and complete their journey normally.
func TestShutdownDrainTimeoutLetsItemsDrain(t *testing.T) {
	cause := errors.New("time to stop")
	c := NewConveyor(OptDrainTimeout(5 * time.Second)) // generous: the items below drain at once
	gate := c.AddStage(OptName("gate")).SetLimit(4)
	commit := c.AddStage(OptName("commit"))

	release := make(chan struct{})
	var created, arrived, canceledEarly atomic.Int64
	var triggered atomic.Bool
	var committed numbers

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	// The items are held until the shutdown has begun, then all drain at once, well inside the drain timeout.
	go func() {
		<-ctx.Done()
		close(release)
	}()

	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		created.Add(1)
		if err := gate.MoveTo(ic); err != nil {
			return err
		}
		if arrived.Add(1) == 4 && triggered.CompareAndSwap(false, true) {
			cancel(cause)
		}
		select {
		case <-release:
		case <-ic.Done():
			canceledEarly.Add(1)
			return context.Cause(ic)
		}
		if err := commit.MoveTo(ic); err != nil {
			return err
		}
		committed.add(no)
		return nil
	})

	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	if canceledEarly.Load() != 0 {
		t.Fatalf("%d items were canceled inside the drain timeout", canceledEarly.Load())
	}
	got := committed.all()
	if int64(len(got)) != created.Load() {
		t.Fatalf("%d of %d created items committed: %v", len(got), created.Load(), got)
	}
	assertStrictlyIncreasing(t, got, "commit order after a graceful shutdown")
}

// TestShutdownDrainTimeoutOverrunCancelsItems: an item still in flight when the drain context expires has its
// context canceled with a ShutdownError carrying the Run context's cause — not the expiry.
func TestShutdownDrainTimeoutOverrunCancelsItems(t *testing.T) {
	cause := errors.New("time to stop")
	c := NewConveyor(OptDrainTimeout(20 * time.Millisecond)) // tiny: the item below overruns it
	hold := c.AddStage(OptName("hold"))                      // exclusive: the item behind blocks in MoveTo

	var mu sync.Mutex
	seen := map[int64]error{}
	record := func(no int64, err error) {
		mu.Lock()
		defer mu.Unlock()
		seen[no] = err
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if err := hold.MoveTo(ic); err != nil {
			record(no, err)
			return err
		}
		if no == 1 {
			cancel(cause)
			<-ic.Done() // nothing releases this item: only the expiring drain context does
			record(no, context.Cause(ic))
			return context.Cause(ic)
		}
		return nil
	})

	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	mu.Lock()
	defer mu.Unlock()
	if _, ok := seen[1]; !ok {
		t.Fatal("item 1 was not canceled when the drain timeout expired")
	}
	for no, e := range seen {
		assertShutdownCause(t, sprintf("item %d", no), e, cause)
	}
}

// TestErrorShutdownIsBoundedByDrainContext: an ItemProcessor error starts a shutdown without the Run context
// being canceled, so the drain context func is the caller's only notice of it — it is asked, it is told the item
// error as the cause, and the context it returns is what releases an earlier item that would otherwise never return.
func TestErrorShutdownIsBoundedByDrainContext(t *testing.T) {
	boom := errors.New("boom")
	askedCause := make(chan error, 1)
	c := NewConveyor(OptDrainContextFunc(func(cause error) (context.Context, context.CancelFunc) {
		askedCause <- cause
		shutdownCtx, cancel := context.WithCancel(context.Background())
		cancel() // at once: item 1 below never returns on its own
		return shutdownCtx, cancel
	}))
	gate := c.AddStage(OptName("gate")).SetLimit(2) // both items below are in flight at the same time

	var blocked atomic.Bool
	var item1Err atomic.Value
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel() // never canceled while the run is live: the error is the only shutdown trigger here
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if e := gate.MoveTo(ic); e != nil {
			return e // an item behind the failing one, canceled as a later item
		}
		switch no {
		case 1:
			blocked.Store(true)
			<-ic.Done() // nothing but the drain context releases this item
			item1Err.Store(context.Cause(ic))
			return context.Cause(ic)
		case 2:
			if !awaitTrue(blocked.Load) {
				return errors.New("timed out waiting for item 1 to block")
			}
			return boom // fails with an earlier item still in flight, which the shutdown must bound
		}
		return nil
	})

	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
	select {
	case got := <-askedCause:
		if got != boom {
			t.Fatalf("drain context func cause = %v, want the item error %v", got, boom)
		}
	default:
		t.Fatal("the drain context func was never asked, so nothing bounded the error shutdown")
	}
	ie, ok := item1Err.Load().(error)
	if !ok {
		t.Fatal("item 1 was never released")
	}
	assertShutdownCause(t, "item 1", ie, boom)
}

// TestDrainContextCancelsItemsLater: a drain context the caller cancels itself makes the shutdown two-phase.
// The items are left alone while it is live — even though the shutdown has begun — and canceled when it is done,
// with the cause that started the shutdown rather than the drain context's own.
func TestDrainContextCancelsItemsLater(t *testing.T) {
	cause := errors.New("time to stop")
	forceCtx, forceNow := context.WithCancelCause(context.Background())
	defer forceNow(nil)

	askedCause := make(chan error, 1)
	asked := make(chan struct{})
	c := NewConveyor(OptDrainContextFunc(func(c error) (context.Context, context.CancelFunc) {
		askedCause <- c
		close(asked)
		return forceCtx, nil // the caller owns it; no CancelFunc to hand over
	}))
	hold := c.AddStage(OptName("hold")) // exclusive: the item behind blocks in MoveTo

	var item1Err atomic.Value
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if e := hold.MoveTo(ic); e != nil {
			return e
		}
		if no != 1 {
			return nil
		}
		cancel(cause) // phase one: graceful — item 1 keeps running
		<-asked       // the conveyor has taken the drain context
		if ic.Err() != nil {
			return errors.New("the item was canceled while the drain context was still live")
		}
		forceNow(errors.New("kill now")) // phase two: forceful
		<-ic.Done()
		item1Err.Store(context.Cause(ic))
		return context.Cause(ic)
	})

	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	if got := <-askedCause; got != cause {
		t.Fatalf("drain context func cause = %v, want the Run context cause %v", got, cause)
	}
	ie, ok := item1Err.Load().(error)
	if !ok {
		t.Fatal("item 1 was never canceled by the drain context")
	}
	// The shutdown cause is the trigger, not the drain context's own cause ("kill now").
	assertShutdownCause(t, "item 1", ie, cause)
}

// TestDrainContextNilMeansNoLimit: a drain context func that returns no context leaves the in-flight items alone, exactly
// like configuring no drain context func at all.
func TestDrainContextNilMeansNoLimit(t *testing.T) {
	asked := make(chan struct{})
	c := NewConveyor(OptDrainContextFunc(func(error) (context.Context, context.CancelFunc) {
		close(asked)
		return nil, nil
	}))
	commit := c.AddStage(OptName("commit"))

	var committed atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if no != 1 {
			return nil
		}
		cancel()
		<-asked // the conveyor has asked for a bound and been given none
		// The item moves on after the shutdown has begun: nothing may have canceled it.
		if e := commit.MoveTo(ic); e != nil {
			return e
		}
		committed.Store(true)
		return nil
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want the Run context cancellation", err)
	}
	if !committed.Load() {
		t.Fatal("the item was canceled although the drain context func returned no drain context")
	}
}

// TestDrainContextCancelFuncCalledBeforeRunReturns: the conveyor releases the drain context it was handed —
// a timer-backed one must not outlive the run.
func TestDrainContextCancelFuncCalledBeforeRunReturns(t *testing.T) {
	asked := make(chan struct{})
	var released atomic.Bool
	c := NewConveyor(OptDrainContextFunc(func(error) (context.Context, context.CancelFunc) {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), testTimeout) // outlives the run
		close(asked)
		return shutdownCtx, func() {
			released.Store(true)
			cancel()
		}
	}))
	s := c.AddStage(OptName("s"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		if e := s.MoveTo(ic); e != nil {
			return e
		}
		cancel()
		<-asked // hold the run open until the conveyor has taken the drain context
		return nil
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want the Run context cancellation", err)
	}
	if !released.Load() {
		t.Fatal("the drain context's CancelFunc was not called before Run returned")
	}
}

// TestDrainTimeoutNoDrainErrorWhenNothingInFlight: a drain context that is done when no item is left in flight (here
// born done, after the only item has failed) is no overrun: DrainError is nil.
func TestDrainTimeoutNoDrainErrorWhenNothingInFlight(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor(OptDrainTimeout(0)).SetItemsLimit(1) // the failing item is the only one, and no later one is made
	err := c.Run(context.Background(), func(ic context.Context) error { return boom })
	var re RunError
	if !errors.As(err, &re) || !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want a RunError wrapping %v", err, boom)
	}
	if de := re.DrainError(); de != nil {
		t.Fatalf("DrainError = %v, want nil", de)
	}
}

// TestDrainTimeoutCountsFromShutdownStart: the drain context a run uses with OptDrainTimeout(d) ends d after
// the moment shutdown began (here, an item error), not after the moment the watcher asks for it.
func TestDrainTimeoutCountsFromShutdownStart(t *testing.T) {
	const d = time.Hour
	boom := errors.New("boom")
	c := NewConveyor(OptDrainTimeout(d)).SetItemsLimit(1)
	ci := implOf(c)
	drainFunc := ci.drainContext
	var deadline, askedAt time.Time
	var hasDeadline bool
	ci.drainContext = func(start time.Time, cause error) (context.Context, context.CancelFunc) {
		askedAt = time.Now()
		ctx, cancel := drainFunc(start, cause)
		deadline, hasDeadline = ctx.Deadline()
		return ctx, cancel
	}
	var r *run
	var failedAt time.Time
	err := c.Run(context.Background(), func(ic context.Context) error {
		r = itemOf(ic).run
		failedAt = time.Now()
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
	// Run joins the watcher, so its writes are visible here.
	if askedAt.IsZero() {
		t.Fatal("the drain context was never asked for")
	}
	r.mu.Lock()
	at := r.shutdownAt
	r.mu.Unlock()
	if at.Before(failedAt) || at.After(askedAt) {
		t.Fatalf("shutdownAt = %v, want between the item error at %v and the drain context request at %v",
			at, failedAt, askedAt)
	}
	if !hasDeadline || !deadline.Equal(at.Add(d)) {
		t.Fatalf("drain context deadline = %v (set: %v), want shutdownAt + %v = %v", deadline, hasDeadline, d, at.Add(d))
	}
}

// TestProcessorErrorCancelsOnlyLaterItems: an ItemProcessor error becomes Run's error, items created before it
// still commit, and items created after it are canceled with a ShutdownError whose Cause is that error.
func TestProcessorErrorCancelsOnlyLaterItems(t *testing.T) {
	const failing = 3
	const inGate = 5
	boom := errors.New("boom")
	c := NewConveyor()
	gate := c.AddStage(OptName("gate")).SetLimit(inGate)
	commit := c.AddStage(OptName("commit")) // exclusive: nobody may commit past the failing item

	release := make(chan struct{})
	var arrived atomic.Int64
	var triggered atomic.Bool
	var committed numbers
	var mu sync.Mutex
	aborted := map[int64]error{}
	record := func(no int64, err error) {
		mu.Lock()
		defer mu.Unlock()
		aborted[no] = err
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if err := gate.MoveTo(ic); err != nil {
			record(no, err)
			return err
		}
		// Hold every item in the gate until all five are inside, so the items after the failing one exist.
		if arrived.Add(1) == inGate && triggered.CompareAndSwap(false, true) {
			close(release)
		}
		select {
		case <-release:
		case <-ic.Done():
			record(no, context.Cause(ic))
			return context.Cause(ic)
		}
		if no == failing {
			return boom
		}
		if err := commit.MoveTo(ic); err != nil {
			record(no, err)
			return err
		}
		committed.add(no)
		return nil
	})

	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
	got := committed.all()
	if len(got) != failing-1 {
		t.Fatalf("committed %v, want exactly the items before the failing one", got)
	}
	assertStrictlyIncreasing(t, got, "commit order before the failure")
	mu.Lock()
	defer mu.Unlock()
	for _, no := range []int64{failing + 1, failing + 2} {
		e, ok := aborted[no]
		if !ok {
			t.Fatalf("item %d was neither canceled nor committed", no)
		}
		assertShutdownCause(t, sprintf("item %d", no), e, boom)
	}
}

// TestFirstItemErrorWins: when several items fail, Run reports the first error recorded, not the last.
func TestFirstItemErrorWins(t *testing.T) {
	first := errors.New("first")
	second := errors.New("second")
	c := NewConveyor()
	gate := c.AddStage(OptName("gate")).SetLimit(2)

	reached2 := make(chan struct{})
	var failedSecond atomic.Bool

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if err := gate.MoveTo(ic); err != nil {
			return err
		}
		switch no {
		case 1:
			<-reached2 // item 2 exists, so it is one of the "later" items item 1's failure cancels
			return first
		case 2:
			close(reached2)
			<-ic.Done() // canceled by item 1's failure, after that failure was recorded
			failedSecond.Store(true)
			return second // a real error of its own, which must not displace the first
		}
		return nil
	})

	if !errors.Is(err, first) {
		t.Fatalf("Run error = %v, want %v", err, first)
	}
	if !failedSecond.Load() {
		t.Fatal("item 2 never reported its own failure, so the test proved nothing")
	}
}

// TestFailureStartsShutdownBeforeBackgroundWorkEnds: a failed item begins the shutdown as soon as its processor
// returns, while its retained work is still running: later items are canceled, the drain context is asked for, and a
// later failure during that work cannot take the first one's place.
func TestFailureStartsShutdownBeforeBackgroundWorkEnds(t *testing.T) {
	first := errors.New("first")
	second := errors.New("second")
	var drainAsked atomic.Bool
	c := NewConveyor(OptDrainContextFunc(func(error) (context.Context, context.CancelFunc) {
		drainAsked.Store(true)
		return nil, nil // no limit: the retained work is left to finish
	}))
	gate := c.AddStage(OptName("gate")).SetLimit(2)

	reached2 := make(chan struct{})
	bgRelease := make(chan struct{})
	var bgDone atomic.Bool

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if err := gate.MoveTo(ic); err != nil {
			return err
		}
		switch no {
		case 1:
			gate.RetainFor(ic, func(context.Context) error {
				<-bgRelease // ignores the item's cancellation: still running while item 1 is joined
				bgDone.Store(true)
				return nil
			})
			<-reached2
			return first
		case 2:
			close(reached2)
			waitFor(t, "item 2 to be canceled while item 1's retained work runs", func() bool { return ic.Err() != nil })
			waitFor(t, "the drain context to be asked for", drainAsked.Load)
			if bgDone.Load() {
				t.Error("the retained work finished before the checks")
			}
			if !shutdownBegun(c) {
				t.Error("the shutdown has not begun")
			}
			close(bgRelease)
			return second // a real error of its own, which must not displace the first
		}
		return nil
	})

	if !errors.Is(err, first) {
		t.Fatalf("Run error = %v, want %v", err, first)
	}
	var re RunError
	if errors.As(err, &re) && re.DrainError() != nil {
		t.Errorf("DrainError = %v, want nil (item 2 was canceled, so its error is an abort)", re.DrainError())
	}
}

// TestShutdownErrorFromProcessorIsNotAFailure: a processor that returns (even wrapped in its own chain) the
// ShutdownError a node call handed it does not make Run fail — Run still reports the Run context's cause.
func TestShutdownErrorFromProcessorIsNotAFailure(t *testing.T) {
	cause := errors.New("stop now")
	c := NewConveyor(OptDrainTimeout(0))
	hold := c.AddStage(OptName("hold")) // exclusive: the item behind blocks in MoveTo

	var wrapped atomic.Int64

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if err := hold.MoveTo(ic); err != nil {
			wrapped.Add(1)
			return fmt.Errorf("item %d aborted: %w", no, err)
		}
		if no == 1 {
			cancel(cause)
			<-ic.Done()
			wrapped.Add(1)
			return fmt.Errorf("item %d aborted: %w", no, context.Cause(ic))
		}
		return nil
	})

	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v, want the run context cause %v", err, cause)
	}
	var se ShutdownError
	if !errors.As(err, &se) {
		t.Fatalf("Run returned %v, want a ShutdownError", err)
	}
	// Item 1 outlives the drain timeout, so the drain reports its end, not a failure: the aborted items are not
	// failures.
	if de := se.DrainError(); !errors.Is(de, context.DeadlineExceeded) {
		t.Fatalf("DrainError = %v, want %v", de, context.DeadlineExceeded)
	}
	if wrapped.Load() == 0 {
		t.Fatal("no item returned a wrapped ShutdownError, so the test proved nothing")
	}
}

// TestGracefulShutdownJoinsChildren: a shutdown does not abandon a lane's children — completing the parent still
// joins them, and with nothing bounding them they run to the end.
func TestGracefulShutdownJoinsChildren(t *testing.T) {
	const children = 4
	cause := errors.New("time to stop")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	inner := lane.AddStage(OptName("inner")).SetLimit(children)

	release := make(chan struct{})
	var started, finished atomic.Int64

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	go func() {
		<-ctx.Done()
		close(release)
	}()

	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if no != 1 {
			<-ctx.Done() // hold the start stage so no further items are created
			return nil
		}
		ferr := fo.MoveTo(ic)
		if ferr == nil {
			ferr = fo.Schedule(ic, lane.NewTasks(children, func(cctx context.Context, i int) error {
				if err := inner.MoveTo(cctx); err != nil {
					return err
				}
				started.Add(1)
				<-release // deliberately ignores cctx: the unbounded default must let this child finish
				finished.Add(1)
				return nil
			}))
		}
		if ferr != nil {
			return ferr
		}
		ok := awaitTrue(func() bool { return started.Load() == children })
		cancel(cause) // shut down with every child in flight
		if !ok {
			return errors.New("timed out waiting for the children to enter the lane")
		}
		return nil // return without joining: completing the item must join them anyway
	})

	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	if finished.Load() != children {
		t.Fatalf("%d of %d children finished before Run returned", finished.Load(), children)
	}
}

// TestShutdownCancelsChildren: when the drain context is done at once, the in-flight children of a lane are
// canceled with the shutdown cause, they unwind, and Run waits for all of them.
func TestShutdownCancelsChildren(t *testing.T) {
	const children = 4
	cause := errors.New("stop now")
	c := NewConveyor(OptDrainTimeout(0))
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	inner := lane.AddStage(OptName("inner")).SetLimit(children)
	commit := c.AddStage(OptName("commit"))

	var started, unwound atomic.Int64
	var childErr, joinErr atomic.Value

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if no != 1 {
			<-ctx.Done() // hold the start stage so no further items are created
			return nil
		}
		ferr := fo.MoveTo(ic)
		if ferr == nil {
			ferr = fo.Schedule(ic, lane.NewTasks(children, func(cctx context.Context, i int) error {
				if err := inner.MoveTo(cctx); err != nil {
					return err
				}
				started.Add(1)
				<-cctx.Done()
				unwound.Add(1)
				childErr.Store(context.Cause(cctx))
				return context.Cause(cctx)
			}))
		}
		if ferr != nil {
			return ferr
		}
		ok := awaitTrue(func() bool { return started.Load() == children })
		cancel(cause)
		if !ok {
			return errors.New("timed out waiting for the children to enter the lane")
		}
		if e := commit.MoveTo(ic); e != nil {
			joinErr.Store(e)
			return e
		}
		return nil
	})

	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	if unwound.Load() != children {
		t.Fatalf("%d of %d children unwound before Run returned", unwound.Load(), children)
	}
	ce, ok := childErr.Load().(error)
	if !ok {
		t.Fatal("no child observed its cancellation cause")
	}
	assertShutdownCause(t, "canceled child", ce, cause)
	je, ok := joinErr.Load().(error)
	if !ok {
		t.Fatal("the parent's commit did not surface the shutdown")
	}
	assertShutdownCause(t, "parent commit", je, cause)
}

// TestRunReusableAfterShutdown: a second Run starts from fresh state — item numbering restarts and no occupancy
// leaks — and Stats is the zero value once a run has returned.
func TestRunReusableAfterShutdown(t *testing.T) {
	const want = 6
	const perItem = 2
	c := NewConveyor()
	a := c.AddStage(OptName("a")).SetLimit(2)
	fo := c.AddFanOut(OptName("fo")).SetLimit(2)
	pool := fo.AddPool(OptName("pool")).SetLimit(2)
	commit := c.AddStage(OptName("commit"))

	assertIdle := func(round int) {
		s := c.Stats()
		if s.Units != nil || s.InFlight != (Gauge{}) || s.LiveWorkers != (Gauge{}) {
			t.Fatalf("round %d: Stats after the run = %+v, want the zero value", round, s)
		}
	}
	assertIdle(0)

	for round := 1; round <= 2; round++ {
		var committed numbers
		var bg atomic.Int64
		runNOK(t, c, want, func(ctx context.Context, no int64) error {
			if err := a.MoveTo(ctx); err != nil {
				return err
			}
			err := fo.MoveTo(ctx)
			if err == nil {
				err = fo.Schedule(ctx, pool.NewTasks(perItem, func(cctx context.Context, i int) error {
					bg.Add(1)
					return nil
				}))
			}
			if err != nil {
				return err
			}
			if err := commit.MoveTo(ctx); err != nil {
				return err
			}
			committed.add(no)
			return nil
		})

		got := committed.all()
		if len(got) != want {
			t.Fatalf("round %d: %d items committed, want %d: %v", round, len(got), want, got)
		}
		assertStrictlyIncreasing(t, got, sprintf("round %d: item numbering", round))
		if bg.Load() != int64(want*perItem) {
			t.Fatalf("round %d: %d pieces of pool work ran, want %d", round, bg.Load(), want*perItem)
		}
		assertIdle(round)
	}
}

// TestRunLeavesNoGoroutines: a full run over a fan-out's pools and a lane's child items unwinds every goroutine it started.
func TestRunLeavesNoGoroutines(t *testing.T) {
	const want = 40
	c := NewConveyor()
	a := c.AddStage(OptName("a")).SetLimit(2)
	fo := c.AddFanOut(OptName("fo")).SetLimit(2)
	plain := fo.AddPool(OptName("plain")).SetLimit(3) // work that cannot travel
	kids := fo.AddLane(OptName("kids"))               // work that becomes child items
	inner := kids.AddStage(OptName("inner")).SetLimit(2)
	commit := c.AddStage(OptName("commit"))

	before := runtime.NumGoroutine()
	var done atomic.Int64
	runNOK(t, c, want, func(ctx context.Context, no int64) error {
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx,
				plain.NewTasks(3, func(cctx context.Context, i int) error { return nil }),
				kids.NewTasks(2, func(cctx context.Context, i int) error { return inner.MoveTo(cctx) }),
			)
		}
		if err != nil {
			return err
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		done.Add(1)
		return nil
	})

	if done.Load() != want {
		t.Fatalf("%d items completed, want %d", done.Load(), want)
	}
	waitFor(t, "the run's goroutines to settle", func() bool { return runtime.NumGoroutine() <= before+2 })
}
