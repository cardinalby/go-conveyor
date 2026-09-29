package conveyor

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// runAsync runs c on its own goroutine and returns the cancel of its Run context and a channel with Run's result.
func runAsync(c Conveyor, proc ItemProcessor) (context.CancelCauseFunc, <-chan error) {
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() {
		tctx, tcancel := context.WithTimeout(ctx, testTimeout)
		defer tcancel()
		done <- c.Run(tctx, proc)
	}()
	return cancel, done
}

// recvErr receives one error from ch, failing the test on timeout.
func recvErr(t *testing.T, what string, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// shutdownBegun reports whether the live run has begun its shutdown.
func shutdownBegun(c Conveyor) bool {
	r := implOf(c).currentRun.Load()
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopCreating
}

// signal closes ch; used by items to report that they reached a place.
func signal(ch chan struct{}) { close(ch) }

func itemNo(ctx context.Context) int64 {
	no, _ := ItemNoFromContext(ctx)
	return no
}

// TestNoAbortPointCancelsItemsBeforeIt: on Run ctx cancel an item blocked in the starting stage is canceled at
// once, while an item in the point finishes normally.
func TestNoAbortPointCancelsItemsBeforeIt(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))
	c.SetNoAbortPoint(write)
	if c.NoAbortPoint() != write {
		t.Fatalf("NoAbortPoint = %v, want write", c.NoAbortPoint())
	}

	inWrite, inStart := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	startErr := make(chan error, 1)
	var committed atomic.Bool
	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := write.MoveTo(ic); err != nil {
				return err
			}
			signal(inWrite)
			<-release
			if ic.Err() != nil {
				return context.Cause(ic)
			}
			if err := commit.MoveTo(ic); err != nil {
				return err
			}
			committed.Store(true)
			return nil
		case 2:
			signal(inStart)
			<-ic.Done()
			startErr <- context.Cause(ic)
			return context.Cause(ic)
		}
		return errors.New("unexpected item")
	})
	<-inWrite
	<-inStart
	cancel(cause)
	assertShutdownCause(t, "item in the starting stage", recvErr(t, "start item cancel", startErr), cause)
	close(release) // item 1 is still in write: it was not canceled

	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	if !committed.Load() {
		t.Fatal("item in the no-abort point did not commit")
	}
}

// TestNoAbortPointCancelsWaitingForIt: an item in the point's waiting room and an item blocked in MoveTo(point)
// have not entered it, so both are canceled.
func TestNoAbortPointCancelsWaitingForIt(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	write := c.AddStage(OptName("write")).SetQueueSize(1)
	c.SetNoAbortPoint(write)

	inWrite := make(chan struct{})
	release := make(chan struct{})
	errs := make(chan error, 2)
	var firstErr atomic.Value
	cancel, done := runAsync(c, func(ic context.Context) error {
		no := itemNo(ic)
		if no > 3 {
			return errors.New("unexpected item")
		}
		err := write.MoveTo(ic)
		if no == 1 {
			if err != nil {
				firstErr.Store(err)
				return err
			}
			signal(inWrite)
			<-release
			if ic.Err() != nil {
				firstErr.Store(context.Cause(ic))
			}
			return nil
		}
		errs <- err
		return err
	})
	<-inWrite
	waitFor(t, "item 2 in the waiting room and item 3 blocked", func() bool {
		return queueOccupancy(c, write) == 1 && parkedOf(c) == 2
	})
	cancel(cause)
	for i := 0; i < 2; i++ {
		assertShutdownCause(t, "item before write", recvErr(t, "canceled item", errs), cause)
	}
	close(release)
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	if e := firstErr.Load(); e != nil {
		t.Fatalf("item in write was canceled: %v", e)
	}
}

// TestNoAbortPointSkippedIsPassed: an item that skipped the point and entered a later node is not canceled.
func TestNoAbortPointSkippedIsPassed(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))
	c.SetNoAbortPoint(write)

	inCommit, inStart := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	startErr := make(chan error, 1)
	var laterErr atomic.Value
	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := commit.MoveTo(ic); err != nil {
				laterErr.Store(err)
				return err
			}
			signal(inCommit)
			<-release
			if ic.Err() != nil {
				laterErr.Store(context.Cause(ic))
			}
			return nil
		case 2:
			signal(inStart)
			<-ic.Done()
			startErr <- context.Cause(ic)
			return context.Cause(ic)
		}
		return errors.New("unexpected item")
	})
	<-inCommit
	<-inStart
	cancel(cause)
	assertShutdownCause(t, "item in the starting stage", recvErr(t, "start item cancel", startErr), cause)
	close(release)
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	if e := laterErr.Load(); e != nil {
		t.Fatalf("item that skipped the point was canceled: %v", e)
	}
}

// TestNoAbortPointFanOut: with a fan-out as the point, items in the stage before it and in the starting stage
// are canceled, an item inside it keeps its work running.
func TestNoAbortPointFanOut(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	pre := c.AddStage(OptName("pre"))
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	c.SetNoAbortPoint(fo)
	if c.NoAbortPoint() != fo {
		t.Fatalf("NoAbortPoint = %v, want fo", c.NoAbortPoint())
	}

	taskStarted := make(chan struct{})
	inPre, inStart := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	errs := make(chan error, 2)
	var taskErr atomic.Value
	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := pre.MoveTo(ic); err != nil {
				return err
			}
			if err := fo.MoveTo(ic); err != nil {
				return err
			}
			return fo.Schedule(ic, pool.NewTasks(1, func(cctx context.Context, _ int) error {
				signal(taskStarted)
				<-release
				if cctx.Err() != nil {
					taskErr.Store(context.Cause(cctx))
				}
				return nil
			}))
		case 2:
			if err := pre.MoveTo(ic); err != nil {
				return err
			}
			signal(inPre)
		case 3:
			signal(inStart)
		default:
			return errors.New("unexpected item")
		}
		<-ic.Done()
		errs <- context.Cause(ic)
		return context.Cause(ic)
	})
	<-taskStarted
	<-inPre
	<-inStart
	cancel(cause)
	for i := 0; i < 2; i++ {
		assertShutdownCause(t, "item before fo", recvErr(t, "canceled item", errs), cause)
	}
	close(release)
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	if e := taskErr.Load(); e != nil {
		t.Fatalf("work of the item inside the point was canceled: %v", e)
	}
}

// TestNoAbortPointAfterFanOut: an item inside a fan-out before the point is canceled: its running task sees the
// cancel and its queued task never runs.
func TestNoAbortPointAfterFanOut(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // limit 1: the second task stays queued
	write := c.AddStage(OptName("write"))
	c.SetNoAbortPoint(write)

	taskStarted, inStart := make(chan struct{}), make(chan struct{})
	var ran [2]atomic.Bool
	runningErr := make(chan error, 1)
	moveErr := make(chan error, 1)
	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := fo.MoveTo(ic); err != nil {
				return err
			}
			if err := fo.Schedule(ic, pool.NewTasks(2, func(cctx context.Context, i int) error {
				ran[i].Store(true)
				if i == 0 {
					signal(taskStarted)
					<-cctx.Done()
					runningErr <- context.Cause(cctx)
					return context.Cause(cctx)
				}
				return nil
			})); err != nil {
				return err
			}
			err := write.MoveTo(ic)
			moveErr <- err
			return err
		case 2:
			signal(inStart)
			<-ic.Done()
			return context.Cause(ic)
		}
		return errors.New("unexpected item")
	})
	<-taskStarted
	<-inStart
	cancel(cause)
	assertShutdownCause(t, "running task", recvErr(t, "running task cancel", runningErr), cause)
	if err := recvErr(t, "MoveTo(write)", moveErr); !isShutdown(err) {
		t.Fatalf("MoveTo(write) = %v, want a ShutdownError", err)
	}
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	if ran[1].Load() {
		t.Fatal("queued task of a canceled item ran")
	}
}

// TestNoAbortPointErrorShutdown: on an item error, an older item still before the point is canceled too, with
// the error as cause; an item past the point finishes.
func TestNoAbortPointErrorShutdown(t *testing.T) {
	errBoom := errors.New("boom")
	c := NewConveyor()
	pre := c.AddStage(OptName("pre")).SetLimit(2)
	write := c.AddStage(OptName("write"))
	c.SetNoAbortPoint(write)

	inWrite, inPre, inStart := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	errs := make(chan error, 2)
	var firstErr atomic.Value
	var committed atomic.Bool
	_, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := write.MoveTo(ic); err != nil {
				firstErr.Store(err)
				return err
			}
			signal(inWrite)
			<-release
			if ic.Err() != nil {
				firstErr.Store(context.Cause(ic))
				return nil
			}
			committed.Store(true)
			return nil
		case 2:
			<-inWrite // keep item 3 (and so the error) until item 1 is in write
			if err := pre.MoveTo(ic); err != nil {
				return err
			}
			signal(inPre)
		case 3:
			if err := pre.MoveTo(ic); err != nil { // enters only after item 2 has
				return err
			}
			<-inStart // item 4 exists before the error stops item creation
			return errBoom
		case 4:
			signal(inStart)
		default:
			return errors.New("unexpected item")
		}
		<-ic.Done()
		errs <- context.Cause(ic)
		return context.Cause(ic)
	})
	<-inPre
	for i := 0; i < 2; i++ { // item 2 (older, cut) and item 4 (younger, error cascade)
		assertShutdownCause(t, "item before write", recvErr(t, "canceled item", errs), errBoom)
	}
	close(release)
	if err := recvErr(t, "Run", done); err != errBoom {
		t.Fatalf("Run error = %v, want %v", err, errBoom)
	}
	if e := firstErr.Load(); e != nil {
		t.Fatalf("item in write was canceled: %v", e)
	}
	if !committed.Load() {
		t.Fatal("item in write did not finish")
	}
}

// startAndWrite is the shape shared by the tests below: item 1 in write until release, item 2 in the starting
// stage until its ctx is done or release2 closes. It reports item 2's cancel cause (nil if not canceled).
func startAndWrite(write Stage, inWrite, inStart, release, release2 chan struct{}, startErr chan<- error) ItemProcessor {
	return func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := write.MoveTo(ic); err != nil {
				return err
			}
			signal(inWrite)
			<-release
			return context.Cause(ic)
		case 2:
			signal(inStart)
			select {
			case <-ic.Done():
			case <-release2:
			}
			startErr <- context.Cause(ic)
			return nil
		}
		return errors.New("unexpected item")
	}
}

// TestNoAbortPointSetDuringShutdown: once shutdown has begun, setting the point aborts nothing: items created while
// the point was the starting stage have entered it.
func TestNoAbortPointSetDuringShutdown(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	writeAborted, startAborted := moveAndCancel(t, c, write, true, func() { c.SetNoAbortPoint(write) })
	if writeAborted || startAborted {
		t.Fatalf("aborted: item in write %v, item in the starting stage %v; want none", writeAborted, startAborted)
	}
}

// TestNoAbortPointReset: the starting stage restores the default: nothing is aborted at shutdown.
func TestNoAbortPointReset(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	if p := c.NoAbortPoint(); p != c.StartingStage() {
		t.Fatalf("default NoAbortPoint = %v, want the starting stage", p)
	}
	write := c.AddStage(OptName("write"))
	c.SetNoAbortPoint(write).SetNoAbortPoint(c.StartingStage())
	if p := c.NoAbortPoint(); p != c.StartingStage() {
		t.Fatalf("NoAbortPoint = %v, want the starting stage", p)
	}

	inWrite, inStart := make(chan struct{}), make(chan struct{})
	release, release2 := make(chan struct{}), make(chan struct{})
	startErr := make(chan error, 1)
	cancel, done := runAsync(c, startAndWrite(write, inWrite, inStart, release, release2, startErr))
	<-inWrite
	<-inStart
	cancel(cause)
	waitFor(t, "shutdown to begin", func() bool { return shutdownBegun(c) })
	close(release2)
	if err := recvErr(t, "start item", startErr); err != nil {
		t.Fatalf("item in the starting stage was aborted: %v", err)
	}
	close(release)
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
}

// TestNoAbortPointWithShutdownContext: items before the point are canceled at once, items past it only when the
// shutdown context is done.
func TestNoAbortPointWithShutdownContext(t *testing.T) {
	cause := errors.New("stop")
	graceCtx, endGrace := context.WithCancel(context.Background())
	defer endGrace()
	var graceOver atomic.Bool
	c := NewConveyor(OptShutdownContext(func(error) (context.Context, context.CancelFunc) {
		return graceCtx, nil
	}))
	write := c.AddStage(OptName("write"))
	c.SetNoAbortPoint(write)

	inWrite, inStart := make(chan struct{}), make(chan struct{})
	startErr, writeErr := make(chan error, 1), make(chan error, 1)
	var earlyCancel atomic.Bool
	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := write.MoveTo(ic); err != nil {
				return err
			}
			signal(inWrite)
			<-ic.Done()
			if !graceOver.Load() {
				earlyCancel.Store(true)
			}
			writeErr <- context.Cause(ic)
			return context.Cause(ic)
		case 2:
			signal(inStart)
			<-ic.Done()
			startErr <- context.Cause(ic)
			return context.Cause(ic)
		}
		return errors.New("unexpected item")
	})
	<-inWrite
	<-inStart
	cancel(cause)
	assertShutdownCause(t, "item in the starting stage", recvErr(t, "start item cancel", startErr), cause)
	graceOver.Store(true)
	endGrace()
	assertShutdownCause(t, "item in write", recvErr(t, "write item cancel", writeErr), cause)
	if earlyCancel.Load() {
		t.Fatal("item past the point was canceled before the shutdown context was done")
	}
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
}

// TestNoAbortPointPanics: only a stage or a fan-out of the conveyor's own series can be the point.
func TestNoAbortPointPanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	lane := fo.AddLane(OptName("lane"))
	laneStage := lane.AddStage(OptName("lane stage"))
	laneFanOut := lane.AddFanOut(OptName("lane fo"))
	foreign := NewConveyor().AddStage(OptName("foreign"))

	cases := []struct {
		name string
		node Unit
		want error
	}{
		{"nil", nil, errInvalidUnit},
		{"pool", pool, errInvalidUnit},
		{"lane", lane, errInvalidUnit},
		{"stage inside a lane", laneStage, errWrongScope},
		{"fan-out inside a lane", laneFanOut, errWrongScope},
		{"foreign conveyor's stage", foreign, errInvalidUnit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertPanics(t, tc.want, func() { c.SetNoAbortPoint(tc.node) })
			if p := c.NoAbortPoint(); p != c.StartingStage() {
				t.Fatalf("NoAbortPoint = %v after a refused set, want the starting stage", p)
			}
		})
	}
}

// moveAndCancel runs item 1 into write and item 2 in the starting stage, calls move before (or, if duringShutdown,
// after) canceling Run, and reports whether each item was aborted.
func moveAndCancel(t *testing.T, c Conveyor, write Stage, duringShutdown bool, move func()) (writeAborted, startAborted bool) {
	t.Helper()
	cause := errors.New("stop")
	inWrite, inStart := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	writeErr, startErr := make(chan error, 1), make(chan error, 1)
	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := write.MoveTo(ic); err != nil {
				return err
			}
			signal(inWrite)
			<-release
			writeErr <- context.Cause(ic)
			return nil
		case 2:
			signal(inStart)
			select {
			case <-ic.Done():
			case <-release:
			}
			startErr <- context.Cause(ic)
			return nil
		}
		return errors.New("unexpected item")
	})
	<-inWrite
	<-inStart
	if !duringShutdown {
		move()
	}
	cancel(cause)
	waitFor(t, "shutdown to begin", func() bool { return shutdownBegun(c) })
	if duringShutdown {
		move()
	}
	// Aborts happen under the run's lock at shutdown begin (and in move), so by now each item's ctx tells.
	close(release)
	writeAborted = recvErr(t, "write item", writeErr) != nil
	startAborted = recvErr(t, "start item", startErr) != nil
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
	return writeAborted, startAborted
}

// TestNoAbortPointMovedLater: an item that entered the old point stays protected; an item before it is aborted.
func TestNoAbortPointMovedLater(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))
	c.SetNoAbortPoint(write)
	writeAborted, startAborted := moveAndCancel(t, c, write, false, func() { c.SetNoAbortPoint(commit) })
	if writeAborted {
		t.Fatal("item that entered the old point was aborted")
	}
	if !startAborted {
		t.Fatal("item in the starting stage was not aborted")
	}
}

// TestNoAbortPointMovedEarlier: an item that has already entered the new point is protected at once.
func TestNoAbortPointMovedEarlier(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))
	c.SetNoAbortPoint(commit)
	writeAborted, startAborted := moveAndCancel(t, c, write, false, func() { c.SetNoAbortPoint(write) })
	if writeAborted {
		t.Fatal("item in the new point was aborted")
	}
	if !startAborted {
		t.Fatal("item in the starting stage was not aborted")
	}
}

// TestNoAbortPointResetDuringRun: moving the point to the starting stage protects every live item.
func TestNoAbortPointResetDuringRun(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))
	c.SetNoAbortPoint(commit)
	writeAborted, startAborted := moveAndCancel(t, c, write, false, func() { c.SetNoAbortPoint(c.StartingStage()) })
	if writeAborted || startAborted {
		t.Fatalf("aborted: item in write %v, item in the starting stage %v; want none", writeAborted, startAborted)
	}
}

// TestNoAbortPointSetFromDefaultDuringRun: items created while the point was the starting stage have entered it,
// so a point set later does not abort them.
func TestNoAbortPointSetFromDefaultDuringRun(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))
	writeAborted, startAborted := moveAndCancel(t, c, write, false, func() { c.SetNoAbortPoint(commit) })
	if writeAborted || startAborted {
		t.Fatalf("aborted: item in write %v, item in the starting stage %v; want none", writeAborted, startAborted)
	}
}

// --- races and edge cases ---

// TestNoAbortPointEnterRacesShutdown: an item enters the point while shutdown begins. Either MoveTo returns a
// ShutdownError (the item was aborted before it entered), or it returns nil and the item is never canceled.
func TestNoAbortPointEnterRacesShutdown(t *testing.T) {
	for i := 0; i < 200; i++ {
		c := NewConveyor()
		write := c.AddStage(OptName("write"))
		c.SetNoAbortPoint(write)
		cause := errors.New("stop")
		ready := make(chan struct{})
		moveErr, ctxErr := make(chan error, 1), make(chan error, 1)
		cancel, done := runAsync(c, func(ic context.Context) error {
			if itemNo(ic) != 1 {
				<-ic.Done()
				return context.Cause(ic)
			}
			signal(ready)
			err := write.MoveTo(ic)
			moveErr <- err
			if err == nil {
				waitFor(t, "shutdown to begin", func() bool { return shutdownBegun(c) })
			}
			ctxErr <- context.Cause(ic)
			return err
		})
		<-ready
		cancel(cause)
		mErr := recvErr(t, "MoveTo", moveErr)
		cErr := recvErr(t, "item ctx", ctxErr)
		switch {
		case mErr == nil && cErr != nil:
			t.Fatalf("iteration %d: item entered the point but was canceled: %v", i, cErr)
		case mErr != nil && !isShutdown(mErr):
			t.Fatalf("iteration %d: MoveTo = %v, want nil or a ShutdownError", i, mErr)
		}
		if err := recvErr(t, "Run", done); err != cause {
			t.Fatalf("iteration %d: Run error = %v, want %v", i, err, cause)
		}
	}
}

// TestNoAbortPointSetRacesEnter: the point is moved to write while an item enters write. Whatever the order, the item
// ends up protected.
func TestNoAbortPointSetRacesEnter(t *testing.T) {
	for i := 0; i < 200; i++ {
		c := NewConveyor()
		write := c.AddStage(OptName("write"))
		commit := c.AddStage(OptName("commit"))
		c.SetNoAbortPoint(commit)
		cause := errors.New("stop")
		ready, inWrite := make(chan struct{}), make(chan struct{})
		release := make(chan struct{})
		writeErr := make(chan error, 1)
		cancel, done := runAsync(c, func(ic context.Context) error {
			if itemNo(ic) != 1 {
				<-ic.Done()
				return context.Cause(ic)
			}
			signal(ready)
			if err := write.MoveTo(ic); err != nil {
				return err
			}
			signal(inWrite)
			<-release
			writeErr <- context.Cause(ic)
			return nil
		})
		<-ready
		c.SetNoAbortPoint(write)
		<-inWrite
		cancel(cause)
		waitFor(t, "shutdown to begin", func() bool { return shutdownBegun(c) })
		close(release)
		if err := recvErr(t, "write item", writeErr); err != nil {
			t.Fatalf("iteration %d: item in the point was aborted: %v", i, err)
		}
		if err := recvErr(t, "Run", done); err != cause {
			t.Fatalf("iteration %d: Run error = %v, want %v", i, err, cause)
		}
	}
}

// TestNoAbortPointTryMoveTo: a TryMoveTo that enters the point protects the item; one that is declined does not.
func TestNoAbortPointTryMoveTo(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	c.SetNoAbortPoint(write)

	inWrite, declined := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	writeErr, declinedErr := make(chan error, 1), make(chan error, 1)
	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if ok, err := write.TryMoveTo(ic); !ok || err != nil {
				return errors.New("item 1 did not enter write")
			}
			signal(inWrite)
			<-release
			writeErr <- context.Cause(ic)
			return nil
		case 2:
			if ok, err := write.TryMoveTo(ic); ok || err != nil { // write is full
				return errors.New("item 2 entered write")
			}
			signal(declined)
			<-ic.Done()
			declinedErr <- context.Cause(ic)
			return context.Cause(ic)
		}
		return errors.New("unexpected item")
	})
	<-inWrite
	<-declined
	cancel(cause)
	assertShutdownCause(t, "declined item", recvErr(t, "declined item", declinedErr), cause)
	close(release)
	if err := recvErr(t, "write item", writeErr); err != nil {
		t.Fatalf("item that entered with TryMoveTo was aborted: %v", err)
	}
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
}

// TestNoAbortPointLaneChild: a child item inside a lane of a fan-out before the point is aborted with its parent.
func TestNoAbortPointLaneChild(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	laneStage := lane.AddStage(OptName("lane stage"))
	write := c.AddStage(OptName("write"))
	c.SetNoAbortPoint(write)

	inLane := make(chan struct{})
	childErr := make(chan error, 1)
	cancel, done := runAsync(c, func(ic context.Context) error {
		if itemNo(ic) != 1 {
			<-ic.Done()
			return context.Cause(ic)
		}
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		if err := fo.Schedule(ic, lane.NewTask(func(cctx context.Context) error {
			if err := laneStage.MoveTo(cctx); err != nil {
				return err
			}
			signal(inLane)
			<-cctx.Done()
			childErr <- context.Cause(cctx)
			return context.Cause(cctx)
		})); err != nil {
			return err
		}
		return write.MoveTo(ic)
	})
	<-inLane
	cancel(cause)
	assertShutdownCause(t, "child item", recvErr(t, "child item", childErr), cause)
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
}

// TestNoAbortPointRetain: background work handed off by Stage.Retain before the point lives with its item: it keeps
// running if the item entered the point, and is canceled if the item is aborted.
func TestNoAbortPointRetain(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	pre := c.AddStage(OptName("pre")).SetLimit(2)
	write := c.AddStage(OptName("write"))
	c.SetNoAbortPoint(write)

	started := make(chan int, 2)
	inWrite, inPre := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	bgErr := [2]chan error{make(chan error, 1), make(chan error, 1)}
	cancel, done := runAsync(c, func(ic context.Context) error {
		no := itemNo(ic)
		if no > 2 {
			<-ic.Done()
			return context.Cause(ic)
		}
		if err := pre.MoveTo(ic); err != nil {
			return err
		}
		pre.Retain(ic, func() error {
			started <- int(no)
			select {
			case <-ic.Done():
			case <-release:
			}
			bgErr[no-1] <- context.Cause(ic)
			return nil
		})
		if no == 2 {
			signal(inPre)
			<-ic.Done()
			return context.Cause(ic)
		}
		if err := write.MoveTo(ic); err != nil {
			return err
		}
		signal(inWrite)
		<-release
		return nil
	})
	<-inWrite
	<-inPre
	<-started
	<-started
	cancel(cause)
	assertShutdownCause(t, "work of the aborted item", recvErr(t, "item 2 work", bgErr[1]), cause)
	close(release)
	if err := recvErr(t, "item 1 work", bgErr[0]); err != nil {
		t.Fatalf("work of the protected item was canceled: %v", err)
	}
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
}

// TestNoAbortPointKeptAcrossRuns: the point stays set for the next Run, and items of a new run start unprotected.
func TestNoAbortPointKeptAcrossRuns(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	c.SetNoAbortPoint(write)
	for run := 0; run < 2; run++ {
		writeAborted, startAborted := moveAndCancel(t, c, write, false, func() {})
		if writeAborted || !startAborted {
			t.Fatalf("run %d: aborted: item in write %v, item in the starting stage %v; want only the start one",
				run, writeAborted, startAborted)
		}
	}
	if c.NoAbortPoint() != write {
		t.Fatalf("NoAbortPoint = %v, want write", c.NoAbortPoint())
	}
}

// --- randomized ---

// checkNoAbortMarks checks the marks of the live root items under the run's lock:
//   - marked items are a prefix of the root list (among items not canceled), so the abort walk from the tail is
//     complete;
//   - every live item that has reached the current point is marked;
//   - once shutdown has begun, every live item is marked or canceled.
func checkNoAbortMarks(r *run) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rank := r.noAbortRankLocked()
	unmarkedSeen := false
	for it := r.scopes[0].head; it != nil; it = it.next {
		canceled := it.ctx.Err() != nil
		switch {
		case it.noAbort && unmarkedSeen:
			return fmt.Errorf("item %d is marked but an older live item is not", it.no)
		case !it.noAbort && !canceled:
			unmarkedSeen = true
			if it.reachedRank >= rank {
				return fmt.Errorf("item %d reached rank %d, point rank %d, but is not marked", it.no, it.reachedRank, rank)
			}
			if r.stopCreating {
				return fmt.Errorf("item %d is neither marked nor canceled after shutdown began", it.no)
			}
		}
	}
	return nil
}

// TestPropertyNoAbortPointJitter runs random topologies while another goroutine moves the no-abort point between
// random root nodes (the starting stage and commit included), cancels Run at a random moment and checks the marks
// throughout. Commit order, capacity and the absence of leaks must hold as usual.
func TestPropertyNoAbortPointJitter(t *testing.T) {
	var aborted atomic.Int64 // over all seeds: the scenarios must abort some items to test anything
	defer func() {
		if aborted.Load() == 0 && !t.Failed() {
			t.Error("no item was ever aborted; the scenarios tested nothing")
		}
	}()
	for i := 0; i < propSeeds; i++ {
		seed := int64(13_000 + i*17)
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rnd := rand.New(rand.NewSource(seed))
			top := buildPropTopology(rnd, seed)
			points := []Unit{top.c.StartingStage(), top.commit}
			for _, nd := range top.nodes {
				if nd.stage != nil {
					points = append(points, nd.stage.st)
				} else {
					points = append(points, nd.fanOut.fo)
				}
			}
			top.c.SetNoAbortPoint(points[rnd.Intn(len(points))])
			cancelAt := int64(3 + rnd.Intn(20))

			base := runtime.NumGoroutine()
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()
			stop := make(chan struct{})
			var checkErr atomic.Value
			check := func() {
				if r := implOf(top.c).currentRun.Load(); r != nil {
					if err := checkNoAbortMarks(r); err != nil {
						checkErr.CompareAndSwap(nil, err)
					}
				}
			}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				jr := rand.New(rand.NewSource(seed * 31))
				for {
					select {
					case <-stop:
						return
					default:
					}
					top.c.SetNoAbortPoint(points[jr.Intn(len(points))])
					check()
					time.Sleep(time.Duration(jr.Intn(100)) * time.Microsecond)
				}
			}()

			err := top.c.Run(ctx, func(ic context.Context) error {
				no, _ := ItemNoFromContext(ic)
				if no == cancelAt {
					go cancel()
				}
				check()
				err := top.process(ic, no, nil)
				if isShutdown(err) {
					aborted.Add(1)
				}
				check()
				return err
			})
			close(stop)
			wg.Wait()

			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("seed %d: Run = %v, want the run context's cause", seed, err)
			}
			if e := checkErr.Load(); e != nil {
				t.Fatalf("seed %d: %v", seed, e)
			}
			top.assertCommitOrder(t)
			top.assertCapacity(t)
			top.assertBranchOrder(t)
			top.assertWavesFinished(t)
			top.assertNoLeaks(t, base)
		})
	}
}

// TestNoAbortPointLaterWaitingRoom: an item that skipped the point and waits in a later node's waiting room has
// passed it and is not aborted; a younger item still in the starting stage is.
func TestNoAbortPointLaterWaitingRoom(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit")).SetQueueSize(1)
	c.SetNoAbortPoint(write)

	inCommit, inStart := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	errs := [3]chan error{make(chan error, 1), make(chan error, 1), make(chan error, 1)}
	cancel, done := runAsync(c, func(ic context.Context) error {
		no := itemNo(ic)
		switch no {
		case 1:
			if err := commit.MoveTo(ic); err != nil {
				return err
			}
			signal(inCommit)
		case 2:
			<-inCommit
			if err := commit.MoveTo(ic); err != nil { // waits in commit's waiting room until item 1 leaves
				errs[1] <- err
				return err
			}
		case 3:
			signal(inStart)
			select {
			case <-ic.Done():
			case <-release:
			}
			errs[2] <- context.Cause(ic)
			return nil
		default:
			<-ic.Done()
			return context.Cause(ic)
		}
		<-release
		errs[no-1] <- context.Cause(ic)
		return nil
	})
	<-inStart
	waitFor(t, "item 2 in commit's waiting room", func() bool { return queueOccupancy(c, commit) == 1 })
	cancel(cause)
	waitFor(t, "shutdown to begin", func() bool { return shutdownBegun(c) })
	close(release)
	for i := 0; i < 2; i++ {
		if err := recvErr(t, fmt.Sprintf("item %d", i+1), errs[i]); err != nil {
			t.Fatalf("item %d was aborted: %v", i+1, err)
		}
	}
	assertShutdownCause(t, "item in the starting stage", recvErr(t, "item 3", errs[2]), cause)
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
}

// TestNoAbortPointSetRacesRunStart: the setter sees no run, then a run starts and its item enters the new point
// before the store. The setter must still protect the item.
func TestNoAbortPointSetRacesRunStart(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))
	c.SetNoAbortPoint(commit)

	inWrite, release := make(chan struct{}), make(chan struct{})
	writeErr := make(chan error, 1)
	var cancel context.CancelCauseFunc
	var done <-chan error
	implOf(c).noAbortStoreHook = func() {
		cancel, done = runAsync(c, func(ic context.Context) error {
			if itemNo(ic) != 1 {
				<-ic.Done()
				return context.Cause(ic)
			}
			if err := write.MoveTo(ic); err != nil {
				return err
			}
			signal(inWrite)
			<-release
			writeErr <- context.Cause(ic)
			return nil
		})
		<-inWrite // item 1 entered write while the point was still commit
	}
	c.SetNoAbortPoint(write)
	implOf(c).noAbortStoreHook = nil
	cancel(cause)
	waitFor(t, "shutdown to begin", func() bool { return shutdownBegun(c) })
	close(release)
	if err := recvErr(t, "write item", writeErr); err != nil {
		t.Fatalf("item in the point was aborted: %v", err)
	}
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
}

// TestNoAbortPointMovedDuringGracePeriod: with OptShutdownContext, moving the point during shutdown changes nothing:
// the aborted item stays aborted, and the protected one is canceled only when the shutdown context is done.
func TestNoAbortPointMovedDuringGracePeriod(t *testing.T) {
	cause := errors.New("stop")
	graceCtx, endGrace := context.WithCancel(context.Background())
	defer endGrace()
	var graceOver atomic.Bool
	c := NewConveyor(OptShutdownContext(func(error) (context.Context, context.CancelFunc) {
		return graceCtx, nil
	}))
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))
	c.SetNoAbortPoint(write)

	inWrite, inStart := make(chan struct{}), make(chan struct{})
	writeErr, startErr := make(chan error, 1), make(chan error, 1)
	var earlyCancel atomic.Bool
	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := write.MoveTo(ic); err != nil {
				return err
			}
			signal(inWrite)
			<-ic.Done()
			if !graceOver.Load() {
				earlyCancel.Store(true)
			}
			writeErr <- context.Cause(ic)
			return context.Cause(ic)
		case 2:
			signal(inStart)
			<-ic.Done()
			startErr <- context.Cause(ic)
			return context.Cause(ic)
		}
		return errors.New("unexpected item")
	})
	<-inWrite
	<-inStart
	cancel(cause)
	assertShutdownCause(t, "item in the starting stage", recvErr(t, "start item cancel", startErr), cause)
	for _, p := range []Unit{commit, c.StartingStage(), write, commit} {
		c.SetNoAbortPoint(p)
		if err := checkNoAbortMarks(implOf(c).currentRun.Load()); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-writeErr:
		t.Fatalf("protected item canceled during the grace period: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	graceOver.Store(true)
	endGrace()
	assertShutdownCause(t, "item in write", recvErr(t, "write item cancel", writeErr), cause)
	if earlyCancel.Load() {
		t.Fatal("protected item was canceled before the shutdown context was done")
	}
	if err := recvErr(t, "Run", done); err != cause {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
}
