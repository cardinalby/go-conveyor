package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestFanOutWorkIsJoinedOnTheWayOut: the tasks are the node's body, so the move that leaves the fan-out returns only
// once every one of them has finished — with no wave named anywhere.
func TestFanOutWorkIsJoinedOnTheWayOut(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")).SetLimit(3)
	commit := c.AddStage(OptName("commit"))

	var events recorder
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx, Tasks{pool.NewTasks(3, func(_ context.Context, i int) error {
			time.Sleep(10 * time.Millisecond)
			events.add("task-%d", i)
			return nil
		})}); err != nil {
			return err
		}
		events.add("scheduled")
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		events.add("commit")
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}

	got := events.all()
	if len(got) != 5 || got[0] != "scheduled" || got[4] != "commit" {
		t.Fatalf("events = %v, want scheduled first, three tasks, then commit", got)
	}
}

// TestFanOutOverlapsLocalWorkWithItsTasks: MoveTo returns once the tasks are scheduled, so the code that follows runs
// alongside them. That is the window the design buys by joining on the way out rather than on the way in.
func TestFanOutOverlapsLocalWorkWithItsTasks(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	taskRunning := make(chan struct{})
	localDone := make(chan struct{})
	var overlapped atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error {
			close(taskRunning)
			<-localDone // the item must be able to make progress while this task is still running
			return nil
		})}); err != nil {
			return err
		}
		<-taskRunning
		overlapped.Store(true) // reached with the task still in flight
		close(localDone)
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !overlapped.Load() {
		t.Fatalf("the item never ran alongside its own task")
	}
}

// TestFanOutWaitingRoomStartsNoWork: a waiting room in front of a fan-out is a waiting room like any other — an item
// standing in it has released the previous node but scheduled nothing, so the node's limit still bounds the work in
// flight. With limit 1 and two items queued, exactly one task may be running at any time.
func TestFanOutWaitingRoomStartsNoWork(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(1).SetQueueSize(2)
	pool := fo.AddPool(OptName("pool")).SetLimit(4) // never the constraint
	commit := c.AddStage(OptName("commit"))

	var live, peak atomic.Int64
	release := make(chan struct{})
	var closed atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		// Two items in the waiting room, one inside: the moment to look at how much work exists.
		waitFor(t, "the fan-out's waiting room to fill", func() bool {
			return queueOccupancy(c, fo) == 2 && occupancyOf(c, fo) == 1
		})
		if got := live.Load(); got != 1 {
			t.Errorf("%d tasks in flight with two items queued, want 1 — a waiting room schedules nothing", got)
		}
		closed.Store(true)
		close(release)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		if err := fo.MoveTo(ic, Tasks{pool.NewTask(func(tctx context.Context) error {
			n := live.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			defer live.Add(-1)
			if !closed.Load() {
				select {
				case <-release:
				case <-tctx.Done():
				}
			}
			return nil
		})}); err != nil {
			return err
		}
		return commit.MoveTo(ic)
	})

	if got := peak.Load(); got > 1 {
		t.Fatalf("peak tasks in flight = %d, want 1 — the limit bounds work, the queue only holds items", got)
	}
}

// TestFanOutToFanOutJoinsTheFirst: entering another fan-out is a way out of this one too, so the first node's work is
// joined before the second node's is scheduled.
func TestFanOutToFanOutJoinsTheFirst(t *testing.T) {
	c := NewConveyor()
	first := c.AddFanOut(OptName("first"))
	firstPool := first.AddPool(OptName("firstPool"))
	second := c.AddFanOut(OptName("second"))
	secondPool := second.AddPool(OptName("secondPool"))

	var events recorder
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := first.MoveTo(ctx, Tasks{firstPool.NewTask(func(context.Context) error {
			time.Sleep(10 * time.Millisecond)
			events.add("first")
			return nil
		})}); err != nil {
			return err
		}
		if err := second.MoveTo(ctx, Tasks{secondPool.NewTask(func(context.Context) error {
			events.add("second")
			return nil
		})}); err != nil {
			return err
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if got := events.all(); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("events = %v, want first then second", got)
	}
}

// TestFanOutWorkJoinedWhenTheProcessorReturns: returning is also a way out. The item never moves on, so nothing joins
// the work explicitly — completing the item does, and an error nobody observed still fails the run.
func TestFanOutWorkJoinedWhenTheProcessorReturns(t *testing.T) {
	boom := errors.New("late boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	var ran atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		return fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error {
			time.Sleep(10 * time.Millisecond)
			ran.Store(true)
			return boom
		})})
	})
	if !errors.Is(err, boom) {
		t.Fatalf("run error = %v, want the task's %v", err, boom)
	}
	if !ran.Load() {
		t.Fatalf("the task never ran")
	}
}

// TestFanOutWorkErrorSurfacesFromTheMoveOut: the failure is reported by the move that would have left the node, and
// the item does not enter the next stage — unlike a named join, which is awaited after entry.
func TestFanOutWorkErrorSurfacesFromTheMoveOut(t *testing.T) {
	boom := errors.New("task boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	var moveErr error
	var entered atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error { return boom })}); err != nil {
			return err
		}
		moveErr = commit.MoveTo(ctx)
		if moveErr != nil {
			return moveErr
		}
		entered.Store(true)
		return nil
	})
	if !errors.Is(moveErr, boom) {
		t.Fatalf("move error = %v, want the task's %v", moveErr, boom)
	}
	if entered.Load() {
		t.Fatalf("the item entered the next stage although its own work had failed")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("run error = %v, want %v", err, boom)
	}
	if occ := occupancyOf(c, commit); occ != 0 {
		t.Fatalf("commit occupancy = %d, want 0 — the failed move must not have entered it", occ)
	}
}

// TestTryMoveToDeclinesWhileFanOutWorkRuns: the non-blocking move will not wait for the item's own tasks either, so it
// declines until they are done — which is what makes polling it a way to do other work meanwhile.
func TestTryMoveToDeclinesWhileFanOutWorkRuns(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	release := make(chan struct{})
	var declines atomic.Int64
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error {
			<-release
			return nil
		})}); err != nil {
			return err
		}
		entered, err := commit.TryMoveTo(ctx)
		if err != nil {
			return err
		}
		if entered {
			t.Error("entered the next stage although the item's own work was still running")
		}
		declines.Add(1)
		close(release)
		// Now that the work can finish, a blocking move gets through.
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if declines.Load() != 1 {
		t.Fatalf("the non-blocking move was never declined")
	}
}

// TestTryMoveToReportsFinishedFanOutFailure: when the work has already failed there is no wait to decline, so the
// failure is reported instead — with entered false, since the item did not move.
func TestTryMoveToReportsFinishedFanOutFailure(t *testing.T) {
	boom := errors.New("try boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	var tryErr error
	var tryEntered atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error { return boom })}); err != nil {
			return err
		}
		// Wait for the work to settle so the call has a finished, failed wave to report rather than a wait to decline.
		waitFor(t, "the failed task to settle", func() bool { return occupancyOf(c, pool) == 0 })
		entered, err := commit.TryMoveTo(ctx)
		tryEntered.Store(entered)
		tryErr = err
		return err
	})
	if !errors.Is(tryErr, boom) {
		t.Fatalf("TryMoveTo error = %v, want the task's %v", tryErr, boom)
	}
	if tryEntered.Load() {
		t.Fatalf("entered = true, want false: the item never moved")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("run error = %v, want %v", err, boom)
	}
}

// TestFanOutWorkPerPoolOrderSurvivesTheWaitingRoom: work is pushed onto the branches when an item is admitted to the
// node, and admission is in item order — so a waiting room in front changes nothing about the order the pools see.
func TestFanOutWorkPerPoolOrderSurvivesTheWaitingRoom(t *testing.T) {
	const items = 6
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetQueueSize(3)
	pool := fo.AddPool(OptName("pool")) // limit 1: strictly ordered
	commit := c.AddStage(OptName("commit"))

	var order recorder
	runNOK(t, c, items, func(ctx context.Context, no int64) error {
		if err := fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error {
			order.add("%d", no)
			return nil
		})}); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	got := order.all()
	if len(got) < items {
		t.Fatalf("only %d of %d tasks ran: %v", len(got), items, got)
	}
	for i := 0; i < items; i++ {
		if want := sprintf("%d", i+1); got[i] != want {
			t.Fatalf("pool ran %v, want the items in order", got[:items])
		}
	}
}

// TestShutdownReleasesAnItemWaitingForItsWork: an item blocked on its own fan-out work is blocked in a MoveTo, so a
// shutdown that cancels in-flight items reaches it there — the wait is not a hole in the cancellation story.
func TestShutdownReleasesAnItemWaitingForItsWork(t *testing.T) {
	cause := errors.New("shutting down")
	c := NewConveyor(optCancelItemsOnShutdown()) // cancel in-flight items at once
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	inWork := make(chan struct{})
	var moveErr atomic.Value
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	go func() {
		<-inWork
		cancel(cause)
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		if err := fo.MoveTo(ic, Tasks{pool.NewTask(func(tctx context.Context) error {
			close(inWork)
			<-tctx.Done() // only the shutdown can end this work
			return context.Cause(tctx)
		})}); err != nil {
			return err
		}
		err := commit.MoveTo(ic) // blocks joining the item's own work
		moveErr.Store(err)
		return err
	})

	err, _ := moveErr.Load().(error)
	if err == nil {
		t.Fatalf("the move out of the fan-out never returned")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("move error = %v, want the shutdown cause %v", err, cause)
	}
}

// TestTravellingLaneWorkCompletesWhileTheItemWaits is the liveness case the design has to survive: the item holds the
// fan-out's slot while its work finishes, and that work is child items travelling through the pool's own interior
// nodes — including a nested fan-out of their own. Nothing a child needs may depend on the parent moving on.
func TestTravellingLaneWorkCompletesWhileTheItemWaits(t *testing.T) {
	const items, children = 6, 3
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")) // limit 1: the parent blocks here until every child is done
	lane := fo.AddLane(OptName("lane"))
	inner := lane.AddStage(OptName("inner"))            // limit 1: children serialize here
	nested := lane.AddFanOut(OptName("nested"))         // a fan-out inside the lane
	nestedPool := nested.AddPool(OptName("nestedPool")) // limit 1
	commit := c.AddStage(OptName("commit"))

	var done atomic.Int64
	runNOK(t, c, items, func(ctx context.Context, no int64) error {
		if err := fo.MoveTo(ctx, Tasks{lane.NewTasks(children, func(cctx context.Context, i int) error {
			if err := inner.MoveTo(cctx); err != nil {
				return err
			}
			if err := nested.MoveTo(cctx, Tasks{nestedPool.NewTasks(2, func(context.Context, int) error {
				return nil
			})}); err != nil {
				return err
			}
			done.Add(1)
			return nil
		})}); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	if got := done.Load(); got < items*children {
		t.Fatalf("%d children finished, want %d", got, items*children)
	}
}
