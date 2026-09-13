package conveyor

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync/atomic"
	"testing"
)

// dormantCount reports how many fan-outs the item behind ctx has prepared work for right now. White-box: prepared
// work is neither occupancy nor backlog, so no public gauge shows it.
func dormantCount(ctx context.Context) int {
	it := itemOf(ctx)
	it.run.mu.Lock()
	defer it.run.mu.Unlock()
	return len(it.dormant)
}

// TestScheduleBeforeEntry_NothingRunsBeforeAdmission: prepared work takes no capacity, stands in no queue, and none
// of its callbacks, generators or channels is touched until the item enters.
func TestScheduleBeforeEntry_NothingRunsBeforeAdmission(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")).SetLimit(3)
	commit := c.AddStage(OptName("commit"))

	var ran, pulled atomic.Int32
	ch := make(chan TaskFunc, 1)
	ch <- func(context.Context) error { ran.Add(1); return nil }
	close(ch)
	gen := func(yield func(TaskFunc) bool) {
		pulled.Add(1)
		yield(func(context.Context) error { ran.Add(1); return nil })
	}
	prepared := make(chan struct{})
	proceed := make(chan struct{})
	go func() {
		<-prepared
		// The item is in read with everything prepared: nothing has moved.
		if got := ran.Load(); got != 0 {
			t.Errorf("%d callbacks ran before entry", got)
		}
		if got := pulled.Load(); got != 0 {
			t.Errorf("the generator was pulled before entry")
		}
		if len(ch) != 1 {
			t.Errorf("the channel was received from before entry")
		}
		if got := occupancyOf(c, pool); got != 0 {
			t.Errorf("pool occupancy = %d before entry, want 0", got)
		}
		if got := queueOccupancy(c, pool); got != 0 {
			t.Errorf("pool backlog = %d before entry, want 0", got)
		}
		if got := occupancyOf(c, fo); got != 0 {
			t.Errorf("fo occupancy = %d before entry, want 0", got)
		}
		close(proceed)
	}()
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx,
			pool.NewTask(func(context.Context) error { ran.Add(1); return nil }),
			pool.NewTasksGen(iter.Seq[TaskFunc](gen)),
		); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTasksChan(ch)); err != nil {
			return err
		}
		close(prepared)
		<-proceed
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if got := ran.Load(); got != 3 {
		t.Fatalf("%d callbacks ran, want 3", got)
	}
}

// TestScheduleBeforeEntry_WorkStartsBeforeMoveToReturns: activation is part of admission, so with a free pool the
// prepared task is dispatched — the pool slot taken, a Balanced hold already over — by the time MoveTo returns.
func TestScheduleBeforeEntry_WorkStartsBeforeMoveToReturns(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	fo := c.AddFanOut(OptName("fo")).SetBackpressure(BackpressureBalanced)
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	release := make(chan struct{})
	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { <-release; return nil })); err != nil {
			return err
		}
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if got := occupancyOf(c, pool); got != 1 {
			t.Errorf("pool occupancy = %d right after MoveTo, want 1: the prepared task is dispatched on entry", got)
		}
		if held := heldItems(c); len(held) != 0 {
			t.Errorf("items holding upstream = %v right after MoveTo, want none: the start ended the hold", held)
		}
		if got := occupancyOf(c, read); got != 0 {
			t.Errorf("read occupancy = %d right after MoveTo, want 0", got)
		}
		checked.Store(true)
		close(release)
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatal("the checks did not run")
	}
}

// TestScheduleBeforeEntry_SeveralCallsFormOneStrictBatch: every pre-entry Schedule call belongs to the initial batch.
// Item 2 prepares p1 and p2 in two calls while item 1 blocks p2; under Strict its read slot is kept until p2 starts,
// even though p1 started at once.
func TestScheduleBeforeEntry_SeveralCallsFormOneStrictBatch(t *testing.T) {
	x := newTwoPoolFanOut(BackpressureStrict, 2)
	c := x.c

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2's p1 task started", func() bool { return x.bt.hasStarted("2/p1") })
		waitFor(t, "item 2's p2 task queued", func() bool { return queueOccupancy(c, x.p2) == 1 })
		if held := heldItems(c); !slices.Equal(held, []int64{2}) {
			t.Errorf("items holding upstream = %v, want [2]: p2 has not started", held)
		}
		if got := inBodyOf(c, x.read); !slices.Equal(got, []int64{2}) {
			t.Errorf("read = %v, want [2]", got)
		}
		close(x.bt.gate("1/p2")) // item 1's task ends: item 2's p2 task starts, the hold ends
		waitFor(t, "item 2's p2 task started", func() bool { return x.bt.hasStarted("2/p2") })
		waitFor(t, "read empty", func() bool { return len(inBodyOf(c, x.read)) == 0 })
		x.bt.releaseAll()
	}()
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if no == 1 {
			return x.enterAndBlockWith(ctx, true, x.p2)
		}
		if err := x.read.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.Schedule(ctx, x.bt.task(x.p1, 2)); err != nil {
			return err
		}
		if err := x.f.Schedule(ctx, x.bt.task(x.p2, 2)); err != nil {
			return err
		}
		if err := x.f.MoveTo(ctx); err != nil {
			return err
		}
		return x.commit.MoveTo(ctx)
	})
	<-done
}

// TestScheduleBeforeEntry_PostEntrySubmissionDoesNotExtendTheBatch: with work prepared, the initial batch is the
// prepared work alone. Item 2 prepares p1, enters, then schedules p2 (blocked by item 1): under Strict the hold ended
// at p1's start, and p2's wait keeps nothing upstream.
func TestScheduleBeforeEntry_PostEntrySubmissionDoesNotExtendTheBatch(t *testing.T) {
	x := newTwoPoolFanOut(BackpressureStrict, 2)
	c := x.c

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2's p2 task queued", func() bool { return queueOccupancy(c, x.p2) == 1 })
		waitFor(t, "item 2's p1 task started", func() bool { return x.bt.hasStarted("2/p1") })
		waitFor(t, "read empty", func() bool { return len(inBodyOf(c, x.read)) == 0 })
		if held := heldItems(c); len(held) != 0 {
			t.Errorf("items holding upstream = %v, want none: p2 is not in the initial batch", held)
		}
		x.bt.releaseAll()
	}()
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if no == 1 {
			return x.enterAndBlockWith(ctx, true, x.p2)
		}
		if err := x.read.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.Schedule(ctx, x.bt.task(x.p1, 2)); err != nil {
			return err
		}
		if err := x.f.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.Schedule(ctx, x.bt.task(x.p2, 2)); err != nil {
			return err
		}
		return x.commit.MoveTo(ctx)
	})
	<-done
}

// TestScheduleBeforeEntry_ExplicitEmptyIsAKnownBatch: an empty Schedule before entry — no tasks, or only statically
// empty ones — makes the initial batch known and empty. Under Strict, item 1's admission then ends its hold at once
// and opens the door: item 2 enters fo while item 1 is still inside without having scheduled anything after entry.
func TestScheduleBeforeEntry_ExplicitEmptyIsAKnownBatch(t *testing.T) {
	for _, static := range []bool{false, true} {
		t.Run(fmt.Sprintf("staticallyEmpty=%v", static), func(t *testing.T) {
			testScheduleBeforeEntryEmptyBatch(t, static)
		})
	}
}

func testScheduleBeforeEntryEmptyBatch(t *testing.T, static bool) {
	c := NewConveyor()
	read := c.AddStage(OptName("read")).SetLimit(2)
	fo := c.AddFanOut(OptName("fo")).SetLimit(2).SetBackpressure(BackpressureStrict)
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit")).SetLimit(2)

	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "both items inside fo", func() bool { return slices.Equal(inBodyOf(c, fo), []int64{1, 2}) })
		if held := heldItems(c); len(held) != 0 {
			t.Errorf("items holding upstream = %v, want none: a known-empty batch is the milestone", held)
		}
		if got := inBodyOf(c, read); len(got) != 0 {
			t.Errorf("read = %v, want empty", got)
		}
		close(release)
	}()
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		var tasks []Task
		if static {
			tasks = append(tasks, pool.NewTasks(0, func(context.Context, int) error { return nil }))
		}
		if err := fo.Schedule(ctx, tasks...); err != nil {
			return err
		}
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		<-release
		return commit.MoveTo(ctx)
	})
	<-done
}

// TestScheduleBeforeEntry_DeclinedTryMoveToKeepsTheWork: a declined TryMoveTo touches nothing — the prepared work
// stays prepared and unrun — and a later MoveTo activates it.
func TestScheduleBeforeEntry_DeclinedTryMoveToKeepsTheWork(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read")).SetLimit(2)
	fo := c.AddFanOut(OptName("fo")) // limit 1: item 1 inside keeps item 2 out
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit")).SetLimit(2)

	var ran atomic.Int32
	item1Inside := make(chan struct{})
	item1Release := make(chan struct{})
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if no == 1 {
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			if err := fo.Schedule(ctx); err != nil { // open the door: item 2 is kept out by the limit alone
				return err
			}
			close(item1Inside)
			<-item1Release
			return commit.MoveTo(ctx)
		}
		<-item1Inside
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { ran.Add(1); return nil })); err != nil {
			return err
		}
		entered, err := fo.TryMoveTo(ctx)
		if err != nil {
			return err
		}
		if entered {
			t.Errorf("TryMoveTo entered fo while item 1 holds its only slot")
		}
		if got := dormantCount(ctx); got != 1 {
			t.Errorf("prepared fan-outs after a declined TryMoveTo = %d, want 1", got)
		}
		if got := ran.Load(); got != 0 {
			t.Errorf("%d callbacks ran after a declined TryMoveTo", got)
		}
		close(item1Release)
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if got := dormantCount(ctx); got != 0 {
			t.Errorf("prepared fan-outs after entering = %d, want 0", got)
		}
		return commit.MoveTo(ctx)
	})
	if got := ran.Load(); got != 1 {
		t.Fatalf("%d callbacks ran, want 1", got)
	}
}

// TestScheduleBeforeEntry_SkippingTheFanOutDiscardsTheWork: moving past a prepared fan-out drops the work — no
// callback runs, a generator is never pulled, and the leave is not a join.
func TestScheduleBeforeEntry_SkippingTheFanOutDiscardsTheWork(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	var ran, pulled atomic.Int32
	genTask := pool.NewTasksGen(func(yield func(TaskFunc) bool) {
		pulled.Add(1)
		yield(func(context.Context) error { ran.Add(1); return nil })
	})
	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { ran.Add(1); return nil }), genTask); err != nil {
			return err
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		if got := dormantCount(ctx); got != 0 {
			t.Errorf("prepared fan-outs after passing fo = %d, want 0", got)
		}
		checked.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatal("the checks did not run")
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("%d discarded callbacks ran", got)
	}
	if got := pulled.Load(); got != 0 {
		t.Fatalf("the discarded generator was pulled")
	}
}

// TestScheduleBeforeEntry_ReturningDiscardsTheWork: a processor that returns — cleanly or with an error — before
// entering the fan-out leaves nothing behind: the prepared work is dropped and completion does not wait for it.
func TestScheduleBeforeEntry_ReturningDiscardsTheWork(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail=%v", fail), func(t *testing.T) {
			c := NewConveyor()
			read := c.AddStage(OptName("read"))
			fo := c.AddFanOut(OptName("fo"))
			pool := fo.AddPool(OptName("pool"))

			var ran atomic.Int32
			boom := errors.New("boom")
			err := runOnce(t, c, func(ctx context.Context) error {
				if err := read.MoveTo(ctx); err != nil {
					return err
				}
				if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { ran.Add(1); return nil })); err != nil {
					return err
				}
				if fail {
					return boom
				}
				return nil
			})
			if fail && !errors.Is(err, boom) {
				t.Fatalf("run error = %v, want boom", err)
			}
			if !fail && err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("run failed: %v", err)
			}
			if got := ran.Load(); got != 0 {
				t.Fatalf("%d discarded callbacks ran", got)
			}
		})
	}
}

// TestScheduleBeforeEntry_CancellationDiscardsTheWork: an item canceled while waiting to enter never activates its
// prepared work; a Schedule after the cancellation prepares nothing either.
func TestScheduleBeforeEntry_CancellationDiscardsTheWork(t *testing.T) {
	c := NewConveyor(optCancelItemsOnShutdown())
	read := c.AddStage(OptName("read")).SetLimit(2)
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit")).SetLimit(2)

	var ran atomic.Int32
	var item2Err error
	var scheduleErr error
	ctx, cancel := context.WithCancel(context.Background())
	item1Release := make(chan struct{})
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if no > 2 {
			<-ic.Done()
			return context.Cause(ic)
		}
		if err := read.MoveTo(ic); err != nil {
			return err
		}
		if no == 1 {
			if err := fo.MoveTo(ic); err != nil {
				return err
			}
			<-item1Release
			return commit.MoveTo(ic)
		}
		if err := fo.Schedule(ic, pool.NewTask(func(context.Context) error { ran.Add(1); return nil })); err != nil {
			return err
		}
		go func() {
			waitFor(t, "item 2 parked at fo", func() bool { return parkedOf(c) >= 1 })
			cancel()
		}()
		item2Err = fo.MoveTo(ic) // item 1 has not scheduled: the door is closed
		scheduleErr = fo.Schedule(ic, pool.NewTask(func(context.Context) error { ran.Add(1); return nil }))
		close(item1Release)
		return item2Err
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context.Canceled", err)
	}
	if item2Err == nil {
		t.Fatal("item 2's MoveTo succeeded, want a cancellation")
	}
	if scheduleErr == nil {
		t.Fatal("Schedule after the cancellation succeeded, want the cancellation cause")
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("%d discarded callbacks ran", got)
	}
}

// TestScheduleBeforeEntry_PreparationDoesNotChangeItemOrder: item 2 prepares its work before item 1 has scheduled
// anything, yet item 1's work starts first: activation happens at item 2's admission, which the ordering gate holds
// back until item 1 has published, and the branch queue places the work by item age.
func TestScheduleBeforeEntry_PreparationDoesNotChangeItemOrder(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read")).SetLimit(2)
	fo := c.AddFanOut(OptName("fo")).SetLimit(2).SetBackpressure(BackpressureBuffered)
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit")).SetLimit(2)

	var order numbers
	item2Prepared := make(chan struct{})
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		task := pool.NewTask(func(context.Context) error { order.add(no); return nil })
		if no == 1 {
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			<-item2Prepared
			if err := fo.Schedule(ctx, task); err != nil {
				return err
			}
			return commit.MoveTo(ctx)
		}
		if err := fo.Schedule(ctx, task); err != nil {
			return err
		}
		close(item2Prepared)
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if got := order.all(); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("task order = %v, want [1 2]", got)
	}
}

// TestScheduleBeforeEntry_LaneChildPreparesForAnInteriorFanOut: a lane child prepares work for a fan-out of its own
// lane the same way a root item does for one of the conveyor's.
func TestScheduleBeforeEntry_LaneChildPreparesForAnInteriorFanOut(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid"))
	inner := lane.AddFanOut(OptName("inner"))
	innerPool := inner.AddPool(OptName("inner-pool"))
	commit := c.AddStage(OptName("commit"))

	var ran atomic.Int32
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
			if err := inner.Schedule(cctx, innerPool.NewTask(func(context.Context) error { ran.Add(1); return nil })); err != nil {
				return err
			}
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			if got := ran.Load(); got != 0 {
				t.Errorf("the prepared task ran before the child entered inner")
			}
			return inner.MoveTo(cctx) // returning joins the body
		})); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if got := ran.Load(); got != 1 {
		t.Fatalf("%d callbacks ran, want 1", got)
	}
}

// TestScheduleBeforeEntry_PoolTaskCannotPrepare: a pool task still schedules only under its own fan-out; preparing
// work for a later fan-out is refused the same way as before.
func TestScheduleBeforeEntry_PoolTaskCannotPrepare(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	later := c.AddFanOut(OptName("later"))
	laterPool := later.AddPool(OptName("later-pool"))

	var got error
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		return fo.Schedule(ctx, pool.NewTask(func(tctx context.Context) error {
			got = recoveredErr(func() {
				_ = later.Schedule(tctx, laterPool.NewTask(func(context.Context) error { return nil }))
			})
			return nil
		}))
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !errors.Is(got, errInvalidUnit) {
		t.Fatalf("pool task preparing for another fan-out: got %v, want a panic with errInvalidUnit", got)
	}
}

// TestScheduleBeforeEntry_WaitAndRetainStillNeedEntry: prepared work is not a body; Wait and Retain before entering
// keep panicking, and cannot activate it.
func TestScheduleBeforeEntry_WaitAndRetainStillNeedEntry(t *testing.T) {
	for _, call := range []string{"wait", "retain"} {
		t.Run(call, func(t *testing.T) {
			c := NewConveyor()
			fo := c.AddFanOut(OptName("fo"))
			pool := fo.AddPool(OptName("pool"))

			var ran atomic.Int32
			panicsInItem(t, c, errStageNotEntered, func(ctx context.Context) {
				if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { ran.Add(1); return nil })); err != nil {
					t.Errorf("prepare failed: %v", err)
				}
				if call == "wait" {
					_ = fo.Wait(ctx)
				} else {
					_ = fo.Retain(ctx)
				}
			})
			if got := ran.Load(); got != 0 {
				t.Fatalf("%d prepared callbacks ran", got)
			}
		})
	}
}

// TestScheduleBeforeEntry_DiscardedTaskStaysConsumed: preparing consumes the Task; discarding the work does not make
// it reusable, so a second submission panics like any resubmission.
func TestScheduleBeforeEntry_DiscardedTaskStaysConsumed(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(2)
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit")).SetLimit(2)

	task := pool.NewTask(func(context.Context) error { return nil })
	item1Done := make(chan struct{})
	var got error
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if no == 1 {
			if err := fo.Schedule(ctx, task); err != nil {
				return err
			}
			if err := commit.MoveTo(ctx); err != nil { // skips fo: the prepared task is discarded
				return err
			}
			close(item1Done)
			return nil
		}
		<-item1Done
		got = recoveredErr(func() { _ = fo.Schedule(ctx, task) })
		return commit.MoveTo(ctx)
	})
	if !errors.Is(got, errTaskReused) {
		t.Fatalf("resubmitting a discarded task: got %v, want a panic with errTaskReused", got)
	}
}

// TestScheduleBeforeEntry_CanceledItemDropsTheWorkAtOnce: a canceled item's prepared work is dropped when a node
// method observes the cancellation, not only when the processor returns, so a processor that winds down slowly
// does not keep the prepared callbacks alive.
func TestScheduleBeforeEntry_CanceledItemDropsTheWorkAtOnce(t *testing.T) {
	c := NewConveyor(optCancelItemsOnShutdown())
	read := c.AddStage(OptName("read")).SetLimit(2)
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit")).SetLimit(2)

	var ran atomic.Int32
	var afterMove, afterSchedule, afterTry int
	ctx, cancel := context.WithCancel(context.Background())
	item1Release := make(chan struct{})
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if no > 2 {
			<-ic.Done()
			return context.Cause(ic)
		}
		if err := read.MoveTo(ic); err != nil {
			return err
		}
		if no == 1 {
			if err := fo.MoveTo(ic); err != nil {
				return err
			}
			<-item1Release
			return commit.MoveTo(ic)
		}
		if err := fo.Schedule(ic, pool.NewTask(func(context.Context) error { ran.Add(1); return nil })); err != nil {
			return err
		}
		go func() {
			waitFor(t, "item 2 parked at fo", func() bool { return parkedOf(c) >= 1 })
			cancel()
		}()
		moveErr := fo.MoveTo(ic) // the door is closed: parks, then wakes canceled
		afterMove = dormantCount(ic)
		// Later calls on the canceled item prepare nothing and leave nothing behind.
		_ = fo.Schedule(ic, pool.NewTask(func(context.Context) error { ran.Add(1); return nil }))
		afterSchedule = dormantCount(ic)
		_, _ = fo.TryMoveTo(ic)
		afterTry = dormantCount(ic)
		close(item1Release)
		return moveErr
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want context.Canceled", err)
	}
	if afterMove != 0 || afterSchedule != 0 || afterTry != 0 {
		t.Fatalf("prepared fan-outs after MoveTo/Schedule/TryMoveTo on the canceled item = %d/%d/%d, want 0/0/0",
			afterMove, afterSchedule, afterTry)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("%d discarded callbacks ran", got)
	}
}

// TestScheduleBeforeEntry_ReusedTaskLeavesTheOthersUnclaimed: a call that lists a consumed Task panics before any
// task of that call is consumed, so the fresh tasks of the call stay usable. The same holds for a Task listed twice.
func TestScheduleBeforeEntry_ReusedTaskLeavesTheOthersUnclaimed(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(fmt.Sprintf("beforeEntry=%v", before), func(t *testing.T) {
			c := NewConveyor()
			fo := c.AddFanOut(OptName("fo"))
			pool := fo.AddPool(OptName("pool"))
			commit := c.AddStage(OptName("commit"))

			var ran atomic.Int32
			var reused, twice error
			err := runOnce(t, c, func(ctx context.Context) error {
				if !before {
					if err := fo.MoveTo(ctx); err != nil {
						return err
					}
				}
				used := pool.NewTask(func(context.Context) error { return nil })
				if err := fo.Schedule(ctx, used); err != nil {
					return err
				}
				fresh := pool.NewTask(func(context.Context) error { ran.Add(1); return nil })
				reused = recoveredErr(func() { _ = fo.Schedule(ctx, fresh, used) })
				twice = recoveredErr(func() { _ = fo.Schedule(ctx, fresh, fresh) })
				if err := fo.Schedule(ctx, fresh); err != nil { // still usable
					return err
				}
				if before {
					if err := fo.MoveTo(ctx); err != nil {
						return err
					}
				}
				return commit.MoveTo(ctx)
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("run failed: %v", err)
			}
			if !errors.Is(reused, errTaskReused) || !errors.Is(twice, errTaskReused) {
				t.Fatalf("panics = %v / %v, want errTaskReused for both", reused, twice)
			}
			if got := ran.Load(); got != 1 {
				t.Fatalf("the fresh task ran %d times, want 1", got)
			}
		})
	}
}

// TestScheduleBeforeEntry_PreparedCallsFormOneBacklogEntry: all pre-entry Schedule calls touching one branch become
// one backlog entry on activation, while a call after entry adds its own.
func TestScheduleBeforeEntry_PreparedCallsFormOneBacklogEntry(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(2)
	pool := fo.AddPool(OptName("pool")) // limit 1: item 1's task blocks the pool
	commit := c.AddStage(OptName("commit")).SetLimit(2)

	release := make(chan struct{})
	item1Started := make(chan struct{})
	var checked atomic.Bool
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if no == 1 {
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
				close(item1Started)
				<-release
				return nil
			})); err != nil {
				return err
			}
			return commit.MoveTo(ctx)
		}
		<-item1Started
		noop := func(context.Context) error { return nil }
		if err := fo.Schedule(ctx, pool.NewTask(noop)); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(noop)); err != nil {
			return err
		}
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if got := inQueueOf(c, pool); !slices.Equal(got, []int64{2}) {
			t.Errorf("pool backlog after entry = %v, want [2]: two prepared calls are one entry", got)
		}
		if err := fo.Schedule(ctx, pool.NewTask(noop)); err != nil {
			return err
		}
		if got := inQueueOf(c, pool); !slices.Equal(got, []int64{2, 2}) {
			t.Errorf("pool backlog after a post-entry call = %v, want [2 2]", got)
		}
		checked.Store(true)
		close(release)
		return commit.MoveTo(ctx)
	})
	if !checked.Load() {
		t.Fatal("the checks did not run")
	}
}
