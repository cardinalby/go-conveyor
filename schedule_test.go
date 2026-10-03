package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// This file pins FanOut.Schedule: adding work to an open body from the ItemProcessor (roots and rounds), from a
// running task or a lane child (spawns), the place spawned work takes in a branch queue, and the calls that are
// refused. Every test enters with MoveTo(ctx) and adds work with Schedule.

// assertEvents fails unless got is exactly want, in order.
func assertEvents(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

// TestScheduleRoundsDecidedByEarlierResults: Schedule, Wait, Schedule, Wait, leave. The second round is planned from
// the first round's results, which Wait guarantees are complete; the body stays open between rounds.
func TestScheduleRoundsDecidedByEarlierResults(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool")).SetLimit(2)
	commit := c.AddStage(WithName("commit"))

	var events recorder
	var found atomic.Int64
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTasks(3, func(_ context.Context, i int) error {
			found.Add(int64(i + 1)) // 1 + 2 + 3
			events.add("round1")
			return nil
		})); err != nil {
			return err
		}
		if err := fo.Wait(ctx); err != nil {
			return err
		}
		n := int(found.Load())
		events.add("plan %d", n)
		if err := fo.Schedule(ctx, pool.NewTasks(n, func(context.Context, int) error {
			events.add("round2")
			return nil
		})); err != nil {
			return err
		}
		if err := fo.Wait(ctx); err != nil {
			return err
		}
		events.add("waited")
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	want := []string{"round1", "round1", "round1", "plan 6"}
	for i := 0; i < 6; i++ {
		want = append(want, "round2")
	}
	assertEvents(t, events.all(), append(want, "waited"))
}

// TestScheduleTreeFromTasksOnOnePool: each task spawns its children before it returns, and the leave waits for the
// whole tree whatever its depth. Several roots grow side by side on one pool.
func TestScheduleTreeFromTasksOnOnePool(t *testing.T) {
	const roots, width, depth = 3, 2, 3
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool")).SetLimit(4)
	commit := c.AddStage(WithName("commit"))

	var visited atomic.Int64
	var visit func(ctx context.Context, d int) error
	visit = func(ctx context.Context, d int) error {
		visited.Add(1)
		if d == depth {
			return nil
		}
		return fo.Schedule(ctx, pool.NewTasks(width, func(ctx context.Context, _ int) error { return visit(ctx, d+1) }))
	}
	var atCommit int64
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTasks(roots, func(ctx context.Context, _ int) error {
			return visit(ctx, 0)
		})); err != nil {
			return err
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		atCommit = visited.Load()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	want := int64(0)
	for d, n := 0, 1; d <= depth; d, n = d+1, n*width {
		want += int64(roots * n)
	}
	if atCommit != want {
		t.Fatalf("%d tree nodes had run when the item reached commit, want %d", atCommit, want)
	}
}

// TestScheduleTreeAcrossTwoPools: a task on one pool spawns onto another pool and onto its own; the body is one tree
// over both branches, and the leave joins all of it.
func TestScheduleTreeAcrossTwoPools(t *testing.T) {
	const pages, depth = 2, 2
	c := New()
	crawl := c.AddFanOut(WithName("crawl"))
	fetch := crawl.AddPool(WithName("fetch")).SetLimit(2)
	store := crawl.AddPool(WithName("store")).SetLimit(2)
	commit := c.AddStage(WithName("commit"))

	var fetched, stored atomic.Int64
	var visit func(ctx context.Context, d int) error
	visit = func(ctx context.Context, d int) error {
		fetched.Add(1)
		next := []Task{store.NewTask(func(context.Context) error {
			stored.Add(1)
			return nil
		})}
		if d < depth {
			next = append(next, fetch.NewTasks(pages, func(ctx context.Context, _ int) error { return visit(ctx, d+1) }))
		}
		return crawl.Schedule(ctx, next...)
	}
	var atCommit [2]int64
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := crawl.MoveTo(ctx); err != nil {
			return err
		}
		if err := crawl.Schedule(ctx, fetch.NewTask(func(ctx context.Context) error { return visit(ctx, 0) })); err != nil {
			return err
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		atCommit = [2]int64{fetched.Load(), stored.Load()}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	const want = 1 + pages + pages*pages
	if atCommit != [2]int64{want, want} {
		t.Fatalf("fetched, stored = %v when the item reached commit, want %d each", atCommit, want)
	}
}

// TestSchedulePaginationChain: each page schedules the next before it returns; on a limit-1 pool the chain runs in
// order and Wait returns after the last page.
func TestSchedulePaginationChain(t *testing.T) {
	const pages = 6
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))

	var order numbers
	var page func(ctx context.Context, n int) error
	page = func(ctx context.Context, n int) error {
		order.add(int64(n))
		if n == pages {
			return nil
		}
		return fo.Schedule(ctx, pool.NewTask(func(ctx context.Context) error { return page(ctx, n+1) }))
	}
	var afterWait []int64
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(ctx context.Context) error { return page(ctx, 1) })); err != nil {
			return err
		}
		if err := fo.Wait(ctx); err != nil {
			return err
		}
		afterWait = order.all()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if len(afterWait) != pages {
		t.Fatalf("%d pages had run when Wait returned, want %d", len(afterWait), pages)
	}
	assertStrictlyIncreasing(t, afterWait, "page order")
}

// TestJoinAsAContinuation: a task must not wait for the work it spawned, so the merge step is itself a task, scheduled
// by the last sibling to finish — while it still holds a slot, so the merge is queued before any slot frees.
func TestJoinAsAContinuation(t *testing.T) {
	const parts = 5
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	work := fo.AddPool(WithName("work")).SetLimit(3)
	merge := fo.AddPool(WithName("merge"))
	commit := c.AddStage(WithName("commit"))

	var done, merged atomic.Int64
	var partsAtMerge int64
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		var remaining atomic.Int64
		remaining.Store(parts)
		if err := fo.Schedule(ctx, work.NewTasks(parts, func(ctx context.Context, _ int) error {
			done.Add(1)
			if remaining.Add(-1) == 0 {
				return fo.Schedule(ctx, merge.NewTask(func(context.Context) error {
					partsAtMerge = done.Load()
					merged.Add(1)
					return nil
				}))
			}
			return nil
		})); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if got := merged.Load(); got != 1 {
		t.Fatalf("the merge ran %d times, want 1", got)
	}
	if partsAtMerge != parts {
		t.Fatalf("%d parts were done when the merge ran, want %d", partsAtMerge, parts)
	}
}

// TestLaneChildSpawnsIntoItsParentsFanOut: a child of one of the fan-out's lanes may Schedule at that fan-out before
// its callback returns; the work joins the parent's body — on a pool, or as a sibling child — and the parent's leave
// waits for it.
func TestLaneChildSpawnsIntoItsParentsFanOut(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	lane := fo.AddLane(WithName("lane"))
	inner := lane.AddStage(WithName("inner"))
	pool := fo.AddPool(WithName("pool"))
	commit := c.AddStage(WithName("commit"))

	var poolRuns, siblingRuns atomic.Int64
	var atCommit [2]int64
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
			if err := inner.MoveTo(cctx); err != nil {
				return err
			}
			return fo.Schedule(cctx,
				pool.NewTask(func(context.Context) error {
					poolRuns.Add(1)
					return nil
				}),
				lane.NewTask(func(cctx context.Context) error {
					if err := inner.MoveTo(cctx); err != nil {
						return err
					}
					siblingRuns.Add(1)
					return nil
				}),
			)
		}))
		if err != nil {
			return err
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		atCommit = [2]int64{poolRuns.Load(), siblingRuns.Load()}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if atCommit != [2]int64{1, 1} {
		t.Fatalf("pool runs, sibling runs = %v when the item reached commit, want 1 each", atCommit)
	}
}

// TestSpawnIntoRetainedTaskGroupKeepsTheSlot: a task of a retained task group may still spawn; the task group finishes
// only when the whole tree is done, and the fan-out slot follows the tree meanwhile.
func TestSpawnIntoRetainedTaskGroupKeepsTheSlot(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool")).SetLimit(2)
	commit := c.AddStage(WithName("commit"))

	inCommit := make(chan struct{})
	spawnRunning := make(chan struct{})
	release := make(chan struct{})
	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.Schedule(ctx, pool.NewTask(func(ctx context.Context) error {
			<-inCommit // the item has moved on before this spawns
			return fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
				close(spawnRunning)
				<-release
				return nil
			}))
		}))
		if err != nil {
			return err
		}
		w := fo.Retain(ctx)
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		close(inCommit)
		<-spawnRunning
		select {
		case <-w.Finished():
			t.Error("the task group finished while spawned work was still running")
		default:
		}
		if got := occupancyOf(c, fo); got != 1 {
			t.Errorf("fan-out occupancy = %d while the retained tree still runs, want 1", got)
		}
		close(release)
		<-w.Finished()
		waitFor(t, "the fan-out slot to be given back", func() bool { return occupancyOf(c, fo) == 0 })
		checked.Store(true)
		return w.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the checks did not run")
	}
}

// TestScheduleFromCanceledItemQueuesNothing: a canceled item's Schedule returns the cause and queues nothing, also
// when the call hides the cancellation with context.WithoutCancel — the item's own context decides.
func TestScheduleFromCanceledItemQueuesNothing(t *testing.T) {
	c := New(WithDrainTimeout(0)) // cancel in-flight items as soon as shutdown starts
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))

	var ran, checked atomic.Bool
	stop, done := runAsync(c, func(ctx context.Context) error {
		if itemNo(ctx) != 1 {
			<-ctx.Done()
			return nil
		}
		if err := fo.MoveTo(ctx); err != nil { // inside, with an empty body
			return err
		}
		<-ctx.Done() // the drain timeout canceled the item
		task := func(context.Context) error {
			ran.Store(true)
			return nil
		}
		for _, cc := range []context.Context{ctx, context.WithoutCancel(ctx)} {
			var se ShutdownError
			if err := fo.Schedule(cc, pool.NewTask(task)); !errors.As(err, &se) {
				t.Errorf("Schedule on a canceled item = %v, want its ShutdownError", err)
			}
		}
		if got := queueOccupancy(c, pool); got != 0 {
			t.Errorf("pool backlog = %d after the refused schedules, want 0", got)
		}
		if got := occupancyOf(c, pool); got != 0 {
			t.Errorf("pool occupancy = %d after the refused schedules, want 0", got)
		}
		checked.Store(true)
		return nil
	})
	waitFor(t, "item 1 to enter fo", func() bool { return occupancyOf(c, fo) == 1 })
	stop(errors.New("stop"))
	err := recvErr(t, "Run", done)
	if _, item := runErrorKinds(err); item {
		t.Fatalf("Run = %v, want no item failure", err)
	}
	if !checked.Load() {
		t.Fatalf("the checks did not run")
	}
	if ran.Load() {
		t.Fatalf("a task ran although its schedule was refused")
	}
}

// TestScheduleIntoFailedBodyQueuesNothing: once a task of the body has failed, Schedule into that body returns the
// TaskError and queues nothing, also with context.WithoutCancel; the item itself is not canceled.
func TestScheduleIntoFailedBodyQueuesNothing(t *testing.T) {
	boom := errors.New("boom")
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))

	var ran, checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return boom })); err != nil {
			return err
		}
		waitFor(t, "the task error to be recorded on the body", func() bool { return bodyErrOf(ctx) != nil })
		if cause := context.Cause(ctx); cause != nil {
			t.Errorf("item canceled with %v; a task failure must not cancel the item", cause)
		}
		task := func(context.Context) error {
			ran.Store(true)
			return nil
		}
		for _, cc := range []context.Context{ctx, context.WithoutCancel(ctx)} {
			var te TaskError
			if err := fo.Schedule(cc, pool.NewTask(task)); !errors.As(err, &te) || te.Unwrap() != boom {
				t.Errorf("Schedule into a failed body = %v, want the body's TaskError with %v", err, boom)
			}
		}
		if got := queueOccupancy(c, pool); got != 0 {
			t.Errorf("pool backlog = %d after the refused schedules, want 0", got)
		}
		if err := fo.Wait(ctx); !errors.Is(err, boom) {
			t.Errorf("Wait on the failed body = %v, want the task error", err)
		}
		checked.Store(true)
		return nil // the joined failure is the processor's to decide: the item goes on
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait joined the failure, so the run should not fail, got %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the checks did not run")
	}
	if ran.Load() {
		t.Fatalf("a task ran although its schedule was refused")
	}
}

// bodyErrOf reads the recorded error of the item's open fan-out body without joining it.
func bodyErrOf(ctx context.Context) error {
	it := itemOf(ctx)
	it.run.mu.Lock()
	defer it.run.mu.Unlock()
	if it.pending == nil {
		return nil
	}
	return it.pending.err
}

// TestScheduleFromFinishedWorkIsStale: a pool goroutine that outlives its task group gets ErrStaleContext; so does a
// lane child's context after the child's callback returned, even while sibling work keeps the parent's task group busy
// — the child's own lifetime is judged before its work would be redirected to the parent's task group.
func TestScheduleFromFinishedWorkIsStale(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))
	lane := fo.AddLane(WithName("lane"))
	inner := lane.AddStage(WithName("inner"))
	commit := c.AddStage(WithName("commit"))

	taskCtx := make(chan context.Context, 1)
	childCtx := make(chan context.Context, 1)
	secondInInner := make(chan struct{})
	release := make(chan struct{})
	var ran atomic.Int64
	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.Schedule(ctx,
			pool.NewTask(func(tctx context.Context) error {
				taskCtx <- tctx
				return nil
			}),
			lane.NewTasks(2, func(cctx context.Context, i int) error {
				if err := inner.MoveTo(cctx); err != nil {
					return err
				}
				if i == 0 {
					childCtx <- cctx
					return nil // inner frees when this child has finished; the next child takes it
				}
				close(secondInInner)
				<-release
				return nil
			}),
		)
		if err != nil {
			return err
		}
		<-secondInInner // child 0 is finished, child 1 keeps the task group busy
		stale := <-childCtx
		err = fo.Schedule(stale, lane.NewTask(func(context.Context) error {
			ran.Add(1)
			return nil
		}))
		if !errors.Is(err, ErrStaleContext) {
			t.Errorf("Schedule with a finished child's context = %v, want ErrStaleContext", err)
		}
		close(release)
		if err := commit.MoveTo(ctx); err != nil { // the task group is finished
			return err
		}
		err = fo.Schedule(<-taskCtx, pool.NewTask(func(context.Context) error {
			ran.Add(1)
			return nil
		}))
		if !errors.Is(err, ErrStaleContext) {
			t.Errorf("Schedule with a finished task group's task context = %v, want ErrStaleContext", err)
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
	if got := ran.Load(); got != 0 {
		t.Fatalf("%d refused tasks ran", got)
	}
}

// --- misuse ---

// TestScheduleForAPassedFanOutPanics: Schedule before entry prepares work for a fan-out ahead of the item (see
// schedule_before_entry_test.go); a fan-out the item has already passed can never be entered, so it is misuse, the
// same as a MoveTo to it.
func TestScheduleForAPassedFanOutPanics(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))
	commit := c.AddStage(WithName("commit"))

	panicsInItem(t, c, errWrongEnterOrder, func(ctx context.Context) {
		if err := commit.MoveTo(ctx); err != nil {
			t.Errorf("move failed: %v", err)
		}
		_ = fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil }))
	})
}

// TestScheduleAfterLeavingPanics: leaving closes the body; the ItemProcessor's path into it is over.
func TestScheduleAfterLeavingPanics(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))
	commit := c.AddStage(WithName("commit"))

	panicsInItem(t, c, errBodyClosed, func(ctx context.Context) {
		if err := fo.MoveTo(ctx); err != nil {
			t.Fatalf("move failed: %v", err)
		}
		if err := commit.MoveTo(ctx); err != nil {
			t.Fatalf("move failed: %v", err)
		}
		_ = fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil }))
	})
}

// TestScheduleAfterRetainPanics: from Retain on the body belongs to the returned task group, whatever its progress.
func TestScheduleAfterRetainPanics(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))

	panicsInItem(t, c, errWorkRetained, func(ctx context.Context) {
		if err := fo.MoveTo(ctx); err != nil {
			t.Fatalf("move failed: %v", err)
		}
		_ = fo.Retain(ctx)
		_ = fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil }))
	})
}

// TestTaskSchedulingAtAnotherFanOutPanics: a task's context may only Schedule at the fan-out its pool belongs to.
// The panic is recovered inside the task: a panic on the runtime's goroutine would crash the test binary.
func TestTaskSchedulingAtAnotherFanOutPanics(t *testing.T) {
	c := New()
	first := c.AddFanOut(WithName("first"))
	firstPool := first.AddPool(WithName("firstPool"))
	second := c.AddFanOut(WithName("second"))
	secondPool := second.AddPool(WithName("secondPool"))

	got := make(chan error, 1)
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := first.MoveTo(ctx); err != nil {
			return err
		}
		if err := first.Schedule(ctx, firstPool.NewTask(func(tctx context.Context) error {
			got <- recoveredErr(func() {
				_ = second.Schedule(tctx, secondPool.NewTask(func(context.Context) error { return nil }))
			})
			return nil
		})); err != nil {
			return err
		}
		return first.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if perr := <-got; !errors.Is(perr, errInvalidUnit) {
		t.Fatalf("Schedule at another fan-out from a task panicked with %v, want errInvalidUnit", perr)
	}
}

// TestScheduleFromRetainCallbackAfterProcessorReturnedPanics: a Retain callback holding the item's context finds the
// body closed once the processor has returned — completion seals it — so its Schedule panics with errBodyClosed
// instead of adding work nobody would wait for. Recovered inside the callback, which runs on a runtime goroutine.
func TestScheduleFromRetainCallbackAfterProcessorReturnedPanics(t *testing.T) {
	c := New()
	s := c.AddStage(WithName("s"))
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))

	got := make(chan error, 1)
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		_ = s.RetainFor(ctx, func(context.Context) error {
			waitFor(t, "completion to close the body", func() bool { return bodyStateOf(ctx, fo) == bodyClosed })
			got <- recoveredErr(func() {
				_ = fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil }))
			})
			return nil
		})
		return fo.MoveTo(ctx) // returns with the body open; completion closes it while the Retain still runs
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if perr := <-got; !errors.Is(perr, errBodyClosed) {
		t.Fatalf("Schedule after the processor returned panicked with %v, want errBodyClosed", perr)
	}
}

// --- leaving with TryMoveTo ---

// TestTryMoveToOutOfBusyBodyLeavesItOpen: a non-blocking leave while the body is busy is declined with (false, nil)
// and touches nothing: the item keeps scheduling into the same body, and the blocking leave then joins all of it.
func TestTryMoveToOutOfBusyBodyLeavesItOpen(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool")).SetLimit(2)
	commit := c.AddStage(WithName("commit"))

	release := make(chan struct{})
	var ran atomic.Int64
	var atCommit int64
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
			<-release
			ran.Add(1)
			return nil
		})); err != nil {
			return err
		}
		if entered, err := commit.TryMoveTo(ctx); entered || err != nil {
			t.Errorf("TryMoveTo out of a busy body = (%v, %v), want (false, nil)", entered, err)
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
			ran.Add(1)
			return nil
		})); err != nil {
			return err
		}
		close(release)
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		atCommit = ran.Load()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if atCommit != 2 {
		t.Fatalf("%d tasks had run when the item reached commit, want 2", atCommit)
	}
}

// TestTryMoveToOutOfIdleBodyIntoFullTargetLeavesItOpen: an idle clean body is closed only when the item is admitted.
// Declined for lack of room, the item is still inside with an open body and may schedule more.
func TestTryMoveToOutOfIdleBodyIntoFullTargetLeavesItOpen(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))
	commit := c.AddStage(WithName("commit"))

	firstInCommit := make(chan struct{})
	releaseFirst := make(chan struct{})
	var ran atomic.Int64
	var checked atomic.Bool
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		switch no {
		case 1:
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			if err := commit.MoveTo(ctx); err != nil {
				return err
			}
			close(firstInCommit)
			<-releaseFirst
			return nil
		case 2:
			<-firstInCommit
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			task := func(context.Context) error {
				ran.Add(1)
				return nil
			}
			if err := fo.Schedule(ctx, pool.NewTask(task)); err != nil {
				return err
			}
			if err := fo.Wait(ctx); err != nil {
				return err
			}
			if entered, err := commit.TryMoveTo(ctx); entered || err != nil {
				t.Errorf("TryMoveTo into a full stage = (%v, %v), want (false, nil)", entered, err)
			}
			if err := fo.Schedule(ctx, pool.NewTask(task)); err != nil { // the body is still open
				return err
			}
			if err := fo.Wait(ctx); err != nil {
				return err
			}
			if got := ran.Load(); got != 2 {
				t.Errorf("%d tasks ran after the declined leave, want 2", got)
			}
			checked.Store(true)
			close(releaseFirst)
			return commit.MoveTo(ctx)
		}
		return nil
	})
	if !checked.Load() {
		t.Fatalf("the checks did not run")
	}
}

// TestTryMoveToOutOfFailedBodyReportsTheTaskError: once the failed body is idle, the non-blocking leave joins it: it
// closes the body and declines with the TaskError, and the item stays in the fan-out. The item is not canceled, so a
// processor that handles the error may still move on.
func TestTryMoveToOutOfFailedBodyReportsTheTaskError(t *testing.T) {
	boom := errors.New("boom")
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))
	commit := c.AddStage(WithName("commit"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return boom })); err != nil {
			return err
		}
		waitFor(t, "the failed body to go idle", func() bool {
			it := itemOf(ctx)
			it.run.mu.Lock()
			defer it.run.mu.Unlock()
			return it.pending.idle() && it.pending.err != nil
		})
		var te TaskError
		if entered, err := commit.TryMoveTo(ctx); entered || !errors.As(err, &te) || te.Unit() != pool ||
			te.Unwrap() != boom {
			t.Errorf("TryMoveTo out of a failed body = (%v, %v), want (false, a TaskError of %s with %v)",
				entered, err, pool, boom)
		}
		if st := bodyStateOf(ctx, fo); st != bodyClosed {
			t.Errorf("body state after the declined leave = %d, want closed (%d)", st, bodyClosed)
		}
		if got := occupancyOf(c, fo); got != 1 {
			t.Errorf("fo occupancy = %d after the declined leave, want 1", got)
		}
		if cause := context.Cause(ctx); cause != nil {
			t.Errorf("item canceled with %v; a task failure must not cancel the item", cause)
		}
		if err := commit.MoveTo(ctx); err != nil {
			t.Errorf("MoveTo after the joined failure = %v, want nil", err)
		}
		checked.Store(true)
		return nil // the joined failure is the processor's to decide: the item goes on
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("TryMoveTo joined the failure, so the run should not fail, got %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the checks did not run")
	}
}

// --- ordering of spawned work ---

// TestSpawnChainOnLimitOnePoolKeepsItemOrder: with a pool limit of 1 an older item's chain that spawns onto the same
// pool from the running task runs to its end before a younger item's queued task starts: each follow-up is queued
// while the spawner still holds the slot, so the freed slot always goes to the older item.
func TestSpawnChainOnLimitOnePoolKeepsItemOrder(t *testing.T) {
	const chain = 5
	c := New()
	fo := c.AddFanOut(WithName("fo")).SetLimit(2)
	pool := fo.AddPool(WithName("pool"))
	commit := c.AddStage(WithName("commit"))

	olderScheduled := make(chan struct{})
	youngerQueued := make(chan struct{})
	var order recorder
	var link func(ctx context.Context, i int) error
	link = func(ctx context.Context, i int) error {
		if i == 0 {
			<-youngerQueued // the younger item's task is queued behind this one before the chain grows
		}
		order.add("A%d", i)
		if i+1 < chain {
			return fo.Schedule(ctx, pool.NewTask(func(ctx context.Context) error { return link(ctx, i+1) }))
		}
		return nil
	}
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		switch no {
		case 1:
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			if err := fo.Schedule(ctx, pool.NewTask(func(ctx context.Context) error { return link(ctx, 0) })); err != nil {
				return err
			}
			close(olderScheduled)
		case 2:
			<-olderScheduled // enter only once the older item has scheduled, whatever opens the door
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
				order.add("B")
				return nil
			})); err != nil {
				return err
			}
			close(youngerQueued)
		}
		return commit.MoveTo(ctx)
	})
	want := make([]string, 0, chain+1)
	for i := 0; i < chain; i++ {
		want = append(want, sprintf("A%d", i))
	}
	assertEvents(t, order.all(), append(want, "B"))
}

// TestSpawnTakesTheFreedSlotAheadOfYoungerQueuedWork: with a pool limit above 1 the younger item's work may be queued
// while the older item's tasks run; the older item's spawn still takes the next freed slot, because it is inserted at
// its item's place, ahead of the younger work.
func TestSpawnTakesTheFreedSlotAheadOfYoungerQueuedWork(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo")).SetLimit(2)
	pool := fo.AddPool(WithName("pool")).SetLimit(2)
	commit := c.AddStage(WithName("commit"))

	olderScheduled := make(chan struct{})
	youngerQueued := make(chan struct{})
	spawnStarted := make(chan struct{})
	var order recorder
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		switch no {
		case 1:
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			err := fo.Schedule(ctx,
				pool.NewTask(func(ctx context.Context) error { // A1 spawns A3 once B is queued, then frees its slot
					<-youngerQueued
					order.add("A1 spawns")
					return fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
						order.add("A3")
						close(spawnStarted)
						return nil
					}))
				}),
				pool.NewTask(func(context.Context) error { // A2 holds the other slot until A3 has started
					<-spawnStarted
					return nil
				}),
			)
			if err != nil {
				return err
			}
			close(olderScheduled)
		case 2:
			<-olderScheduled
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
				order.add("B")
				return nil
			})); err != nil {
				return err
			}
			close(youngerQueued)
		}
		return commit.MoveTo(ctx)
	})
	// While A2 holds its slot only A1's slot cycles, and it goes to A3, the head of the queue; B starts after A3 or
	// A2 gives a slot back, both after A3 has recorded its start.
	assertEvents(t, order.all(), []string{"A1 spawns", "A3", "B"})
}

// --- concurrency ---

// TestConcurrentSchedulesAllLandOnce: Schedule is safe from any goroutine. Tasks spawn while the ItemProcessor keeps
// adding to the same body, every callback runs exactly once, a spawn with no tasks is a no-op, and the blocking leave
// joins all of it.
func TestConcurrentSchedulesAllLandOnce(t *testing.T) {
	const roots, perRoot, extra = 4, 5, 3
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool")).SetLimit(3)
	commit := c.AddStage(WithName("commit"))

	var runs atomic.Int64
	var atCommit int64
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTasks(roots, func(ctx context.Context, i int) error {
			runs.Add(1)
			if i == 0 {
				return fo.Schedule(ctx) // legal, and nothing happens
			}
			return fo.Schedule(ctx, pool.NewTasks(perRoot, func(context.Context, int) error {
				runs.Add(1)
				return nil
			}))
		})); err != nil {
			return err
		}
		for i := 0; i < extra; i++ { // rounds added while the tasks spawn
			if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
				runs.Add(1)
				return nil
			})); err != nil {
				return err
			}
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		atCommit = runs.Load()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if want := int64(roots + (roots-1)*perRoot + extra); atCommit != want {
		t.Fatalf("%d callbacks had run when the item reached commit, want %d", atCommit, want)
	}
}

// TestChildRunsRoundsAndSpawnsInsideItsLane: a lane child is an item in its own scope. At an interior fan-out it
// schedules roots, waits for them and their spawns, schedules another round, and leaves — all inside the lane.
func TestChildRunsRoundsAndSpawnsInsideItsLane(t *testing.T) {
	const children = 2
	c := New()
	fo := c.AddFanOut(WithName("outer"))
	lane := fo.AddLane(WithName("lane"))
	inner := lane.AddFanOut(WithName("inner"))
	innerPool := inner.AddPool(WithName("innerPool")).SetLimit(2)
	after := lane.AddStage(WithName("after"))
	commit := c.AddStage(WithName("commit"))

	var round2 atomic.Int64
	var shortWaits atomic.Int64
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.Schedule(ctx, lane.NewTasks(children, func(cctx context.Context, _ int) error {
			var mine atomic.Int64 // this child's round-1 tasks and their spawns
			if err := inner.MoveTo(cctx); err != nil {
				return err
			}
			if err := inner.Schedule(cctx, innerPool.NewTasks(2, func(tctx context.Context, _ int) error {
				mine.Add(1)
				return inner.Schedule(tctx, innerPool.NewTask(func(context.Context) error {
					mine.Add(1)
					return nil
				}))
			})); err != nil {
				return err
			}
			if err := inner.Wait(cctx); err != nil {
				return err
			}
			if mine.Load() != 4 {
				shortWaits.Add(1)
			}
			if err := inner.Schedule(cctx, innerPool.NewTask(func(context.Context) error {
				round2.Add(1)
				return nil
			})); err != nil {
				return err
			}
			return after.MoveTo(cctx) // joins round 2
		}))
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if got := shortWaits.Load(); got != 0 {
		t.Fatalf("%d children saw Wait return before their tree was done", got)
	}
	if got := round2.Load(); got != children {
		t.Fatalf("%d second-round tasks ran, want %d", got, children)
	}
}

// TestScheduleFromReturnedChildIsStale: a lane child's callback has returned but the child is not finished yet, because
// it retained work at an interior fan-out that is still running. Its context is over for its own code: a Schedule into
// the parent's fan-out with it is refused with ErrStaleContext, not redirected into the parent's task group.
func TestScheduleFromReturnedChildIsStale(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	lane := fo.AddLane(WithName("lane"))
	inner := lane.AddFanOut(WithName("inner"))
	ipool := inner.AddPool(WithName("ipool"))

	childCtx := make(chan context.Context, 1)
	release := make(chan struct{})
	var ran atomic.Int64
	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
			if err := inner.MoveTo(cctx); err != nil {
				return err
			}
			err := inner.Schedule(cctx, ipool.NewTask(func(context.Context) error {
				<-release
				return nil
			}))
			if err != nil {
				return err
			}
			_ = inner.Retain(cctx) // the child returns while its work runs on: returned, not finished
			childCtx <- cctx
			return nil
		}))
		if err != nil {
			return err
		}
		cctx := <-childCtx
		child := itemOf(cctx)
		waitFor(t, "the child's callback to return", func() bool {
			child.run.mu.Lock()
			defer child.run.mu.Unlock()
			return child.returned && !child.finished
		})
		err = fo.Schedule(cctx, lane.NewTask(func(context.Context) error {
			ran.Add(1)
			return nil
		}))
		if !errors.Is(err, ErrStaleContext) {
			t.Errorf("Schedule with a returned child's context = %v, want ErrStaleContext", err)
		}
		checked.Store(true)
		close(release)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the check did not run")
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("%d refused tasks ran", got)
	}
}
