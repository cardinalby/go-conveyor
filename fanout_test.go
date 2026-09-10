package conveyor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestFanOutLimitBoundsItemsInside is the central backpressure guarantee of a fan-out, and the reason its limit
// counts items rather than tasks.
//
// Topology: start(1) -> write(1) -> fo{fast, slow}.SetLimit(3) -> commit(1).
// Item 1 schedules on both lanes and its slow task blocks; every other item schedules on the fast lane only.
// Item 1 cannot leave the fan-out while that task runs — the work is the node's body. Items 2 and 3 finish their
// fast work but may not pass item 1 to reach commit, so they wait at its door, each still holding a fan-out slot.
// That fills the node, and the jam propagates backwards:
//
//	fan-out full (items 1,2,3) <- write(item 4) <- start(item 5) <- item 6 never created
//
// So the fast lane runs exactly 3 tasks and exactly 5 items exist. The number that matters is the first one: the
// limit bounds how many items have work outstanding, whatever they do afterwards.
func TestFanOutLimitBoundsItemsInside(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	fo := c.AddFanOut(OptName("fo")).SetLimit(3)
	fast := fo.AddPool(OptName("fast"))
	slow := fo.AddPool(OptName("slow"))
	commit := c.AddStage(OptName("commit"))

	var fastDone, created atomic.Int64
	block := make(chan struct{})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		// The pipeline jams with 3 fast tasks done and 5 items alive; then release the slow task.
		waitFor(t, "the pipeline to jam", func() bool {
			return fastDone.Load() >= 3 && created.Load() >= 5 && occupancyOf(c, fo) == 3
		})
		if got := occupancyOf(c, fo); got != 3 {
			t.Errorf("fan-out occupancy = %d, want exactly its limit 3", got)
		}
		if got := created.Load(); got != 5 {
			t.Errorf("%d items alive while jammed, want exactly 5 (the sum of the limits)", got)
		}
		if got := fastDone.Load(); got != 3 {
			t.Errorf("fast lane ran %d tasks while jammed, want exactly 3", got)
		}
		close(block)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		created.Add(1)
		if err := write.MoveTo(ic); err != nil {
			return err
		}
		tasks := []Task{fast.NewTask(func(context.Context) error {
			fastDone.Add(1)
			return nil
		})}
		if no == 1 {
			tasks = append(tasks, slow.NewTask(func(context.Context) error {
				select {
				case <-block:
				case <-ic.Done():
				}
				return nil
			}))
		}
		err := fo.MoveTo(ic)
		if err == nil {
			err = fo.Schedule(ic, tasks...)
		}
		if err != nil {
			return err
		}
		return commit.MoveTo(ic)
	})
}

// TestFanOutSlotHeldUntilNextAdmission pins down when the fan-out slot is given up: not when the work finishes,
// but when the item is admitted to the next node. An item whose tasks are long done still occupies the fan-out
// while it waits at the next stage's door.
// With a limit-1 fan-out and item 1 parked in commit, exactly two tasks may ever run: item 1's, and item 2's —
// after which item 2 sits at the commit door still holding the only fan-out slot, so item 3 cannot get in. If the
// slot were released when the work finished instead, item 3 would enter and a third task would run.
func TestFanOutSlotHeldUntilNextAdmission(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(1)
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	var tasksDone atomic.Int64
	held := make(chan struct{}) // closed once item 1 is inside commit
	release := make(chan struct{})
	var first atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		<-held
		waitFor(t, "item 2's task to finish", func() bool { return tasksDone.Load() >= 2 })
		waitFor(t, "item 2 to park inside the fan-out", func() bool { return occupancyOf(c, fo) == 1 })
		if got := tasksDone.Load(); got != 2 {
			t.Errorf("%d tasks ran while item 1 held commit, want exactly 2: the fan-out slot was released early", got)
		}
		close(release)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		err := fo.MoveTo(ic)
		if err == nil {
			err = fo.Schedule(ic, pool.NewTask(func(context.Context) error {
				tasksDone.Add(1)
				return nil
			}))
		}
		if err != nil {
			return err
		}
		if err := commit.MoveTo(ic); err != nil {
			return err
		}
		if first.CompareAndSwap(false, true) {
			close(held)
			select {
			case <-release:
			case <-ic.Done():
			}
		}
		return nil
	})
}

// TestFanOutReleasesPreviousStageAtAdmission: the previous stage is freed as soon as the item is admitted to the
// fan-out — before it schedules anything, and long before that work finishes. The fan-out's door is another matter:
// the item behind cannot enter the node until the item ahead has scheduled once, whatever the limit says.
func TestFanOutReleasesPreviousStageAtAdmission(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	fo := c.AddFanOut(OptName("fo")).SetLimit(4)
	pool := fo.AddPool(OptName("pool")).SetLimit(4)

	secondAtDoor := make(chan struct{})
	secondInside := make(chan struct{})
	block := make(chan struct{})
	checked := make(chan struct{})

	go func() {
		defer close(checked)
		<-secondAtDoor
		// Item 2 has passed write (released at item 1's admission) and stands at the fan-out's door. It must stay
		// there while item 1 has not scheduled: a limit of 4 does not open the door.
		time.Sleep(20 * time.Millisecond)
		if occ := occupancyOf(c, fo); occ != 1 {
			t.Errorf("fan-out occupancy = %d before the first Schedule, want 1 (the door is closed)", occ)
		}
		close(block) // item 1 schedules now; the door opens
		<-secondInside
	}()

	runNOK(t, c, 2, func(ic context.Context, no int64) error {
		if err := write.MoveTo(ic); err != nil {
			return err
		}
		if no == 2 {
			// Only possible because item 1 released write when it entered the fan-out, not when it scheduled.
			close(secondAtDoor)
		}
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		if no == 2 {
			close(secondInside)
		}
		<-block // both items stay inside until the door check is done, so occupancy tells who got in
		return fo.Schedule(ic, pool.NewTask(func(context.Context) error { return nil }))
	})
	<-checked
}

// TestPoolRunsTasksInParallelUpToLimit: a pool's limit is its task concurrency.
func TestPoolRunsTasksInParallelUpToLimit(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")).SetLimit(4)

	g := &gauge{}
	barrier := make(chan struct{})
	var arrived atomic.Int64
	var opened atomic.Bool

	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, pool.NewTasks(8, func(_ context.Context, i int) error {
				return g.hold(func() error {
					if arrived.Add(1) == 4 && opened.CompareAndSwap(false, true) {
						close(barrier)
					}
					select {
					case <-barrier:
					case <-ctx.Done():
					}
					return nil
				})
			}))
		}
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
	if peak, entries := g.snapshot(); peak != 4 || entries != 8 {
		t.Fatalf("pool peak=%d entries=%d, want peak 4 and 8 tasks", peak, entries)
	}
}

// TestPoolRunsSequentiallyByDefault: a default pool runs its tasks one at a time, in submission order.
func TestPoolRunsSequentiallyByDefault(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	g := &gauge{}
	var order numbers
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, pool.NewTasks(6, func(_ context.Context, i int) error {
				return g.hold(func() error {
					order.add(int64(i))
					return nil
				})
			}))
		}
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
	if peak, _ := g.snapshot(); peak != 1 {
		t.Fatalf("default pool ran %d tasks at once, want 1", peak)
	}
	got := order.all()
	for i, v := range got {
		if v != int64(i) {
			t.Fatalf("task order = %v, want 0..5 in order", got)
		}
	}
}

// TestPoolFIFOAcrossItems: everything an older item scheduled on a pool starts before anything a younger item
// scheduled there.
func TestPoolFIFOAcrossItems(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(4)
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	var order numbers
	runNOK(t, c, 10, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, pool.NewTasks(3, func(_ context.Context, i int) error {
				order.add(no)
				return nil
			}))
		}
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	got := order.all()
	assertNonDecreasing(t, got, "per-pool work order across items")
	if len(got) < 30 {
		t.Fatalf("expected 30 tasks, got %d", len(got))
	}
	// Every item's three tasks must form one contiguous run.
	for i := 0; i+1 < len(got); i++ {
		if got[i] != got[i+1] && i%3 != 2 {
			t.Fatalf("item %d's tasks were interleaved with another item's: %v", got[i], got)
		}
	}
}

// TestFanOutOverSubscribedPoolDrains: scheduling far more tasks than a pool's capacity is fine — they drain
// through the pool's own completions.
func TestFanOutOverSubscribedPoolDrains(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")).SetLimit(2)

	var done atomic.Int64
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, pool.NewTasks(50, func(_ context.Context, i int) error {
				done.Add(1)
				return nil
			}))
		}
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
	if got := done.Load(); got != 50 {
		t.Fatalf("%d of 50 tasks ran", got)
	}
}

// TestFanOutMultiplePoolsDifferentLimits exercises several pools with different capacities in one wave.
func TestFanOutMultiplePoolsDifferentLimits(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(2)
	seq := fo.AddPool(OptName("seq"))
	par := fo.AddPool(OptName("par")).SetLimit(3)
	commit := c.AddStage(OptName("commit"))

	seqG, parG := &gauge{}, &gauge{}
	runNOK(t, c, 8, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx,
				seq.NewTasks(2, func(_ context.Context, i int) error { return seqG.hold(func() error { return nil }) }),
				par.NewTasks(6, func(_ context.Context, i int) error { return parG.hold(func() error { return nil }) }),
			)
		}
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	if peak, entries := seqG.snapshot(); peak != 1 || entries < 16 {
		t.Fatalf("sequential lane: peak=%d entries=%d, want peak 1 and >= 16", peak, entries)
	}
	if peak, entries := parG.snapshot(); peak > 3 || entries < 48 {
		t.Fatalf("parallel lane: peak=%d entries=%d, want peak <= 3 and >= 48", peak, entries)
	}
}

// TestFanOutEmptyScheduleIsLegal: scheduling nothing is legal — the item is inside the node with an empty body, and
// detaching it hands back an already-finished wave.
func TestFanOutEmptyScheduleIsLegal(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	for _, tc := range []struct {
		name  string
		tasks []Task
	}{
		{name: "nil tasks", tasks: nil},
		{name: "empty tasks", tasks: []Task{}},
		{name: "statically empty task", tasks: []Task{pool.NewTasks(0, func(context.Context, int) error { return nil })}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reached bool
			err := runOnce(t, c, func(ctx context.Context) error {
				err := fo.MoveTo(ctx)
				if err == nil {
					err = fo.Schedule(ctx, tc.tasks...)
				}
				if err != nil {
					return err
				}
				w := fo.Detach(ctx)
				select {
				case <-w.Finished():
				default:
					t.Errorf("an empty wave should be born finished")
				}
				if err := w.Err(); err != nil {
					t.Errorf("empty wave error = %v, want nil", err)
				}
				if err := commit.MoveTo(ctx); err != nil {
					return err
				}
				reached = true
				return nil
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("run failed: %v", err)
			}
			if !reached {
				t.Fatalf("the item did not reach commit after an empty fan-out move")
			}
		})
	}
}

// TestFanOutSubsetOfPools: an item may use only some of the branches; unused ones must not block anything.
func TestFanOutSubsetOfPools(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(2)
	a := fo.AddPool(OptName("a"))
	b := fo.AddPool(OptName("b"))
	commit := c.AddStage(OptName("commit"))

	var aRuns, bRuns atomic.Int64
	runNOK(t, c, 12, func(ctx context.Context, no int64) error {
		var tasks []Task
		if no%2 == 1 {
			tasks = append(tasks, a.NewTask(func(context.Context) error { aRuns.Add(1); return nil }))
		} else {
			tasks = append(tasks, b.NewTask(func(context.Context) error { bRuns.Add(1); return nil }))
		}
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, tasks...)
		}
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	if aRuns.Load() < 5 || bRuns.Load() < 5 {
		t.Fatalf("lane usage a=%d b=%d, want both >= 5", aRuns.Load(), bRuns.Load())
	}
}

// TestFanOutJoinHappensBeforeNewWorkStarts: waves named in a fan-out move are joined *before* the new work is
// enqueued, so scheduled work may depend on them.
func TestFanOutJoinHappensBeforeNewWorkStarts(t *testing.T) {
	c := NewConveyor()
	first := c.AddFanOut(OptName("first"))
	firstPool := first.AddPool(OptName("firstPool"))
	second := c.AddFanOut(OptName("second"))
	secondPool := second.AddPool(OptName("secondPool"))
	commit := c.AddStage(OptName("commit"))

	var events recorder
	err := runOnce(t, c, func(ctx context.Context) error {
		err := first.MoveTo(ctx)
		if err == nil {
			err = first.Schedule(ctx, firstPool.NewTasks(3, func(_ context.Context, i int) error {
				events.add("first-%d", i)
				return nil
			}))
		}
		if err != nil {
			return err
		}
		w1 := first.Detach(ctx)
		err = second.MoveTo(ctx, w1)
		if err == nil {
			err = second.Schedule(ctx, secondPool.NewTasks(2, func(_ context.Context, i int) error {
				events.add("second-%d", i)
				return nil
			}))
		} // join w1 here: all of first's work must be done before second's starts
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}

	got := events.all()
	if len(got) != 5 {
		t.Fatalf("events = %v, want 5", got)
	}
	for i, ev := range got {
		wantPrefix := "first-"
		if i >= 3 {
			wantPrefix = "second-"
		}
		if len(ev) < len(wantPrefix) || ev[:len(wantPrefix)] != wantPrefix {
			t.Fatalf("event %d = %q, want prefix %q (order: %v)", i, ev, wantPrefix, got)
		}
	}
}

// TestDeferredJoinOverlapsInlineWork: not naming the wave at the next stage is what buys the overlap — the stage's
// inline work runs while the wave is still going.
func TestDeferredJoinOverlapsInlineWork(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	transform := c.AddStage(OptName("transform"))
	commit := c.AddStage(OptName("commit"))

	taskRunning := make(chan struct{})
	inlineRan := make(chan struct{})
	var overlapped atomic.Bool

	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
				close(taskRunning)
				<-inlineRan // the task is still running while the inline stage works
				return nil
			}))
		}
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		if err := transform.MoveTo(ctx); err != nil { // no join here
			return err
		}
		<-taskRunning
		select {
		case <-w.Finished():
			// The wave finished before the inline work: no overlap.
		default:
			overlapped.Store(true)
		}
		close(inlineRan)
		return commit.MoveTo(ctx) // joined here
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !overlapped.Load() {
		t.Fatalf("the deferred wave did not overlap the inline stage work")
	}
}

// TestFanOutTaskErrorFailsRun: a task error is fail-fast — it cancels the item and becomes Run's error.
func TestFanOutTaskErrorFailsRun(t *testing.T) {
	boom := errors.New("task boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	var committed atomic.Bool
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		var tasks []Task
		if no == 1 {
			tasks = append(tasks, pool.NewTask(func(context.Context) error { return boom }))
		} else {
			tasks = append(tasks, pool.NewTask(func(context.Context) error { return nil }))
		}
		err := fo.MoveTo(ic)
		if err == nil {
			err = fo.Schedule(ic, tasks...)
		}
		if err != nil {
			return err
		}
		if err := commit.MoveTo(ic); err != nil {
			return err
		}
		if no == 1 {
			committed.Store(true)
		}
		return nil
	})

	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
	if committed.Load() {
		t.Fatalf("the item ran its commit work although the joined wave had failed")
	}
}

// TestFanOutSiblingTasksSeeCancellation: when one task fails, the item's context is canceled so its siblings can
// bail out promptly.
func TestFanOutSiblingTasksSeeCancellation(t *testing.T) {
	boom := errors.New("sibling boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	failing := fo.AddPool(OptName("failing"))
	waiting := fo.AddPool(OptName("waiting"))

	sawCancel := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		err := fo.MoveTo(ic)
		if err == nil {
			err = fo.Schedule(ic,
				failing.NewTask(func(context.Context) error { return boom }),
				waiting.NewTask(func(context.Context) error {
					<-ic.Done() // must be released by the fail-fast cancellation
					close(sawCancel)
					return nil
				}),
			)
		}
		if err != nil {
			return err
		}
		w := fo.Detach(ic)
		<-w.Finished()
		return w.Err()
	})

	<-sawCancel
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
}

// TestFanOutTasksFromForeignBranchPanics: a wiring mistake must be loud; Schedule panics before touching the context.
func TestFanOutTasksFromForeignBranchPanics(t *testing.T) {
	c := NewConveyor()
	fo1 := c.AddFanOut(OptName("fo1"))
	pool1 := fo1.AddPool(OptName("pool1"))
	fo2 := c.AddFanOut(OptName("fo2"))
	_ = fo2.AddPool(OptName("lane2"))

	err := runOnce(t, c, func(ctx context.Context) error {
		assertPanics(t, errInvalidUnit, func() {
			_ = fo2.MoveTo(ctx)
			_ = fo2.Schedule(ctx, pool1.NewTask(func(context.Context) error { return nil }))
		})
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestTaskReusePanics: tasks are single-use, even within one Schedule call.
func TestTaskReusePanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	err := runOnce(t, c, func(ctx context.Context) error {
		task := pool.NewTask(func(context.Context) error { return nil })
		assertPanics(t, errTaskReused, func() {
			_ = fo.MoveTo(ctx)
			_ = fo.Schedule(ctx, task, task)
		})
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Logf("run ended with %v", err)
	}
}

// TestFanOutBranchesAccessor covers the inspection helper.
func TestFanOutBranchesAccessor(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	a := fo.AddPool(OptName("a"))
	b := fo.AddPool()

	branches := fo.Branches()
	if len(branches) != 2 || branches[0] != a || branches[1] != b {
		t.Fatalf("Branches() = %v, want [a b]", branches)
	}
	branches[0] = nil // must be a copy
	if fo.Branches()[0] != a {
		t.Fatalf("Branches() handed out its internal slice")
	}
	if got := fmt.Sprint(b); got != "fo.2" {
		t.Fatalf("unnamed branch name = %q, want %q", got, "fo.2")
	}
}
