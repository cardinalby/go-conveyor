package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// TestNewTaskRunsExactlyOneCallback: NewTask is one callback = one slot's worth of work, and the callback receives a
// usable item context.
func TestNewTaskRunsExactlyOneCallback(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))

	var runs atomic.Int64
	var ctxUsable atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, pool.NewTask(func(tctx context.Context) error {
				runs.Add(1)
				no, ok := ItemNoFromContext(tctx)
				ctxUsable.Store(ok && no == 1 && tctx.Err() == nil)
				return nil
			}))
		}
		if err != nil {
			return err
		}
		w := fo.Retain(ctx)
		return w.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("NewTask ran its callback %d times, want exactly 1", got)
	}
	if !ctxUsable.Load() {
		t.Fatalf("the callback's context did not carry a live item")
	}
}

// TestNewTasksAreBuiltLazilyOnePerFreedSlot: NewTasks materializes one callback per freed slot, so on a limit-1
// pool only one exists at a time, the indexes start in order, and the task group is not fully handed out until the last
// one has been pulled.
func TestNewTasksAreBuiltLazilyOnePerFreedSlot(t *testing.T) {
	const count = 5
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool")) // limit 1

	var order numbers
	g := &gauge{}
	firstIn := make(chan struct{})
	release := make(chan struct{})
	var startedWhileBlocked int
	var stillHandingOut atomic.Bool

	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, pool.NewTasks(count, func(_ context.Context, i int) error {
				return g.hold(func() error {
					order.add(int64(i))
					if i == 0 {
						close(firstIn)
						<-release
					}
					return nil
				})
			}))
		}
		if err != nil {
			return err
		}
		w := fo.Retain(ctx)
		<-firstIn
		// Index 0 holds the pool's only slot: no later index can have been built or started, and the task group cannot
		// have handed out all of its work yet.
		startedWhileBlocked = len(order.all())
		// The rest of the collection is still in the pool's backlog.
		stillHandingOut.Store(queueOccupancy(c, pool) == 1)
		close(release)
		return w.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if startedWhileBlocked != 1 {
		t.Fatalf("%d callbacks had started while index 0 held the only slot, want exactly 1", startedWhileBlocked)
	}
	if !stillHandingOut.Load() {
		t.Fatalf("the task group was fully handed out while index 0 still held the only slot: the callbacks were not lazy")
	}
	assertStrictlyIncreasing(t, incremented(order.all()), "NewTasks index order on a limit-1 pool")
	if peak, entries := g.snapshot(); peak != 1 || entries != count {
		t.Fatalf("pool peak=%d entries=%d, want peak 1 and %d callbacks", peak, entries, count)
	}
}

// TestNewTasksZeroCountIsNoOp: a count of 0 yields an empty task — no callback is built and the task group is born
// finished.
func TestNewTasksZeroCountIsNoOp(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))
	commit := c.AddStage(WithName("commit"))

	for _, count := range []int{0} {
		var runs atomic.Int64
		var bornFinished atomic.Bool
		err := runOnce(t, c, func(ctx context.Context) error {
			err := fo.MoveTo(ctx)
			if err == nil {
				err = fo.Schedule(ctx, pool.NewTasks(count, func(context.Context, int) error {
					runs.Add(1)
					return nil
				}))
			}
			if err != nil {
				return err
			}
			w := fo.Retain(ctx)
			select {
			case <-w.Finished():
				bornFinished.Store(true)
			default:
			}
			return commit.MoveTo(ctx)
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("count %d: run failed: %v", count, err)
		}
		if got := runs.Load(); got != 0 {
			t.Fatalf("count %d: %d callbacks ran, want none", count, got)
		}
		if !bornFinished.Load() {
			t.Fatalf("count %d: the task group was not born finished", count)
		}
	}
}

// TestMixedSourcesOnOnePoolConsumeInSubmissionOrder: several sources submitted for one pool in a single MoveTo are
// one collection — consumed in submission order, and all covered by the one task group.
func TestMixedSourcesOnOnePoolConsumeInSubmissionOrder(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool")) // limit 1: the order is total

	var events recorder
	want := []string{"single", "count-0", "count-1", "single-2", "count2-0", "count2-1"}

	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx,
				pool.NewTask(func(context.Context) error { events.add("single"); return nil }),
				pool.NewTasks(2, func(_ context.Context, i int) error { events.add("count-%d", i); return nil }),
				pool.NewTask(func(context.Context) error { events.add("single-2"); return nil }),
				pool.NewTasks(2, func(_ context.Context, i int) error { events.add("count2-%d", i); return nil }),
			)
		}
		if err != nil {
			return err
		}
		w := fo.Retain(ctx)
		werr := w.Wait(ctx)
		if got := len(events.all()); got != len(want) {
			t.Errorf("%d callbacks had run when the task group finished, want all %d", got, len(want))
		}
		return werr
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	got := events.all()
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

// TestMixedSourcesAcrossPoolsInOneSchedule: sources for different pools submitted in one Schedule each become that
// pool's collection, and the single task group covers all of them.
func TestMixedSourcesAcrossPoolsInOneSchedule(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	a := fo.AddPool(WithName("a"))
	b := fo.AddPool(WithName("b"))
	d := fo.AddPool(WithName("d"))

	var aEvents, bEvents, dEvents numbers
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx,
				a.NewTask(func(context.Context) error { aEvents.add(0); return nil }),
				a.NewTasks(2, func(_ context.Context, i int) error { aEvents.add(int64(i + 1)); return nil }),
				b.NewTasks(3, func(_ context.Context, i int) error { bEvents.add(int64(i)); return nil }),
				d.NewTask(func(context.Context) error { dEvents.add(0); return nil }),
				d.NewTask(func(context.Context) error { dEvents.add(1); return nil }),
			)
		}
		if err != nil {
			return err
		}
		w := fo.Retain(ctx)
		werr := w.Wait(ctx)
		if la, lb, ld := len(aEvents.all()), len(bEvents.all()), len(dEvents.all()); la != 3 || lb != 3 || ld != 2 {
			t.Errorf("when the task group finished: a=%d b=%d d=%d, want 3/3/2", la, lb, ld)
		}
		return werr
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	assertStrictlyIncreasing(t, incremented(aEvents.all()), "lane a order")
	assertStrictlyIncreasing(t, incremented(bEvents.all()), "lane b order")
	assertStrictlyIncreasing(t, incremented(dEvents.all()), "lane d order")
}

// TestMixedSourcesFeedLaneWithInteriorStages: each callback of every source kind becomes a child item that travels
// the lane's interior stages.
func TestMixedSourcesFeedLaneWithInteriorStages(t *testing.T) {
	const items, perSource = 2, 3
	c := New()
	fo := c.AddFanOut(WithName("fo")).SetLimit(2)
	lane := fo.AddLane(WithName("lane"))
	mid := lane.AddStage(WithName("mid")) // exclusive interior stage
	commit := c.AddStage(WithName("commit"))

	midGauge := &gauge{}
	var children atomic.Int64
	runNOK(t, c, items, func(ctx context.Context, no int64) error {
		journey := func(cctx context.Context) error {
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			return midGauge.hold(func() error { children.Add(1); return nil })
		}
		tasks := []Task{lane.NewTasks(perSource, func(cctx context.Context, _ int) error { return journey(cctx) })}
		for i := 0; i < perSource; i++ {
			tasks = append(tasks, lane.NewTask(journey))
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

	if got := children.Load(); got != items*perSource*2 {
		t.Fatalf("%d children finished their journey, want %d", got, items*perSource*2)
	}
	if peak, _ := midGauge.snapshot(); peak != 1 {
		t.Fatalf("the exclusive interior stage ran %d children at once", peak)
	}
}

// TestOverSubscribedPoolDrains: far more callbacks than the lane's limit all run, draining through the lane's own
// completions, whichever source they came from.
func TestOverSubscribedPoolDrains(t *testing.T) {
	const each = 20
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool")).SetLimit(2)

	g := &gauge{}
	err := runOnce(t, c, func(ctx context.Context) error {
		tasks := []Task{
			pool.NewTask(func(context.Context) error { return g.hold(func() error { return nil }) }),
			pool.NewTasks(each, func(context.Context, int) error { return g.hold(func() error { return nil }) }),
		}
		for i := 0; i < 2*each; i++ {
			tasks = append(tasks, pool.NewTask(func(context.Context) error { return g.hold(func() error { return nil }) }))
		}

		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, tasks...)
		}
		if err != nil {
			return err
		}
		w := fo.Retain(ctx)
		return w.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	peak, entries := g.snapshot()
	if want := 3*each + 1; entries != want {
		t.Fatalf("%d callbacks ran, want all %d", entries, want)
	}
	if peak > 2 {
		t.Fatalf("pool peak = %d, want at most its limit 2", peak)
	}
}

// incremented shifts 0-based indexes to 1-based so assertStrictlyIncreasing (which expects 1..n) can check them.
func incremented(vals []int64) []int64 {
	out := make([]int64, len(vals))
	for i, v := range vals {
		out[i] = v + 1
	}
	return out
}
