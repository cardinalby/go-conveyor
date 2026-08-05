package conveyor

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
)

// TestNewTaskRunsExactlyOneCallback: NewTask is one callback = one slot's worth of work, and the callback receives a
// usable item context.
func TestNewTaskRunsExactlyOneCallback(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	var runs atomic.Int64
	var ctxUsable atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTask(func(tctx context.Context) error {
			runs.Add(1)
			no, ok := ItemNoFromContext(tctx)
			ctxUsable.Store(ok && no == 1 && tctx.Err() == nil)
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
	if got := runs.Load(); got != 1 {
		t.Fatalf("NewTask ran its callback %d times, want exactly 1", got)
	}
	if !ctxUsable.Load() {
		t.Fatalf("the callback's context did not carry a live item")
	}
}

// TestNewTasksAreBuiltLazilyOnePerFreedSlot: NewTasks materializes one callback per freed slot, so on a limit-1
// pool only one exists at a time, the indexes start in order, and the wave is not fully handed out until the last
// one has been pulled.
func TestNewTasksAreBuiltLazilyOnePerFreedSlot(t *testing.T) {
	const count = 5
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // limit 1

	var order numbers
	g := &gauge{}
	firstIn := make(chan struct{})
	release := make(chan struct{})
	var startedWhileBlocked int
	var stillHandingOut atomic.Bool

	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTasks(count, func(_ context.Context, i int) error {
			return g.hold(func() error {
				order.add(int64(i))
				if i == 0 {
					close(firstIn)
					<-release
				}
				return nil
			})
		})})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		<-firstIn
		// Index 0 holds the pool's only slot: no later index can have been built or started, and the wave cannot
		// have handed out all of its work yet.
		startedWhileBlocked = len(order.all())
		select {
		case <-w.Started():
		default:
			stillHandingOut.Store(true)
		}
		close(release)
		<-w.Started()
		<-w.Finished()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if startedWhileBlocked != 1 {
		t.Fatalf("%d callbacks had started while index 0 held the only slot, want exactly 1", startedWhileBlocked)
	}
	if !stillHandingOut.Load() {
		t.Fatalf("the wave was fully handed out while index 0 still held the only slot: the callbacks were not lazy")
	}
	assertStrictlyIncreasing(t, incremented(order.all()), "NewTasks index order on a limit-1 pool")
	if peak, entries := g.snapshot(); peak != 1 || entries != count {
		t.Fatalf("pool peak=%d entries=%d, want peak 1 and %d callbacks", peak, entries, count)
	}
}

// TestNewTasksNonPositiveCountIsNoOp: a count <= 0 yields an empty task — no callback is built and the wave is born
// finished.
func TestNewTasksNonPositiveCountIsNoOp(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	for _, count := range []int{0, -1, -7} {
		var runs atomic.Int64
		var bornFinished atomic.Bool
		err := runOnce(t, c, func(ctx context.Context) error {
			err := fo.MoveTo(ctx, Tasks{pool.NewTasks(count, func(context.Context, int) error {
				runs.Add(1)
				return nil
			})})
			if err != nil {
				return err
			}
			w := fo.Detach(ctx)
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
			t.Fatalf("count %d: the wave was not born finished", count)
		}
	}
}

// TestNewTasksGenPulledOnePerFreedSlot: the generator is pulled one item at a time as slots free — never drained
// upfront — and the wave is handed out only once the generator is exhausted.
func TestNewTasksGenPulledOnePerFreedSlot(t *testing.T) {
	const count = 5
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // limit 1

	var pulls atomic.Int64
	var order numbers
	firstIn := make(chan struct{})
	release := make(chan struct{})
	var pullsWhileBlocked int64
	var stillHandingOut atomic.Bool

	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTasksGen(func(yield func(TaskFunc) bool) {
			for i := 0; i < count; i++ {
				pulls.Add(1)
				i := i
				if !yield(func(context.Context) error {
					order.add(int64(i))
					if i == 0 {
						close(firstIn)
						<-release
					}
					return nil
				}) {
					return
				}
			}
		})})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		<-firstIn
		pullsWhileBlocked = pulls.Load()
		select {
		case <-w.Started():
		default:
			stillHandingOut.Store(true)
		}
		close(release)
		<-w.Started()
		<-w.Finished()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if pullsWhileBlocked != 1 {
		t.Fatalf("the generator had been pulled %d times while the first callback held the only slot, want 1", pullsWhileBlocked)
	}
	if !stillHandingOut.Load() {
		t.Fatalf("Started closed before the generator was exhausted")
	}
	if got := pulls.Load(); got != count {
		t.Fatalf("the generator was pulled %d times, want %d", got, count)
	}
	assertStrictlyIncreasing(t, incremented(order.all()), "generated callback order on a limit-1 pool")
}

// TestNewTasksGenPullIsSerializedAndHoldsASlot: at most one pull of a generator is ever in flight, and the slot it
// will fill is reserved meanwhile — so a pool with free capacity starts nothing else from that source.
func TestNewTasksGenPullIsSerializedAndHoldsASlot(t *testing.T) {
	const count = 3
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")).SetLimit(3)

	var started atomic.Int64
	pulling := make(chan struct{}) // closed once the second pull is in flight
	resume := make(chan struct{})  // releases both the first callback and the blocked pull

	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTasksGen(func(yield func(TaskFunc) bool) {
			for i := 0; i < count; i++ {
				if i == 1 {
					close(pulling) // the generator body only runs while a pull is in flight
					<-resume
				}
				i := i
				if !yield(func(context.Context) error {
					started.Add(1)
					if i == 0 {
						<-resume
					}
					return nil
				}) {
					return
				}
			}
		})})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		<-pulling
		waitFor(t, "the pending pull to reserve a pool slot", func() bool {
			return started.Load() == 1 && occupancyOf(c, pool) == 2
		})
		// One slot runs callback 0, one is reserved for the pull in flight — and the third stays free, because a
		// second pull may not run concurrently with the first.
		if got := started.Load(); got != 1 {
			t.Errorf("%d callbacks started while a pull was in flight, want exactly 1", got)
		}
		if got := occupancyOf(c, pool); got != 2 {
			t.Errorf("pool occupancy during a pull = %d, want 2 (one running, one reserved)", got)
		}
		close(resume)
		<-w.Finished()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if got := started.Load(); got != count {
		t.Fatalf("%d of %d generated callbacks ran", got, count)
	}
}

// TestNewTasksGenStopsPullingWhenItemIsCanceled: once the item's context is canceled the generator is no longer
// pulled and the ungenerated work is skipped, so the number of pulls is bounded by what the pool consumed rather
// than by the generator's total.
func TestNewTasksGenStopsPullingWhenItemIsCanceled(t *testing.T) {
	boom := errors.New("gen boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // limit 1: pulls and callbacks strictly alternate

	var pulls, ran atomic.Int64
	err := runOnce(t, c, func(ctx context.Context) error {
		ferr := fo.MoveTo(ctx, Tasks{pool.NewTasksGen(func(yield func(TaskFunc) bool) {
			for i := 0; i < 1000; i++ {
				pulls.Add(1)
				i := i
				if !yield(func(context.Context) error {
					ran.Add(1)
					if i == 2 {
						return boom // fail-fast: cancels the item, so nothing more is pulled
					}
					return nil
				}) {
					return
				}
			}
		})})
		if ferr != nil {
			return ferr
		}
		w := fo.Detach(ctx)
		<-w.Started()
		<-w.Finished()
		return w.Err()
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
	if got := pulls.Load(); got != 3 {
		t.Fatalf("the generator was pulled %d times, want exactly 3 (the work the pool consumed)", got)
	}
	if got := ran.Load(); got != 3 {
		t.Fatalf("%d generated callbacks ran, want exactly 3: the rest must be skipped", got)
	}
}

// TestNewTasksGenNilIsNoOp: a nil generator schedules nothing.
func TestNewTasksGenNilIsNoOp(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	var bornFinished, committed atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTasksGen(nil)})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		select {
		case <-w.Finished():
			bornFinished.Store(true)
		default:
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		committed.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !bornFinished.Load() || !committed.Load() {
		t.Fatalf("a nil generator was not a no-op (bornFinished=%v committed=%v)",
			bornFinished.Load(), committed.Load())
	}
}

// TestNewTasksChanArrivesAsSentAndStartedWaitsForClose: work arrives as the producer (running in its own goroutine,
// concurrently with the ItemProcessor) sends it, and an open channel means "more work coming" — the wave is handed
// out only once the channel is closed.
func TestNewTasksChanArrivesAsSentAndStartedWaitsForClose(t *testing.T) {
	const count = 3
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // limit 1
	mid := c.AddStage(OptName("mid"))

	var order numbers
	ranCh := make([]chan struct{}, count)
	for i := range ranCh {
		ranCh[i] = make(chan struct{})
	}
	ch := make(chan TaskFunc)
	sentAll := make(chan struct{})
	letClose := make(chan struct{})
	var openWhileSending atomic.Bool

	err := runOnce(t, c, func(ctx context.Context) error {
		go func() { // the producer must run in its own goroutine
			defer close(ch)
			for i := 0; i < count; i++ {
				if i > 0 {
					select { // send the next piece only after the previous one has run
					case <-ranCh[i-1]:
					case <-ctx.Done():
						return
					}
				}
				i := i
				fn := func(context.Context) error {
					order.add(int64(i))
					close(ranCh[i])
					return nil
				}
				select {
				case ch <- fn:
				case <-ctx.Done():
					return
				}
			}
			close(sentAll)
			<-letClose
		}()

		err := fo.MoveTo(ctx, Tasks{pool.NewTasksChan(ch)})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		if err := mid.MoveTo(ctx); err != nil { // the conveyor consumes the channel while the item moves on
			return err
		}
		<-sentAll
		select {
		case <-w.Started():
		default:
			openWhileSending.Store(true)
		}
		close(letClose)
		<-w.Started()
		<-w.Finished()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !openWhileSending.Load() {
		t.Fatalf("Started closed while the channel was still open")
	}
	assertStrictlyIncreasing(t, incremented(order.all()), "channel-sourced callback order")
	if got := len(order.all()); got != count {
		t.Fatalf("%d of %d sent callbacks ran", got, count)
	}
}

// TestNewTasksChanStopsConsumingWhenItemIsCanceled: cancellation of the item ends consumption, and the producer's
// blocked send is released by the same context.
func TestNewTasksChanStopsConsumingWhenItemIsCanceled(t *testing.T) {
	boom := errors.New("chan boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // limit 1: receives and callbacks strictly alternate

	var sent, ran atomic.Int64
	ch := make(chan TaskFunc)
	producerDone := make(chan struct{})

	err := runOnce(t, c, func(ctx context.Context) error {
		go func() {
			defer close(producerDone)
			defer close(ch)
			for i := 0; i < 50; i++ {
				i := i
				fn := func(context.Context) error {
					ran.Add(1)
					if i == 1 {
						return boom // fail-fast: cancels the item, so nothing more is received
					}
					return nil
				}
				select {
				case ch <- fn:
					sent.Add(1)
				case <-ctx.Done():
					return
				}
			}
		}()

		ferr := fo.MoveTo(ctx, Tasks{pool.NewTasksChan(ch)})
		if ferr != nil {
			return ferr
		}
		w := fo.Detach(ctx)
		<-w.Finished()
		return w.Err()
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
	<-producerDone // the producer's pending send must be released by the item's cancellation
	if got := ran.Load(); got != 2 {
		t.Fatalf("%d received callbacks ran, want exactly 2: consumption must stop at the cancellation", got)
	}
	if got := sent.Load(); got != 2 {
		t.Fatalf("%d callbacks were consumed from the channel, want exactly 2", got)
	}
}

// TestNewTasksChanNilIsNoOp: a nil channel schedules nothing.
func TestNewTasksChanNilIsNoOp(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	var bornFinished, committed atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTasksChan(nil)})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		select {
		case <-w.Finished():
			bornFinished.Store(true)
		default:
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		committed.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !bornFinished.Load() || !committed.Load() {
		t.Fatalf("a nil channel was not a no-op (bornFinished=%v committed=%v)",
			bornFinished.Load(), committed.Load())
	}
}

// TestMixedSourcesOnOnePoolConsumeInSubmissionOrder: several sources submitted for one pool in a single MoveTo are
// one collection — consumed in submission order, and all covered by the one wave.
func TestMixedSourcesOnOnePoolConsumeInSubmissionOrder(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // limit 1: the order is total

	var events recorder
	want := []string{"single", "count-0", "count-1", "gen-0", "gen-1", "chan-0", "chan-1"}

	err := runOnce(t, c, func(ctx context.Context) error {
		ch := make(chan TaskFunc, 2)
		for i := 0; i < 2; i++ {
			i := i
			ch <- func(context.Context) error { events.add("chan-%d", i); return nil }
		}
		close(ch)

		err := fo.MoveTo(ctx, Tasks{
			pool.NewTask(func(context.Context) error { events.add("single"); return nil }),
			pool.NewTasks(2, func(_ context.Context, i int) error { events.add("count-%d", i); return nil }),
			pool.NewTasksGen(func(yield func(TaskFunc) bool) {
				for i := 0; i < 2; i++ {
					i := i
					if !yield(func(context.Context) error { events.add("gen-%d", i); return nil }) {
						return
					}
				}
			}),
			pool.NewTasksChan(ch),
		})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		<-w.Finished()
		if got := len(events.all()); got != len(want) {
			t.Errorf("%d callbacks had run when the wave finished, want all %d", got, len(want))
		}
		return w.Err()
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

// TestMixedSourcesAcrossPoolsInOneMove: sources for different pools submitted in one MoveTo each become that pool's
// collection, and the single wave covers all of them.
func TestMixedSourcesAcrossPoolsInOneMove(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	a := fo.AddPool(OptName("a"))
	b := fo.AddPool(OptName("b"))
	d := fo.AddPool(OptName("d"))

	var aEvents, bEvents, dEvents numbers
	err := runOnce(t, c, func(ctx context.Context) error {
		ch := make(chan TaskFunc, 2)
		for i := 0; i < 2; i++ {
			i := i
			ch <- func(context.Context) error { dEvents.add(int64(i)); return nil }
		}
		close(ch)

		err := fo.MoveTo(ctx, Tasks{
			a.NewTask(func(context.Context) error { aEvents.add(0); return nil }),
			a.NewTasks(2, func(_ context.Context, i int) error { aEvents.add(int64(i + 1)); return nil }),
			b.NewTasksGen(func(yield func(TaskFunc) bool) {
				for i := 0; i < 3; i++ {
					i := i
					if !yield(func(context.Context) error { bEvents.add(int64(i)); return nil }) {
						return
					}
				}
			}),
			d.NewTasksChan(ch),
		})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		<-w.Finished()
		if la, lb, ld := len(aEvents.all()), len(bEvents.all()), len(dEvents.all()); la != 3 || lb != 3 || ld != 2 {
			t.Errorf("when the wave finished: a=%d b=%d d=%d, want 3/3/2", la, lb, ld)
		}
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	assertStrictlyIncreasing(t, incremented(aEvents.all()), "lane a order")
	assertStrictlyIncreasing(t, incremented(bEvents.all()), "lane b order")
	assertStrictlyIncreasing(t, incremented(dEvents.all()), "lane d order")
}

// TestStreamingSourcesFeedLaneWithInteriorStages: each callback a streaming source yields becomes a child item that
// travels the lane's interior stages.
func TestStreamingSourcesFeedLaneWithInteriorStages(t *testing.T) {
	const items, perSource = 2, 3
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(2)
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid")) // exclusive interior stage
	commit := c.AddStage(OptName("commit"))

	midGauge := &gauge{}
	var children atomic.Int64
	runNOK(t, c, items, func(ctx context.Context, no int64) error {
		ch := make(chan TaskFunc, perSource)
		for i := 0; i < perSource; i++ {
			ch <- func(cctx context.Context) error {
				if err := mid.MoveTo(cctx); err != nil {
					return err
				}
				return midGauge.hold(func() error { children.Add(1); return nil })
			}
		}
		close(ch)

		err := fo.MoveTo(ctx, Tasks{
			lane.NewTasksGen(func(yield func(TaskFunc) bool) {
				for i := 0; i < perSource; i++ {
					if !yield(func(cctx context.Context) error {
						if err := mid.MoveTo(cctx); err != nil {
							return err
						}
						return midGauge.hold(func() error { children.Add(1); return nil })
					}) {
						return
					}
				}
			}),
			lane.NewTasksChan(ch),
		})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	if got := children.Load(); got != items*perSource*2 {
		t.Fatalf("%d streamed children finished their journey, want %d", got, items*perSource*2)
	}
	if peak, _ := midGauge.snapshot(); peak != 1 {
		t.Fatalf("the exclusive interior stage ran %d children at once", peak)
	}
}

// TestOverSubscribedStreamingPoolDrains: far more callbacks than the lane's limit all run, draining through the
// lane's own completions, whichever source they came from.
func TestOverSubscribedStreamingPoolDrains(t *testing.T) {
	const each = 20
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")).SetLimit(2)

	g := &gauge{}
	err := runOnce(t, c, func(ctx context.Context) error {
		ch := make(chan TaskFunc, each)
		for i := 0; i < each; i++ {
			ch <- func(context.Context) error { return g.hold(func() error { return nil }) }
		}
		close(ch)

		err := fo.MoveTo(ctx, Tasks{
			pool.NewTask(func(context.Context) error { return g.hold(func() error { return nil }) }),
			pool.NewTasks(each, func(context.Context, int) error { return g.hold(func() error { return nil }) }),
			pool.NewTasksGen(func(yield func(TaskFunc) bool) {
				for i := 0; i < each; i++ {
					if !yield(func(context.Context) error { return g.hold(func() error { return nil }) }) {
						return
					}
				}
			}),
			pool.NewTasksChan(ch),
		})
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

// TestGenSourceStoppedWhenItsWorkIsDropped: a generator suspends a coroutine between pulls, and the only thing that
// resumes it is being stopped. Every ordinary ending does that from the pull itself — exhausted, nil yield, item
// canceled mid-pull — but an item canceled *between* pulls is different: its collection is dropped without another
// pull ever happening, so the drop is what has to let the generator go.
//
// One lane per generator, so a single canceled item leaves as many suspended generators behind as there are lanes and
// the leak is far larger than the tolerance for the run's own transient goroutines.
func TestGenSourceStoppedWhenItsWorkIsDropped(t *testing.T) {
	const lanes = 12
	boom := errors.New("boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	var ls []Pool
	for i := 0; i < lanes; i++ {
		ls = append(ls, fo.AddPool(OptName(sprintf("lane%d", i)))) // limit 1: only the first callback starts
	}

	base := runtime.NumGoroutine()
	var started atomic.Int64
	err := runOnce(t, c, func(ctx context.Context) error {
		var tasks Tasks
		for _, l := range ls {
			tasks = append(tasks, l.NewTasksGen(func(yield func(TaskFunc) bool) {
				for i := 0; i < 50; i++ { // plenty left ungenerated when the item is canceled
					if !yield(func(context.Context) error { started.Add(1); return nil }) {
						return
					}
				}
			}))
		}
		if err := fo.MoveTo(ctx, tasks); err != nil {
			return err
		}
		waitFor(t, "every lane to have pulled from its generator", func() bool {
			return started.Load() >= lanes
		})
		return boom // cancels the item, so the rest of every generator's work is dropped unpulled
	})
	if !errors.Is(err, boom) {
		t.Fatalf("run error = %v, want %v", err, boom)
	}
	if !waitFor(t, "the generators' pull coroutines to be released", func() bool {
		return runtime.NumGoroutine() <= base+3
	}) {
		t.Errorf("goroutines settled at %d, started from %d: %d suspended generators were never stopped",
			runtime.NumGoroutine(), base, runtime.NumGoroutine()-base)
	}
}
