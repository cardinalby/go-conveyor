package conveyor

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestSetLimitRaiseStageAdmitsWaitingItems: raising a running stage's limit admits the items already waiting at its
// door immediately — the resize itself is the wake-up, no other event is needed.
func TestSetLimitRaiseStageAdmitsWaitingItems(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	g := &gauge{}
	release := make(chan struct{})

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		waitFor(t, "the first item to fill the exclusive stage", func() bool { return occupancyOf(c, s) == 1 })
		s.SetLimit(3) // the only thing that happens from here on
		waitFor(t, "the raise to admit the waiting items", func() bool { return occupancyOf(c, s) == 3 })
		close(release)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		if err := s.MoveTo(ic); err != nil {
			return err
		}
		return g.hold(func() error {
			select {
			case <-release:
			case <-ic.Done():
			}
			return nil
		})
	})

	if peak, _ := g.snapshot(); peak != 3 {
		t.Fatalf("stage peak concurrency = %d, want the raised limit 3", peak)
	}
	if got := s.Limit(); got != 3 {
		t.Fatalf("Limit = %d, want 3", got)
	}
}

// TestSetLimitLowerStageDoesNotEvict: lowering a running stage's limit never evicts the items inside it — the stage
// shrinks as they leave, admitting nobody new until occupancy has dropped below the new limit.
func TestSetLimitLowerStageDoesNotEvict(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetLimit(3)

	holds := map[int64]chan struct{}{1: make(chan struct{}), 2: make(chan struct{}), 3: make(chan struct{})}
	var entered, left atomic.Int64
	after := &gauge{} // concurrency seen by the items admitted after the lower

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		waitFor(t, "the stage to fill to its limit", func() bool {
			return occupancyOf(c, s) == 3 && entered.Load() == 3
		})
		s.SetLimit(1)
		if got := occupancyOf(c, s); got != 3 {
			t.Errorf("lowering the limit evicted items: occupancy = %d, want 3", got)
		}
		for i := int64(1); i <= 3; i++ {
			close(holds[i])
			waitFor(t, "an item to leave the stage", func() bool { return left.Load() >= i })
			if i < 3 {
				// Occupancy is still >= 1, so the lowered stage must not have admitted anybody new.
				if got := entered.Load(); got != 3 {
					t.Errorf("%d items entered the stage while its occupancy was still at the new limit 1", got)
				}
			}
		}
		waitFor(t, "the shrunken stage to admit new items", func() bool { return entered.Load() >= 4 })
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if err := s.MoveTo(ic); err != nil {
			return err
		}
		entered.Add(1)
		if h, ok := holds[no]; ok {
			select {
			case <-h:
			case <-ic.Done():
			}
			left.Add(1)
			return nil
		}
		return after.hold(func() error { return nil })
	})

	if peak, _ := after.snapshot(); peak > 1 {
		t.Fatalf("the stage ran %d items at once after being lowered to 1", peak)
	}
}

// TestSetLimitRaisePoolStartsQueuedWork: raising a running lane's limit starts the work already queued on it at
// once.
func TestSetLimitRaisePoolStartsQueuedWork(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // one task at a time

	release := make(chan struct{})
	g := &gauge{}

	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTasks(8, func(ic context.Context, _ int) error {
			return g.hold(func() error {
				select {
				case <-release:
				case <-ic.Done():
				}
				return nil
			})
		})})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		waitFor(t, "one task to run on the sequential pool", func() bool { return occupancyOf(c, pool) == 1 })
		pool.SetLimit(4)
		waitFor(t, "the raise to start the queued work at once", func() bool {
			peak, _ := g.snapshot()
			return peak == 4
		})
		close(release)
		<-w.Finished()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if peak, entries := g.snapshot(); peak != 4 || entries != 8 {
		t.Fatalf("pool peak=%d entries=%d, want peak 4 and all 8 tasks", peak, entries)
	}
}

// TestSetLimitLowerPoolDrainsOversubscribed: lowering a running pool's limit never aborts the tasks already
// running; the pool converges to the new limit as they complete, and far more tasks than its capacity still drain.
func TestSetLimitLowerPoolDrainsOversubscribed(t *testing.T) {
	const total = 40
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")).SetLimit(4)

	gate := make(chan struct{}) // holds the first four tasks while the pool is shrunk
	later := &gauge{}           // concurrency of the tasks started after the lower
	var done atomic.Int64

	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTasks(total, func(ic context.Context, i int) error {
			if i < 4 {
				select {
				case <-gate:
				case <-ic.Done():
				}
				done.Add(1)
				return nil
			}
			return later.hold(func() error {
				done.Add(1)
				return nil
			})
		})})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		waitFor(t, "the pool to fill to its limit", func() bool { return occupancyOf(c, pool) == 4 })
		pool.SetLimit(1)
		if got := occupancyOf(c, pool); got != 4 {
			t.Errorf("lowering the limit aborted running work: pool occupancy = %d, want 4", got)
		}
		close(gate)
		<-w.Finished()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if got := done.Load(); got != total {
		t.Fatalf("%d of %d tasks ran, want all of them to drain after the lower", got, total)
	}
	if peak, entries := later.snapshot(); peak != 1 || entries != total-4 {
		t.Fatalf("after the lower: peak=%d entries=%d, want peak 1 and %d tasks", peak, entries, total-4)
	}
	if got := pool.Limit(); got != 1 {
		t.Fatalf("pool Limit = %d, want 1", got)
	}
}

// TestSetLimitRaiseFanOutAdmitsMoreItems: raising a running fan-out's limit lets more items be inside it — items
// parked at the next stage's door still occupy the node, so its occupancy grows to the new limit.
func TestSetLimitRaiseFanOutAdmitsMoreItems(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	park := make(chan struct{})
	var first atomic.Bool

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		waitFor(t, "one item to fill the fan-out", func() bool { return occupancyOf(c, fo) == 1 })
		fo.SetLimit(3)
		waitFor(t, "the raise to admit more items into the fan-out", func() bool { return occupancyOf(c, fo) == 3 })
		close(park)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		err := fo.MoveTo(ic, Tasks{pool.NewTask(func(context.Context) error { return nil })})
		if err != nil {
			return err
		}
		if err := commit.MoveTo(ic); err != nil {
			return err
		}
		if first.CompareAndSwap(false, true) {
			select { // the first item holds commit, so the followers pile up inside the fan-out
			case <-park:
			case <-ic.Done():
			}
		}
		return nil
	})

	if got := fo.Limit(); got != 3 {
		t.Fatalf("fan-out Limit = %d, want 3", got)
	}
}

// TestSetLimitLowerFanOutDoesNotEvict: lowering a running fan-out's limit evicts nobody, and once the node has
// drained the new limit bounds how many items may be inside it.
//
// Phase 1 (limit 3): item 1 parks in commit and items 2, 3, 4 pile up inside the fan-out, so item 5 waits at the
// start and 5 items exist. Phase 2 (limit 1): item 5 parks in commit and only item 6 fits inside the node, so
// exactly 7 items exist.
func TestSetLimitLowerFanOutDoesNotEvict(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(3)
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	releaseFirst := make(chan struct{})
	releaseFifth := make(chan struct{})
	var created atomic.Int64

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		waitFor(t, "the fan-out to fill to its limit", func() bool {
			return occupancyOf(c, fo) == 3 && created.Load() >= 5
		})
		if got := created.Load(); got != 5 {
			t.Errorf("%d items alive while jammed, want exactly 5", got)
		}
		fo.SetLimit(1)
		if got := occupancyOf(c, fo); got != 3 {
			t.Errorf("lowering the limit evicted items: fan-out occupancy = %d, want 3", got)
		}
		if got := created.Load(); got != 5 {
			t.Errorf("%d items alive right after the lower, want 5: the lower must admit nobody", got)
		}
		close(releaseFirst)
		waitFor(t, "the fan-out to converge to its new limit", func() bool {
			return occupancyOf(c, fo) == 1 && created.Load() >= 7
		})
		if got := created.Load(); got != 7 {
			t.Errorf("%d items alive with the lowered limit, want exactly 7", got)
		}
		close(releaseFifth)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		created.Add(1)
		err := fo.MoveTo(ic, Tasks{pool.NewTask(func(context.Context) error { return nil })})
		if err != nil {
			return err
		}
		if err := commit.MoveTo(ic); err != nil {
			return err
		}
		switch no {
		case 1:
			select {
			case <-releaseFirst:
			case <-ic.Done():
			}
		case 5:
			select {
			case <-releaseFifth:
			case <-ic.Done():
			}
		}
		return nil
	})
}

// TestSetQueueResizeIsAdmissionOnly: resizing a stage's waiting room while running follows the same admission-only
// rules — the items already queued are never evicted, and once the queue has drained the new size bounds it.
//
// Phase 1 (queue 3): item 1 holds the stage, items 2, 3, 4 wait in the queue and item 5 waits at the start, so 5
// items exist. Phase 2 (queue 1): item 5 holds the stage and only item 6 fits in the queue, so exactly 7 exist.
func TestSetQueueResizeIsAdmissionOnly(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetQueueSize(3)

	queueOcc := func() int { return queueOccupancy(c, s) }

	releaseFirst := make(chan struct{})
	releaseFifth := make(chan struct{})
	var created atomic.Int64

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		waitFor(t, "the waiting room to fill", func() bool {
			return queueOcc() == 3 && created.Load() >= 5
		})
		if got := created.Load(); got != 5 {
			t.Errorf("%d items alive while the queue was full, want exactly 5", got)
		}
		s.SetQueueSize(1)
		if got := queueOcc(); got != 3 {
			t.Errorf("resizing the queue down evicted queued items: occupancy = %d, want 3", got)
		}
		close(releaseFirst)
		waitFor(t, "the waiting room to converge to its new size", func() bool {
			return queueOcc() == 1 && created.Load() >= 7
		})
		if got := created.Load(); got != 7 {
			t.Errorf("%d items alive with the smaller waiting room, want exactly 7", got)
		}
		close(releaseFifth)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		created.Add(1)
		if err := s.MoveTo(ic); err != nil {
			return err
		}
		switch no {
		case 1:
			select {
			case <-releaseFirst:
			case <-ic.Done():
			}
		case 5:
			select {
			case <-releaseFifth:
			case <-ic.Done():
			}
		}
		return nil
	})

	if got := s.QueueSize(); got != 1 {
		t.Fatalf("QueueSize after the resize = %d, want 1", got)
	}
}

// TestSetLimitConcurrentWhileRunning: SetLimit is safe from any goroutine at any time (run with -race), and the
// limit it leaves behind is one of the values that were set.
func TestSetLimitConcurrentWhileRunning(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetLimit(2)
	fo := c.AddFanOut(OptName("fo")).SetLimit(2)
	pool := fo.AddPool(OptName("pool")).SetLimit(2)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	setters := []func(int){
		func(n int) { s.SetLimit(n) },
		func(n int) { fo.SetLimit(n) },
		func(n int) { pool.SetLimit(n) },
	}
	for i, set := range setters {
		for dup := 0; dup < 2; dup++ {
			wg.Add(1)
			go func(set func(int), seed int) {
				defer wg.Done()
				for n := seed; ; n++ {
					select {
					case <-stop:
						return
					default:
					}
					set(1 + n%4)
					runtime.Gosched()
				}
			}(set, i+dup)
		}
	}

	runNOK(t, c, 30, func(ctx context.Context, no int64) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.MoveTo(ctx, Tasks{pool.NewTasks(3, func(context.Context, int) error { return nil })})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		<-w.Finished()
		return w.Err()
	})

	close(stop)
	wg.Wait()

	for _, got := range []struct {
		what  string
		limit int
	}{{"stage", s.Limit()}, {"fan-out", fo.Limit()}, {"pool", pool.Limit()}} {
		if got.limit < 1 || got.limit > 4 {
			t.Fatalf("%s Limit = %d, want one of the values that were set (1..4)", got.what, got.limit)
		}
	}
}

// TestLimitsAreAlwaysAtLeastOne: a non-positive limit is clamped to 1 on every kind of node, and a pool clamped
// that way still drains all the work it was given.
func TestLimitsAreAlwaysAtLeastOne(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetLimit(0)
	fo := c.AddFanOut(OptName("fo")).SetLimit(-1)
	pool := fo.AddPool(OptName("pool")).SetLimit(-5)

	if got := s.Limit(); got != 1 {
		t.Fatalf("stage SetLimit(0): Limit = %d, want the clamp to 1", got)
	}
	if got := fo.Limit(); got != 1 {
		t.Fatalf("fan-out SetLimit(-1): Limit = %d, want the clamp to 1", got)
	}
	if got := pool.Limit(); got != 1 {
		t.Fatalf("pool SetLimit(-5): Limit = %d, want the clamp to 1", got)
	}

	const total = 12
	var done atomic.Int64
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.MoveTo(ctx, Tasks{pool.NewTasks(total, func(context.Context, int) error {
			done.Add(1)
			return nil
		})})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		pool.SetLimit(0) // clamped to 1 while the pool's work is still queued
		<-w.Finished()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if got := pool.Limit(); got != 1 {
		t.Fatalf("pool Limit after SetLimit(0) = %d, want 1", got)
	}
	if got := done.Load(); got != total {
		t.Fatalf("%d of %d tasks ran: a clamped pool stranded queued work", got, total)
	}
}
