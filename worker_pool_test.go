package conveyor

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
)

// liveWorkers samples the current size of the worker pool. Stats resets its window on every read, so only Last is
// meaningful to a repeated sampler.
func liveWorkers(c *Conveyor) int { return c.Stats().LiveWorkers.Last }

// recordMax raises dst to v if v is larger, for lock-free peak tracking.
func recordMax(dst *atomic.Int64, v int64) {
	for {
		cur := dst.Load()
		if v <= cur || dst.CompareAndSwap(cur, v) {
			return
		}
	}
}

// TestWorkerPoolGrowsWithItemsInFlight: one worker runs one root item, so the pool grows to at least the number of
// items the topology lets be in flight at once.
func TestWorkerPoolGrowsWithItemsInFlight(t *testing.T) {
	const gateLimit = 8
	const inFlight = gateLimit + 1 // the extra item holds the start stage while it waits for a gate slot
	c := NewConveyor()
	gate := c.AddStage(OptName("gate")).SetLimit(gateLimit)

	release := make(chan struct{})
	var arrived, peak atomic.Int64
	var triggered atomic.Bool

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		if err := gate.MoveTo(ic); err != nil {
			return err
		}
		if arrived.Add(1) == gateLimit && triggered.CompareAndSwap(false, true) {
			ok := awaitTrue(func() bool {
				n := int64(liveWorkers(c))
				recordMax(&peak, n)
				return n >= inFlight
			})
			cancel()
			close(release)
			if !ok {
				return fmt.Errorf("the pool grew to only %d workers, want >= %d", peak.Load(), inFlight)
			}
			return nil
		}
		select {
		case <-release:
		case <-ic.Done():
			return context.Cause(ic)
		}
		return nil
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if peak.Load() < inFlight {
		t.Fatalf("peak live workers = %d, want >= %d", peak.Load(), inFlight)
	}
}

// TestWorkerPoolShrinksAfterBurst: once a burst has drained and items pass straight through, the extra workers
// retire instead of piling up as idle waiters at the start stage.
func TestWorkerPoolShrinksAfterBurst(t *testing.T) {
	const gateLimit = 8
	const grown = gateLimit + 1
	const shrunk = 4 // one worker parked at the start stage, one running the current item, plus slack
	c := NewConveyor()
	gate := c.AddStage(OptName("gate")).SetLimit(gateLimit)

	release := make(chan struct{})
	var drained atomic.Bool
	obs := make(chan error, 1)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	go func() {
		defer cancel()
		if !awaitTrue(func() bool { return liveWorkers(c) >= grown }) {
			obs <- fmt.Errorf("the pool grew to only %d workers during the burst, want >= %d",
				liveWorkers(c), grown)
			close(release)
			return
		}
		drained.Store(true)
		close(release)
		if !awaitTrue(func() bool { return liveWorkers(c) <= shrunk }) {
			obs <- fmt.Errorf("the pool stayed at %d workers after the burst drained, want <= %d",
				liveWorkers(c), shrunk)
			return
		}
		obs <- nil
	}()

	err := c.Run(ctx, func(ic context.Context) error {
		if drained.Load() {
			return nil // the burst is over: items pass straight through, so nothing occupies a worker for long
		}
		if err := gate.MoveTo(ic); err != nil {
			return err
		}
		select {
		case <-release:
		case <-ic.Done():
			return context.Cause(ic)
		}
		return nil
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if oErr := <-obs; oErr != nil {
		t.Fatal(oErr)
	}
}

// TestWorkerPoolRegrowsAfterShrinking: retiring workers costs nothing later — a second burst grows the pool again.
func TestWorkerPoolRegrowsAfterShrinking(t *testing.T) {
	const gateLimit = 6
	const grown = gateLimit + 1
	const shrunk = 4
	const (
		burst1 = iota
		idle
		burst2
	)
	c := NewConveyor()
	gate1 := c.AddStage(OptName("gate1")).SetLimit(gateLimit)
	gate2 := c.AddStage(OptName("gate2")).SetLimit(gateLimit)

	release1, release2 := make(chan struct{}), make(chan struct{})
	var phase atomic.Int32
	var peak atomic.Int64
	obs := make(chan error, 1)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	go func() {
		defer cancel()
		defer close(release2)
		if !awaitTrue(func() bool { return liveWorkers(c) >= grown }) {
			obs <- fmt.Errorf("the pool grew to only %d workers during the first burst", liveWorkers(c))
			close(release1)
			return
		}
		phase.Store(idle)
		close(release1)
		if !awaitTrue(func() bool { return liveWorkers(c) <= shrunk }) {
			obs <- fmt.Errorf("the pool stayed at %d workers between the bursts", liveWorkers(c))
			return
		}
		phase.Store(burst2)
		if !awaitTrue(func() bool {
			n := int64(liveWorkers(c))
			recordMax(&peak, n)
			return n >= grown
		}) {
			obs <- fmt.Errorf("the pool regrew to only %d workers, want >= %d", peak.Load(), grown)
			return
		}
		obs <- nil
	}()

	err := c.Run(ctx, func(ic context.Context) error {
		switch phase.Load() {
		case burst1:
			if err := gate1.MoveTo(ic); err != nil {
				return err
			}
			select {
			case <-release1:
			case <-ic.Done():
				return context.Cause(ic)
			}
		case burst2:
			if err := gate2.MoveTo(ic); err != nil {
				return err
			}
			select {
			case <-release2:
			case <-ic.Done():
				return context.Cause(ic)
			}
		case idle:
			// Items enter nothing and pass straight through, so the pool has nothing to hold workers for.
		}
		return nil
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if oErr := <-obs; oErr != nil {
		t.Fatal(oErr)
	}
	if peak.Load() < grown {
		t.Fatalf("peak live workers in the second burst = %d, want >= %d", peak.Load(), grown)
	}
}

// TestWorkerPoolChurnStaysBounded: a long run of fast and slow items keeps the pool bounded by the number of
// item-holding slots the topology declares, and leaves nothing behind.
func TestWorkerPoolChurnStaysBounded(t *testing.T) {
	const want = 300
	c := NewConveyor()
	a := c.AddStage(OptName("a")).SetLimit(2)
	b := c.AddStage(OptName("b")).SetLimit(3).SetQueueSize(2)
	fo := c.AddFanOut(OptName("fo")).SetLimit(2)
	pool := fo.AddPool(OptName("pool")).SetLimit(2)
	commit := c.AddStage(OptName("commit"))

	// Every in-flight root item holds at least one slot of the root series, so this is the ceiling on items in
	// flight; the slack covers workers between an item's completion and their retirement.
	const slots = 1 + 2 + 2 + 3 + 2 + 1
	const bound = slots + 4

	var peak, processed, bg atomic.Int64
	before := runtime.NumGoroutine()

	runNOK(t, c, want, func(ctx context.Context, no int64) error {
		recordMax(&peak, int64(liveWorkers(c)))
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		if no%13 == 0 {
			return nil // a fast item that leaves the pipeline early
		}
		if err := b.MoveTo(ctx); err != nil {
			return err
		}
		tasks := 2
		if no%9 == 0 {
			tasks = 12 // a slow item: more pool work than the pool can run at once
		}
		err := fo.MoveTo(ctx, Tasks{pool.NewTasks(tasks, func(cctx context.Context, i int) error {
			bg.Add(1)
			return nil
		})})
		if err != nil {
			return err
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		processed.Add(1)
		return nil
	})

	if processed.Load() < want-want/13-1 {
		t.Fatalf("only %d items reached the commit stage", processed.Load())
	}
	if bg.Load() == 0 {
		t.Fatal("no pool work ran")
	}
	if peak.Load() < 2 {
		t.Fatalf("peak live workers = %d: the pool never grew, so the bound proves nothing", peak.Load())
	}
	if peak.Load() > bound {
		t.Fatalf("peak live workers = %d, want <= %d (%d item-holding slots plus slack)",
			peak.Load(), bound, slots)
	}
	waitFor(t, "the run's goroutines to settle", func() bool { return runtime.NumGoroutine() <= before+2 })
}

// TestWorkerPoolDoesNotSpawnPerItem: the replacement-spawn decision must count workers that exist but have not been
// scheduled yet, not just parked ones (see run.spawning). Otherwise an ItemProcessor that returns without blocking
// spawns a fresh goroutine per item — the pool never gets a chance to park, so it grows without bound. It is worst
// on a single-P runtime, where the new goroutines just pile up on the run queue, so that is what this pins.
func TestWorkerPoolDoesNotSpawnPerItem(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	c := NewConveyor() // no nodes at all: every item is created, runs, and finishes at the start stage
	before := runtime.NumGoroutine()

	var peak atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	sampled := make(chan struct{})
	go func() { // sample from another goroutine: the processor below never yields
		defer close(sampled)
		for range 20 {
			recordMax(&peak, int64(runtime.NumGoroutine()))
			runtime.Gosched()
		}
		cancel()
	}()

	var items atomic.Int64
	if err := c.Run(ctx, func(context.Context) error {
		items.Add(1)
		return nil
	}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	<-sampled

	if items.Load() < 1000 {
		t.Fatalf("only %d items ran; the sampler did not observe a saturated pool", items.Load())
	}
	// The start stage admits one item at a time, so the run needs a couple of workers however many items it churns
	// through. The slack covers the sampler and the shutdown watcher.
	if grew := peak.Load() - int64(before); grew > 16 {
		t.Fatalf("goroutines grew by %d over %d items, want a small constant: the pool is spawning per item",
			grew, items.Load())
	}
}
