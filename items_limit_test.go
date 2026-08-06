package conveyor

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestItemsLimitDefaultsToUnlimitedAndClampsNegative: a fresh conveyor has no items cap, and a negative value
// clamps to 0 (unlimited) the same way 0 does.
func TestItemsLimitDefaultsToUnlimitedAndClampsNegative(t *testing.T) {
	c := NewConveyor()
	if got := c.ItemsLimit(); got != 0 {
		t.Fatalf("ItemsLimit before any SetItemsLimit = %d, want 0 (unlimited)", got)
	}
	if got := c.SetItemsLimit(-5).ItemsLimit(); got != 0 {
		t.Fatalf("SetItemsLimit(-5): ItemsLimit = %d, want the clamp to 0", got)
	}
}

// TestSetItemsLimitBoundsConcurrentItems: the cap bounds how many root items are in flight at once, on top of
// whatever a node's own (much larger) limit would otherwise allow.
func TestSetItemsLimitBoundsConcurrentItems(t *testing.T) {
	const limit = 3
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetLimit(100) // plenty of node capacity; only the items cap should bind
	c.SetItemsLimit(limit)

	g := &gauge{}
	release := make(chan struct{})

	go func() {
		waitFor(t, "concurrent items to reach the cap", func() bool { return g.current() == limit })
		close(release)
	}()

	runNOK(t, c, 20, func(ctx context.Context, no int64) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		return g.hold(func() error {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		})
	})

	if peak, entries := g.snapshot(); peak != limit || entries != 20 {
		t.Fatalf("peak=%d entries=%d, want peak %d (the cap) and all 20 items", peak, entries, limit)
	}
}

// TestSetItemsLimitLowerDoesNotEvict: lowering the cap on a running conveyor never evicts the items already in
// flight — it converges to the new cap as they finish, admitting nobody new until in-flight has dropped below it.
func TestSetItemsLimitLowerDoesNotEvict(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetLimit(100)
	c.SetItemsLimit(3)

	holds := map[int64]chan struct{}{1: make(chan struct{}), 2: make(chan struct{}), 3: make(chan struct{})}
	var entered, left atomic.Int64
	after := &gauge{} // concurrency seen by the items admitted after the lower

	go func() {
		waitFor(t, "3 items to fill the items cap", func() bool { return entered.Load() == 3 })
		c.SetItemsLimit(1)
		if got := entered.Load(); got != 3 {
			t.Errorf("lowering the cap evicted items: entered = %d, want 3", got)
		}
		for i := int64(1); i <= 3; i++ {
			close(holds[i])
			waitFor(t, "an item to leave", func() bool { return left.Load() >= i })
			if i < 3 {
				// In-flight is still at (or above) the new cap 1, so nobody new must have been admitted.
				if got := entered.Load(); got != 3 {
					t.Errorf("%d items entered while in-flight was still at the new cap 1", got)
				}
			}
		}
		waitFor(t, "the shrunken cap to admit new items", func() bool { return entered.Load() >= 4 })
	}()

	runNOK(t, c, 6, func(ctx context.Context, no int64) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		entered.Add(1)
		if h, ok := holds[no]; ok {
			select {
			case <-h:
			case <-ctx.Done():
			}
			left.Add(1)
			return nil
		}
		return after.hold(func() error { return nil })
	})

	if peak, _ := after.snapshot(); peak > 1 {
		t.Fatalf("after lowering the cap to 1, %d items ran concurrently, want <= 1", peak)
	}
}

// TestSetItemsLimitRaiseAdmitsMoreItems: raising the cap on a running conveyor lets more items be created at
// once — the raise itself is the wake-up, no other event is needed.
func TestSetItemsLimitRaiseAdmitsMoreItems(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetLimit(100)
	c.SetItemsLimit(1)

	g := &gauge{}
	release := make(chan struct{})

	go func() {
		waitFor(t, "the cap to hold concurrency at 1", func() bool { return g.current() == 1 })
		c.SetItemsLimit(4) // the only thing that happens from here on
		waitFor(t, "the raise to admit more items", func() bool { return g.current() == 4 })
		close(release)
	}()

	runNOK(t, c, 20, func(ctx context.Context, no int64) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		return g.hold(func() error {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		})
	})

	if peak, _ := g.snapshot(); peak != 4 {
		t.Fatalf("peak concurrent items = %d, want the raised cap 4", peak)
	}
	if got := c.ItemsLimit(); got != 4 {
		t.Fatalf("ItemsLimit = %d, want 4", got)
	}
}

// TestSetItemsLimitConcurrentWhileRunning: SetItemsLimit is safe from any goroutine at any time (run with -race),
// and the cap it leaves behind is one of the values that were set.
func TestSetItemsLimitConcurrentWhileRunning(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetLimit(8)
	c.SetItemsLimit(4)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for n := seed; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				c.SetItemsLimit(1 + n%4)
				runtime.Gosched()
			}
		}(i)
	}

	runNOK(t, c, 60, func(ctx context.Context, no int64) error {
		return s.MoveTo(ctx)
	})

	close(stop)
	wg.Wait()

	if got := c.ItemsLimit(); got < 1 || got > 4 {
		t.Fatalf("ItemsLimit = %d, want one of the values that were set (1..4)", got)
	}
}
