package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// TestLinearPipelinePreservesOrder is the flagship shape: read -> write -> commit, all exclusive. Every stage
// must see items 1..N in order, and the three stages must overlap (a saturated pipeline).
func TestLinearPipelinePreservesOrder(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))

	var writes, commits numbers
	writeGauge, commitGauge := &gauge{}, &gauge{}

	runNOK(t, c, 20, func(ctx context.Context, no int64) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		// Each gauge region must stay strictly inside the item's occupancy of that stage: entering commit is what
		// releases write, so the write region has to close first — otherwise the next item legitimately enters write
		// while this closure is still counted.
		if err := writeGauge.hold(func() error {
			writes.add(no)
			return nil
		}); err != nil {
			return err
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		return commitGauge.hold(func() error {
			commits.add(no)
			return nil
		})
	})

	assertStrictlyIncreasing(t, writes.all(), "write order")
	assertStrictlyIncreasing(t, commits.all(), "commit order")
	if peak, _ := writeGauge.snapshot(); peak != 1 {
		t.Fatalf("exclusive write stage ran %d items at once", peak)
	}
	if peak, _ := commitGauge.snapshot(); peak != 1 {
		t.Fatalf("exclusive commit stage ran %d items at once", peak)
	}
}

// TestPipelineOverlaps proves the stages actually run concurrently: with 3 exclusive stages, three different
// items must be inside them at the same time at some point.
func TestPipelineOverlaps(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))
	cc := c.AddStage(OptName("c"))

	var inside atomic.Int64
	var peak atomic.Int64
	step := func(ctx context.Context, s Stage) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		n := inside.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		return nil
	}

	runNOK(t, c, 30, func(ctx context.Context, no int64) error {
		defer inside.Add(-3)
		if err := step(ctx, a); err != nil {
			return err
		}
		if err := step(ctx, b); err != nil {
			return err
		}
		return step(ctx, cc)
	})

	if peak.Load() < 3 {
		t.Fatalf("expected 3 stages busy at once, peak was %d", peak.Load())
	}
}

// TestSharedStageBoundedConcurrency: SetLimit(n) admits exactly n items concurrently, no more.
func TestSharedStageBoundedConcurrency(t *testing.T) {
	c := NewConveyor()
	shared := c.AddStage(OptName("shared")).SetLimit(3)
	commit := c.AddStage(OptName("commit"))

	g := &gauge{}
	// The first three items block until all three are inside, proving the stage really runs them concurrently;
	// the barrier then stays open for the rest.
	barrier := make(chan struct{})
	var arrived atomic.Int64
	var opened atomic.Bool

	runNOK(t, c, 12, func(ctx context.Context, no int64) error {
		if err := shared.MoveTo(ctx); err != nil {
			return err
		}
		err := g.hold(func() error {
			if arrived.Add(1) == 3 && opened.CompareAndSwap(false, true) {
				close(barrier)
			}
			select {
			case <-barrier:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	if peak, entries := g.snapshot(); peak != 3 || entries < 12 {
		t.Fatalf("shared stage: peak=%d entries=%d, want peak 3 and >= 12 entries", peak, entries)
	}
}

// TestItemMayReturnEarly: an item that skips the rest of the pipeline releases its slots and does not stall the
// items behind it.
func TestItemMayReturnEarly(t *testing.T) {
	c := NewConveyor()
	mid := c.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	var commits numbers
	runNOK(t, c, 10, func(ctx context.Context, no int64) error {
		if err := mid.MoveTo(ctx); err != nil {
			return err
		}
		if no%2 == 0 {
			return nil // skip the commit stage entirely
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		commits.add(no)
		return nil
	})

	got := commits.all()
	if len(got) < 4 {
		t.Fatalf("expected the odd items to commit, got %v", got)
	}
	assertNonDecreasing(t, got, "commit order with skips")
	for _, v := range got {
		if v%2 == 0 {
			t.Fatalf("even item %d should have skipped commit: %v", v, got)
		}
	}
}

// TestStageSkippedEntirely: a stage nobody enters must not block the pipeline.
func TestStageSkippedEntirely(t *testing.T) {
	c := NewConveyor()
	_ = c.AddStage(OptName("never"))
	commit := c.AddStage(OptName("commit"))

	var commits numbers
	runNOK(t, c, 8, func(ctx context.Context, no int64) error {
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		commits.add(no)
		return nil
	})
	assertStrictlyIncreasing(t, commits.all(), "commit order with an unused stage")
}

// TestStartStageGatesCreation: only one item may sit at the implicit start stage, so item N+1 is not created until N
// has moved off it. The assertion reads the runtime's own occupancy window (which is maintained under the lock)
// rather than counting in user code — by the time the processor could decrement a counter, MoveTo has already
// released the slot and the next item may legitimately hold it.
func TestStartStageGatesCreation(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))

	var startPeak, startLimit atomic.Int64
	runNOK(t, c, 15, func(ctx context.Context, no int64) error {
		if err := write.MoveTo(ctx); err != nil { // leaves the start stage
			return err
		}
		if no == 15 {
			// One read at the end: the window covers the whole run.
			s := c.Stats()
			startPeak.Store(int64(s.Units[0].Occupied.Max))
			startLimit.Store(int64(s.Units[0].Limit))
		}
		return nil
	})

	if got := startPeak.Load(); got != 1 {
		t.Fatalf("start stage held %d items at once, want 1", got)
	}
	if got := startLimit.Load(); got != 1 {
		t.Fatalf("start stage limit = %d, want 1", got)
	}
}

// TestItemErrorShutsDownGracefully: a failing item becomes Run's error, earlier items finish, later ones abort.
func TestItemErrorShutsDownGracefully(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	work := c.AddStage(OptName("work"))
	commit := c.AddStage(OptName("commit"))

	var commits numbers
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if err := work.MoveTo(ic); err != nil {
			return err
		}
		if no == 5 {
			return boom
		}
		if err := commit.MoveTo(ic); err != nil {
			return err
		}
		commits.add(no)
		return nil
	})

	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
	got := commits.all()
	if len(got) < 4 {
		t.Fatalf("items before the failure should have committed, got %v", got)
	}
	for _, v := range got {
		if v >= 5 {
			t.Fatalf("item %d committed after the failing item 5: %v", v, got)
		}
	}
}

// TestRunReturnsContextCause: a clean shutdown reports the run context's cause, not an item error.
func TestRunReturnsContextCause(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage()
	cause := errors.New("time to stop")

	ctx, cancel := context.WithCancelCause(context.Background())
	var n atomic.Int64
	err := c.Run(ctx, func(ic context.Context) error {
		if n.Add(1) == 3 {
			cancel(cause)
		}
		return s.MoveTo(ic)
	})
	if !errors.Is(err, cause) {
		t.Fatalf("Run error = %v, want %v", err, cause)
	}
}

// TestAlreadyRunning: a second concurrent Run is refused.
func TestAlreadyRunning(t *testing.T) {
	c := NewConveyor()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	inside := make(chan struct{})
	stop := make(chan struct{}) // keeps the first run busy until the assertion is made
	finished := make(chan struct{})
	var once atomic.Bool
	go func() {
		defer close(finished)
		_ = c.Run(ctx, func(ic context.Context) error {
			if once.CompareAndSwap(false, true) {
				close(inside)
			}
			<-stop
			return nil
		})
	}()
	<-inside
	if err := c.Run(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrConveyorAlreadyRunning) {
		t.Fatalf("second Run = %v, want ErrConveyorAlreadyRunning", err)
	}
	cancel(context.Canceled)
	close(stop)
	<-finished
}

// TestRunAgainAfterReturn: the conveyor is reusable, with fresh per-run state.
func TestRunAgainAfterReturn(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage()

	for round := 0; round < 3; round++ {
		var seen numbers
		runNOK(t, c, 5, func(ctx context.Context, no int64) error {
			if err := s.MoveTo(ctx); err != nil {
				return err
			}
			seen.add(no)
			return nil
		})
		got := seen.all()
		if len(got) == 0 || got[0] != 1 {
			t.Fatalf("round %d: item numbering did not restart: %v", round, got)
		}
	}
}
