package conveyor

import (
	"context"
	"sync/atomic"
	"testing"
)

// TestQueueDoesNotRelaxWorkExclusivity is the point of having a queue as a separate knob: SetQueueSize(k) lets k items
// wait, but the stage's own limit still bounds how many run its code.
func TestQueueDoesNotRelaxWorkExclusivity(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetQueueSize(3)

	g := &gauge{}
	runNOK(t, c, 15, func(ctx context.Context, no int64) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		return g.hold(func() error { return nil })
	})

	peak, entries := g.snapshot()
	if peak != 1 {
		t.Fatalf("stage with a queue ran %d items at once, want 1", peak)
	}
	if entries < 15 {
		t.Fatalf("only %d items ran the stage, want >= 15", entries)
	}
}

// TestQueuePreservesOrder: the waiting room is FIFO in item order, so an exclusive stage behind it still sees
// 1..N in order.
func TestQueuePreservesOrder(t *testing.T) {
	c := NewConveyor()
	first := c.AddStage(OptName("first"))
	second := c.AddStage(OptName("second")).SetQueueSize(3)

	var seen numbers
	runNOK(t, c, 20, func(ctx context.Context, no int64) error {
		if err := first.MoveTo(ctx); err != nil {
			return err
		}
		if err := second.MoveTo(ctx); err != nil {
			return err
		}
		seen.add(no)
		return nil
	})
	assertStrictlyIncreasing(t, seen.all(), "order through a queue")
}

// TestQueueDecouplesPreviousStage is the payoff: with a slow exclusive stage behind it, a queue lets items step
// off the previous stage instead of squatting on it, so that stage keeps working.
//
// Topology: s1(1) -> s2(1) with a queue of 2. One item is held inside s2; meanwhile
//   - item 2 and 3 finish s1's work and wait in the queue,
//   - item 4 finishes s1's work and blocks entering the (full) queue while holding s1,
//
// so 4 items complete s1's work. Without the queue only 2 can (item 2 squats on s1).
func TestQueueDecouplesPreviousStage(t *testing.T) {
	for _, tc := range []struct {
		name      string
		queue     int
		wantS1Ops int64
	}{
		{name: "no queue", queue: 0, wantS1Ops: 2},
		{name: "queue 2", queue: 2, wantS1Ops: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewConveyor()
			s1 := c.AddStage(OptName("s1"))
			s2 := c.AddStage(OptName("s2"))
			if tc.queue > 0 {
				s2.SetQueueSize(tc.queue)
			}

			var s1Ops atomic.Int64
			block := make(chan struct{})
			var blocked atomic.Bool

			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			go func() {
				// Once s1 has done as much work as the topology allows, release the item stuck in s2 and stop.
				waitFor(t, "s1 throughput", func() bool { return s1Ops.Load() >= tc.wantS1Ops })
				close(block)
				cancel()
			}()

			_ = c.Run(ctx, func(ic context.Context) error {
				if err := s1.MoveTo(ic); err != nil {
					return err
				}
				s1Ops.Add(1) // s1's work is done; the item now wants s2
				if err := s2.MoveTo(ic); err != nil {
					return err
				}
				if blocked.CompareAndSwap(false, true) {
					select { // the first item holds s2 for the duration of the test
					case <-block:
					case <-ic.Done():
					}
				}
				return nil
			})

			if got := s1Ops.Load(); got < tc.wantS1Ops {
				t.Fatalf("s1 completed %d items while s2 was blocked, want >= %d", got, tc.wantS1Ops)
			}
		})
	}
}

// TestQueueNeverExceedsItsLimit checks the waiting room is genuinely bounded, by watching live occupancy.
func TestQueueNeverExceedsItsLimit(t *testing.T) {
	c := NewConveyor()
	s1 := c.AddStage(OptName("s1"))
	s2 := c.AddStage(OptName("s2")).SetQueueSize(2)

	var peakQueued atomic.Int64
	block := make(chan struct{})
	var blocked atomic.Bool

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	go func() {
		// Sample the queue's occupancy while the pipeline is jammed behind the held item.
		waitFor(t, "queue to fill", func() bool {
			q := queueOccupancy(c, s2)
			for {
				p := peakQueued.Load()
				if int64(q) <= p || peakQueued.CompareAndSwap(p, int64(q)) {
					break
				}
			}
			return peakQueued.Load() >= 2
		})
		close(block)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		if err := s1.MoveTo(ic); err != nil {
			return err
		}
		if err := s2.MoveTo(ic); err != nil {
			return err
		}
		if blocked.CompareAndSwap(false, true) {
			select {
			case <-block:
			case <-ic.Done():
			}
		}
		return nil
	})

	if got := peakQueued.Load(); got != 2 {
		t.Fatalf("queue occupancy peaked at %d, want exactly its limit 2", got)
	}
}

// TestQueueOnFanOut: a fan-out takes a waiting room the same way a stage does.
func TestQueueOnFanOut(t *testing.T) {
	c := NewConveyor()
	s1 := c.AddStage(OptName("s1"))
	fo := c.AddFanOut(OptName("fo")).SetQueueSize(2)
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	if fo.QueueSize() != 2 {
		t.Fatalf("fan-out QueueSize = %d, want 2", fo.QueueSize())
	}

	var done atomic.Int64
	runNOK(t, c, 12, func(ctx context.Context, no int64) error {
		if err := s1.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error {
			done.Add(1)
			return nil
		})})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	if got := done.Load(); got < 12 {
		t.Fatalf("only %d tasks ran, want >= 12", got)
	}
}

// TestQueueSizeGetters covers the reported sizes, including the clamp of a negative size to "no waiting room".
func TestQueueSizeGetters(t *testing.T) {
	c := NewConveyor()
	plain := c.AddStage()
	queued := c.AddStage().SetQueueSize(4)
	clamped := c.AddStage().SetQueueSize(-2)

	if got := plain.QueueSize(); got != 0 {
		t.Fatalf("stage without a queue: QueueSize = %d, want 0", got)
	}
	if got := queued.QueueSize(); got != 4 {
		t.Fatalf("QueueSize = %d, want 4", got)
	}
	if got := clamped.QueueSize(); got != 0 {
		t.Fatalf("SetQueueSize(-2): QueueSize = %d, want the clamp to 0", got)
	}
	if got := plain.Limit(); got != 1 {
		t.Fatalf("default stage Limit = %d, want 1", got)
	}
}

// TestQueueResizableAtRuntime: a waiting room is capacity, not topology, so resizing one while the conveyor runs is
// allowed (admission-only, like SetLimit).
func TestQueueResizableAtRuntime(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetQueueSize(1)

	runNOK(t, c, 6, func(ctx context.Context, no int64) error {
		if no == 2 {
			s.SetQueueSize(3) // resize while running
		}
		return s.MoveTo(ctx)
	})
	if got := s.QueueSize(); got != 3 {
		t.Fatalf("QueueSize after resize = %d, want 3", got)
	}
}

// TestQueueCreatedAtRuntime is the point of a waiting room being capacity rather than topology: a node built
// without one can be given one while the conveyor is running, and items start using it — no rebuild, no restart.
//
// The stage is held by item 1 throughout, so before the change the only place to wait is the previous node; after
// it, items step aside into the new waiting room. Observing queue occupancy rise from 0 is what proves the new
// room is real and not just a reported number.
func TestQueueCreatedAtRuntime(t *testing.T) {
	c := NewConveyor()
	prev := c.AddStage(OptName("prev"))
	s := c.AddStage(OptName("s")) // no waiting room at build time

	release := make(chan struct{})
	var held atomic.Bool

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	go func() {
		waitFor(t, "the stage to be held", func() bool { return held.Load() })
		if got := queueOccupancy(c, s); got != 0 {
			t.Errorf("a node with no waiting room reports %d items waiting, want 0", got)
		}
		s.SetQueueSize(2) // the waiting room appears mid-run
		waitFor(t, "items to fill the new waiting room", func() bool { return queueOccupancy(c, s) == 2 })
		close(release)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		if err := prev.MoveTo(ic); err != nil {
			return err
		}
		if err := s.MoveTo(ic); err != nil {
			return err
		}
		if held.CompareAndSwap(false, true) {
			select {
			case <-release:
			case <-ic.Done():
			}
		}
		return nil
	})

	if got := s.QueueSize(); got != 2 {
		t.Fatalf("QueueSize after the runtime creation = %d, want 2", got)
	}
}

// TestQueueRemovedAtRuntime is the other direction, and it must be admission-only like every other shrink: setting
// the size to 0 stops admitting items to the waiting room, but never evicts the ones already in it — they stay
// until the stage takes them, which is what keeps a live change from stranding an item that has already given up
// the previous node.
func TestQueueRemovedAtRuntime(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetQueueSize(2)

	release := make(chan struct{})
	var held atomic.Bool
	var seen numbers

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	go func() {
		waitFor(t, "the waiting room to fill", func() bool { return queueOccupancy(c, s) == 2 })
		s.SetQueueSize(0)
		if got := queueOccupancy(c, s); got != 2 {
			t.Errorf("removing the waiting room evicted the items in it: %d waiting, want 2", got)
		}
		close(release)
		// From here the room admits nobody, so it drains to empty and stays there.
		waitFor(t, "the waiting room to drain", func() bool { return queueOccupancy(c, s) == 0 })
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if err := s.MoveTo(ic); err != nil {
			return err
		}
		if held.CompareAndSwap(false, true) {
			select {
			case <-release:
			case <-ic.Done():
			}
		}
		seen.add(no)
		return nil
	})

	if got := s.QueueSize(); got != 0 {
		t.Fatalf("QueueSize after removal = %d, want 0", got)
	}
	// Whatever the size did mid-run, the stage stayed exclusive and in order.
	assertStrictlyIncreasing(t, seen.all(), "order across a queue removal")
}

// TestTryMoveToDoesNotJumpAQueuedItem pins the invariant the waiting room's own (lower) rank exists to protect: an
// item waiting in front of a stage has published that lower rank, so a *younger* item must not be able to take the
// stage slot out from under it — not even with TryMoveTo, which bypasses the waiting room and would otherwise walk
// straight in the moment the stage frees up.
//
// Item 1 holds the stage and item 2 waits in the room. Both are released at once, and item 3 — which only ever
// tries, never waits — must be refused for as long as item 2 has not been admitted.
func TestTryMoveToDoesNotJumpAQueuedItem(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetQueueSize(1)

	release := make(chan struct{})
	var order numbers
	var jumped atomic.Bool
	var declines atomic.Int64

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	go func() {
		// Hold the jam until a try has actually been refused, so the assertion below cannot pass vacuously.
		waitFor(t, "a try to be declined while an item is queued", func() bool {
			return occupancyOf(c, s) == 1 && queueOccupancy(c, s) == 1 && declines.Load() > 0
		})
		close(release)
		waitFor(t, "the queued item to be admitted", func() bool { return len(order.all()) >= 2 })
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if no >= 3 {
			// Never waits: while item 2 is queued ahead, every attempt must be declined.
			entered, err := s.TryMoveTo(ic)
			if err != nil {
				return err
			}
			if !entered {
				declines.Add(1)
				return nil
			}
			if len(order.all()) < 2 {
				jumped.Store(true)
			}
			order.add(no)
			return nil
		}
		if err := s.MoveTo(ic); err != nil {
			return err
		}
		if no == 1 {
			select {
			case <-release:
			case <-ic.Done():
			}
		}
		order.add(no)
		return nil
	})

	if jumped.Load() {
		t.Fatalf("TryMoveTo entered the stage ahead of an item already waiting in front of it")
	}
	if declines.Load() == 0 {
		t.Fatalf("no try was ever declined, so the test proved nothing")
	}
	if got := order.all(); len(got) < 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("stage entry order = %v, want it to start 1, 2", got)
	}
}

// TestSharedStageWithWaitingRoomAdmitsLimitPlusQueue: the two capacities are independent and add up. A stage with
// limit 2 and a waiting room of 2 accounts for four items — two running its code, two standing in front — and the
// waiting room never relaxes the limit, however full it gets.
func TestSharedStageWithWaitingRoomAdmitsLimitPlusQueue(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetLimit(2).SetQueueSize(2)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	release := make(chan struct{})
	sampled := make(chan struct{})
	var entered atomic.Int64
	g := &gauge{}

	go func() {
		defer close(sampled)
		// Both items must be inside the stage's *code*, not merely admitted: a slot is taken inside MoveTo, before the
		// item gets to run, so releasing on occupancy alone could let the first one leave before the second arrives.
		waitFor(t, "the stage full with a full waiting room", func() bool {
			return g.current() == 2 && occupancyOf(c, s) == 2 && queuedOf(c, s) == 2
		})
		// Everything is blocked, so nothing can move between the check above and this read.
		if got := entered.Load(); got != 2 {
			t.Errorf("%d items entered the stage, want 2 — its limit, whatever the waiting room holds", got)
		}
		close(release)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		if err := s.MoveTo(ic); err != nil {
			return err
		}
		entered.Add(1)
		return g.hold(func() error {
			select {
			case <-release:
			case <-ic.Done():
			}
			return nil
		})
	})
	<-sampled

	if peak, _ := g.snapshot(); peak != 2 {
		t.Fatalf("peak concurrency inside the stage = %d, want 2 — the waiting room is not extra capacity", peak)
	}
}

// TestFanOutQueueCreatedAtRuntime: a waiting room is capacity rather than topology on a fan-out too, so one can be
// given to a running fan-out — and the raise reaches the items already blocked at its door, which step aside at once
// instead of waiting for the node itself.
func TestFanOutQueueCreatedAtRuntime(t *testing.T) {
	c := NewConveyor()
	prev := c.AddStage(OptName("prev")).SetLimit(4)
	fo := c.AddFanOut(OptName("fo")) // no waiting room at build time
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit")).SetLimit(4)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	release := make(chan struct{})
	sampled := make(chan struct{})
	var atPrev, inFo atomic.Int64
	var held atomic.Bool

	go func() {
		defer close(sampled)
		// One item is inside the fan-out and holding it; the two behind it are blocked at its door, since it has
		// nowhere for them to stand.
		waitFor(t, "two items blocked at the fan-out's door", func() bool {
			return inFo.Load() == 1 && atPrev.Load() >= 3
		})
		if q := queuedOf(c, fo); q != 0 {
			t.Errorf("fan-out reports %d items waiting before it has a waiting room, want 0", q)
		}
		fo.SetQueueSize(2)
		if !waitFor(t, "the blocked items to step into the new waiting room", func() bool {
			return queuedOf(c, fo) == 2
		}) {
			t.Errorf("queued = %d after the raise, want 2", queuedOf(c, fo))
		}
		close(release)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		if err := prev.MoveTo(ic); err != nil {
			return err
		}
		atPrev.Add(1)
		err := fo.MoveTo(ic, Tasks{pool.NewTask(func(context.Context) error { return nil })})
		if err != nil {
			return err
		}
		inFo.Add(1)
		if held.CompareAndSwap(false, true) {
			select { // the first item stays inside the fan-out while the sample is taken
			case <-release:
			case <-ic.Done():
			}
		}
		return commit.MoveTo(ic)
	})
	<-sampled
}
