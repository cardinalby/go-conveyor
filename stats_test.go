package conveyor

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
)

// statNames lists the names of a snapshot's entries, for failure messages.
func statNames(s Stats) []string {
	names := make([]string, 0, len(s.Units))
	for _, u := range s.Units {
		names = append(names, u.Unit.String())
	}
	return names
}

// statOf finds the entry a handle produced, by identity.
func statOf(t *testing.T, s Stats, u Unit) UnitStat {
	t.Helper()
	for _, e := range s.Units {
		if e.Unit == u {
			return e
		}
	}
	t.Fatalf("no Stats entry for %s, got %v", u, statNames(s))
	return UnitStat{}
}

// queuedOf reports the live occupancy of a node's waiting room (0 when it has none), so a test can synchronize on
// it without consuming a Stats window.
func queuedOf(c *Conveyor, u Unit) int { return queueOccupancy(c, u) }

// TestStatsZeroOutsideRun: Stats reports runtime state, and outside a run — before the first one and after the last
// has returned — there is none, so it is the zero value rather than a stale snapshot.
func TestStatsZeroOutsideRun(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	if got := c.Stats(); !reflect.DeepEqual(got, Stats{}) {
		t.Fatalf("Stats before the first run = %+v, want the zero Stats", got)
	}
	runNOK(t, c, 3, func(ctx context.Context, no int64) error {
		return s.MoveTo(ctx)
	})
	if got := c.Stats(); !reflect.DeepEqual(got, Stats{}) {
		t.Fatalf("Stats after the run returned = %+v, want the zero Stats", got)
	}
}

// TestStatsOneEntryPerNodeAndBranch: a snapshot has exactly one entry per node and per lane, in creation order, and a
// queue is folded into the node it fronts instead of getting an entry of its own.
func TestStatsOneEntryPerNodeAndBranch(t *testing.T) {
	c := NewConveyor()
	s1 := c.AddStage(OptName("s1")).SetQueueSize(2)
	fo := c.AddFanOut(OptName("fo")).SetQueueSize(3)
	l1 := fo.AddLane(OptName("l1"))
	l2 := fo.AddPool(OptName("l2"))
	in := l1.AddStage(OptName("in"))
	commit := c.AddStage(OptName("commit"))

	var got Stats
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := s1.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.MoveTo(ctx, Tasks{
			l1.NewTask(func(cctx context.Context) error { return in.MoveTo(cctx) }),
			l2.NewTask(func(context.Context) error { return nil }),
		})
		if err != nil {
			return err
		}
		got = c.Stats()
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}

	want := []Unit{c.StartUnit(), s1, fo, l1, l2, in, commit}
	if len(got.Units) != len(want) {
		t.Fatalf("Stats has %d entries (%v), want %d", len(got.Units), statNames(got), len(want))
	}
	for i, u := range want {
		if got.Units[i].Unit != u {
			t.Fatalf("Stats entry %d = %s, want %s", i, got.Units[i].Unit, u)
		}
	}
	for _, l := range []Unit{l1, l2} {
		if e := statOf(t, got, l); e.Limit != 1 {
			t.Fatalf("lane %s: Limit=%d, want 1", l, e.Limit)
		}
	}
	// The configured waiting-room size is not part of a stat; it is read back from the handle.
	if s1.QueueSize() != 2 || fo.QueueSize() != 3 {
		t.Fatalf("QueueSize: s1=%d fo=%d, want 2 and 3", s1.QueueSize(), fo.QueueSize())
	}
	if e := statOf(t, got, commit); e.Queued != (Gauge{}) {
		t.Fatalf("commit has no queue, yet reports Queued=%+v", e.Queued)
	}
}

// TestStatsSeparatesNodeAndQueueOccupancy: Occupied/Limit describe the node itself and Queued its waiting room, so a
// jammed stage is reported as one item working with the queue full behind it.
func TestStatsSeparatesNodeAndQueueOccupancy(t *testing.T) {
	c := NewConveyor()
	s1 := c.AddStage(OptName("s1"))
	s2 := c.AddStage(OptName("s2")).SetQueueSize(2)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	release := make(chan struct{})
	sampled := make(chan struct{})
	var blocked atomic.Bool
	var got Stats

	go func() {
		defer close(sampled)
		waitFor(t, "s2 busy with a full queue behind it", func() bool {
			return occupancyOf(c, s2) == 1 && queuedOf(c, s2) == 2
		})
		got = c.Stats()
		close(release)
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
			select { // the first item jams s2 while the sample is taken
			case <-release:
			case <-ic.Done():
			}
		}
		return nil
	})
	<-sampled

	e := statOf(t, got, s2)
	if e.Occupied.Last != 1 || e.Limit != 1 {
		t.Fatalf("s2: Occupied.Last=%d Limit=%d, want 1 and 1", e.Occupied.Last, e.Limit)
	}
	if e.Occupied.Max != 1 {
		t.Fatalf("s2: Occupied.Max=%d, want 1 — its own limit bounds the work", e.Occupied.Max)
	}
	if e.Queued.Last != 2 {
		t.Fatalf("s2: Queued.Last=%d, want 2 (its waiting room is full)", e.Queued.Last)
	}
	if e := statOf(t, got, s1); e.Queued != (Gauge{}) {
		t.Fatalf("s1 has no queue, yet reports Queued=%+v", e.Queued)
	}
}

// TestStatsWindowCapturesTransientsAndResets is the point of the windowed gauges: an occupancy that came and went
// between two reads is still reported by Max, even though Last is 0 at read time — and the window restarts there.
func TestStatsWindowCapturesTransientsAndResets(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	after := c.AddStage(OptName("after"))

	var transient, reset Stats
	err := runOnce(t, c, func(ctx context.Context) error {
		_ = c.Stats() // opens a fresh window with s empty
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		if err := after.MoveTo(ctx); err != nil { // entering the next stage releases s
			return err
		}
		transient = c.Stats()
		reset = c.Stats()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}

	e := statOf(t, transient, s)
	if e.Occupied.Last != 0 {
		t.Fatalf("s: Occupied.Last=%d, want 0 — the item had already moved on", e.Occupied.Last)
	}
	if e.Occupied.Max != 1 {
		t.Fatalf("s: Occupied.Max=%d, want 1 — the window must catch the transient", e.Occupied.Max)
	}
	if e.Occupied.Min != 0 {
		t.Fatalf("s: Occupied.Min=%d, want 0", e.Occupied.Min)
	}
	if e := statOf(t, transient, after); e.Occupied.Last != 1 {
		t.Fatalf("after: Occupied.Last=%d, want 1 — the item is inside it", e.Occupied.Last)
	}
	if e := statOf(t, reset, s); e.Occupied != (Gauge{}) {
		t.Fatalf("s on the next read: %+v, want a window reset to the current value 0", e.Occupied)
	}
}

// TestStatsInFlightCountsOwnItemsOnly: InFlight counts the conveyor's own items, so a lane turning each item into
// many child journeys must not inflate it; LiveWorkers reports the pool that runs those items.
func TestStatsInFlightCountsOwnItemsOnly(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")) // limit 1
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid")).SetLimit(4)
	tail := lane.AddStage(OptName("tail")).SetLimit(4)
	commit := c.AddStage(OptName("commit"))

	var maxInFlight, minWorkers atomic.Int64
	minWorkers.Store(-1)
	runNOK(t, c, 5, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTasks(8, func(cctx context.Context, i int) error {
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			return tail.MoveTo(cctx)
		})})
		if err != nil {
			return err
		}
		s := c.Stats()
		for {
			m := maxInFlight.Load()
			if int64(s.InFlight.Max) <= m || maxInFlight.CompareAndSwap(m, int64(s.InFlight.Max)) {
				break
			}
		}
		for {
			m := minWorkers.Load()
			if (m >= 0 && int64(s.LiveWorkers.Last) >= m) ||
				minWorkers.CompareAndSwap(m, int64(s.LiveWorkers.Last)) {
				break
			}
		}
		return commit.MoveTo(ctx)
	})

	// start(1) + fan-out(1) + commit(1) = 3 items of the conveyor itself; the 8 children per item are not counted.
	if got := maxInFlight.Load(); got < 1 || got > 3 {
		t.Fatalf("InFlight.Max reached %d, want 1..3 (children must not be counted)", got)
	}
	if got := minWorkers.Load(); got < 1 {
		t.Fatalf("LiveWorkers.Last was %d during the run, want > 0", got)
	}
}

// TestStatsReportsPoolBacklog: a lane's queue is its waiting room, so items whose work it has accepted but not yet
// started show up as the lane's Queued. That is the only thing separating a saturated lane from a merely busy one —
// Occupied pins at the limit either way — and, because a collection leaves the queue as soon as its work is handed
// out, the backlog counts unstarted work rather than work still running.
func TestStatsReportsPoolBacklog(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(3) // three items may be inside, so three may enqueue
	pool := fo.AddPool(OptName("pool"))          // limit 1: one piece of work at a time

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	release := make(chan struct{})
	sampled := make(chan struct{})
	var got Stats

	go func() {
		defer close(sampled)
		// Item 1's work is running, and left the queue when it was handed out; items 2 and 3 are the backlog.
		waitFor(t, "two items' work queued behind the running one", func() bool {
			return occupancyOf(c, pool) == 1 && queuedOf(c, pool) == 2
		})
		got = c.Stats()
		close(release)
		// Reaching zero is the leak check: a backlog that is counted on enqueue but not uncounted on dequeue would
		// only ever grow.
		waitFor(t, "the backlog to drain", func() bool { return queuedOf(c, pool) == 0 })
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		err := fo.MoveTo(ic, Tasks{pool.NewTask(func(tctx context.Context) error {
			select {
			case <-release:
			case <-tctx.Done():
			}
			return nil
		})})
		return err
	})
	<-sampled

	e := statOf(t, got, pool)
	if e.Queued.Last != 2 {
		t.Fatalf("pool: Queued.Last=%d, want 2 items of work waiting", e.Queued.Last)
	}
	if e.Occupied.Last != 1 || e.Limit != 1 {
		t.Fatalf("pool: Occupied.Last=%d Limit=%d, want 1 and 1 — a backlog is not occupancy",
			e.Occupied.Last, e.Limit)
	}
}
