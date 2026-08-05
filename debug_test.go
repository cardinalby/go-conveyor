package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// occupantNames lists the names of a snapshot's entries, for failure messages.
func occupantNames(list []UnitOccupants) []string {
	names := make([]string, 0, len(list))
	for _, e := range list {
		names = append(names, e.Unit.String())
	}
	return names
}

// occupantsOf finds the DebugUnitOccupants entry a handle produced, by identity.
func occupantsOf(t *testing.T, list []UnitOccupants, u Unit) UnitOccupants {
	t.Helper()
	for _, e := range list {
		if e.Unit == u {
			return e
		}
	}
	t.Fatalf("no DebugUnitOccupants entry for %s, got %v", u, occupantNames(list))
	return UnitOccupants{}
}

// TestDebugUnitOccupantsNilOutsideRun: like Stats, DebugUnitOccupants reports nothing outside a run.
func TestDebugUnitOccupantsNilOutsideRun(t *testing.T) {
	c := NewConveyor()
	c.AddStage(OptName("s"))

	if got := c.DebugUnitOccupants(); got != nil {
		t.Fatalf("DebugUnitOccupants before the first run = %v, want nil", got)
	}
	runNOK(t, c, 1, func(ctx context.Context, no int64) error { return nil })
	if got := c.DebugUnitOccupants(); got != nil {
		t.Fatalf("DebugUnitOccupants after the run returned = %v, want nil", got)
	}
}

// TestDebugUnitOccupantsOneEntryPerNodeAndBranch mirrors TestStatsOneEntryPerNodeAndBranch: one entry per node and per
// lane, in creation order, matching Stats' own shape.
func TestDebugUnitOccupantsOneEntryPerNodeAndBranch(t *testing.T) {
	c := NewConveyor()
	s1 := c.AddStage(OptName("s1")).SetQueueSize(2)
	fo := c.AddFanOut(OptName("fo")).SetQueueSize(3)
	l1 := fo.AddLane(OptName("l1"))
	l2 := fo.AddPool(OptName("l2"))
	in := l1.AddStage(OptName("in"))
	commit := c.AddStage(OptName("commit"))

	var got []UnitOccupants
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
		got = c.DebugUnitOccupants()
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}

	want := []Unit{c.StartUnit(), s1, fo, l1, l2, in, commit}
	if len(got) != len(want) {
		t.Fatalf("DebugUnitOccupants has %d entries (%v), want %d", len(got), occupantNames(got), len(want))
	}
	for i, u := range want {
		if got[i].Unit != u {
			t.Fatalf("entry %d = %s, want %s", i, got[i].Unit, u)
		}
	}
}

// TestDebugUnitOccupantsStartUnit: an item occupies the implicit start stage's body from creation until its first
// move, and DebugUnitOccupants reports it there.
func TestDebugUnitOccupantsStartUnit(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	release := make(chan struct{})
	sampled := make(chan struct{})
	entered := make(chan struct{})
	var got []UnitOccupants

	go func() {
		defer close(sampled)
		<-entered
		got = c.DebugUnitOccupants()
		close(release)
	}()

	runNOK(t, c, 1, func(ctx context.Context, no int64) error {
		close(entered)
		<-release
		return s.MoveTo(ctx)
	})
	<-sampled

	e := occupantsOf(t, got, c.StartUnit())
	if len(e.InBody) != 1 || e.InBody[0] != 1 {
		t.Fatalf("start InBody = %v, want [1]", e.InBody)
	}
}

// TestDebugUnitOccupantsBodyAndQueueOrder mirrors TestStatsSeparatesNodeAndQueueOccupancy's setup, but checks item
// identity rather than counts: the item running s2's code, and the two behind it in FIFO order in its waiting room.
func TestDebugUnitOccupantsBodyAndQueueOrder(t *testing.T) {
	c := NewConveyor()
	s1 := c.AddStage(OptName("s1"))
	s2 := c.AddStage(OptName("s2")).SetQueueSize(2)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	release := make(chan struct{})
	sampled := make(chan struct{})
	var blocked atomic.Bool
	var got []UnitOccupants

	go func() {
		defer close(sampled)
		waitFor(t, "s2 busy with a full queue behind it", func() bool {
			return occupancyOf(c, s2) == 1 && queuedOf(c, s2) == 2
		})
		got = c.DebugUnitOccupants()
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

	e := occupantsOf(t, got, s2)
	if len(e.InBody) != 1 || e.InBody[0] != 1 {
		t.Fatalf("s2 InBody = %v, want [1]", e.InBody)
	}
	if len(e.InQueue) != 2 || e.InQueue[0] != 2 || e.InQueue[1] != 3 {
		t.Fatalf("s2 InQueue = %v, want [2 3] in FIFO order", e.InQueue)
	}
}

// TestDebugUnitOccupantsSharedStageOrder: a shared stage admits items in arrival order even though several may be
// inside at once, and DebugUnitOccupants reports InBody in that same order.
func TestDebugUnitOccupantsSharedStageOrder(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetLimit(3)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	release := make(chan struct{})
	sampled := make(chan struct{})
	var got []UnitOccupants

	go func() {
		defer close(sampled)
		waitFor(t, "three items inside the shared stage", func() bool { return occupancyOf(c, s) == 3 })
		got = c.DebugUnitOccupants()
		close(release)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		if err := s.MoveTo(ic); err != nil {
			return err
		}
		select {
		case <-release:
		case <-ic.Done():
		}
		return nil
	})
	<-sampled

	e := occupantsOf(t, got, s)
	if len(e.InBody) != 3 || e.InBody[0] != 1 || e.InBody[1] != 2 || e.InBody[2] != 3 {
		t.Fatalf("s InBody = %v, want [1 2 3]", e.InBody)
	}
}

// TestDebugUnitOccupantsPoolBodyAndQueue mirrors TestStatsReportsPoolBacklog's setup, but checks item identity: the
// item whose task is running on the lane, and the two behind it whose batches are queued, in FIFO order. Each
// queued item contributes one entry (one scheduled batch), matching how the lane's Queued gauge counts.
func TestDebugUnitOccupantsPoolBodyAndQueue(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(3) // three items may be inside, so three may enqueue
	pool := fo.AddPool(OptName("pool"))          // limit 1: one piece of work at a time

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	release := make(chan struct{})
	sampled := make(chan struct{})
	var got []UnitOccupants

	go func() {
		defer close(sampled)
		waitFor(t, "one item's work running with two queued behind it", func() bool {
			return occupancyOf(c, pool) == 1 && queuedOf(c, pool) == 2
		})
		got = c.DebugUnitOccupants()
		close(release)
		waitFor(t, "the backlog to drain", func() bool { return queuedOf(c, pool) == 0 })
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		return fo.MoveTo(ic, Tasks{pool.NewTask(func(tctx context.Context) error {
			select {
			case <-release:
			case <-tctx.Done():
			}
			return nil
		})})
	})
	<-sampled

	e := occupantsOf(t, got, pool)
	if len(e.InBody) != 1 || e.InBody[0] != 1 {
		t.Fatalf("pool InBody = %v, want [1]", e.InBody)
	}
	if len(e.InQueue) != 2 || e.InQueue[0] != 2 || e.InQueue[1] != 3 {
		t.Fatalf("pool InQueue = %v, want [2 3] in FIFO order", e.InQueue)
	}
}
