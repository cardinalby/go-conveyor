package conveyor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

// TestUnitIndexesFollowCreationOrder pins the flat index space: index 0 is the implicit start stage, and every unit
// built afterwards — stages, fan-outs and branches — takes the next index in creation order. A waiting room adds no
// unit, so it takes no index: it is capacity on the node it fronts.
func TestUnitIndexesFollowCreationOrder(t *testing.T) {
	c := New()
	s1 := c.AddStage(WithName("s1"))
	fo := c.AddFanOut(WithName("fo"))
	l1 := fo.AddLane(WithName("l1"))
	l2 := fo.AddPool(WithName("l2"))
	s2 := c.AddStage(WithName("s2")).SetQueueSize(2) // a waiting room, which creates no unit of its own
	in := l1.AddStage(WithName("in"))                // built into a lane, but indexed in the one flat space

	start := c.StartingStage().unit()
	if start.index != 0 || start.kind != kindStart {
		t.Fatalf("start unit: index=%d kind=%d, want index 0 and the start kind", start.index, start.kind)
	}
	units := implOf(c).units
	want := []*unit{start, s1.unit(), fo.unit(), l1.unit(), l2.unit(), s2.unit(), in.unit()}
	if len(units) != len(want) {
		t.Fatalf("conveyor has %d units, want %d", len(units), len(want))
	}
	for i, u := range want {
		if u.index != i {
			t.Fatalf("%s has index %d, want %d", u, u.index, i)
		}
		if units[i] != u {
			t.Fatalf("units[%d] = %s, want %s", i, units[i], u)
		}
	}
}

// TestRanksReserveTwoForEveryNode pins the rank layout of the root series: the start gate is rank 0, then every
// node owns two consecutive ranks — its waiting room first, the node itself second — whether or not a queue is
// configured. Reserving the pair unconditionally is what lets a queue appear at runtime without renumbering
// anything, and the lower rank is what keeps a waiting item from letting a follower into the node ahead of it.
func TestRanksReserveTwoForEveryNode(t *testing.T) {
	c := New()
	a := c.AddStage(WithName("a"))
	b := c.AddStage(WithName("b")).SetQueueSize(2)
	fo := c.AddFanOut(WithName("fo")).SetQueueSize(3)
	d := c.AddStage(WithName("d"))
	ci := implOf(c)
	ci.finalize()

	for _, tc := range []struct {
		u    *unit
		rank int
	}{
		{c.StartingStage().unit(), 0},
		{a.unit(), 2},
		{b.unit(), 4},
		{fo.unit(), 6},
		{d.unit(), 8},
	} {
		if tc.u.rank != tc.rank {
			t.Fatalf("%s has rank %d, want %d", tc.u, tc.u.rank, tc.rank)
		}
		if tc.u.scope != 0 {
			t.Fatalf("%s has scope %d, want the root series (0)", tc.u, tc.u.scope)
		}
	}
	// Every node owns the rank below its own, queue or no queue, and no other unit may hold it.
	for _, u := range []*unit{a.unit(), b.unit(), fo.unit(), d.unit()} {
		if got := ci.describeRank(0, u.queueRank()); got != u.queueName() {
			t.Fatalf("rank %d of %s is named %q, want it reserved for the waiting room", u.queueRank(), u, got)
		}
	}
}

// TestBranchIsItsOwnRankSpace pins the recursion: every branch is a scope of its own whose unit is rank 0, a lane's
// interior nodes take the reserved rank pairs that follow (2, 4, ...), and a nested fan-out's own branches open
// further scopes.
func TestBranchIsItsOwnRankSpace(t *testing.T) {
	c := New()
	pre := c.AddStage(WithName("pre"))
	fo := c.AddFanOut(WithName("fo"))
	l1 := fo.AddLane(WithName("l1"))
	l2 := fo.AddPool(WithName("l2"))
	i1 := l1.AddStage(WithName("i1"))
	inner := l1.AddFanOut(WithName("inner")).SetQueueSize(2)
	il := inner.AddLane(WithName("il"))
	ii := il.AddStage(WithName("ii"))
	implOf(c).finalize()

	rootScope := c.StartingStage().unit().scope
	for _, tc := range []struct {
		u     *unit
		scope int
		rank  int
	}{
		{c.StartingStage().unit(), rootScope, 0},
		{pre.unit(), rootScope, 2},
		{fo.unit(), rootScope, 4},
		// l1's scope: the lane's unit is rank 0, then its interior nodes.
		{l1.unit(), branchScope(l1), 0},
		{i1.unit(), branchScope(l1), 2},
		{inner.unit(), branchScope(l1), 4},
		// The pool and the nested fan-out's lane are scopes of their own, each starting again at rank 0.
		{l2.unit(), branchScope(l2), 0},
		{il.unit(), branchScope(il), 0},
		{ii.unit(), branchScope(il), 2},
	} {
		if tc.u.scope != tc.scope || tc.u.rank != tc.rank {
			t.Fatalf("%s has scope=%d rank=%d, want scope=%d rank=%d", tc.u, tc.u.scope, tc.u.rank, tc.scope, tc.rank)
		}
	}
	scopes := map[int]bool{rootScope: true}
	for _, l := range []Branch{l1, l2, il} {
		if scopes[branchScope(l)] {
			t.Fatalf("branch %s shares scope %d with another series", l, branchScope(l))
		}
		scopes[branchScope(l)] = true
	}
}

// TestPositionalNames pins the naming fallback: WithName wins, an unnamed node is named after its ordinal among its
// series' nodes, a branch after its fan-out, a node built in a lane is prefixed by that lane, and a queue by the node
// it fronts. The ordinal counts nodes, not ranks, so a name never moves when a queue is added.
func TestPositionalNames(t *testing.T) {
	c := New()
	s1 := c.AddStage()                  // 1st node
	fo := c.AddFanOut(WithName("fo"))   // 2nd node, named
	l1 := fo.AddLane()                  // 1st branch of fo
	l2 := fo.AddPool(WithName("named")) // 2nd branch, named
	in := l1.AddStage()                 // 1st node of l1's own scope
	s2 := c.AddStage(WithName("s")).SetQueueSize(2)
	s3 := c.AddStage().SetQueueSize(2) // 4th node, queued
	fo2 := c.AddFanOut()               // 5th node
	implOf(c).finalize()

	for _, tc := range []struct{ got, want string }{
		{fmt.Sprint(c.StartingStage()), "start"},
		{fmt.Sprint(s1), "stage 1"},
		{fmt.Sprint(fo), "fo"},
		{fmt.Sprint(l1), "fo.1"},
		{fmt.Sprint(l2), "named"},
		{fmt.Sprint(in), "fo.1 / stage 1"},
		{fmt.Sprint(s2), "s"},
		{s2.unit().queueName(), "s.queue"},
		{fmt.Sprint(s3), "stage 4"},
		{s3.unit().queueName(), "stage 4.queue"},
		{fmt.Sprint(fo2), "fan-out 5"},
	} {
		if tc.got != tc.want {
			t.Fatalf("name = %q, want %q", tc.got, tc.want)
		}
	}
}

// TestPositionalNamesInStats: the same positional names identify the nodes in Stats, where a caller that named
// nothing still has to be able to tell the entries apart.
func TestPositionalNamesInStats(t *testing.T) {
	c := New()
	s := c.AddStage()
	fo := c.AddFanOut()
	lane := fo.AddLane()
	in := lane.AddStage()

	var got Stats
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
				return in.MoveTo(cctx)
			}))
		}
		if err != nil {
			return err
		}
		w := fo.Retain(ctx)
		got = c.Stats()
		<-w.Finished()
		return groupErr(w)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	for _, name := range []string{"start", "stage 1", "fan-out 2", "fan-out 2.1", "fan-out 2.1 / stage 1"} {
		if _, ok := unitStatByName(got, name); !ok {
			t.Fatalf("no Stats entry named %q, got %v", name, statNames(got))
		}
	}
}

// TestLimitsDefaultToOne: every node is born exclusive, and a node without SetQueueSize reports no waiting room.
func TestLimitsDefaultToOne(t *testing.T) {
	c := New()
	s := c.AddStage()
	fo := c.AddFanOut()
	pool := fo.AddPool()

	if s.Limit() != 1 || fo.Limit() != 1 || pool.Limit() != 1 {
		t.Fatalf("default limits: stage=%d fan-out=%d pool=%d, want 1 each", s.Limit(), fo.Limit(), pool.Limit())
	}
	if s.QueueSize() != 0 || fo.QueueSize() != 0 {
		t.Fatalf("QueueSize without SetQueueSize: stage=%d fan-out=%d, want 0 each", s.QueueSize(), fo.QueueSize())
	}
}

// TestSettersPanicOutOfRange: a value outside a setter's range is a programmer error and panics at the call, leaving
// the old value; the boundary values are accepted (limit 1, queue size 0, item limit 0, the zero mode, count 0).
func TestSettersPanicOutOfRange(t *testing.T) {
	c := New()
	s := c.AddStage(WithName("s")).SetLimit(3).SetQueueSize(2)
	fo := c.AddFanOut(WithName("fo")).SetLimit(3).SetQueueSize(2).SetBackpressure(BackpressureStrict)
	pool := fo.AddPool(WithName("pool")).SetLimit(3)
	lane := fo.AddLane(WithName("lane"))
	c.SetItemLimit(3)

	for _, n := range []int{0, -1} {
		assertPanics(t, errInvalidValue, func() { s.SetLimit(n) })
		assertPanics(t, errInvalidValue, func() { fo.SetLimit(n) })
		assertPanics(t, errInvalidValue, func() { pool.SetLimit(n) })
	}
	assertPanics(t, errInvalidValue, func() { s.SetQueueSize(-1) })
	assertPanics(t, errInvalidValue, func() { fo.SetQueueSize(-1) })
	assertPanics(t, errInvalidValue, func() { c.SetItemLimit(-1) })
	for _, m := range []FanOutBackpressure{BackpressureStrict + 1, -1} {
		assertPanics(t, errInvalidValue, func() { fo.SetBackpressure(m) })
	}
	fn := func(context.Context, int) error { return nil }
	assertPanics(t, errInvalidValue, func() { pool.NewTasks(-1, fn) })
	assertPanics(t, errInvalidValue, func() { lane.NewTasks(-1, nil) })

	if s.Limit() != 3 || fo.Limit() != 3 || pool.Limit() != 3 || s.QueueSize() != 2 || fo.QueueSize() != 2 ||
		c.ItemLimit() != 3 || fo.Backpressure() != BackpressureStrict {
		t.Fatalf("a panicking setter changed a value: stage=%d/%d fan-out=%d/%d/%v pool=%d items=%d",
			s.Limit(), s.QueueSize(), fo.Limit(), fo.QueueSize(), fo.Backpressure(), pool.Limit(), c.ItemLimit())
	}

	if s.SetLimit(1).Limit() != 1 || fo.SetLimit(1).Limit() != 1 || pool.SetLimit(1).Limit() != 1 {
		t.Fatalf("SetLimit(1) not accepted: stage=%d fan-out=%d pool=%d", s.Limit(), fo.Limit(), pool.Limit())
	}
	if s.SetQueueSize(0).QueueSize() != 0 || fo.SetQueueSize(0).QueueSize() != 0 {
		t.Fatalf("SetQueueSize(0) not accepted: stage=%d fan-out=%d", s.QueueSize(), fo.QueueSize())
	}
	if got := c.SetItemLimit(0).ItemLimit(); got != 0 {
		t.Fatalf("SetItemLimit(0): ItemLimit = %d, want 0", got)
	}
	if got := fo.SetBackpressure(FanOutBackpressure(0)).Backpressure(); got != BackpressureBalanced {
		t.Fatalf("SetBackpressure(0): Backpressure = %v, want Balanced", got)
	}
	_ = pool.NewTasks(0, nil)
	_ = lane.NewTasks(0, fn)
}

// TestTopologyFrozenFromTheFirstRun: extending the topology is build-time only — it panics while a run is active
// and after one has returned. Capacity changes (SetLimit and SetQueueSize) are not topology and stay legal
// throughout, including giving a node a waiting room it never had.
func TestTopologyFrozenFromTheFirstRun(t *testing.T) {
	c := New()
	s := c.AddStage(WithName("s"))
	queued := c.AddStage(WithName("queued")).SetQueueSize(1)
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))

	// Assert on the test goroutine while a run is provably in flight: the first item parks until released.
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	runDone := make(chan struct{})
	var first atomic.Bool
	go func() {
		defer close(runDone)
		_ = c.Run(ctx, func(ic context.Context) error {
			if first.CompareAndSwap(false, true) {
				close(started)
				select {
				case <-release:
				case <-ic.Done():
				}
			}
			return nil
		})
	}()
	<-started

	assertPanics(t, errConveyorRunning, func() { c.AddStage() })
	assertPanics(t, errConveyorRunning, func() { c.AddFanOut() })
	assertPanics(t, errConveyorRunning, func() { fo.AddLane() })
	s.SetQueueSize(2)      // a waiting room where there was none: capacity, not topology
	queued.SetQueueSize(4) // and resizing an existing one
	s.SetLimit(3)
	pool.SetLimit(2)
	if queued.QueueSize() != 4 || s.QueueSize() != 2 || s.Limit() != 3 || pool.Limit() != 2 {
		t.Fatalf("resize while running: queue=%d new queue=%d stage=%d pool=%d, want 4/2/3/2",
			queued.QueueSize(), s.QueueSize(), s.Limit(), pool.Limit())
	}

	close(release)
	cancel()
	<-runDone

	assertPanics(t, errConveyorFinalized, func() { c.AddStage() })
	assertPanics(t, errConveyorFinalized, func() { c.AddFanOut() })
	assertPanics(t, errConveyorFinalized, func() { fo.AddLane() })
	queued.SetQueueSize(7)
	s.SetQueueSize(0) // and taking a waiting room away again
	s.SetLimit(5)
	if queued.QueueSize() != 7 || s.QueueSize() != 0 || s.Limit() != 5 {
		t.Fatalf("resize after the run: queue=%d removed queue=%d stage=%d, want 7/0/5",
			queued.QueueSize(), s.QueueSize(), s.Limit())
	}
}

// TestStartingStageHandle: the implicit start stage is a real unit with limit 1, named "start", and it leads the Stats
// entries so a caller can watch the gate that paces item creation.
func TestStartingStageHandle(t *testing.T) {
	c := New()
	s := c.AddStage(WithName("s"))

	start := c.StartingStage()
	if got := fmt.Sprint(start); got != "start" {
		t.Fatalf("StartingStage name = %q, want %q", got, "start")
	}
	if got := int(start.unit().limit.Load()); got != 1 {
		t.Fatalf("start unit limit = %d, want 1", got)
	}

	var got Stats
	err := runOnce(t, c, func(ctx context.Context) error {
		got = c.Stats()
		return s.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if len(got.Units) == 0 {
		t.Fatalf("Stats reported no units")
	}
	if got.Units[0].Unit != c.StartingStage() {
		t.Fatalf("first Stats entry = %s, want the start unit", got.Units[0].Unit)
	}
	if got.Units[0].Limit != 1 {
		t.Fatalf("start unit Stats limit = %d, want 1", got.Units[0].Limit)
	}
}

// TestBranchesAccessorIsAnOrderedCopy: Branches() hands out the branches — pools and lanes alike — in creation order
// and never the internal slice.
func TestBranchesAccessorIsAnOrderedCopy(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	a := fo.AddPool(WithName("a"))
	b := fo.AddLane() // both kinds land in one list, sharing one numbering sequence
	_ = b.AddStage(WithName("b.inner"))
	d := fo.AddPool(WithName("d"))

	branches := fo.Branches()
	if len(branches) != 3 || branches[0] != a || branches[1] != b || branches[2] != d {
		t.Fatalf("Branches() = %v, want [a fo.2 d] in creation order", branches)
	}
	// The unnamed lane is the 2nd branch, so it is "fo.2" — pools and lanes are numbered together.
	if got := sprintf("%s", branches[1]); got != "fo.2" {
		t.Fatalf("unnamed lane name = %q, want %q", got, "fo.2")
	}
	branches[1] = nil
	if fo.Branches()[1] != b {
		t.Fatalf("Branches() handed out its internal slice")
	}
}
