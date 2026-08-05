package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// TestBatchSplitsIntoChildJourneys is the flagship 1->N shape: one item reads a batch, each message becomes a
// child item travelling the lane's interior stages, and the commit joins them all.
func TestBatchSplitsIntoChildJourneys(t *testing.T) {
	const batch = 6
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	fo := c.AddFanOut(OptName("split")).SetLimit(2)
	lane := fo.AddLane(OptName("messages"))
	enrich := lane.AddStage(OptName("enrich")) // exclusive interior stage
	commit := c.AddStage(OptName("commit"))

	var enriched numbers
	enrichGauge := &gauge{}
	var committed atomic.Int64
	var enrichedAtCommit atomic.Int64

	runNOK(t, c, 4, func(ctx context.Context, no int64) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.MoveTo(ctx, Tasks{lane.NewTasks(batch, func(cctx context.Context, i int) error {
			if err := enrich.MoveTo(cctx); err != nil { // a child moving through its lane's stage
				return err
			}
			return enrichGauge.hold(func() error {
				enriched.add(int64(i))
				return nil
			})
		})})
		if err != nil {
			return err
		}
		if err := commit.MoveTo(ctx); err != nil { // joins every child of this item
			return err
		}
		committed.Add(1)
		enrichedAtCommit.Store(int64(len(enriched.all())))
		return nil
	})

	if got := committed.Load(); got < 4 {
		t.Fatalf("only %d items committed, want >= 4", got)
	}
	if got := len(enriched.all()); got < int(committed.Load())*batch {
		t.Fatalf("%d children ran, want >= %d", got, committed.Load()*batch)
	}
	if peak, _ := enrichGauge.snapshot(); peak != 1 {
		t.Fatalf("the exclusive interior stage ran %d children at once", peak)
	}
	// The commit of item N must have seen all N*batch children finished.
	if got := enrichedAtCommit.Load(); got < int64(batch) {
		t.Fatalf("commit ran with only %d children finished", got)
	}
}

// TestChildReleasesLaneEntranceOnFirstMove: a lane's entrance admits one child at a time, so a child that has moved
// into an interior stage is what lets the next one start — and more children end up inside the lane than the one its
// entrance holds.
func TestChildReleasesLaneEntranceOnFirstMove(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane")) // a lane entrance always admits one child at a time
	mid := lane.AddStage(OptName("mid"))

	inLane := &gauge{}
	proceed := make(chan struct{})
	var closeOnce atomic.Bool

	go func() {
		waitFor(t, "two children to be inside the lane at once", func() bool {
			peak, _ := inLane.snapshot()
			return peak >= 2
		})
		if closeOnce.CompareAndSwap(false, true) {
			close(proceed)
		}
	}()

	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTasks(4, func(cctx context.Context, i int) error {
			return inLane.hold(func() error {
				if err := mid.MoveTo(cctx); err != nil { // frees the lane entrance for the next child
					return err
				}
				select { // hold the interior stage so a second child piles up at the entrance
				case <-proceed:
				case <-cctx.Done():
				}
				return nil
			})
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
	if peak, entries := inLane.snapshot(); entries != 4 {
		t.Fatalf("children inside the lane: peak=%d entries=%d, want all 4 to have run", peak, entries)
	}
}

// TestChildrenPreserveOrderAcrossItems: children start in index order within an item, and all of an older item's
// children start before any of a younger item's.
func TestChildrenPreserveOrderAcrossItems(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(3)
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	var order recorder
	var itemOrder numbers
	runNOK(t, c, 5, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTasks(3, func(cctx context.Context, i int) error {
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			order.add("%d.%d", no, i)
			itemOrder.add(no)
			return nil
		})})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	// The interior stage is exclusive, so its order is total: item-major, index-ascending.
	assertNonDecreasing(t, itemOrder.all(), "children order across items")
	got := order.all()
	if len(got) < 15 {
		t.Fatalf("expected 15 children, got %d: %v", len(got), got)
	}
	for i, ev := range got {
		wantItem := int64(i/3) + 1
		wantIdx := i % 3
		if ev != sprintf("%d.%d", wantItem, wantIdx) {
			t.Fatalf("child %d = %s, want %d.%d (order: %v)", i, ev, wantItem, wantIdx, got)
		}
	}
}

// TestChildTicketOrderAcrossTasksAndItems exercises all three ordering rules at once, which the test above cannot:
// it submits *several* Tasks per item to the same lane, so the middle rule — the order they were added to the Tasks
// value — has something to decide.
//
// A child's place is stamped when its work is pulled off the lane's queue, not when its callback starts, so the total
// order at an exclusive interior stage is: older item first, then submission order, then index within a task.
func TestChildTicketOrderAcrossTasksAndItems(t *testing.T) {
	const items = 3
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(items) // let several items have work here at once
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid")) // exclusive: the order is total, so it is observable
	commit := c.AddStage(OptName("commit"))

	var order recorder
	runNOK(t, c, items, func(ctx context.Context, no int64) error {
		rec := func(label string) { order.add("%d.%s", no, label) }
		// Three Tasks for one lane, built with different constructors, deliberately not in index order.
		var ts Tasks
		ts.Add(lane.NewTask(func(cctx context.Context) error {
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			rec("A")
			return nil
		}))
		ts.Add(lane.NewTasks(2, func(cctx context.Context, i int) error {
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			rec(sprintf("B%d", i))
			return nil
		}))
		ts.Add(lane.NewTask(func(cctx context.Context) error {
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			rec("C")
			return nil
		}))
		if err := fo.MoveTo(ctx, ts); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	perItem := []string{"A", "B0", "B1", "C"}
	var want []string
	for no := 1; no <= items; no++ {
		for _, l := range perItem {
			want = append(want, sprintf("%d.%s", no, l))
		}
	}
	got := order.all()
	if len(got) != len(want) {
		t.Fatalf("%d children ran, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ticket order = %v, want %v", got, want)
		}
	}
}

// TestChildCannotMoveIntoASiblingLane covers the direction the two tests around it do not: not a child reaching *out*
// to the conveyor, nor an item reaching *in*, but sideways — a child of one lane reaching into a sibling lane's
// interior. Both scopes are non-root, so this exercises a comparison neither of the others does, and each lane's
// interior stays private to the children born on it.
func TestChildCannotMoveIntoASiblingLane(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	laneA := fo.AddLane(OptName("laneA"))
	midA := laneA.AddStage(OptName("midA"))
	laneB := fo.AddLane(OptName("laneB"))
	midB := laneB.AddStage(OptName("midB"))
	commit := c.AddStage(OptName("commit"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{
			laneA.NewTask(func(cctx context.Context) error {
				if err := midA.MoveTo(cctx); err != nil {
					return err
				}
				// midB belongs to laneB's scope, which this child does not run in.
				if e := recoveredErr(func() { _ = midB.MoveTo(cctx) }); e == nil || !errors.Is(e, errWrongScope) {
					t.Errorf("moving into a sibling lane = %v, want %v", e, errWrongScope)
				}
				checked.Store(true)
				return nil
			}),
			// laneB needs work of its own, or its scope would have no children at all.
			laneB.NewTask(func(cctx context.Context) error { return midB.MoveTo(cctx) }),
		})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the sibling-lane check did not run")
	}
}

// TestChildErrorFailsItemAndRun: a child's error escalates through the wave to its item and then to the run.
func TestChildErrorFailsItemAndRun(t *testing.T) {
	boom := errors.New("child boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	var committed atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		err := fo.MoveTo(ic, Tasks{lane.NewTasks(4, func(cctx context.Context, i int) error {
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			if i == 2 {
				return boom
			}
			return nil
		})})
		if err != nil {
			return err
		}
		if err := commit.MoveTo(ic); err != nil {
			return err
		}
		committed.Store(true)
		return nil
	})

	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
	if committed.Load() {
		t.Fatalf("the item committed although one of its children failed")
	}
}

// TestChildSeesParentCancellation: a child's context is its parent's, so a failing sibling (or shutdown) releases
// children blocked in their own work.
func TestChildSeesParentCancellation(t *testing.T) {
	boom := errors.New("sibling boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid")).SetLimit(3)

	released := make(chan struct{})
	waiting := make(chan struct{}) // closed once both siblings are parked on their own ctx
	var parked, releasedCount atomic.Int64
	var onceWaiting, onceReleased atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	err := c.Run(ctx, func(ic context.Context) error {
		err := fo.MoveTo(ic, Tasks{lane.NewTasks(3, func(cctx context.Context, i int) error {
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			if i == 0 {
				<-waiting // fail only once the siblings are actually blocked, so the test is not racy
				return boom
			}
			if parked.Add(1) == 2 && onceWaiting.CompareAndSwap(false, true) {
				close(waiting)
			}
			<-cctx.Done() // must be released by the fail-fast cancellation
			if releasedCount.Add(1) == 2 && onceReleased.CompareAndSwap(false, true) {
				close(released)
			}
			return nil
		})})
		if err != nil {
			return err
		}
		w := fo.Detach(ic)
		<-w.Finished()
		return w.Err()
	})

	<-released
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
}

// TestChildInheritsItemNumber: numbers identify the conveyor item, so a child reports its parent's.
func TestChildInheritsItemNumber(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	var mismatch atomic.Int64
	runNOK(t, c, 4, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTasks(2, func(cctx context.Context, i int) error {
			if childNo, ok := ItemNoFromContext(cctx); !ok || childNo != no {
				mismatch.Add(1)
			}
			return mid.MoveTo(cctx)
		})})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if got := mismatch.Load(); got != 0 {
		t.Fatalf("%d children reported a number other than their item's", got)
	}
}

// TestInFlightCountsItemsNotChildren: Stats.InFlight stays bounded by the conveyor's own nodes however many
// children a lane runs.
func TestInFlightCountsItemsNotChildren(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")) // limit 1
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid")).SetLimit(4)
	commit := c.AddStage(OptName("commit"))

	var maxInFlight atomic.Int64
	runNOK(t, c, 6, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTasks(10, func(cctx context.Context, i int) error {
			return mid.MoveTo(cctx)
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
		return commit.MoveTo(ctx)
	})

	// start(1) + fan-out(1) + commit(1) = 3 items of the conveyor itself; children must not inflate this.
	if got := maxInFlight.Load(); got > 3 {
		t.Fatalf("InFlight reached %d, want <= 3 (children must not be counted)", got)
	}
}

// TestChildCannotMoveOutsideItsLane: a child may only move through its own lane's nodes.
func TestChildCannotMoveOutsideItsLane(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTask(func(cctx context.Context) error {
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			assertPanics(t, errWrongScope, func() {
				_ = commit.MoveTo(cctx) // a node of the conveyor, not of this lane
			})
			checked.Store(true)
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
	if !checked.Load() {
		t.Fatalf("the scope check did not run")
	}
}

// TestItemCannotMoveIntoALane: the mirror image — the conveyor's own item may not reach into a lane's nodes.
func TestItemCannotMoveIntoALane(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid"))

	err := runOnce(t, c, func(ctx context.Context) error {
		assertPanics(t, errWrongScope, func() {
			_ = mid.MoveTo(ctx)
		})
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestNonTravellingWorkCannotMove: on a lane without interior nodes there is nowhere to go, and the attempt says so
// instead of moving the item that scheduled the work.
func TestNonTravellingWorkCannotMove(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // no interior nodes
	commit := c.AddStage(OptName("commit"))

	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTask(func(cctx context.Context) error {
			assertPanics(t, errCannotMove, func() {
				_ = commit.MoveTo(cctx)
			})
			checked.Store(true)
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
	if !checked.Load() {
		t.Fatalf("the cannot-move check did not run")
	}
}

// TestNestedFanOutInsideLane: a pool's interior may itself fan out — the model recurses.
func TestNestedFanOutInsideLane(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("outer")).SetLimit(2)
	lane := fo.AddLane(OptName("outerLane"))
	inner := lane.AddFanOut(OptName("inner"))
	innerPool := inner.AddPool(OptName("innerPool")).SetLimit(2)
	after := lane.AddStage(OptName("after"))
	commit := c.AddStage(OptName("commit"))

	var innerRuns, afterRuns atomic.Int64
	runNOK(t, c, 3, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTasks(2, func(cctx context.Context, i int) error {
			err := inner.MoveTo(cctx, Tasks{innerPool.NewTasks(3, func(_ context.Context, j int) error {
				innerRuns.Add(1)
				return nil
			})})
			if err != nil {
				return err
			}
			if err := after.MoveTo(cctx); err != nil { // joins the nested wave
				return err
			}
			afterRuns.Add(1)
			return nil
		})})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	if got := afterRuns.Load(); got < 6 {
		t.Fatalf("%d children finished their nested journey, want >= 6", got)
	}
	if got := innerRuns.Load(); got < 18 {
		t.Fatalf("%d nested tasks ran, want >= 18", got)
	}
}

// TestNestedWaveErrorFailsEverything: an error deep inside a lane's own fan-out reaches the run.
func TestNestedWaveErrorFailsEverything(t *testing.T) {
	boom := errors.New("nested boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("outer"))
	lane := fo.AddLane(OptName("outerLane"))
	inner := lane.AddFanOut(OptName("inner"))
	innerPool := inner.AddPool(OptName("innerPool"))
	after := lane.AddStage(OptName("after"))
	commit := c.AddStage(OptName("commit"))

	var afterRan atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		err := fo.MoveTo(ic, Tasks{lane.NewTask(func(cctx context.Context) error {
			err := inner.MoveTo(cctx, Tasks{innerPool.NewTask(func(context.Context) error { return boom })})
			if err != nil {
				return err
			}
			if err := after.MoveTo(cctx); err != nil {
				return err
			}
			afterRan.Store(true)
			return nil
		})})
		if err != nil {
			return err
		}
		return commit.MoveTo(ic)
	})

	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
	if afterRan.Load() {
		t.Fatalf("the child continued past a failed nested wave")
	}
}

// TestInteriorStageWithQueue: a lane's interior nodes take the same knobs as the conveyor's.
func TestInteriorStageWithQueue(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	first := lane.AddStage(OptName("first"))
	second := lane.AddStage(OptName("second")).SetQueueSize(2)
	commit := c.AddStage(OptName("commit"))

	g := &gauge{}
	var done atomic.Int64
	runNOK(t, c, 3, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTasks(5, func(cctx context.Context, i int) error {
			if err := first.MoveTo(cctx); err != nil {
				return err
			}
			if err := second.MoveTo(cctx); err != nil {
				return err
			}
			return g.hold(func() error {
				done.Add(1)
				return nil
			})
		})})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	if peak, _ := g.snapshot(); peak != 1 {
		t.Fatalf("the queued interior stage ran %d children at once, want 1", peak)
	}
	if got := done.Load(); got < 15 {
		t.Fatalf("%d children finished, want >= 15", got)
	}
}

// TestStreamingChildren: a lane with interior nodes can be fed by a streaming source too.
func TestStreamingChildren(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	var ran atomic.Int64
	runNOK(t, c, 3, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTasksGen(func(yield func(TaskFunc) bool) {
			for i := 0; i < 4; i++ {
				if !yield(func(cctx context.Context) error {
					if err := mid.MoveTo(cctx); err != nil {
						return err
					}
					ran.Add(1)
					return nil
				}) {
					return
				}
			}
		})})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	if got := ran.Load(); got < 12 {
		t.Fatalf("%d streamed children ran, want >= 12", got)
	}
}

// TestGrandchildJourneysThroughANestedLane exercises the recursion the model claims: a lane is a series, so a lane's
// interior fan-out has lanes of its own, and if one of those has interior nodes its work becomes child items
// scheduled by a child item. Every other test stops one level down; this one builds the second, where newChildItem
// runs with a child as the scheduling item.
func TestGrandchildJourneysThroughANestedLane(t *testing.T) {
	const (
		items         = 3
		children      = 2
		grandchildren = 2
	)
	c := NewConveyor()
	fo := c.AddFanOut(OptName("outer")).SetLimit(2)
	lane := fo.AddLane(OptName("outerLane"))
	inner := lane.AddFanOut(OptName("inner")) // built inside the lane, so its lanes are the lane's own
	innerLane := inner.AddLane(OptName("innerLane"))
	deep := innerLane.AddStage(OptName("deep")) // gives innerLane an interior => its work travels
	after := lane.AddStage(OptName("after")).SetLimit(2)
	commit := c.AddStage(OptName("commit"))

	var deepRuns atomic.Int64
	var wrongNo atomic.Int64
	deepG := &gauge{}

	runNOK(t, c, items, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTasks(children, func(cctx context.Context, ci int) error {
			err := inner.MoveTo(cctx, Tasks{innerLane.NewTasks(grandchildren, func(gctx context.Context, gi int) error {
				// A grandchild is a full item in the inner lane's scope: it moves through that lane's nodes.
				if err := deep.MoveTo(gctx); err != nil {
					return err
				}
				return deepG.hold(func() error {
					deepRuns.Add(1)
					// The item number is inherited twice over, so it still names the conveyor item this work
					// belongs to rather than anything about the nesting.
					if gno, ok := ItemNoFromContext(gctx); !ok || gno != no {
						wrongNo.Add(1)
					}
					return nil
				})
			})})
			if err != nil {
				return err
			}
			return after.MoveTo(cctx)
		})})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})

	if got, want := deepRuns.Load(), int64(items*children*grandchildren); got != want {
		t.Fatalf("%d grandchild journeys completed, want %d", got, want)
	}
	if got := wrongNo.Load(); got != 0 {
		t.Fatalf("%d grandchildren reported the wrong item number", got)
	}
	if peak, _ := deepG.snapshot(); peak != 1 {
		t.Fatalf("deep peak concurrency = %d, want 1 — an exclusive stage inside a doubly-nested lane", peak)
	}
}
