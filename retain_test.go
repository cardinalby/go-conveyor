package conveyor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// TestRetainKeepsStageOccupiedAfterMovingOn: the retained slot is not given up when the item moves on. While the
// bgOp runs, the stage stays occupied and no other item may enter it.
func TestRetainKeepsStageOccupiedAfterMovingOn(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	release := make(chan struct{}) // lets the bgOp return
	inB := make(chan struct{})     // item 1 has moved on to b
	park := make(chan struct{})    // keeps item 1 inside b
	var created, enteredA atomic.Int64

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		<-inB
		waitFor(t, "a follower item to be waiting at the retained stage", func() bool { return created.Load() >= 2 })
		if got := occupancyOf(c, a); got != 1 {
			t.Errorf("retained stage occupancy = %d while the bgOp ran, want 1", got)
		}
		if got := enteredA.Load(); got != 1 {
			t.Errorf("%d items entered the retained stage while the bgOp ran, want 1", got)
		}
		close(release)
		waitFor(t, "the follower to enter once the bgOp returned", func() bool { return enteredA.Load() >= 2 })
		close(park)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		created.Add(1)
		if err := a.MoveTo(ic); err != nil {
			return err
		}
		enteredA.Add(1)
		if no != 1 {
			return nil
		}
		w := a.Retain(ic, func() error {
			select {
			case <-release:
			case <-ic.Done():
			}
			return nil
		})
		if err := b.MoveTo(ic); err != nil {
			return err
		}
		close(inB)
		select {
		case <-park:
		case <-ic.Done():
		}
		<-w.Finished()
		return w.Err()
	})
}

// TestRetainSlotHeldUntilItemMovesOn is the other half of the contract: when the bgOp returns while the item is
// still inside the retained stage, the slot stays held until the item actually moves on.
func TestRetainSlotHeldUntilItemMovesOn(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	g := &gauge{}
	var created, enteredA atomic.Int64

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		created.Add(1)
		if err := a.MoveTo(ic); err != nil {
			return err
		}
		enteredA.Add(1)
		g.enter()
		if no != 1 {
			g.leave()
			return nil
		}
		w := a.Retain(ic, func() error { return nil })
		<-w.Finished() // the bgOp is done, but the item has not moved on yet
		waitFor(t, "a follower item to be waiting at the retained stage", func() bool { return created.Load() >= 2 })
		if got := occupancyOf(c, a); got != 1 {
			t.Errorf("stage occupancy = %d after the bgOp returned while the item was still inside, want 1", got)
		}
		if got := enteredA.Load(); got != 1 {
			t.Errorf("%d items were inside the exclusive stage, want 1", got)
		}
		g.leave()
		if err := b.MoveTo(ic); err != nil {
			return err
		}
		waitFor(t, "the retained slot to be released once the item moved on", func() bool {
			return enteredA.Load() >= 2
		})
		cancel()
		return w.Err()
	})

	if peak, _ := g.snapshot(); peak != 1 {
		t.Fatalf("exclusive stage held %d items at once, want 1", peak)
	}
}

// TestRetainJoinWaitsForBgOp: a retain wave is joined like any other — naming it in a later MoveTo blocks until
// the background work has finished, so that node's work observes its effect.
func TestRetainJoinWaitsForBgOp(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))

	release := make(chan struct{})
	var bgDone, joined atomic.Bool

	go func() {
		// The bgOp cannot finish before the item is inside commit and blocked in the join.
		waitFor(t, "the item to enter commit and block in the join", func() bool { return occupancyOf(c, commit) == 1 })
		close(release)
	}()

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.Retain(ctx, func() error {
			<-release
			bgDone.Store(true)
			return nil
		})
		if err := commit.MoveTo(ctx, w); err != nil {
			return err
		}
		if !bgDone.Load() {
			t.Errorf("the join returned before the retained work had finished")
		}
		joined.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !joined.Load() {
		t.Fatalf("the item never reached the joining stage")
	}
}

// TestRetainJoinObservesBackgroundEffect: by the time the joining MoveTo returns nil, the retained work's effect
// is visible to the code that runs in that stage.
func TestRetainJoinObservesBackgroundEffect(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))

	var mu sync.Mutex
	effects := map[int64]bool{}

	runNOK(t, c, 8, func(ctx context.Context, no int64) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.Retain(ctx, func() error {
			mu.Lock()
			effects[no] = true
			mu.Unlock()
			return nil
		})
		if err := commit.MoveTo(ctx, w); err != nil {
			return err
		}
		mu.Lock()
		seen := effects[no]
		mu.Unlock()
		if !seen {
			t.Errorf("item %d ran the stage after the join without the retained work's effect", no)
		}
		return nil
	})
}

// TestRetainJoinReturnsBgOpError: the bgOp's error surfaces from the MoveTo that names the wave, and that node's
// work does not run.
func TestRetainJoinReturnsBgOpError(t *testing.T) {
	boom := errors.New("retain boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))

	var joinErr error
	var committed atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.Retain(ctx, func() error { return boom })
		joinErr = commit.MoveTo(ctx, w)
		if joinErr != nil {
			return joinErr
		}
		committed.Store(true)
		return nil
	})

	if !errors.Is(joinErr, boom) {
		t.Fatalf("join error = %v, want %v", joinErr, boom)
	}
	if committed.Load() {
		t.Fatalf("the joining stage ran its work although the retained work had failed")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
}

// TestRetainErrorCancelsItemContext: a bgOp error is fail-fast — it poisons the item's context, so the item's next
// node call fails even though the wave was never joined.
func TestRetainErrorCancelsItemContext(t *testing.T) {
	boom := errors.New("fail-fast boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	mid := c.AddStage(OptName("mid"))

	var moveErr error
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.Retain(ctx, func() error { return boom })
		<-w.Finished()
		moveErr = mid.MoveTo(ctx) // not joined here: the failure must reach the item through its context
		if moveErr == nil {
			t.Errorf("the next node call succeeded although the retained work had failed")
		}
		return w.Err()
	})
	if !errors.Is(moveErr, boom) {
		t.Fatalf("next MoveTo error = %v, want %v", moveErr, boom)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
}

// TestRetainUnobservedErrorFailsRun is the safety net: a retain failure nobody joined and nobody read still fails
// the run when the item completes.
func TestRetainUnobservedErrorFailsRun(t *testing.T) {
	boom := errors.New("unobserved retain boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if err := write.MoveTo(ic); err != nil {
			return err
		}
		if no == 1 {
			_ = write.Retain(ic, func() error { return boom }) // never joined, never read
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want the unobserved %v", err, boom)
	}
}

// TestRetainObservedErrorDoesNotFailRun: reading Err after Finished counts as observing the outcome, so the same
// failure is not reported again at item completion.
func TestRetainObservedErrorDoesNotFailRun(t *testing.T) {
	boom := errors.New("observed retain boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))

	var got error
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.Retain(ctx, func() error { return boom })
		<-w.Finished()
		got = w.Err()
		return nil
	})
	if !errors.Is(got, boom) {
		t.Fatalf("Wave.Err = %v, want %v", got, boom)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("observing the retain error should not fail the run, got %v", err)
	}
}

// TestRetainOnSharedStageHoldsOneSlot: on a stage with several slots a retain holds exactly one of them, so the
// others keep admitting items.
//
// Topology: start(1) -> shared(3) -> next(1). Item 1 retains its shared slot and parks in next; items 2 and 3
// take the other two shared slots and pile up at next's door, so item 4 waits at the start:
//
//	next(item 1) <- shared full (retain + items 2,3) <- start(item 4) <- item 5 never created
func TestRetainOnSharedStageHoldsOneSlot(t *testing.T) {
	c := NewConveyor()
	shared := c.AddStage(OptName("shared")).SetLimit(3)
	next := c.AddStage(OptName("next"))

	release := make(chan struct{}) // lets the bgOp return
	park := make(chan struct{})    // keeps item 1 inside next
	var created, enteredShared atomic.Int64

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		waitFor(t, "the pipeline to jam behind the retained slot", func() bool {
			return occupancyOf(c, shared) == 3 && created.Load() >= 4
		})
		if got := created.Load(); got != 4 {
			t.Errorf("%d items alive while jammed, want exactly 4: the retain must hold exactly one of the 3 slots", got)
		}
		if got := enteredShared.Load(); got != 3 {
			t.Errorf("%d items entered the shared stage, want 3", got)
		}
		close(release)
		close(park)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		created.Add(1)
		if err := shared.MoveTo(ic); err != nil {
			return err
		}
		enteredShared.Add(1)
		if no != 1 {
			return next.MoveTo(ic)
		}
		w := shared.Retain(ic, func() error {
			select {
			case <-release:
			case <-ic.Done():
			}
			return nil
		})
		if err := next.MoveTo(ic); err != nil {
			return err
		}
		select {
		case <-park:
		case <-ic.Done():
		}
		<-w.Finished()
		return w.Err()
	})
}

// TestRetainOnCanceledItemSkipsBgOp: when the item's context is already canceled, the bgOp is not run and the
// returned wave is born finished carrying the cancellation cause.
func TestRetainOnCanceledItemSkipsBgOp(t *testing.T) {
	boom := errors.New("poison")
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	var ran atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		// The first retain fails, which cancels the item's context with its error.
		first := s.Retain(ctx, func() error { return boom })
		<-first.Finished()
		if got := first.Err(); !errors.Is(got, boom) {
			t.Errorf("first wave error = %v, want %v", got, boom)
		}
		second := s.Retain(ctx, func() error {
			ran.Store(true)
			return nil
		})
		select {
		case <-second.Finished():
		default:
			t.Errorf("a retain on a canceled item must hand back an already-finished wave")
		}
		if got := second.Err(); !errors.Is(got, boom) {
			t.Errorf("second wave error = %v, want the cancellation cause %v", got, boom)
		}
		return nil
	})
	if ran.Load() {
		t.Fatalf("the bgOp ran although the item's context was already canceled")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("both waves were observed, so the run should not fail, got %v", err)
	}
}

// TestRetainSeveralStagesJoinedTogether: an item may retain every stage it occupies at once; each stage stays held
// until its own bgOp returns, and all of them are joined by one MoveTo.
func TestRetainSeveralStagesJoinedTogether(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))
	commit := c.AddStage(OptName("commit"))

	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	var aDone, bDone atomic.Bool

	go func() {
		waitFor(t, "the item to block in the join at commit", func() bool { return occupancyOf(c, commit) == 1 })
		// Both retained stages must still be held while their work runs.
		if got := occupancyOf(c, a); got != 1 {
			t.Errorf("stage a occupancy = %d while its retained work ran, want 1", got)
		}
		if got := occupancyOf(c, b); got != 1 {
			t.Errorf("stage b occupancy = %d while its retained work ran, want 1", got)
		}
		close(releaseA)
		close(releaseB)
	}()

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		wa := a.Retain(ctx, func() error {
			<-releaseA
			aDone.Store(true)
			return nil
		})
		if err := b.MoveTo(ctx); err != nil {
			return err
		}
		wb := b.Retain(ctx, func() error {
			<-releaseB
			bDone.Store(true)
			return nil
		})
		if err := commit.MoveTo(ctx, wa, wb); err != nil {
			return err
		}
		if !aDone.Load() || !bDone.Load() {
			t.Errorf("the join returned with a=%v b=%v, want both done", aDone.Load(), bDone.Load())
		}
		if got := occupancyOf(c, a); got != 0 {
			t.Errorf("stage a occupancy = %d after its retained work finished and the item moved on, want 0", got)
		}
		if got := occupancyOf(c, b); got != 0 {
			t.Errorf("stage b occupancy = %d after its retained work finished and the item moved on, want 0", got)
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestRetainTwiceOnSameStageBothJoined: two retains on one stage are two independent waves, and naming both in a
// later MoveTo waits for both.
func TestRetainTwiceOnSameStageBothJoined(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))
	commit := c.AddStage(OptName("commit"))

	release := make(chan struct{})
	var fastDone, slowDone atomic.Bool

	go func() {
		waitFor(t, "the item to block in the join at commit", func() bool { return occupancyOf(c, commit) == 1 })
		close(release)
	}()

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		fast := a.Retain(ctx, func() error {
			fastDone.Store(true)
			return nil
		})
		slow := a.Retain(ctx, func() error {
			<-release
			slowDone.Store(true)
			return nil
		})
		if err := b.MoveTo(ctx); err != nil {
			return err
		}
		if err := commit.MoveTo(ctx, fast, slow); err != nil {
			return err
		}
		if !fastDone.Load() || !slowDone.Load() {
			t.Errorf("the join returned with fast=%v slow=%v, want both done", fastDone.Load(), slowDone.Load())
		}
		if got := occupancyOf(c, a); got != 0 {
			t.Errorf("stage a occupancy = %d after both retains finished, want 0", got)
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestRetainCompletionWaitsForBgOp: an item that returns without moving on still waits for its retained work, and
// the slot it kept is released only then.
func TestRetainCompletionWaitsForBgOp(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))

	release := make(chan struct{})
	var created, enteredA atomic.Int64
	var returned atomic.Bool

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		waitFor(t, "item 1 to return with its retained work still running", func() bool {
			return returned.Load() && created.Load() >= 2
		})
		if got := occupancyOf(c, a); got != 1 {
			t.Errorf("stage occupancy = %d while the retained work of a returned item ran, want 1", got)
		}
		if got := enteredA.Load(); got != 1 {
			t.Errorf("%d items entered the stage while the retained work ran, want 1", got)
		}
		close(release)
		waitFor(t, "the follower to enter once the item's completion released the slot", func() bool {
			return enteredA.Load() >= 2
		})
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		created.Add(1)
		if err := a.MoveTo(ic); err != nil {
			return err
		}
		enteredA.Add(1)
		if no != 1 {
			return nil
		}
		_ = a.Retain(ic, func() error {
			select {
			case <-release:
			case <-ic.Done():
			}
			return nil
		})
		returned.Store(true)
		return nil // returns without ever moving on
	})
}

// TestRetainByChildHoldsLaneInteriorStage: Retain is not a root-item feature. A child item travelling a lane's
// interior nodes may retain one of them, and the contract holds inside the lane's own rank space exactly as it does
// on the conveyor: the slot stays held while the bgOp runs, the child moves on without it, and the next child cannot
// enter until the bgOp returns.
func TestRetainByChildHoldsLaneInteriorStage(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))  // entrance admits one child at a time; mid/tail carry the concurrency
	mid := lane.AddStage(OptName("mid")) // exclusive: one child inside at a time
	tail := lane.AddStage(OptName("tail")).SetLimit(2)
	commit := c.AddStage(OptName("commit"))

	var started atomic.Int64
	var bgRan atomic.Bool
	var joinSawBgOp atomic.Bool
	rec := &recorder{}

	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTasks(2, func(cctx context.Context, i int) error {
			started.Add(1)
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			rec.add("mid-in-%d", i)
			// Child 0 enters mid first: children are linked in creation order and the ordering gate holds child 1
			// out until child 0 has published mid's rank.
			if i != 0 {
				return tail.MoveTo(cctx)
			}
			rw := mid.Retain(cctx, func() error {
				// The other child is running and blocked at mid's door; the retained slot is what holds it there.
				waitFor(t, "the sibling child to be running", func() bool { return started.Load() == 2 })
				if occ := occupancyOf(c, mid); occ != 1 {
					t.Errorf("mid occupancy during the retained bgOp = %d, want 1 (the slot is still held)", occ)
				}
				bgRan.Store(true)
				rec.add("bg-done")
				return nil
			})
			if err := tail.MoveTo(cctx, rw); err != nil {
				return err
			}
			joinSawBgOp.Store(bgRan.Load())
			return nil
		})})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}

	if !joinSawBgOp.Load() {
		t.Fatalf("the child's join at tail returned before its retained bgOp had finished")
	}
	events := rec.all()
	want := []string{"mid-in-0", "bg-done", "mid-in-1"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i, e := range events {
		if e != want[i] {
			t.Fatalf("events = %v, want %v — the sibling entered mid before the retained slot was freed", events, want)
		}
	}
}

// TestRetainByChildUnobservedErrorFailsRun: a child's retained bgOp is charged to a wave the *child* owns, so its
// error has two levels to climb — the child's completion, then the wave that created the child — before it can fail
// the run. Nothing joins it here, so it travels the unobserved path the whole way.
func TestRetainByChildUnobservedErrorFailsRun(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	err := runUntil(t, c, 3, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTask(func(cctx context.Context) error {
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			_ = mid.Retain(cctx, func() error { return boom }) // the wave is never joined and never read
			return nil
		})})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if !errors.Is(err, boom) {
		t.Fatalf("run error = %v, want the child's retained bgOp error %v", err, boom)
	}
}
