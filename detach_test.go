package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestDetachLetsTheItemMoveOnWithoutWaiting: a detached fan-out no longer holds the item back, so it passes the nodes
// after it while its work is still running, and the wave it was handed is what finally waits.
func TestDetachLetsTheItemMoveOnWithoutWaiting(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	mid := c.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	pastMid := make(chan struct{})
	var events recorder
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
			<-pastMid // the item must reach mid without this having finished
			events.add("task")
			return nil
		})); err != nil {
			return err
		}
		w := fo.Detach(ctx)
		if err := mid.MoveTo(ctx); err != nil {
			return err
		}
		events.add("mid")
		close(pastMid)
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		if err := w.Wait(ctx); err != nil {
			return err
		}
		events.add("commit")
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	want := []string{"mid", "task", "commit"}
	got := events.all()
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

// TestDetachedSlotOutlivesTheItemsPresence is the reason detaching is safe: the fan-out's slot follows the work, not
// the item. So the node is still occupied while the item sits in a later stage, and its limit still bounds how many
// items have work outstanding.
func TestDetachedSlotOutlivesTheItemsPresence(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(1)
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))
	final := c.AddStage(OptName("final"))

	release := make(chan struct{})
	inCommit := make(chan struct{})
	var occWhileInCommit atomic.Int64
	var first atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		<-inCommit
		occWhileInCommit.Store(int64(occupancyOf(c, fo)))
		close(release)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		if err := fo.Schedule(ic, pool.NewTask(func(tctx context.Context) error {
			select {
			case <-release:
			case <-tctx.Done():
			}
			return nil
		})); err != nil {
			return err
		}
		w := fo.Detach(ic)
		if err := commit.MoveTo(ic); err != nil {
			return err
		}
		if first.CompareAndSwap(false, true) {
			close(inCommit)
			<-release
		}
		if err := final.MoveTo(ic); err != nil {
			return err
		}
		return w.Wait(ic)
	})

	if got := occWhileInCommit.Load(); got != 1 {
		t.Fatalf("fan-out occupancy = %d while the item was in commit, want 1 — the work still holds its slot", got)
	}
}

// TestDetachedWorkStillBoundedByTheLimit: detaching moves the wait, not the ceiling. Even though every item leaves the
// fan-out immediately, no more than its limit may have work outstanding at once.
func TestDetachedWorkStillBoundedByTheLimit(t *testing.T) {
	const items = 8
	const limit = 2
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(limit)
	pool := fo.AddPool(OptName("pool")).SetLimit(items) // never the constraint
	commit := c.AddStage(OptName("commit")).SetQueueSize(3)

	var wg workGauge
	runNOK(t, c, items, func(ctx context.Context, no int64) error {
		w := wg.item(1)
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
			defer w.taskDone()
			time.Sleep(20 * time.Millisecond)
			return nil
		})); err != nil {
			return err
		}
		wave := fo.Detach(ctx)
		w.scheduled()
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		return wave.Wait(ctx)
	})

	if peak := wg.peakValue(); peak > limit {
		t.Fatalf("%d items had detached work outstanding at once, fan-out limit is %d", peak, limit)
	}
}

// TestDetachedErrorNobodyJoinedFailsTheItem: a detached wave is the caller's to join, and if nobody ever looks at it
// its failure still reaches the run — delayed, never lost.
func TestDetachedErrorNobodyJoinedFailsTheItem(t *testing.T) {
	boom := errors.New("detached boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return boom })); err != nil {
			return err
		}
		_ = fo.Detach(ctx) // deliberately never joined
		return commit.MoveTo(ctx)
	})
	if !errors.Is(err, boom) {
		t.Fatalf("run error = %v, want the detached work's %v", err, boom)
	}
}

// TestDetachAfterWorkAlreadyFinished: the task may finish before the ItemProcessor gets around to calling Detach —
// nothing about a wave settling depends on Detach ever being called (see run.joinPending, which waits for it just
// the same). Detaching such a fan-out must still succeed, handing back a wave that is already finished.
func TestDetachAfterWorkAlreadyFinished(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	taskRan := make(chan struct{})
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
			close(taskRan)
			return nil
		})); err != nil {
			return err
		}
		// MoveTo does not return until the task's slot on the pool is taken (see run.startWork), so once the task
		// itself has run — confirmed by the channel receive, a real happens-before edge, not a poll that could catch
		// it either side of that instant — the slot going back to 0 can only mean runWork already released it and
		// settled the wave under the same lock hold (see run.runWork). That is what guarantees the work, and the
		// wave, are already done by the time Detach is called below.
		<-taskRan
		waitFor(t, "the task to finish", func() bool { return occupancyOf(c, pool) == 0 })

		w := fo.Detach(ctx) // must not panic, even though there is nothing left to wait for
		select {
		case <-w.Finished():
		default:
			t.Fatalf("expected the handed-over wave to already be finished")
		}
		if err := w.Err(); err != nil {
			t.Fatalf("wave err = %v, want nil", err)
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		return w.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestDetachWithoutWorkPanics: there is nothing to hand over if the item never scheduled anything here.
func TestDetachWithoutWorkPanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	_ = fo.AddPool(OptName("lane"))

	panicsInItem(t, c, errNothingToDetach, func(ctx context.Context) {
		if err := fo.MoveTo(ctx); err != nil {
			t.Fatalf("move failed: %v", err)
		}
		_ = fo.Detach(ctx) // legal: an empty submission still has a wave to hand over
		_ = fo.Detach(ctx) // misuse: it has already been handed over
	})
}

// TestDetachNodeNotOccupiedPanics: the slot to hand over must be one the item is holding.
func TestDetachNodeNotOccupiedPanics(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	fo := c.AddFanOut(OptName("fo"))
	_ = fo.AddPool(OptName("lane"))

	panicsInItem(t, c, errStageNotEntered, func(ctx context.Context) {
		if err := s.MoveTo(ctx); err != nil {
			t.Fatalf("move failed: %v", err)
		}
		_ = fo.Detach(ctx) // the item is in s, not in the fan-out
	})
}

// TestDetachAfterMovingOnPanics: leaving the fan-out joins its work and closes the body, so afterwards there is nothing
// left to detach — the body state answers, whether or not the item still occupies the node.
func TestDetachAfterMovingOnPanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	panicsInItem(t, c, errNothingToDetach, func(ctx context.Context) {
		if err := fo.MoveTo(ctx); err != nil {
			t.Fatalf("move failed: %v", err)
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil })); err != nil {
			t.Fatalf("move failed: %v", err)
		}
		if err := commit.MoveTo(ctx); err != nil {
			t.Fatalf("move failed: %v", err)
		}
		_ = fo.Detach(ctx)
	})
}

// TestDetachOfAnEarlierFanOutPanics: an item that detached at one fan-out still occupies it, so occupancy alone cannot
// say which work is pending. Detaching there again must not reach for the work of the node the item is in now.
func TestDetachOfAnEarlierFanOutPanics(t *testing.T) {
	c := NewConveyor()
	first := c.AddFanOut(OptName("first"))
	firstPool := first.AddPool(OptName("firstPool"))
	second := c.AddFanOut(OptName("second"))
	secondPool := second.AddPool(OptName("secondPool"))

	// The work must still be running when the second Detach is attempted, so that first is still occupied — and it is
	// released from inside the item, since an unlimited shutdown timeout would otherwise wait for it forever.
	release := make(chan struct{})
	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := first.MoveTo(ctx); err != nil {
			return err
		}
		if err := first.Schedule(ctx, firstPool.NewTask(func(tctx context.Context) error {
			select {
			case <-release:
			case <-tctx.Done():
			}
			return nil
		})); err != nil {
			return err
		}
		_ = first.Detach(ctx) // not joined here: joining it would wait for the work that is holding first's slot
		if err := second.MoveTo(ctx); err != nil {
			return err
		}
		if err := second.Schedule(ctx, secondPool.NewTask(func(context.Context) error { return nil })); err != nil {
			return err
		}
		if occ := occupancyOf(c, first); occ != 1 {
			t.Errorf("first occupancy = %d, want 1 — the detached work still holds its slot", occ)
		}
		// first is occupied — by the detached work — but its pending wave is gone.
		assertPanics(t, errNothingToDetach, func() { _ = first.Detach(ctx) })
		checked.Store(true)
		close(release)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the panic check did not run")
	}
}

// TestDetachedSlotFreedWhenTheWorkFinishes is the other half of the contract: once the detached work is done, the
// fan-out's slot goes back even though the item is still sitting in a later stage. Held until the work finishes, not
// until the item does.
func TestDetachedSlotFreedWhenTheWorkFinishes(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(1)
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))
	final := c.AddStage(OptName("final"))

	inCommit := make(chan struct{})
	taskDone := make(chan struct{})
	stay := make(chan struct{})
	var freed atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		<-taskDone
		freed.Store(waitFor(t, "the detached slot to be given back", func() bool {
			return occupancyOf(c, fo) == 0
		}))
		close(stay)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if no > 1 {
			<-stay // the followers must stay out of the fan-out: the slot under test is item 1's
			return nil
		}
		// The work settles only once the item is parked in commit, so nothing but the work's own completion can hand
		// the fan-out's slot back: the item's next move comes later, after the check.
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		if err := fo.Schedule(ic, pool.NewTask(func(context.Context) error {
			<-inCommit
			return nil
		})); err != nil {
			return err
		}
		w := fo.Detach(ic)
		if err := commit.MoveTo(ic); err != nil {
			return err
		}
		close(inCommit)
		<-w.Finished()
		close(taskDone)
		<-stay // hold commit while the slot is checked
		if err := final.MoveTo(ic); err != nil {
			return err
		}
		return w.Wait(ic)
	})

	if !freed.Load() {
		t.Fatalf("the fan-out slot was still held after its detached work had finished")
	}
}

// TestDetachTwiceAfterMovingOnPanics: a body once detached stays detached. After the detached work finished and the
// item moved on, the node is free — the diagnostic is still errNothingToDetach, from the body state, not
// errStageNotEntered from the empty occupancy.
func TestDetachTwiceAfterMovingOnPanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	panicsInItem(t, c, errNothingToDetach, func(ctx context.Context) {
		if err := fo.MoveTo(ctx); err != nil {
			t.Fatalf("move failed: %v", err)
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil })); err != nil {
			t.Fatalf("schedule failed: %v", err)
		}
		w := fo.Detach(ctx)
		<-w.Finished()
		if err := commit.MoveTo(ctx); err != nil {
			t.Fatalf("move failed: %v", err)
		}
		if err := w.Wait(ctx); err != nil {
			t.Fatalf("wait failed: %v", err)
		}
		if occ := occupancyOf(c, fo); occ != 0 {
			t.Errorf("fan-out occupancy = %d after the item moved on, want 0", occ)
		}
		_ = fo.Detach(ctx)
	})
}
