package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// TestHoldKeepsStageUntilReleased: a stage kept with Retain stays occupied after the item moves on, and the next
// item enters it only once release is called.
func TestHoldKeepsStageUntilReleased(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	var created, enteredA atomic.Int64
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		created.Add(1)
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		enteredA.Add(1)
		if no != 1 {
			return nil
		}
		release := a.Retain(ctx)
		if err := b.MoveTo(ctx); err != nil {
			return err
		}
		waitFor(t, "a follower item to wait at the held stage", func() bool { return created.Load() >= 2 })
		if got := occupancyOf(c, a); got != 1 {
			t.Errorf("held stage occupancy = %d, want 1", got)
		}
		if got := enteredA.Load(); got != 1 {
			t.Errorf("%d items entered the held stage, want 1", got)
		}
		release()
		waitFor(t, "the follower to enter once the hold was released", func() bool { return enteredA.Load() >= 2 })
		release() // a second call does nothing
		return nil
	})
}

// TestHoldReleasedInsideStageIsNoop: a release called while the item is still in the stage frees nothing; the stage
// is freed by the next move as usual.
func TestHoldReleasedInsideStageIsNoop(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	var created, enteredA atomic.Int64
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		created.Add(1)
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		enteredA.Add(1)
		if no != 1 {
			return nil
		}
		a.Retain(ctx)()
		waitFor(t, "a follower item to wait at the stage", func() bool { return created.Load() >= 2 })
		if got := enteredA.Load(); got != 1 {
			t.Errorf("%d items entered the stage after an early release, want 1", got)
		}
		if n := len(itemOf(ctx).stageHolds); n != 0 {
			t.Errorf("item has %d live holds after release, want 0", n)
		}
		if err := b.MoveTo(ctx); err != nil {
			return err
		}
		waitFor(t, "the follower to enter once the item moved on", func() bool { return enteredA.Load() >= 2 })
		return nil
	})
}

// TestHoldReleasedByPoolTask: stage1 and pool1 use the same resource. The item keeps stage1 until its pool1 task is
// done, so no next item enters stage1 while that task runs. pool2 work does not keep stage1.
func TestHoldReleasedByPoolTask(t *testing.T) {
	c := NewConveyor()
	stage1 := c.AddStage(OptName("stage1"))
	fo := c.AddFanOut(OptName("fo"))
	pool1 := fo.AddPool(OptName("pool1")).SetLimit(1)
	pool2 := fo.AddPool(OptName("pool2")).SetLimit(1)

	g := &gauge{} // users of the shared resource: the item in stage1 and its pool1 task
	runNOK(t, c, 6, func(ctx context.Context, no int64) error {
		if err := stage1.MoveTo(ctx); err != nil {
			return err
		}
		g.enter()
		release := stage1.Retain(ctx)
		if err := fo.MoveTo(ctx); err != nil {
			g.leave()
			release()
			return err
		}
		return fo.Schedule(ctx,
			pool1.NewTask(func(ctx context.Context) error {
				defer release()
				defer g.leave()
				return nil
			}),
			pool2.NewTask(func(ctx context.Context) error { return nil }),
		)
	})
	if peak, entries := g.snapshot(); peak != 1 || entries != 6 {
		t.Fatalf("resource peak = %d over %d entries, want 1 over 6", peak, entries)
	}
}

// TestHoldTwoReleasesBothNeeded: each Retain holds the stage until its own release; the stage is freed after the
// last one.
func TestHoldTwoReleasesBothNeeded(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	var created, enteredA atomic.Int64
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		created.Add(1)
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		enteredA.Add(1)
		if no != 1 {
			return nil
		}
		r1 := a.Retain(ctx)
		r2 := a.Retain(ctx)
		if err := b.MoveTo(ctx); err != nil {
			return err
		}
		waitFor(t, "a follower item to wait at the held stage", func() bool { return created.Load() >= 2 })
		r1()
		r1() // does not release r2's hold
		if got := occupancyOf(c, a); got != 1 {
			t.Errorf("stage occupancy = %d with one hold left, want 1", got)
		}
		if got := enteredA.Load(); got != 1 {
			t.Errorf("%d items entered the stage with one hold left, want 1", got)
		}
		r2()
		waitFor(t, "the follower to enter once both holds were released", func() bool { return enteredA.Load() >= 2 })
		return nil
	})
}

// TestHoldMixedWithRetainFor: a stage kept by both Retain and RetainFor is freed only when the release is called and
// the bgOp has returned.
func TestHoldMixedWithRetainFor(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	bgDone := make(chan struct{})
	var created, enteredA atomic.Int64
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		created.Add(1)
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		enteredA.Add(1)
		if no != 1 {
			return nil
		}
		release := a.Retain(ctx)
		w := a.RetainFor(ctx, func(context.Context) error { <-bgDone; return nil })
		if err := b.MoveTo(ctx); err != nil {
			return err
		}
		waitFor(t, "a follower item to wait at the held stage", func() bool { return created.Load() >= 2 })
		release()
		if got := enteredA.Load(); got != 1 {
			t.Errorf("%d items entered the stage while the bgOp ran, want 1", got)
		}
		close(bgDone)
		if err := w.Wait(ctx); err != nil {
			return err
		}
		waitFor(t, "the follower to enter once both were done", func() bool { return enteredA.Load() >= 2 })
		return nil
	})
}

// TestHoldNotReleasedEndsAtCompletion: a hold never released ends when the item completes, so later items are not
// blocked forever.
func TestHoldNotReleasedEndsAtCompletion(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	var releases []func()
	runNOK(t, c, 3, func(ctx context.Context, no int64) error {
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		releases = append(releases, a.Retain(ctx)) // items pass a one by one, so the append is not racy
		return b.MoveTo(ctx)
	})
	if len(releases) != 3 {
		t.Fatalf("%d items passed the held stage, want 3", len(releases))
	}
	for _, release := range releases {
		release() // after the item finished: does nothing
	}
}

// TestHoldAfterMovingOnPanics: Retain and RetainFor may be called only while the item is in the stage, even if a
// running RetainFor still keeps the stage occupied.
func TestHoldAfterMovingOnPanics(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	bgDone := make(chan struct{})
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		w := a.RetainFor(ctx, func(context.Context) error { <-bgDone; return nil })
		if err := b.MoveTo(ctx); err != nil {
			return err
		}
		if got := occupancyOf(c, a); got != 1 {
			t.Errorf("stage occupancy = %d while the bgOp runs, want 1", got)
		}
		assertPanics(t, errStageNotEntered, func() { a.Retain(ctx) })
		assertPanics(t, errStageNotEntered, func() { a.RetainFor(ctx, func(context.Context) error { return nil }) })
		close(bgDone)
		return w.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestHoldForeignContextPanics: with no error to return, Retain panics on a context that carries no item.
func TestHoldForeignContextPanics(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	assertPanics(t, ErrForeignContext, func() { a.Retain(context.Background()) })
}

// TestHoldWithCanceledCallContext: a Retain called with a canceled derived context still keeps the stage, because
// the item itself is not canceled and moves on with its own context.
func TestHoldWithCanceledCallContext(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	var created, enteredA atomic.Int64
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		created.Add(1)
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		enteredA.Add(1)
		if no != 1 {
			return nil
		}
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		release := a.Retain(cctx)
		if err := b.MoveTo(ctx); err != nil {
			return err
		}
		waitFor(t, "a follower item to wait at the held stage", func() bool { return created.Load() >= 2 })
		if got := occupancyOf(c, a); got != 1 {
			t.Errorf("held stage occupancy = %d, want 1", got)
		}
		if got := enteredA.Load(); got != 1 {
			t.Errorf("%d items entered the held stage, want 1", got)
		}
		release()
		waitFor(t, "the follower to enter once the hold was released", func() bool { return enteredA.Load() >= 2 })
		return nil
	})
}

// TestHoldOnCanceledItemEndsAtCompletion: on a canceled item Retain still records the hold, and the hold ends when
// the item completes.
func TestHoldOnCanceledItemEndsAtCompletion(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))

	var held *item
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		it := itemOf(ctx)
		it.cancel(context.Canceled)
		_ = a.Retain(ctx)
		if n := len(it.stageHolds); n != 1 {
			t.Errorf("item has %d live holds after Retain on a canceled item, want 1", n)
		}
		held = it
		return ctx.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if held != nil && len(held.stageHolds) != 0 {
		t.Errorf("item has %d live holds after completion, want 0", len(held.stageHolds))
	}
}

// TestHoldStartingStageDelaysNextItem: the starting stage kept with Retain lets no new item be created until release.
func TestHoldStartingStageDelaysNextItem(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	start := c.StartingStage()

	var created atomic.Int64
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		created.Add(1)
		if no != 1 {
			return s.MoveTo(ctx)
		}
		release := start.Retain(ctx)
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		waitFor(t, "a worker to park for the start stage", func() bool { return idleOf(c) == 1 })
		if got := created.Load(); got != 1 {
			t.Errorf("created %d items while the start stage was held, want 1", got)
		}
		release()
		waitFor(t, "the next item to be created once released", func() bool { return created.Load() >= 2 })
		return nil
	})
}
