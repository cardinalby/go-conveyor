package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// idleOf reports how many workers are parked in acquireItem, waiting for the start stage to free up.
func idleOf(c Conveyor) int {
	r := implOf(c).currentRun.Load()
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.idle
}

// TestStartingStageRetainDelaysNextItem: item 1 retains the starting stage and moves on to s. While the bgOp runs,
// a worker is parked for the start stage and still only one item exists; once it returns, item 2 is created.
func TestStartingStageRetainDelaysNextItem(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	start := c.StartingStage()

	release := make(chan struct{})
	inS := make(chan struct{})
	var created atomic.Int64

	go func() {
		<-inS
		waitFor(t, "a worker to park for the start stage", func() bool { return idleOf(c) == 1 })
		if got := created.Load(); got != 1 {
			t.Errorf("created %d items while the start stage was retained, want 1", got)
		}
		if got := occupancyOf(c, start); got != 1 {
			t.Errorf("start occupancy = %d while retained, want 1", got)
		}
		close(release)
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		created.Add(1)
		if no != 1 {
			return s.MoveTo(ctx)
		}
		w := start.Retain(ctx, func() error {
			<-release
			return nil
		})
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		close(inS)
		return w.Wait(ctx)
	})
}

// TestStartingStageRetainAfterFirstMovePanics: the item leaves the starting stage on its first MoveTo, so Retain
// after it is misuse.
func TestStartingStageRetainAfterFirstMovePanics(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	panicsInItem(t, c, errStageNotEntered, func(ctx context.Context) {
		if err := s.MoveTo(ctx); err != nil {
			panic(err)
		}
		c.StartingStage().Retain(ctx, func() error { return nil })
	})
}

// TestStartingStageRetainUnobservedErrorFailsRun: a bgOp error nobody joined still fails the run.
func TestStartingStageRetainUnobservedErrorFailsRun(t *testing.T) {
	boom := errors.New("start retain boom")
	c := NewConveyor()
	s := c.AddStage(OptName("s"))

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		if no, _ := ItemNoFromContext(ic); no == 1 {
			_ = c.StartingStage().Retain(ic, func() error { return boom })
		}
		return s.MoveTo(ic)
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
}

// TestStartingStageRetainByChildPanics: the starting stage belongs to root items; a lane child must use its lane.
func TestStartingStageRetainByChildPanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid"))

	var got error
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
			got = recoveredErr(func() { c.StartingStage().Retain(cctx, func() error { return nil }) })
			return mid.MoveTo(cctx)
		})); err != nil {
			return err
		}
		return fo.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !errors.Is(got, errWrongScope) {
		t.Fatalf("panic = %v, want %v", got, errWrongScope)
	}
}

// TestLaneRetainDelaysNextChild: child 0 retains the lane's entrance and moves on to mid. The lane creates child 1
// only after the bgOp returns, although mid has room for both.
func TestLaneRetainDelaysNextChild(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid")).SetLimit(2)

	inMid := make(chan struct{})
	var started atomic.Int64
	rec := &recorder{}

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, lane.NewTasks(2, func(cctx context.Context, i int) error {
			started.Add(1)
			rec.add("start-%d", i)
			if i != 0 {
				return mid.MoveTo(cctx)
			}
			w := lane.Retain(cctx, func() error {
				<-inMid
				if got := started.Load(); got != 1 {
					t.Errorf("%d children started while the lane entrance was retained, want 1", got)
				}
				if got := occupancyOf(c, lane); got != 1 {
					t.Errorf("lane occupancy = %d while retained, want 1", got)
				}
				rec.add("bg-done")
				return nil
			})
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			close(inMid)
			return w.Wait(cctx)
		})); err != nil {
			return err
		}
		return fo.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	events := rec.all()
	want := []string{"start-0", "bg-done", "start-1"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i, e := range events {
		if e != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
}

// TestLaneRetainByRootItemPanics: the lane's entrance belongs to its children, not to the ItemProcessor.
func TestLaneRetainByRootItemPanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	lane.AddStage(OptName("mid"))
	panicsInItem(t, c, errWrongScope, func(ctx context.Context) {
		lane.Retain(ctx, func() error { return nil })
	})
}

// TestLaneRetainAfterFirstMovePanics: a child leaves the lane's entrance on its first MoveTo, so Retain after it
// is misuse.
func TestLaneRetainAfterFirstMovePanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid"))

	var got error
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
			if err := mid.MoveTo(cctx); err != nil {
				return err
			}
			got = recoveredErr(func() { lane.Retain(cctx, func() error { return nil }) })
			return nil
		})); err != nil {
			return err
		}
		return fo.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !errors.Is(got, errStageNotEntered) {
		t.Fatalf("panic = %v, want %v", got, errStageNotEntered)
	}
}

// TestLaneRetainWithoutNodesPanics: a lane with no interior nodes runs its tasks like a pool, with no child item,
// so there is nothing to move on and Retain is refused.
func TestLaneRetainWithoutNodesPanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))

	var got error
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
			got = recoveredErr(func() { lane.Retain(cctx, func() error { return nil }) })
			return nil
		})); err != nil {
			return err
		}
		return fo.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !errors.Is(got, errCannotMove) {
		t.Fatalf("panic = %v, want %v", got, errCannotMove)
	}
}

// TestLaneRetainUnobservedErrorFailsRun: a child's bgOp error nobody joined climbs to the run, the same way as a
// child's Retain of an interior stage.
func TestLaneRetainUnobservedErrorFailsRun(t *testing.T) {
	boom := errors.New("lane retain boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	err := runUntil(t, c, 3, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
				_ = lane.Retain(cctx, func() error { return boom }) // never joined, never read
				return mid.MoveTo(cctx)
			}))
		}
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
}
