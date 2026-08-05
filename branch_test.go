package conveyor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// This file covers the Pool/Lane split itself: what each kind promises, what they share, and the degenerate case of
// a lane that was never given interior nodes. The behaviour of children travelling a lane lives in children_test.go.

// Both kinds are Branches — the shared task-construction surface. Asserted at compile time, since that is the whole
// point of the interface.
var (
	_ Branch = Pool(nil)
	_ Branch = Lane(nil)
	_ Branch = (*branch)(nil)
	_ Pool   = (*branch)(nil)
	_ Lane   = (*branch)(nil)
)

// TestBranchServesBothKindsUniformly: Branch is what a Pool and a Lane have in common, so code that only builds work
// — a topology assembled from configuration, a helper handed "somewhere to put this task" — needs one variable and
// one code path whichever kind it got.
func TestBranchServesBothKindsUniformly(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(FanOut) (Branch, Stage)
	}{
		{"pool", func(fo FanOut) (Branch, Stage) { return fo.AddPool(OptName("b")).SetLimit(2), nil }},
		{"lane", func(fo FanOut) (Branch, Stage) {
			l := fo.AddLane(OptName("b"))
			return l, l.AddStage(OptName("inner")).SetLimit(2)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewConveyor()
			fo := c.AddFanOut(OptName("fo"))
			b, inner := tc.build(fo)
			commit := c.AddStage(OptName("commit"))

			const tasks = 3
			var ran atomic.Int64
			err := runOnce(t, c, func(ctx context.Context) error {
				// One call site for both kinds: this is what Branch buys.
				err := fo.MoveTo(ctx, Tasks{b.NewTasks(tasks, func(cctx context.Context, _ int) error {
					if inner != nil {
						if err := inner.MoveTo(cctx); err != nil {
							return err
						}
					}
					ran.Add(1)
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
			if got := ran.Load(); got != tasks {
				t.Fatalf("%d of %d tasks ran", got, tasks)
			}
		})
	}
}

// TestLaneEntranceAdmitsOneChildAtATime is the pair of guarantees that define a lane's entrance, and the reason it has
// no SetLimit:
//
//   - it paces child creation exactly as the conveyor's implicit start paces items, so a child that is slow BEFORE its
//     first move keeps the next one from being created at all — even with the whole set already queued;
//   - the parallelism is on the interior stages instead, so once the children move in, all of them are inside at once.
func TestLaneEntranceAdmitsOneChildAtATime(t *testing.T) {
	const children = 4
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	// Room for every child, so the entrance is the only thing that can possibly pace them.
	inner := lane.AddStage(OptName("inner")).SetLimit(children)
	commit := c.AddStage(OptName("commit"))

	inInner := &gauge{}
	release := make(chan struct{})
	var releaseOnce sync.Once
	var created atomic.Int64
	var checked atomic.Bool

	// holdInner keeps the interior stage occupied until every child has reached it, so the peak the gauge records is
	// the number genuinely inside together. It is a positive assertion: if the entrance never released, this blocks and
	// the test's own timeout reports it.
	holdInner := func(ctx context.Context) error {
		inInner.enter()
		defer inInner.leave()
		if inInner.current() == children {
			releaseOnce.Do(func() { close(release) })
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}

	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTasks(children, func(cctx context.Context, i int) error {
			created.Add(1)
			if i == 0 {
				// Still holding the entrance, and the other three are queued behind it. The whole set was handed over
				// in one MoveTo, and the lane is pumped under the run's lock before that call returns — so if the
				// entrance admitted more than one, the siblings would already exist. Nothing to wait for.
				if occ := occupancyOf(c, lane); occ != 1 {
					t.Errorf("lane entrance occupancy = %d while the first child holds it, want 1", occ)
				}
				if n := created.Load(); n != 1 {
					t.Errorf("%d children created while the first one holds the entrance, want 1", n)
				}
				checked.Store(true)
			}
			if err := inner.MoveTo(cctx); err != nil { // frees the entrance for the next child
				return err
			}
			return holdInner(cctx)
		})})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the entrance check did not run")
	}
	// The other half: every child got in, and the interior stage — not the entrance — is where they ran together.
	if peak, entries := inInner.snapshot(); peak != children || entries != children {
		t.Fatalf("interior stage peak=%d entries=%d, want %d/%d — the entrance must release on the first move",
			peak, entries, children, children)
	}
}

// TestLaneWithoutInteriorNodesActsAsAPool: a lane nobody gave stages to is legal — a topology built from
// configuration may end up that way — and it degrades to exactly what it is, a pool pinned at concurrency 1. The
// sharp end is that its work takes the non-travelling path, so it cannot move.
func TestLaneWithoutInteriorNodesActsAsAPool(t *testing.T) {
	const tasks = 4
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane")) // deliberately never given interior nodes
	commit := c.AddStage(OptName("commit"))

	g := &gauge{}
	var moveErr atomic.Value
	var checked atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{lane.NewTasks(tasks, func(cctx context.Context, i int) error {
			return g.hold(func() error {
				if i == 0 {
					// The work runs on the entrance slot, so it has nowhere to go: the pool's panic, not a scope
					// error. Captured rather than asserted here — see recoveredErr.
					if e := recoveredErr(func() { _ = commit.MoveTo(cctx) }); e != nil {
						moveErr.Store(e)
					}
					checked.Store(true)
				}
				spin()
				return nil
			})
		})})
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !checked.Load() {
		t.Fatalf("the cannot-move check did not run")
	}
	if e, _ := moveErr.Load().(error); e == nil || !errors.Is(e, errCannotMove) {
		t.Fatalf("moving with an empty lane's task ctx = %v, want %v (its work must take the pool path)",
			e, errCannotMove)
	}
	if peak, entries := g.snapshot(); entries != tasks || peak != 1 {
		t.Fatalf("empty lane ran peak=%d entries=%d, want peak 1 and all %d tasks", peak, entries, tasks)
	}
}

// TestPoolTasksStartInSubmissionOrder: several Tasks for one branch become a single collection whose sources are
// drained front to back, so the order they were added to the Tasks value is the order their work starts in — across
// constructors, not just within one.
func TestPoolTasksStartInSubmissionOrder(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // limit 1: the order is total, so it is observable
	commit := c.AddStage(OptName("commit"))

	rec := &recorder{}
	err := runOnce(t, c, func(ctx context.Context) error {
		var ts Tasks
		ts.Add(pool.NewTask(func(context.Context) error { rec.add("A"); return nil }))
		ts.Add(pool.NewTasks(2, func(_ context.Context, i int) error { rec.add("B%d", i); return nil }))
		ts.Add(pool.NewTasksGen(func(yield func(TaskFunc) bool) {
			yield(func(context.Context) error { rec.add("C"); return nil })
		}))
		ts.Add(pool.NewTask(func(context.Context) error { rec.add("D"); return nil }))
		if err := fo.MoveTo(ctx, ts); err != nil {
			return err
		}
		return commit.MoveTo(ctx) // joins the wave: all five callbacks have run by here
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	want := []string{"A", "B0", "B1", "C", "D"}
	got := rec.all()
	if len(got) != len(want) {
		t.Fatalf("ran %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("submission order = %v, want %v", got, want)
		}
	}
}
