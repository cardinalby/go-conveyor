package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// This file pins what happens to a fan-out body when the item's processor returns while the body is still busy, and
// how a shutdown reaches an item blocked in Wait.

// TestCompletionSealsBusyBodyAndJoinsTheTree: the processor returns nil while its body still spawns. Completion seals
// the body, the tree runs to the end, and an error nobody observed fails the item.
func TestCompletionSealsBusyBodyAndJoinsTheTree(t *testing.T) {
	boom := errors.New("boom")
	for _, tc := range []struct {
		name string
		fail bool
	}{{"clean tree", false}, {"failing leaf", true}} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewConveyor()
			fo := c.AddFanOut(OptName("fo"))
			pool := fo.AddPool(OptName("pool")).SetLimit(2)

			const depth = 3
			var ran atomic.Int64
			var chain func(level int) TaskFunc
			chain = func(level int) TaskFunc {
				return func(tctx context.Context) error {
					ran.Add(1)
					if level == depth {
						if tc.fail {
							return boom
						}
						return nil
					}
					return fo.Schedule(tctx, pool.NewTask(chain(level+1)))
				}
			}

			var itemCtx context.Context
			returned := make(chan struct{})
			err := runOnce(t, c, func(ctx context.Context) error {
				itemCtx = ctx
				defer close(returned) // the root task waits for this, so the body is busy when the processor returns
				if err := fo.MoveTo(ctx); err != nil {
					return err
				}
				return fo.Schedule(ctx, pool.NewTask(func(tctx context.Context) error {
					<-returned
					return chain(1)(tctx)
				}))
			})

			if tc.fail {
				if !errors.Is(err, boom) {
					t.Fatalf("Run = %v, want the unobserved leaf error %v", err, boom)
				}
			} else if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want the run context's cause", err)
			}
			if got := ran.Load(); got != depth {
				t.Errorf("%d links of the chain ran, want %d", got, depth)
			}
			if st := bodyStateOf(itemCtx, fo); st != bodyClosed {
				t.Errorf("body state after completion = %d, want closed (%d)", st, bodyClosed)
			}
		})
	}
}

// TestCompletionWithErrorStopsTheSpawningTree: the processor returns an error while its tree still spawns. The run
// ends with that error, Schedule from the running task returns it as the cause, and the queued spawn is dropped —
// recorded on the wave as the reason its work did not run.
func TestCompletionWithErrorStopsTheSpawningTree(t *testing.T) {
	boom := errors.New("boom")
	for _, detach := range []bool{false, true} {
		name := "open body"
		if detach {
			name = "detached body"
		}
		t.Run(name, func(t *testing.T) {
			c := NewConveyor()
			fo := c.AddFanOut(OptName("fo"))
			pool := fo.AddPool(OptName("pool")) // limit 1: task B queues behind the running task A

			running := make(chan struct{})
			var bRan atomic.Int64
			var schedErr error // set by task A before it returns; read once Run is over
			var w Wave
			err := runOnce(t, c, func(ctx context.Context) error {
				if err := fo.MoveTo(ctx); err != nil {
					return err
				}
				err := fo.Schedule(ctx,
					pool.NewTask(func(tctx context.Context) error {
						close(running)
						<-tctx.Done() // the processor's error cancels the item
						schedErr = fo.Schedule(tctx, pool.NewTask(func(context.Context) error { return nil }))
						return nil
					}),
					pool.NewTask(func(context.Context) error {
						bRan.Add(1)
						return nil
					}),
				)
				if err != nil {
					return err
				}
				<-running
				if detach {
					w = fo.Detach(ctx)
				}
				return boom
			})

			if !errors.Is(err, boom) {
				t.Fatalf("Run = %v, want %v", err, boom)
			}
			if !errors.Is(schedErr, boom) {
				t.Errorf("Schedule from the running task = %v, want the cause %v", schedErr, boom)
			}
			if bRan.Load() != 0 {
				t.Errorf("the queued task ran although its item was canceled")
			}
			if detach {
				if werr := w.Err(); !errors.Is(werr, boom) {
					t.Errorf("wave error = %v, want the cause %v recorded for the dropped work", werr, boom)
				}
			}
		})
	}
}

// TestCompletionShutdownErrorYieldsToUnobservedWaveError: a processor that returns the ShutdownError it was handed
// does not hide a real failure of its work — the unobserved wave error becomes the item's error, and so Run's.
func TestCompletionShutdownErrorYieldsToUnobservedWaveError(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor(optCancelItemsOnShutdown())
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	taskDone := make(chan struct{})
	var once atomic.Bool
	err := c.Run(ctx, func(ic context.Context) error {
		if !once.CompareAndSwap(false, true) {
			return nil // later items take no part
		}
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		err := fo.Schedule(ic, pool.NewTask(func(tctx context.Context) error {
			defer close(taskDone)
			<-tctx.Done() // canceled by the shutdown, not by a failure
			return boom
		}))
		if err != nil {
			return err
		}
		cancel() // the shutdown context is already done: the item is canceled at once
		<-ic.Done()
		<-taskDone
		cause := context.Cause(ic)
		if !isShutdown(cause) {
			t.Errorf("item cancellation cause = %v, want a ShutdownError", cause)
		}
		return cause
	})

	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the wave's unobserved error %v", err, boom)
	}
	if isShutdown(err) {
		t.Fatalf("Run returned a ShutdownError (%v); the wave error should have replaced it", err)
	}
}

// TestShutdownWhileBlockedInWaitDropsQueuedSpawns: a shutdown wakes an item blocked in Wait with the shutdown cause.
// The body stays open, the running task finishes, and the queued work is dropped with the cause recorded on the wave.
func TestShutdownWhileBlockedInWaitDropsQueuedSpawns(t *testing.T) {
	cause := errors.New("stop now")
	c := NewConveyor(optCancelItemsOnShutdown())
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // limit 1: B queues behind A

	var aDone, bRan atomic.Int64
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	var once atomic.Bool
	err := c.Run(ctx, func(ic context.Context) error {
		if !once.CompareAndSwap(false, true) {
			return nil
		}
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		err := fo.Schedule(ic,
			pool.NewTask(func(tctx context.Context) error {
				<-tctx.Done()
				aDone.Add(1)
				return nil
			}),
			pool.NewTask(func(context.Context) error {
				bRan.Add(1)
				return nil
			}),
		)
		if err != nil {
			return err
		}
		go func() {
			waitFor(t, "task A to run with B queued behind it", func() bool {
				return occupancyOf(c, pool) == 1 && queueOccupancy(c, pool) == 1
			})
			cancel(cause)
		}()
		werr := fo.Wait(ic)
		assertShutdownCause(t, "Wait during shutdown", werr, cause)
		w := fo.Detach(ic) // the body is still open after Wait returned
		<-w.Finished()
		assertShutdownCause(t, "the wave of the dropped work", w.Err(), cause)
		return werr
	})

	if err != cause {
		t.Fatalf("Run = %v, want the run context's cause %v", err, cause)
	}
	if aDone.Load() != 1 {
		t.Errorf("the running task did not finish")
	}
	if bRan.Load() != 0 {
		t.Errorf("the queued task ran although the item was canceled")
	}
}
