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
				if !runFailedWith(err, boom) {
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
	for _, retain := range []bool{false, true} {
		name := "open body"
		if retain {
			name = "retained body"
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
				if retain {
					w = fo.Retain(ctx)
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
			if retain {
				if werr := w.Err(); !errors.Is(werr, boom) {
					t.Errorf("wave error = %v, want the cause %v recorded for the dropped work", werr, boom)
				}
			}
		})
	}
}

// TestCompletionShutdownErrorYieldsToUnobservedWaveError: a processor that returns a ShutdownError does not hide a
// real failure of its work — the unobserved wave error becomes the item's error, here the first failure of the drain.
func TestCompletionShutdownErrorYieldsToUnobservedWaveError(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failTask := make(chan struct{})
	var once atomic.Bool
	err := c.Run(ctx, func(ic context.Context) error {
		if !once.CompareAndSwap(false, true) {
			return nil // later items take no part
		}
		pre := UntilShutdown(ic)
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		err := fo.Schedule(ic, pool.NewTask(func(context.Context) error {
			<-failTask
			return boom // the item is alive: a real failure
		}))
		if err != nil {
			return err
		}
		cancel()
		<-pre.Done()
		close(failTask)
		<-ic.Done() // canceled by the task failure
		if cause := context.Cause(ic); !errors.Is(cause, boom) {
			t.Errorf("item cancellation cause = %v, want %v", cause, boom)
		}
		return context.Cause(pre) // a ShutdownError
	})

	if !runFailedWith(err, boom) {
		t.Fatalf("Run = %v, want the wave's unobserved error %v", err, boom)
	}
	var ie ItemError
	if !errors.As(err.(RunError).DrainError(), &ie) || ie.Unwrap() != boom {
		t.Fatalf("DrainError = %v, want an ItemError with %v", err.(RunError).DrainError(), boom)
	}
}

// TestShutdownWhileBlockedInWaitDropsQueuedSpawns: a shutdown wakes an item blocked in Wait with the shutdown cause.
// The body stays open, the running task finishes, and the queued work is dropped with the cause recorded on the wave.
func TestShutdownWhileBlockedInWaitDropsQueuedSpawns(t *testing.T) {
	cause := errors.New("stop now")
	c := NewConveyor(OptDrainTimeout(0))
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
		w := fo.Retain(ic) // the body is still open after Wait returned
		<-w.Finished()
		assertShutdownCause(t, "the wave of the dropped work", w.Err(), cause)
		return werr
	})

	if !errors.Is(err, cause) {
		t.Fatalf("Run = %v, want the run context's cause %v", err, cause)
	}
	if aDone.Load() != 1 {
		t.Errorf("the running task did not finish")
	}
	if bRan.Load() != 0 {
		t.Errorf("the queued task ran although the item was canceled")
	}
}

// TestCompletionAbandonmentDoesNotHideALaterTaskError: a failed RetainFor cancels the item; a channel source's pull
// ends first and records the abandonment on the wave; then the running task fails for real. The real failure is the
// wave's error, not the cancellation cause it happened to follow. (After a cancellation by the conveyor, a task error
// is an abort instead.)
func TestCompletionAbandonmentDoesNotHideALaterTaskError(t *testing.T) {
	boom := errors.New("boom")
	errRetain := errors.New("retained work failed")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	work := fo.AddPool(OptName("work"))
	feed := fo.AddPool(OptName("feed"))

	never := make(chan TaskFunc) // nobody feeds it: the pull ends only with the item's cancellation
	failRetain := make(chan struct{})
	taskDone := make(chan struct{})
	err := runFirstItem(t, c, func(ic context.Context) error {
		c.StartingStage().RetainFor(ic, func() error {
			<-failRetain
			return errRetain
		})
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		it := itemOf(ic)
		waveErr := func() error {
			it.run.mu.Lock()
			defer it.run.mu.Unlock()
			return it.pending.err
		}
		err := fo.Schedule(ic,
			work.NewTask(func(tctx context.Context) error {
				defer close(taskDone)
				<-tctx.Done()
				waitFor(t, "the abandoned pull to be recorded on the wave", func() bool { return waveErr() != nil })
				return boom
			}),
			feed.NewTasksChan(never),
		)
		if err != nil {
			return err
		}
		close(failRetain)
		<-ic.Done()
		<-taskDone
		// The task callback has returned; wait until runWork has also reported its error to the wave.
		waitFor(t, "the task's error to be recorded on the wave", func() bool {
			it.run.mu.Lock()
			defer it.run.mu.Unlock()
			return it.pending.running == 0
		})
		if cause := context.Cause(ic); cause != errRetain {
			t.Errorf("item cancellation cause = %v, want %v", cause, errRetain)
		}
		return waveErr()
	})

	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the task's own error %v", err, boom)
	}
}
