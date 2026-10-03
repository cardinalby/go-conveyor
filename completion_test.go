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
			c := New()
			fo := c.AddFanOut(WithName("fo"))
			pool := fo.AddPool(WithName("pool")).SetLimit(2)

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
// recorded on the task group as the reason its work did not run.
func TestCompletionWithErrorStopsTheSpawningTree(t *testing.T) {
	boom := errors.New("boom")
	for _, retain := range []bool{false, true} {
		name := "open body"
		if retain {
			name = "retained body"
		}
		t.Run(name, func(t *testing.T) {
			c := New()
			fo := c.AddFanOut(WithName("fo"))
			pool := fo.AddPool(WithName("pool")) // limit 1: task B queues behind the running task A

			running := make(chan struct{})
			var bRan atomic.Int64
			var schedErr error // set by task A before it returns; read once Run is over
			var w TaskGroup
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
				if werr := groupErr(w); !errors.Is(werr, boom) {
					t.Errorf("task group error = %v, want the cause %v recorded for the dropped work", werr, boom)
				}
			}
		})
	}
}

// TestCompletionShutdownErrorYieldsToUnobservedTaskGroupError: a processor that returns a ShutdownError does not hide a
// real failure of its work — the unobserved task group error becomes the item's error, here the first failure of the
// drain.
func TestCompletionShutdownErrorYieldsToUnobservedTaskGroupError(t *testing.T) {
	boom := errors.New("boom")
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool")).SetLimit(2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failTask := make(chan struct{})
	bodyCanceled := make(chan error, 1)
	var once atomic.Bool
	err := c.Run(ctx, func(ic context.Context) error {
		if !once.CompareAndSwap(false, true) {
			return nil // later items take no part
		}
		pre := UntilShutdown(ic)
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		err := fo.Schedule(ic,
			pool.NewTask(func(context.Context) error {
				<-failTask
				return boom // the item is alive: a real failure
			}),
			pool.NewTask(func(tctx context.Context) error {
				<-tctx.Done() // canceled by the sibling's failure
				bodyCanceled <- context.Cause(tctx)
				return nil
			}),
		)
		if err != nil {
			return err
		}
		cancel()
		<-pre.Done()
		close(failTask)
		if cause := <-bodyCanceled; !errors.Is(cause, boom) {
			t.Errorf("body cancellation cause = %v, want %v", cause, boom)
		}
		if cause := context.Cause(ic); cause != nil {
			t.Errorf("item canceled with %v; a task failure must not cancel the item", cause)
		}
		return context.Cause(pre) // a ShutdownError
	})

	if !runFailedWith(err, boom) {
		t.Fatalf("Run = %v, want the task group's unobserved error %v", err, boom)
	}
	var ie ItemError
	if !errors.As(err.(RunError).DrainError(), &ie) {
		t.Fatalf("DrainError = %v, want an ItemError", err.(RunError).DrainError())
	}
	var te TaskError
	if !errors.As(ie.Unwrap(), &te) || te.Unwrap() != boom || te.Unit() != pool {
		t.Fatalf("ItemError.Unwrap() = %v, want a TaskError of %s with %v", ie.Unwrap(), pool, boom)
	}
}

// TestShutdownWhileBlockedInWaitDropsQueuedSpawns: a shutdown wakes an item blocked in Wait with the shutdown cause.
// The body stays open, the running task finishes, and the queued work is dropped with the cause recorded on the task
// group.
func TestShutdownWhileBlockedInWaitDropsQueuedSpawns(t *testing.T) {
	cause := errors.New("stop now")
	c := New(WithDrainTimeout(0))
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool")) // limit 1: B queues behind A

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
		assertShutdownCause(t, "the task group of the dropped work", groupErr(w), cause)
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

// TestCompletionAbandonmentDoesNotHideALaterTaskError: the processor retains its body and returns an error, which
// cancels the item; a task queued behind a busy pool is dropped first and records the abandonment on the task group;
// then the running task fails for real. The real failure is the task group's error, not the cancellation cause it
// happened to follow. (After a cancellation by the conveyor, a task error is an abort instead.)
func TestCompletionAbandonmentDoesNotHideALaterTaskError(t *testing.T) {
	boom := errors.New("boom")
	errProc := errors.New("processor failed")
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	work := fo.AddPool(WithName("work"))
	feed := fo.AddPool(WithName("feed"))

	groups := make(chan TaskGroup, 1)
	workStarted := make(chan struct{})
	feedStarted := make(chan struct{})
	err := runFirstItem(t, c, func(ic context.Context) error {
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		err := fo.Schedule(ic,
			work.NewTask(func(tctx context.Context) error {
				close(workStarted)
				<-tctx.Done()
				w := <-groups
				groups <- w
				waitFor(t, "the dropped task to be recorded on the task group", func() bool { return groupErr(w) != nil })
				return boom
			}),
			// feed has limit 1: the first task holds it until the cancellation, then the second one is dropped.
			feed.NewTask(func(tctx context.Context) error {
				close(feedStarted)
				<-tctx.Done()
				return nil
			}),
			feed.NewTask(func(context.Context) error { return nil }),
		)
		if err != nil {
			return err
		}
		<-workStarted
		<-feedStarted
		groups <- fo.Retain(ic)
		return errProc
	})

	if !errors.Is(err, errProc) {
		t.Fatalf("Run = %v, want the processor's error %v", err, errProc)
	}
	w := <-groups
	var te TaskError
	if got := groupErr(w); !errors.As(got, &te) || te.Unwrap() != boom || te.Unit() != work {
		t.Fatalf("task group error = %v, want a TaskError of %s with the task's own error %v", got, work, boom)
	}
}
