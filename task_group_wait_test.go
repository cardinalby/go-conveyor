package conveyor

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This file pins TaskGroup.Wait: who may call it, what it answers when its tasks fail or the item or call context is
// canceled before, during and after the task group finishes, and that the processor chooses which slot the item holds
// while it waits.

// TestWaitAfterMoveBlockedByBusySlotJoins: the task group's task fails while the item waits for a slot in commit that
// another item holds. A task failure does not cancel the item, so the move is not woken: it enters once the slot is
// free. The Wait that follows reports the task group's TaskError and joins it, so the processor may end the item clean.
// The task fails only once the item is blocked at commit's door, and the holder leaves only once the failure is
// recorded and the item is still blocked.
func TestWaitAfterMoveBlockedByBusySlotJoins(t *testing.T) {
	boom := errors.New("boom")
	c := New()
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))
	commit := c.AddStage(WithName("commit"))

	holderIn := make(chan struct{})
	groups := make(chan TaskGroup, 1)
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		switch no {
		case 1: // holds commit until item 2's task has failed while item 2 is blocked at the door
			if err := commit.MoveTo(ctx); err != nil {
				return err
			}
			close(holderIn)
			w := <-groups
			waitFor(t, "the task failure to be recorded", func() bool { return groupErr(w) != nil })
			if n := parkedOf(c); n != 1 {
				t.Errorf("parked = %d after the task failed, want 1: the failure must not wake the blocked move", n)
			}
			return nil
		default:
			<-holderIn
			if err := fo.MoveTo(ctx); err != nil {
				return err
			}
			if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
				waitFor(t, "item 2 to block at commit's door", func() bool { return parkedOf(c) == 1 })
				return boom
			})); err != nil {
				return err
			}
			w := fo.Retain(ctx)
			groups <- w
			if err := commit.MoveTo(ctx); err != nil {
				t.Errorf("MoveTo = %v, want nil: a task failure does not cancel the item", err)
				return err
			}
			var te TaskError
			if err := w.Wait(ctx); !errors.Is(err, boom) {
				t.Errorf("Wait = %v, want the task group's %v", err, boom)
			} else if !errors.As(err, &te) || te.Unit() != Unit(pool) {
				t.Errorf("Wait = %q, want a TaskError of the pool", err)
			}
			if !joinedOf(ctx, w) {
				t.Errorf("Wait on the finished task group did not join it")
			}
			return nil // the error was reported to us and we handled it
		}
	})
}

// TestWaitOnRetainTaskGroupAfterBlockedMoveJoins: the same for a RetainFor task group, whose TaskError is named after
// the stage.
func TestWaitOnRetainTaskGroupAfterBlockedMoveJoins(t *testing.T) {
	boom := errors.New("boom")
	c := New()
	write := c.AddStage(WithName("write"))
	commit := c.AddStage(WithName("commit"))

	holderIn := make(chan struct{})
	groups := make(chan TaskGroup, 1)
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		switch no {
		case 1:
			if err := commit.MoveTo(ctx); err != nil {
				return err
			}
			close(holderIn)
			w := <-groups
			waitFor(t, "the task failure to be recorded", func() bool { return groupErr(w) != nil })
			if n := parkedOf(c); n != 1 {
				t.Errorf("parked = %d after the task failed, want 1: the failure must not wake the blocked move", n)
			}
			return nil
		default:
			<-holderIn
			if err := write.MoveTo(ctx); err != nil {
				return err
			}
			w := write.RetainFor(ctx, func(context.Context) error {
				waitFor(t, "item 2 to block at commit's door", func() bool { return parkedOf(c) == 1 })
				return boom
			})
			groups <- w
			if err := commit.MoveTo(ctx); err != nil {
				t.Errorf("MoveTo = %v, want nil: a task failure does not cancel the item", err)
				return err
			}
			var te TaskError
			if err := w.Wait(ctx); !errors.Is(err, boom) {
				t.Errorf("Wait = %v, want the task group's %v", err, boom)
			} else if !errors.As(err, &te) || te.Unit() != Unit(write) || !strings.HasPrefix(err.Error(), "write task: ") {
				t.Errorf("Wait = %q, want a TaskError named after the retained stage", err)
			}
			if !joinedOf(ctx, w) {
				t.Errorf("Wait on the finished task group did not join it")
			}
			return nil
		}
	})
}

// TestWaitOnUnfinishedFailedTaskGroupReturnsCause: a task has failed but a sibling is still running, so the task group
// is not finished. The failure does not cancel the item, so only a canceled call context ends the Wait early: it
// returns that cause and joins nothing — the outcome is not final. Then either a Wait after Finished reports the
// TaskError and joins it (the item ends clean), or the task group is never joined and its error fails the item.
func TestWaitOnUnfinishedFailedTaskGroupReturnsCause(t *testing.T) {
	for _, joinLater := range []bool{true, false} {
		name := "never joined"
		if joinLater {
			name = "joined after Finished"
		}
		t.Run(name, func(t *testing.T) {
			boom := errors.New("boom")
			callCause := errors.New("call canceled")
			c := New()
			fo := c.AddFanOut(WithName("fo"))
			pool := fo.AddPool(WithName("pool")).SetLimit(2)
			commit := c.AddStage(WithName("commit"))

			release := make(chan struct{})
			err := runOnce(t, c, func(ctx context.Context) error {
				if err := fo.MoveTo(ctx); err != nil {
					return err
				}
				err := fo.Schedule(ctx,
					pool.NewTask(func(context.Context) error { return boom }),
					// Ignores its canceled body context on purpose, so the task group stays unfinished.
					pool.NewTask(func(context.Context) error { <-release; return nil }),
				)
				if err != nil {
					return err
				}
				w := fo.Retain(ctx)
				waitFor(t, "the task failure to be recorded", func() bool { return groupErr(w) != nil })
				if ctx.Err() != nil {
					t.Errorf("the item was canceled by a task failure: %v", context.Cause(ctx))
				}
				if err := commit.MoveTo(ctx); err != nil {
					t.Errorf("MoveTo = %v, want nil: a task failure does not cancel the item", err)
				}

				cctx, cancel := context.WithCancelCause(ctx)
				defer cancel(nil)
				go func() {
					waitFor(t, "the Wait to block", func() bool { return parkedOf(c) == 1 })
					cancel(callCause)
				}()
				if err := w.Wait(cctx); !errors.Is(err, callCause) {
					t.Errorf("Wait on the unfinished task group = %v, want the call cause %v", err, callCause)
				}
				select {
				case <-w.Finished():
					t.Fatalf("the task group finished although a task is still blocked")
				default:
				}
				if joinedOf(ctx, w) {
					t.Errorf("an unfinished task group was joined")
				}

				close(release)
				<-w.Finished()
				if !joinLater {
					return nil // the error was never reported to us
				}
				if err := w.Wait(ctx); !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), "pool task: ") {
					t.Errorf("Wait after Finished = %v, want the task group's TaskError named after the pool", err)
				}
				if !joinedOf(ctx, w) {
					t.Errorf("the finished task group was not joined")
				}
				return nil
			})
			if joinLater {
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("Run = %v; the task group error was joined, completion must not raise it", err)
				}
				return
			}
			if te := runTaskError(err); te == nil || !errors.Is(te, boom) {
				t.Fatalf("Run = %v, want the unjoined TaskError of %v", err, boom)
			}
		})
	}
}

// TestUnwaitedFailedTaskGroupStillFailsCompletion: joining one task group says nothing about the others. The processor
// waits for the first failed task group and returns nil; the second, never waited for, fails the item at completion.
func TestUnwaitedFailedTaskGroupStillFailsCompletion(t *testing.T) {
	errA := errors.New("a boom")
	errB := errors.New("b boom")
	c := New()
	write := c.AddStage(WithName("write"))

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		// Both tasks fail only once both task groups exist, so both are really running when they fail.
		release := make(chan struct{})
		wa := write.RetainFor(ctx, func(context.Context) error { <-release; return errA })
		wb := write.RetainFor(ctx, func(context.Context) error { <-release; return errB })
		close(release)
		<-wa.Finished()
		<-wb.Finished()
		if err := wa.Wait(ctx); !errors.Is(err, errA) {
			t.Errorf("wa.Wait = %v, want %v", err, errA)
		}
		return nil
	})
	if !runFailedWith(err, errB) {
		t.Fatalf("Run = %v, want the unjoined %v", err, errB)
	}
}

// TestWaitBeforeMoveHoldsPreviousSlot: waiting before the move keeps the item in the node it stands in. The item
// behind cannot enter that node meanwhile, and the target is still free.
func TestWaitBeforeMoveHoldsPreviousSlot(t *testing.T) {
	c := New()
	fo := c.AddFanOut(WithName("fo")).SetLimit(2)
	pool := fo.AddPool(WithName("pool"))
	mid := c.AddStage(WithName("mid"))
	commit := c.AddStage(WithName("commit"))

	release := make(chan struct{})
	waiting := make(chan struct{})
	var secondInMid atomic.Bool
	go func() {
		<-waiting
		// Item 1's retained work holds one fan-out slot; item 2 inside the fan-out is the second. From there its only
		// way on is mid, which item 1 holds.
		waitFor(t, "item 2 to be inside the fan-out", func() bool { return occupancyOf(c, fo) == 2 })
		if occ := occupancyOf(c, mid); occ != 1 {
			t.Errorf("mid occupancy = %d while item 1 waits there, want 1", occ)
		}
		if occ := occupancyOf(c, commit); occ != 0 {
			t.Errorf("commit occupancy = %d while item 1 waits at mid, want 0", occ)
		}
		if secondInMid.Load() {
			t.Errorf("item 2 entered mid while item 1 was waiting there")
		}
		close(release)
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if no != 1 {
			if err := mid.MoveTo(ctx); err != nil {
				return err
			}
			secondInMid.Store(true)
			return commit.MoveTo(ctx)
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { <-release; return nil })); err != nil {
			return err
		}
		w := fo.Retain(ctx)
		if err := mid.MoveTo(ctx); err != nil {
			return err
		}
		close(waiting)
		if err := w.Wait(ctx); err != nil { // holds mid while the retained work runs
			return err
		}
		return commit.MoveTo(ctx)
	})
}

// TestWaitCallerRules: a standalone task group answers anyone with its stored error; an owned task group needs its
// item's context.
func TestWaitCallerRules(t *testing.T) {
	c := New()
	write := c.AddStage(WithName("write"))
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))

	// Standalone: Retain with a context that carries no item hands back a finished task group carrying the reason.
	sw := write.RetainFor(context.Background(), func(context.Context) error { return nil })
	if err := sw.Wait(context.Background()); !errors.Is(err, ErrForeignContext) {
		t.Fatalf("standalone Wait = %v, want ErrForeignContext", err)
	}

	fromTask := make(chan error, 1)
	var saved context.Context
	var w TaskGroup
	err := runOnce(t, c, func(ctx context.Context) error {
		saved = ctx
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w = write.RetainFor(ctx, func(context.Context) error { return nil })
		if err := w.Wait(context.Background()); !errors.Is(err, ErrForeignContext) {
			t.Errorf("Wait with a context without an item = %v, want ErrForeignContext", err)
		}
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(tctx context.Context) error {
			fromTask <- recoveredErr(func() { _ = w.Wait(tctx) })
			return nil
		})); err != nil {
			return err
		}
		return fo.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if perr := <-fromTask; !errors.Is(perr, errCannotMove) {
		t.Fatalf("Wait from a pool task panicked with %v, want errCannotMove", perr)
	}
	// Stale: the item has finished; a context decoupled from its cancellation is refused.
	if err := w.Wait(context.WithoutCancel(saved)); !errors.Is(err, ErrStaleContext) {
		t.Fatalf("Wait after the item finished = %v, want ErrStaleContext", err)
	}
}

// TestWaitByLaneChildOnParentsTaskGroupPanics: a lane child is an item of its own. It may wait on its own task groups
// (see TestRetainByChildHoldsLaneInteriorStage) but not on its parent's — a task group is only meaningful to the item
// that created it. Recovered inside the child's callback.
func TestWaitByLaneChildOnParentsTaskGroupPanics(t *testing.T) {
	c := New()
	write := c.AddStage(WithName("write"))
	fo := c.AddFanOut(WithName("fo"))
	lane := fo.AddLane(WithName("lane"))
	inner := lane.AddStage(WithName("inner"))

	got := make(chan error, 1)
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		pw := write.RetainFor(ctx, func(context.Context) error { return nil })
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
			if err := inner.MoveTo(cctx); err != nil {
				return err
			}
			got <- recoveredErr(func() { _ = pw.Wait(cctx) })
			return nil
		})); err != nil {
			return err
		}
		if err := fo.Wait(ctx); err != nil {
			return err
		}
		return pw.Wait(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if perr := <-got; !errors.Is(perr, errForeignTaskGroup) {
		t.Fatalf("Wait by a lane child on the parent's task group panicked with %v, want errForeignTaskGroup", perr)
	}
}

// TestWaitCleanTaskGroupOnCanceledItemReturnsCause: a canceled item never gets nil, even for a task group that finished
// clean, and the cancellation branch joins nothing — there is no error on the task group to join.
func TestWaitCleanTaskGroupOnCanceledItemReturnsCause(t *testing.T) {
	boom := errors.New("boom")
	c := New()
	write := c.AddStage(WithName("write"))

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.RetainFor(ctx, func(context.Context) error { return nil })
		<-w.Finished()
		itemOf(ctx).cancel(boom)
		if err := w.Wait(ctx); !errors.Is(err, boom) {
			t.Errorf("Wait on a clean task group of a canceled item = %v, want the cause %v", err, boom)
		}
		if joinedOf(ctx, w) {
			t.Errorf("a clean task group was joined by a Wait that returned the cancellation cause")
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, boom) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestWaitHonorsCallContextDeadline: a deadline the caller adds ends the wait with that deadline; the task group is not
// joined, and a later Wait with the item's context still works.
func TestWaitHonorsCallContextDeadline(t *testing.T) {
	c := New()
	write := c.AddStage(WithName("write"))

	release := make(chan struct{})
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.RetainFor(ctx, func(context.Context) error { <-release; return nil })
		dctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		if err := w.Wait(dctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Wait with an expired deadline = %v, want DeadlineExceeded", err)
		}
		if joinedOf(ctx, w) {
			t.Errorf("a running task group was joined by a Wait that hit its deadline")
		}
		close(release)
		if err := w.Wait(ctx); err != nil {
			return err
		}
		if !joinedOf(ctx, w) {
			t.Errorf("a Wait that returned nil on a finished task group did not join it")
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestWaitItemCauseWinsOverCallContextCause: both contexts are canceled with different causes; the item's own cause
// is the answer, as everywhere in the runtime (item.cancelCause). The task group stays unjoined: it is still running.
func TestWaitItemCauseWinsOverCallContextCause(t *testing.T) {
	itemCause := errors.New("item cause")
	callCause := errors.New("call cause")
	c := New()
	write := c.AddStage(WithName("write"))

	release := make(chan struct{})
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.RetainFor(ctx, func(context.Context) error { <-release; return nil })
		cctx, cancel := context.WithCancelCause(ctx)
		cancel(callCause)
		itemOf(ctx).cancel(itemCause)
		if err := w.Wait(cctx); !errors.Is(err, itemCause) {
			t.Errorf("Wait with both contexts canceled = %v, want the item's cause %v", err, itemCause)
		}
		if joinedOf(ctx, w) {
			t.Errorf("a running task group was joined")
		}
		// Only the call context canceled: its cause is the answer.
		w2 := write.RetainFor(ctx, func(context.Context) error { <-release; return nil }) // canceled item: a finished task group carrying the cause
		if err := w2.Wait(cctx); !errors.Is(err, itemCause) {
			t.Errorf("Wait on the task group of a canceled Retain = %v, want the cause %v", err, itemCause)
		}
		close(release)
		<-w.Finished()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, itemCause) {
		t.Fatalf("run failed: %v", err)
	}
}

// TestWaitOnCanceledRetainTaskGroupReturnsRawError: a Retain on an already canceled item runs no callback and hands
// back a finished task group carrying the cause, with no task behind it. Wait returns that error raw — not a
// TaskError — and joins it.
func TestWaitOnCanceledRetainTaskGroupReturnsRawError(t *testing.T) {
	boom := errors.New("boom")
	c := New()
	write := c.AddStage(WithName("write"))

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		itemOf(ctx).cancel(boom)
		w := write.RetainFor(ctx, func(context.Context) error {
			t.Errorf("the callback of a Retain on a canceled item ran")
			return nil
		})
		select {
		case <-w.Finished():
		default:
			t.Fatalf("the task group of a canceled Retain is not finished")
		}
		if err := w.Wait(ctx); err != boom { //nolint:errorlint // identity: the raw stored error, not a wrapping
			t.Errorf("Wait = %v (%q), want the raw %v", err, err, boom)
		}
		if !joinedOf(ctx, w) {
			t.Errorf("the finished task group was not joined")
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, boom) {
		t.Fatalf("run failed: %v; the task group was joined, completion must not raise it", err)
	}
}

// TestWaitConcurrentWaiters: several goroutines of the item wait on one task group at the same time. On a clean task
// group all get nil; on a failed task group all get the task group's TaskError and it is joined; a waiter whose own
// call context is canceled gets that cancellation while the others are unaffected; the recorded outcome read afterwards
// is the same. The processor stays alive until every waiter has returned.
func TestWaitConcurrentWaiters(t *testing.T) {
	const n = 8
	boom := errors.New("boom")
	c := New()
	write := c.AddStage(WithName("write"))
	fo := c.AddFanOut(WithName("fo"))
	pool := fo.AddPool(WithName("pool"))

	waitAll := func(ctx context.Context, w TaskGroup, park <-chan struct{}) []error {
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-park
				errs[i] = w.Wait(ctx)
			}()
		}
		wg.Wait()
		return errs
	}

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		// Clean task group: the waiters park until the callback is about to finish, so they overlap with each other.
		release := make(chan struct{})
		clean := write.RetainFor(ctx, func(context.Context) error { <-release; return nil })
		park := make(chan struct{})
		go func() {
			waitFor(t, "the waiters to be about to block", func() bool { return parkedOf(c) == 0 }) // nothing else waits
			close(park)
			waitFor(t, "every waiter to be parked", func() bool { return parkedOf(c) == n })
			close(release)
		}()
		for i, err := range waitAll(ctx, clean, park) {
			if err != nil {
				t.Errorf("waiter %d on the clean task group = %v, want nil", i, err)
			}
		}

		// One waiter with its own canceled call context: only it is affected. The task group is still running.
		release2 := make(chan struct{})
		running := write.RetainFor(ctx, func(context.Context) error { <-release2; return nil })
		cctx, cancel := context.WithCancelCause(ctx)
		mine := errors.New("mine")
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := running.Wait(cctx); !errors.Is(err, mine) {
				t.Errorf("Wait with a canceled call context = %v, want %v", err, mine)
			}
		}()
		other := make(chan error, 1)
		go func() { other <- running.Wait(ctx) }()
		waitFor(t, "both waiters to be parked", func() bool { return parkedOf(c) == 2 })
		cancel(mine)
		wg.Wait()
		if joinedOf(ctx, running) {
			t.Errorf("a running task group was joined by a canceled call")
		}
		close(release2)
		if err := <-other; err != nil {
			t.Errorf("the other waiter = %v, want nil", err)
		}

		// Failed task group: every waiter gets the same TaskError, named after the pool; the recorded outcome agrees.
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		fail := make(chan struct{})
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { <-fail; return boom })); err != nil {
			return err
		}
		failed := fo.Retain(ctx)
		park2 := make(chan struct{})
		go func() {
			close(park2)
			waitFor(t, "every waiter to be parked on the failed task group", func() bool { return parkedOf(c) == n })
			close(fail)
		}()
		errs := waitAll(ctx, failed, park2)
		recorded := groupErr(failed)
		for i, err := range errs {
			if !errors.Is(err, boom) || !strings.HasPrefix(err.Error(), "pool task: ") {
				t.Errorf("waiter %d on the failed task group = %v, want the task group's TaskError named after the pool", i, err)
			} else if err != recorded { //nolint:errorlint // identity: every waiter gets the recorded value
				t.Errorf("waiter %d on the failed task group = %v, want the recorded %v", i, err, recorded)
			}
		}
		if !errors.Is(recorded, boom) {
			t.Errorf("recorded outcome after the waiters = %v, want %v", recorded, boom)
		}
		if !joinedOf(ctx, failed) {
			t.Errorf("the failed task group was not joined")
		}
		return nil // every waiter reported the error; completion must not raise it again
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want a clean completion", err)
	}
}
