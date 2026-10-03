package conveyor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// TestRetainKeepsStageOccupiedAfterMovingOn: the retained slot is not given up when the item moves on. While the
// task runs, the stage stays occupied and no other item may enter it.
func TestRetainKeepsStageOccupiedAfterMovingOn(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	release := make(chan struct{}) // lets the task return
	inB := make(chan struct{})     // item 1 has moved on to b
	park := make(chan struct{})    // keeps item 1 inside b
	var created, enteredA atomic.Int64

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		<-inB
		waitFor(t, "a follower item to be waiting at the retained stage", func() bool { return created.Load() >= 2 })
		if got := occupancyOf(c, a); got != 1 {
			t.Errorf("retained stage occupancy = %d while the task ran, want 1", got)
		}
		if got := enteredA.Load(); got != 1 {
			t.Errorf("%d items entered the retained stage while the task ran, want 1", got)
		}
		close(release)
		waitFor(t, "the follower to enter once the task returned", func() bool { return enteredA.Load() >= 2 })
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
		w := a.RetainFor(ic, func(context.Context) error {
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
		return w.Wait(ic)
	})
}

// TestRetainSlotHeldUntilItemMovesOn is the other half of the contract: when the task returns while the item is
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
		w := a.RetainFor(ic, func(context.Context) error { return nil })
		<-w.Finished() // the task is done, but the item has not moved on yet
		waitFor(t, "a follower item to be waiting at the retained stage", func() bool { return created.Load() >= 2 })
		if got := occupancyOf(c, a); got != 1 {
			t.Errorf("stage occupancy = %d after the task returned while the item was still inside, want 1", got)
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
		return w.Wait(ic)
	})

	if peak, _ := g.snapshot(); peak != 1 {
		t.Fatalf("exclusive stage held %d items at once, want 1", peak)
	}
}

// TestRetainJoinWaitsForBgOp: a retain task group is joined like any other — Wait in a later node blocks until the
// background work has finished, so that node's work observes its effect.
func TestRetainJoinWaitsForBgOp(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))

	release := make(chan struct{})
	var bgDone, joined atomic.Bool

	go func() {
		// The task cannot finish before the item is inside commit and blocked in the join.
		waitFor(t, "the item to enter commit and block in the join", func() bool { return occupancyOf(c, commit) == 1 })
		close(release)
	}()

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.RetainFor(ctx, func(context.Context) error {
			<-release
			bgDone.Store(true)
			return nil
		})
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		if err := w.Wait(ctx); err != nil {
			return err
		}
		if !bgDone.Load() {
			t.Errorf("Wait returned before the retained work had finished")
		}
		joined.Store(true)
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !joined.Load() {
		t.Fatalf("the item never waited for the task group")
	}
}

// TestRetainJoinObservesBackgroundEffect: by the time TaskGroup.Wait returns nil, the retained work's effect is visible
// to the code that runs in that stage.
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
		w := write.RetainFor(ctx, func(context.Context) error {
			mu.Lock()
			effects[no] = true
			mu.Unlock()
			return nil
		})
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		if err := w.Wait(ctx); err != nil {
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

// TestRetainJoinReturnsBgOpError: the task's error does not stop the next MoveTo; it surfaces from Wait, and the
// work after the join does not run.
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
		w := write.RetainFor(ctx, func(context.Context) error { return boom })
		<-w.Finished()
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		joinErr = w.Wait(ctx)
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
		t.Fatalf("the stage ran its work although the retained work had failed")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
}

// TestRetainErrorDoesNotCancelItem: a RetainFor task error does not cancel the item. The item's context stays live,
// its next node call works, and Wait reports the error as a TaskError of the retained stage. The processor returns
// it, so the item fails with it.
func TestRetainErrorDoesNotCancelItem(t *testing.T) {
	boom := errors.New("retain boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	mid := c.AddStage(OptName("mid"))

	var ctxErr, moveErr, joinErr error
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.RetainFor(ctx, func(context.Context) error { return boom })
		<-w.Finished()
		ctxErr = context.Cause(ctx)
		moveErr = mid.MoveTo(ctx) // not joined yet: the failure must not reach the item through its context
		joinErr = w.Wait(ctx)
		return joinErr
	})
	if ctxErr != nil {
		t.Fatalf("item context cause = %v after the retained task failed, want nil", ctxErr)
	}
	if moveErr != nil {
		t.Fatalf("next MoveTo error = %v, want nil: a task error does not cancel the item", moveErr)
	}
	var te TaskError
	if !errors.As(joinErr, &te) || te.Unwrap() != boom || te.Unit() != Unit(write) {
		t.Fatalf("Wait = %v, want a TaskError of %s wrapping %v", joinErr, write, boom)
	}
	if got, want := joinErr.Error(), "write task: retain boom"; got != want {
		t.Fatalf("Wait = %q, want %q", got, want)
	}
	var ie ItemError
	if !errors.As(err, &ie) || ie.Unit() != Unit(mid) || !errors.As(ie.Unwrap(), &te) || !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want an ItemError at %s wrapping the TaskError", err, mid)
	}
}

// TestRetainUnjoinedErrorFailsItemWithTaskError: a RetainFor error the processor never joined fails the item when the
// processor returns nil. The item's error is the TaskError, which names the retained stage.
func TestRetainUnjoinedErrorFailsItemWithTaskError(t *testing.T) {
	boom := errors.New("unjoined retain boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	mid := c.AddStage(OptName("mid"))

	_, done := runFirstItemAsync(c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.RetainFor(ctx, func(context.Context) error { return boom })
		<-w.Finished()
		return mid.MoveTo(ctx) // nil: the item moves on and never joins the task group
	})
	err := recvErr(t, "Run", done) // nothing cancels the run: the item failure must end it
	var ie ItemError
	var te TaskError
	if !errors.As(err, &ie) || !errors.As(ie.Unwrap(), &te) || te.Unwrap() != boom || te.Unit() != Unit(write) {
		t.Fatalf("Run error = %v, want an ItemError wrapping a TaskError of %s with %v", err, write, boom)
	}
	if ie.Unit() != Unit(mid) {
		t.Fatalf("ItemError unit = %v, want %s", ie.Unit(), mid)
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
			_ = write.RetainFor(ic, func(context.Context) error { return boom }) // never joined, never read
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want the unobserved %v", err, boom)
	}
}

// TestRetainJoinedErrorDoesNotFailRun: after Wait has reported the RetainFor error, the processor decides. It returns
// nil here, so the same failure is not reported again at item completion.
func TestRetainJoinedErrorDoesNotFailRun(t *testing.T) {
	boom := errors.New("joined retain boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))

	var got error
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.RetainFor(ctx, func(context.Context) error { return boom })
		got = w.Wait(ctx)
		return nil
	})
	if !errors.Is(got, boom) {
		t.Fatalf("Wait = %v, want %v", got, boom)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("a joined retain error the processor dropped should not fail the run, got %v", err)
	}
}

// TestRetainReadingErrorWithoutJoinStillFailsRun: only a join hands the outcome to the processor. Seeing the task group
// finished and its error recorded is not a join, so the error still fails the item.
func TestRetainReadingErrorWithoutJoinStillFailsRun(t *testing.T) {
	boom := errors.New("read retain boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))

	var got error
	_, done := runFirstItemAsync(c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.RetainFor(ctx, func(context.Context) error { return boom })
		<-w.Finished()
		got = groupErr(w) // reads the outcome without joining it
		return nil
	})
	err := recvErr(t, "Run", done) // nothing cancels the run: the item failure must end it
	if !errors.Is(got, boom) {
		t.Fatalf("recorded task group error = %v, want %v", got, boom)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want the unjoined %v", err, boom)
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

	release := make(chan struct{}) // lets the task return
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
		w := shared.RetainFor(ic, func(context.Context) error {
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
		return w.Wait(ic)
	})
}

// TestRetainOnCanceledItemSkipsTask: when the item's context is already canceled (here by a shutdown with no drain
// time), the task is not run and the returned task group is born finished carrying the cancellation cause.
func TestRetainOnCanceledItemSkipsTask(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor(OptDrainTimeout(0)) // cancel in-flight items as soon as shutdown starts
	s := c.AddStage(OptName("s"))

	inS := make(chan struct{})
	var ran atomic.Bool
	var tgErr, joinErr error
	var bornFinished bool
	cancel, done := runFirstItemAsync(c, func(ctx context.Context) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		signal(inS)
		<-ctx.Done() // the shutdown cancels the item
		w := s.RetainFor(ctx, func(context.Context) error {
			ran.Store(true)
			return nil
		})
		select {
		case <-w.Finished():
			bornFinished = true
		default:
		}
		tgErr = groupErr(w)
		joinErr = w.Wait(ctx)
		return joinErr
	})
	<-inS
	cancel(cause)
	err := recvErr(t, "Run to return", done)

	if ran.Load() {
		t.Fatalf("the task ran although the item's context was already canceled")
	}
	if !bornFinished {
		t.Fatalf("a retain on a canceled item must hand back an already-finished task group")
	}
	var se ShutdownError
	if !errors.As(tgErr, &se) || !errors.Is(tgErr, cause) {
		t.Fatalf("task group error = %v, want the item's ShutdownError cause", tgErr)
	}
	if !errors.As(joinErr, &se) || !errors.Is(joinErr, cause) {
		t.Fatalf("Wait = %v, want the item's ShutdownError cause", joinErr)
	}
	var ie ItemError
	if !errors.As(err, &se) || !errors.Is(err, cause) || errors.As(err.(RunError).DrainError(), &ie) {
		t.Fatalf("Run error = %v, want a ShutdownError with %v and no item failure", err, cause)
	}
}

// TestRetainTaskContextCanceledByShutdown: the RetainFor task's context is canceled exactly when the item is: a
// shutdown that cancels the item reaches the running task with the item's ShutdownError cause.
func TestRetainTaskContextCanceledByShutdown(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor(OptDrainTimeout(0)) // cancel in-flight items as soon as shutdown starts
	s := c.AddStage(OptName("s"))

	running := make(chan struct{})
	var taskCause, itemCause error
	cancel, done := runFirstItemAsync(c, func(ctx context.Context) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		w := s.RetainFor(ctx, func(tctx context.Context) error {
			signal(running)
			<-tctx.Done()
			taskCause = context.Cause(tctx)
			return taskCause
		})
		<-w.Finished()
		itemCause = context.Cause(ctx)
		return w.Wait(ctx)
	})
	<-running
	cancel(cause)
	err := recvErr(t, "Run to return", done)

	var se ShutdownError
	if !errors.As(taskCause, &se) || !errors.Is(taskCause, cause) {
		t.Fatalf("task context cause = %v, want a ShutdownError with %v", taskCause, cause)
	}
	if taskCause != itemCause {
		t.Fatalf("task context cause = %v, want the item's own cause %v", taskCause, itemCause)
	}
	var ie ItemError
	if !errors.As(err, &se) || errors.As(err.(RunError).DrainError(), &ie) {
		t.Fatalf("Run error = %v, want a ShutdownError with no item failure: the task's error is an abort", err)
	}
}

// TestRetainTaskContextCannotDriveTheItem: the context handed to a RetainFor task is marked as a background task's.
// Node calls with it, and a wait for the task's own task group, panic with errCannotMove instead of acting for the item.
func TestRetainTaskContextCannotDriveTheItem(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s"))
	next := c.AddStage(OptName("next"))
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	groups := make(chan TaskGroup, 1)
	got := map[string]error{}
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		w := s.RetainFor(ctx, func(tctx context.Context) error {
			own := <-groups
			got["MoveTo"] = recoveredErr(func() { _ = next.MoveTo(tctx) })
			got["TryMoveTo"] = recoveredErr(func() { _, _ = next.TryMoveTo(tctx) })
			got["Retain"] = recoveredErr(func() { s.Retain(tctx)() })
			got["RetainFor"] = recoveredErr(func() { s.RetainFor(tctx, func(context.Context) error { return nil }) })
			got["Schedule"] = recoveredErr(func() {
				_ = fo.Schedule(tctx, pool.NewTask(func(context.Context) error { return nil }))
			})
			got["TaskGroup.Wait"] = recoveredErr(func() { _ = own.Wait(tctx) })
			return nil
		})
		groups <- w
		if err := w.Wait(ctx); err != nil {
			return err
		}
		return next.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	for _, call := range []string{"MoveTo", "TryMoveTo", "Retain", "RetainFor", "Schedule", "TaskGroup.Wait"} {
		if e := got[call]; !errors.Is(e, errCannotMove) {
			t.Errorf("%s with a RetainFor task's context: panic = %v, want %v", call, e, errCannotMove)
		}
	}
}

// TestRetainSeveralStagesJoinedTogether: an item may retain every stage it occupies at once; each stage stays held
// until its own task returns, and each is joined by its own Wait in a later stage.
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
		wa := a.RetainFor(ctx, func(context.Context) error {
			<-releaseA
			aDone.Store(true)
			return nil
		})
		if err := b.MoveTo(ctx); err != nil {
			return err
		}
		wb := b.RetainFor(ctx, func(context.Context) error {
			<-releaseB
			bDone.Store(true)
			return nil
		})
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		if err := wa.Wait(ctx); err != nil {
			return err
		}
		if err := wb.Wait(ctx); err != nil {
			return err
		}
		if !aDone.Load() || !bDone.Load() {
			t.Errorf("the waits returned with a=%v b=%v, want both done", aDone.Load(), bDone.Load())
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

// TestRetainTwiceOnSameStageBothJoined: two retains on one stage are two independent task groups, and waiting for both
// in a later stage waits for both.
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
		fast := a.RetainFor(ctx, func(context.Context) error {
			fastDone.Store(true)
			return nil
		})
		slow := a.RetainFor(ctx, func(context.Context) error {
			<-release
			slowDone.Store(true)
			return nil
		})
		if err := b.MoveTo(ctx); err != nil {
			return err
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		if err := fast.Wait(ctx); err != nil {
			return err
		}
		if err := slow.Wait(ctx); err != nil {
			return err
		}
		if !fastDone.Load() || !slowDone.Load() {
			t.Errorf("the waits returned with fast=%v slow=%v, want both done", fastDone.Load(), slowDone.Load())
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
		_ = a.RetainFor(ic, func(context.Context) error {
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
// on the conveyor: the slot stays held while the task runs, the child moves on without it, and the next child cannot
// enter until the task returns.
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
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, lane.NewTasks(2, func(cctx context.Context, i int) error {
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
				rw := mid.RetainFor(cctx, func(context.Context) error {
					// The other child is running and blocked at mid's door; the retained slot is what holds it there.
					waitFor(t, "the sibling child to be running", func() bool { return started.Load() == 2 })
					if occ := occupancyOf(c, mid); occ != 1 {
						t.Errorf("mid occupancy during the retained task = %d, want 1 (the slot is still held)", occ)
					}
					bgRan.Store(true)
					rec.add("bg-done")
					return nil
				})
				if err := tail.MoveTo(cctx); err != nil {
					return err
				}
				if err := rw.Wait(cctx); err != nil {
					return err
				}
				joinSawBgOp.Store(bgRan.Load())
				return nil
			}))
		}
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}

	if !joinSawBgOp.Load() {
		t.Fatalf("the child's join at tail returned before its retained task had finished")
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

// TestRetainByChildUnobservedErrorFailsRun: a child's retained task is charged to a task group the *child* owns, so its
// error has two levels to climb — the child's completion, then the task group that created the child — before it
// can fail the run. Nothing joins it here, so it travels the unobserved path the whole way.
func TestRetainByChildUnobservedErrorFailsRun(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	mid := lane.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	err := runUntil(t, c, 3, func(ctx context.Context, no int64) error {
		err := fo.MoveTo(ctx)
		if err == nil {
			err = fo.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
				if err := mid.MoveTo(cctx); err != nil {
					return err
				}
				_ = mid.RetainFor(cctx, func(context.Context) error { return boom }) // the task group is never joined and never read
				return nil
			}))
		}
		if err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	if !errors.Is(err, boom) {
		t.Fatalf("run error = %v, want the child's retained task error %v", err, boom)
	}
}
