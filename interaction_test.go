package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestCanceledItemsQueuedWorkIsSkipped: once an item is canceled, work it scheduled but that has not started yet is
// dropped rather than invoked with a dead context — for every source kind, not just the streaming ones. The wave
// still resolves, so a joiner is never left hanging.
func TestCanceledItemsQueuedWorkIsSkipped(t *testing.T) {
	c := NewConveyor(optCancelItemsOnShutdown()) // cancel in-flight items as soon as shutdown starts
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")) // limit 1: the tasks run strictly one at a time

	const scheduled = 10
	var ran atomic.Int64
	firstRunning := make(chan struct{})
	var once atomic.Bool
	resolved := make(chan int64, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-firstRunning
		cancel() // cancels the in-flight item, so its 9 queued tasks must never start
	}()

	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if no > 1 {
			return nil
		}
		err := fo.MoveTo(ic, Tasks{pool.NewTasks(scheduled, func(tctx context.Context, i int) error {
			ran.Add(1)
			if once.CompareAndSwap(false, true) {
				close(firstRunning)
				<-tctx.Done() // released by the shutdown cancellation
			}
			return nil
		})})
		if err != nil {
			return err
		}
		w := fo.Detach(ic)
		<-w.Finished() // must resolve even though most of the work was dropped
		resolved <- ran.Load()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}

	select {
	case got := <-resolved:
		if got != 1 {
			t.Fatalf("%d of %d tasks ran after the item was canceled, want only the one already running",
				got, scheduled)
		}
	default:
		t.Fatalf("the wave never resolved")
	}
}

// TestRetainReleasesWhileItemSitsInFanOut pins the Retain contract at its trickiest moment: the item has moved on
// into a fan-out but has not published that rank yet (it is waiting there for the retained work itself), so the only
// thing still holding the previous stage is the bgOp. When the bgOp returns, that stage must be released even though
// the item's published position has not changed — otherwise the item behind it is stuck until the item moves again.
//
// The test proves it structurally: item 1 will not proceed until item 2 has entered `write`, which can only happen if
// the retained slot was freed at the right moment.
func TestRetainReleasesWhileItemSitsInFanOut(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	releaseBg := make(chan struct{})
	item2InWrite := make(chan struct{})
	proceed := make(chan struct{})
	var bgDone, taskRan atomic.Bool
	var closeOnce atomic.Bool

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		// Let the retained work finish only once item 1 is *inside* the fan-out — that is the window where its
		// published position still says "write", so releasing on that alone would not free the slot.
		waitFor(t, "item 1 to be admitted to the fan-out", func() bool { return occupancyOf(c, fo) == 1 })
		close(releaseBg)
		select {
		case <-item2InWrite:
		case <-time.After(10 * time.Second):
			t.Errorf("item 2 never entered write: the retained slot was not released when the bgOp returned, " +
				"even though the item had already moved into the fan-out")
		}
		close(proceed)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if err := write.MoveTo(ic); err != nil {
			return err
		}
		if no == 2 {
			if closeOnce.CompareAndSwap(false, true) {
				close(item2InWrite)
			}
			return nil
		}
		if no > 2 {
			return nil
		}
		// Item 1: hand write's slot to a background op, then move into the fan-out and wait for that op *there*.
		w := write.Retain(ic, func() error {
			<-releaseBg // released once the test has seen this item inside the fan-out
			bgDone.Store(true)
			return nil
		})
		// Joining w here means the item is admitted to the fan-out and then waits, before its own work is enqueued:
		// exactly the window where its published rank still says "write".
		err := fo.MoveTo(ic, Tasks{pool.NewTask(func(context.Context) error {
			taskRan.Store(true)
			return nil
		})}, w)
		if err != nil {
			return err
		}
		// Do not move on until the test has confirmed item 2 got into write: the release must have come from the
		// finished bgOp, not from this next move.
		<-proceed
		return commit.MoveTo(ic)
	})

	if !bgDone.Load() {
		t.Fatalf("the retained background op never completed")
	}
	if !taskRan.Load() {
		t.Fatalf("the fan-out work was never scheduled after the join")
	}
}

// TestEmptyFanOutIsAUsableNode: a fan-out with no branches is a degenerate but legal node — it still occupies a
// position, can be entered (releasing the previous node), and hands back an already-finished wave.
func TestEmptyFanOutIsAUsableNode(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	empty := c.AddFanOut(OptName("empty"))
	commit := c.AddStage(OptName("commit"))

	var seen numbers
	runNOK(t, c, 8, func(ctx context.Context, no int64) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		err := empty.MoveTo(ctx, nil)
		if err != nil {
			return err
		}
		w := empty.Detach(ctx)
		select {
		case <-w.Finished():
		default:
			t.Errorf("an empty fan-out's wave should be born finished")
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		seen.add(no)
		return nil
	})
	assertStrictlyIncreasing(t, seen.all(), "order through an empty fan-out")
	if got := len(empty.Branches()); got != 0 {
		t.Fatalf("Branches() = %d, want 0", got)
	}
}

// TestSeveralRetainsOnOneStageKeepItHeld: two Retains share the stage's one slot, so the first bgOp to return must
// not release it — the stage stays exclusive until the last live retain is done.
func TestSeveralRetainsOnOneStageKeepItHeld(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	b := c.AddStage(OptName("b"))

	release := make(chan struct{})
	var enteredA atomic.Int64
	var occWhileRetained atomic.Int64
	var enteredWhileRetained atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		if err := a.MoveTo(ic); err != nil {
			return err
		}
		enteredA.Add(1)
		if no > 1 {
			return nil // a follower: it may only get in once the last retain has finished
		}
		fast := a.Retain(ic, func() error { return nil })
		slow := a.Retain(ic, func() error {
			<-release
			return nil
		})
		if err := b.MoveTo(ic); err != nil {
			return err
		}
		<-fast.Finished()
		occWhileRetained.Store(int64(occupancyOf(c, a)))
		enteredWhileRetained.Store(enteredA.Load())
		close(release)
		<-slow.Finished()
		cancel()
		return slow.Err()
	})

	if got := occWhileRetained.Load(); got != 1 {
		t.Fatalf("stage a occupancy = %d while a second retain was still running, want 1", got)
	}
	if got := enteredWhileRetained.Load(); got != 1 {
		t.Fatalf("%d items had entered stage a while it was still retained, want 1", got)
	}
}

// TestQueueRaiseAdmitsWaitingItemsImmediately: resizing a waiting room follows the same admission-only contract as
// SetLimit, so a raise must let items in at once rather than waiting for some other event to wake them.
func TestQueueRaiseAdmitsWaitingItemsImmediately(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetQueueSize(1)

	held := make(chan struct{})
	release := make(chan struct{})
	var inside atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	go func() {
		<-held
		// One item is inside s, one fills the queue, the next is blocked at the queue's door.
		waitFor(t, "the queue to fill", func() bool { return occupancyOf(c, s) == 1 && queueOccupancy(c, s) == 1 })
		s.SetQueueSize(3) // the raise is the only event: nothing else changes state
		waitFor(t, "the raise to admit the waiting items", func() bool { return queueOccupancy(c, s) >= 2 })
		close(release)
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		if err := s.MoveTo(ic); err != nil {
			return err
		}
		if inside.CompareAndSwap(false, true) {
			close(held)
			select {
			case <-release:
			case <-ic.Done():
			}
		}
		return nil
	})
}

// TestRetainedStageFillsItsWaitingRoom crosses the two features that both act on the same stage from opposite sides: a
// Retain keeps the stage's slot held after its item has moved on, while the items behind pile into the waiting room in
// front of it. Both must hold at once — the retained slot still bounds who runs the stage's code, and the waiting room
// still lets the followers release the node behind them — and the queue must drain the moment the bgOp returns.
func TestRetainedStageFillsItsWaitingRoom(t *testing.T) {
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetQueueSize(2) // limit 1, plus room for two to wait
	after := c.AddStage(OptName("after")).SetLimit(4)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	release := make(chan struct{})
	sampled := make(chan struct{})
	var entered atomic.Int64
	var retainer atomic.Bool

	go func() {
		defer close(sampled)
		// The first item has moved on to `after`, yet s is still occupied — by its retained slot — and the two items
		// behind it are standing in the waiting room rather than on the node behind them.
		waitFor(t, "the retained stage to be held with a full waiting room", func() bool {
			return occupancyOf(c, s) == 1 && queuedOf(c, s) == 2
		})
		if got := entered.Load(); got != 1 {
			t.Errorf("%d items ran the stage's code, want 1 — a waiting room does not relax the retained slot", got)
		}
		close(release)
		// Freeing the retained slot is what admits the queue: the follower runs the stage's code only now.
		if !waitFor(t, "a queued item to be admitted once the bgOp returned", func() bool {
			return entered.Load() >= 2
		}) {
			t.Errorf("entered = %d after the bgOp returned, want at least 2", entered.Load())
		}
		cancel()
	}()

	_ = c.Run(ctx, func(ic context.Context) error {
		if err := s.MoveTo(ic); err != nil {
			return err
		}
		entered.Add(1)
		if retainer.CompareAndSwap(false, true) {
			w := s.Retain(ic, func() error {
				select {
				case <-release:
				case <-ic.Done():
				}
				return nil
			})
			// Moving on does NOT give up s: the retained slot outlives the move, so the queue stays blocked.
			return after.MoveTo(ic, w)
		}
		return after.MoveTo(ic)
	})
	<-sampled
}
