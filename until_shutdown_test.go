package conveyor

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestUntilShutdownBeforeShutdown: before shutdown the context is not done, keeps the item's values and drives node
// methods. Repeated calls for the item's own context return the same context.
func TestUntilShutdownBeforeShutdown(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))
	err := runOnce(t, c, func(ic context.Context) error {
		pre := UntilShutdown(ic)
		if pre.Err() != nil {
			return fmt.Errorf("pre done before shutdown: %v", context.Cause(pre))
		}
		if no, ok := ItemNoFromContext(pre); !ok || no != 1 {
			return fmt.Errorf("ItemNoFromContext(pre) = %d, %v", no, ok)
		}
		if again := UntilShutdown(ic); again != pre {
			return errors.New("second call for the item context returned a new context")
		}
		if again := UntilShutdown(pre); again != pre {
			return errors.New("UntilShutdown(pre) returned a new context")
		}
		if err := write.MoveTo(pre); err != nil {
			return fmt.Errorf("MoveTo(write, pre): %w", err)
		}
		if ok, err := commit.TryMoveTo(pre); !ok || err != nil {
			return fmt.Errorf("TryMoveTo(commit, pre) = %v, %v", ok, err)
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
}

// TestUntilShutdownForeignContext: a context without an item is returned unchanged.
func TestUntilShutdownForeignContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxKey(100), 1)
	if got := UntilShutdown(ctx); got != ctx {
		t.Fatal("foreign context was not returned unchanged")
	}
}

// TestUntilShutdownOnRunCancel: the context is done once the Run context is canceled, with a ShutdownError that
// unwraps to the Run context's cause, while the item's own context stays alive.
func TestUntilShutdownOnRunCancel(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	ready := make(chan struct{})
	got := make(chan error, 2)
	cancel, done := runAsync(c, func(ic context.Context) error {
		if itemNo(ic) != 1 {
			<-ic.Done()
			return context.Cause(ic)
		}
		pre := UntilShutdown(ic)
		signal(ready)
		<-pre.Done()
		got <- context.Cause(pre)
		got <- context.Cause(ic)
		return nil
	})
	<-ready
	cancel(cause)
	assertShutdownCause(t, "pre", recvErr(t, "pre cause", got), cause)
	if err := recvErr(t, "item cause", got); err != nil {
		t.Fatalf("item context canceled: %v", err)
	}
	err := recvErr(t, "Run", done)
	assertShutdownCause(t, "Run", err, cause)
	if re := err.(RunError); len(re.ItemErrors()) != 0 {
		t.Fatalf("ItemErrors = %v, want none", re.ItemErrors())
	}
}

// TestUntilShutdownOnItemError: an item failure begins the shutdown too: an older item's context is done with a
// ShutdownError that unwraps to the failure.
func TestUntilShutdownOnItemError(t *testing.T) {
	boom := errors.New("boom")
	c := NewConveyor()
	s := c.AddStage(OptName("s")).SetLimit(2)
	inS := make(chan struct{})
	got := make(chan error, 1)
	_, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			pre := UntilShutdown(ic)
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			signal(inS)
			<-pre.Done()
			got <- context.Cause(pre)
			return nil
		case 2:
			<-inS
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			return boom
		}
		<-ic.Done()
		return context.Cause(ic)
	})
	cause := recvErr(t, "pre cause", got)
	assertShutdownCause(t, "pre", cause, boom)
	err := recvErr(t, "Run", done)
	var ie ItemError
	if !errors.As(err, &ie) || !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want an ItemError with %v", err, boom)
	}
}

// TestUntilShutdownBornDone: called once shutdown has begun, the context is already done, also for a context cached
// from an earlier call and for a derived context. Node methods fail with it even when its cancellation is stripped.
func TestUntilShutdownBornDone(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	ready := make(chan struct{})
	res := make(chan error, 1)
	cancel, done := runAsync(c, func(ic context.Context) error {
		if itemNo(ic) != 1 {
			<-ic.Done()
			return context.Cause(ic)
		}
		cached := UntilShutdown(ic)
		signal(ready)
		if !awaitTrue(func() bool { return shutdownBegun(c) }) {
			res <- errors.New("shutdown never began")
			return nil
		}
		derived := context.WithValue(ic, ctxKey(100), 1)
		for name, ctx := range map[string]context.Context{
			"item ctx":    ic,
			"derived ctx": derived,
			"cached pre":  cached,
		} {
			pre := UntilShutdown(ctx)
			if !isShutdown(context.Cause(pre)) {
				res <- fmt.Errorf("%s: pre cause = %v right after the call, want a ShutdownError",
					name, context.Cause(pre))
				return nil
			}
		}
		// A context with the marker but without cancellation: only the check under the run's lock stops it.
		stripped := context.WithoutCancel(UntilShutdown(derived))
		if ok, err := write.TryMoveTo(stripped); ok || !isShutdown(err) {
			res <- fmt.Errorf("TryMoveTo(stripped pre) = %v, %v, want a ShutdownError", ok, err)
			return nil
		}
		if err := write.MoveTo(stripped); !isShutdown(err) {
			res <- fmt.Errorf("MoveTo(stripped pre) = %v, want a ShutdownError", err)
			return nil
		}
		if err := write.MoveTo(ic); err != nil {
			res <- fmt.Errorf("MoveTo(ctx) after the refused moves = %v", err)
			return nil
		}
		res <- nil
		return nil
	})
	<-ready
	cancel(cause)
	if err := recvErr(t, "item", res); err != nil {
		t.Fatal(err)
	}
	assertShutdownCause(t, "Run", recvErr(t, "Run", done), cause)
}

// TestUntilShutdownItemCanceled: the context follows the item's own context: done with the cause of a failed task,
// and done once the item is over.
func TestUntilShutdownItemCanceled(t *testing.T) {
	t.Run("failed task", func(t *testing.T) {
		errTask := errors.New("task failed")
		c := NewConveyor()
		fo := c.AddFanOut(OptName("fo"))
		pool := fo.AddPool(OptName("pool"))
		var cause error
		err := runOnce(t, c, func(ic context.Context) error {
			pre := UntilShutdown(ic)
			if err := fo.MoveTo(ic); err != nil {
				return err
			}
			if err := fo.Schedule(ic, pool.NewTask(func(context.Context) error { return errTask })); err != nil {
				return err
			}
			<-pre.Done()
			cause = context.Cause(pre)
			return context.Cause(ic) // nil would let runOnce cancel the run before the task failure is recorded
		})
		if !errors.Is(cause, errTask) {
			t.Fatalf("pre cause = %v, want %v", cause, errTask)
		}
		if !errors.Is(err, errTask) {
			t.Fatalf("Run = %v, want %v", err, errTask)
		}
	})
	t.Run("item over", func(t *testing.T) {
		c := NewConveyor()
		var pre, derived context.Context
		err := runOnce(t, c, func(ic context.Context) error {
			pre = UntilShutdown(ic)
			derived = UntilShutdown(context.WithValue(ic, ctxKey(100), 1))
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v", err)
		}
		if pre.Err() == nil || derived.Err() == nil {
			t.Fatal("UntilShutdown context not done after the item finished")
		}
	})
}

// TestUntilShutdownMoveToBlocked: an item waiting in a node's waiting room and an item blocked in MoveTo with an
// UntilShutdown context both get a ShutdownError when shutdown begins. With the item's own context they enter as
// usual. The item already inside finishes either way.
func TestUntilShutdownMoveToBlocked(t *testing.T) {
	for _, usePre := range []bool{true, false} {
		t.Run(fmt.Sprintf("pre=%v", usePre), func(t *testing.T) {
			cause := errors.New("stop")
			c := NewConveyor()
			write := c.AddStage(OptName("write")).SetQueueSize(1)
			commit := c.AddStage(OptName("commit"))

			inWrite, release := make(chan struct{}), make(chan struct{})
			errs := make(chan error, 2)
			var committed atomic.Int64
			var firstErr atomic.Value
			cancel, done := runAsync(c, func(ic context.Context) error {
				no := itemNo(ic)
				if no > 3 {
					<-ic.Done()
					return context.Cause(ic)
				}
				moveCtx := ic
				if usePre && no > 1 {
					moveCtx = UntilShutdown(ic)
				}
				err := write.MoveTo(moveCtx)
				if no == 1 {
					if err != nil {
						firstErr.Store(err)
						return err
					}
					signal(inWrite)
					<-release
				} else {
					errs <- err
					if err != nil {
						return err
					}
				}
				if err := commit.MoveTo(ic); err != nil {
					if no == 1 {
						firstErr.Store(err)
					}
					return err
				}
				committed.Add(1)
				return nil
			})
			<-inWrite
			waitFor(t, "item 2 in the waiting room and item 3 blocked", func() bool {
				return queueOccupancy(c, write) == 1 && parkedOf(c) == 2
			})
			cancel(cause)
			if usePre {
				for i := 0; i < 2; i++ {
					assertShutdownCause(t, "blocked item", recvErr(t, "blocked item", errs), cause)
				}
			} else {
				waitFor(t, "shutdown to begin", func() bool { return shutdownBegun(c) })
			}
			close(release)
			if !usePre {
				for i := 0; i < 2; i++ {
					if err := recvErr(t, "blocked item", errs); err != nil {
						t.Fatalf("MoveTo(ctx) = %v, want nil", err)
					}
				}
			}
			err := recvErr(t, "Run", done)
			assertShutdownCause(t, "Run", err, cause)
			if re := err.(RunError); len(re.ItemErrors()) != 0 {
				t.Fatalf("ItemErrors = %v, want none", re.ItemErrors())
			}
			if e := firstErr.Load(); e != nil {
				t.Fatalf("item in write failed: %v", e)
			}
			want := int64(1)
			if !usePre {
				want = 3
			}
			if n := committed.Load(); n != want {
				t.Fatalf("committed %d items, want %d", n, want)
			}
		})
	}
}

// TestUntilShutdownPartialBatch: an item reads a batch with its UntilShutdown context, then goes on with the partial
// batch using its own context and returns nil: no error, and a younger item is not canceled.
func TestUntilShutdownPartialBatch(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))

	input := make(chan int)
	reading, item1Done := make(chan struct{}), make(chan struct{})
	var batchLen atomic.Int64
	var committed atomic.Bool
	youngerCause := make(chan error, 1)
	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			defer close(item1Done)
			if err := read.MoveTo(ic); err != nil {
				return err
			}
			pre := UntilShutdown(ic)
			var batch []int
		loop:
			for len(batch) < 10 {
				if len(batch) == 2 {
					signal(reading)
				}
				select {
				case v := <-input:
					batch = append(batch, v)
				case <-pre.Done():
					break loop
				}
			}
			batchLen.Store(int64(len(batch)))
			if err := write.MoveTo(ic); err != nil {
				return err
			}
			if err := commit.MoveTo(ic); err != nil {
				return err
			}
			committed.Store(true)
			return nil
		case 2:
			select {
			case <-ic.Done():
			case <-item1Done:
				// Item 1 has returned; wait until its completion is over too, which is where a cascade would cancel us.
				r := itemOf(ic).run
				awaitTrue(func() bool {
					r.mu.Lock()
					defer r.mu.Unlock()
					return r.scopes[0].head == itemOf(ic)
				})
			}
			youngerCause <- context.Cause(ic)
			return nil
		}
		<-ic.Done()
		return context.Cause(ic)
	})
	input <- 1
	input <- 2
	<-reading
	waitFor(t, "item 2 in the starting stage", func() bool { return occupancyOf(c, c.StartingStage()) == 1 })
	cancel(cause)
	if err := recvErr(t, "item 2", youngerCause); err != nil {
		t.Fatalf("younger item canceled: %v", err)
	}
	err := recvErr(t, "Run", done)
	assertShutdownCause(t, "Run", err, cause)
	if re := err.(RunError); len(re.ItemErrors()) != 0 {
		t.Fatalf("ItemErrors = %v, want none", re.ItemErrors())
	}
	if !committed.Load() || batchLen.Load() != 2 {
		t.Fatalf("committed = %v with %d values, want a partial batch of 2", committed.Load(), batchLen.Load())
	}
}

// TestUntilShutdownWrappedCanceledIsAbort: an error wrapping context.Canceled returned once shutdown has begun is an
// abort, not a failure, for either trigger. Before any shutdown it is a failure.
func TestUntilShutdownWrappedCanceledIsAbort(t *testing.T) {
	fetch := func(ctx context.Context) error {
		<-ctx.Done()
		return fmt.Errorf("fetch: %w", context.Canceled)
	}
	t.Run("run canceled", func(t *testing.T) {
		cause := errors.New("stop")
		c := NewConveyor()
		ready := make(chan struct{})
		cancel, done := runAsync(c, func(ic context.Context) error {
			if itemNo(ic) != 1 {
				<-ic.Done()
				return context.Cause(ic)
			}
			signal(ready)
			return fetch(UntilShutdown(ic))
		})
		<-ready
		cancel(cause)
		err := recvErr(t, "Run", done)
		assertShutdownCause(t, "Run", err, cause)
		if re := err.(RunError); len(re.ItemErrors()) != 0 {
			t.Fatalf("ItemErrors = %v, want none", re.ItemErrors())
		}
	})
	t.Run("item failed", func(t *testing.T) {
		boom := errors.New("boom")
		c := NewConveyor()
		s := c.AddStage(OptName("s")).SetLimit(2)
		inS := make(chan struct{})
		_, done := runAsync(c, func(ic context.Context) error {
			switch itemNo(ic) {
			case 1:
				if err := s.MoveTo(ic); err != nil {
					return err
				}
				signal(inS)
				return fetch(UntilShutdown(ic))
			case 2:
				<-inS
				if err := s.MoveTo(ic); err != nil {
					return err
				}
				return boom
			}
			<-ic.Done()
			return context.Cause(ic)
		})
		err := recvErr(t, "Run", done)
		var ie ItemError
		if !errors.As(err, &ie) || !errors.Is(err, boom) {
			t.Fatalf("Run = %v, want an ItemError with %v", err, boom)
		}
		if len(ie.ItemErrors()) != 0 {
			t.Fatalf("ItemErrors = %v, want none", ie.ItemErrors())
		}
	})
	t.Run("no shutdown", func(t *testing.T) {
		c := NewConveyor()
		err := runOnce(t, c, func(ic context.Context) error {
			return fmt.Errorf("own: %w", context.Canceled)
		})
		var ie ItemError
		if !errors.As(err, &ie) {
			t.Fatalf("Run = %v, want an ItemError", err)
		}
	})
}

// TestUntilShutdownAbortCascade: an older item that stops through its UntilShutdown context cancels a younger item
// that uses its own context, so the younger one never enters a later node. The run reports no item errors.
func TestUntilShutdownAbortCascade(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	read := c.AddStage(OptName("read")).SetLimit(2)
	write := c.AddStage(OptName("write"))

	inRead := make(chan struct{})
	youngerErr := make(chan error, 1)
	var youngerWrote atomic.Bool
	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			pre := UntilShutdown(ic)
			if err := read.MoveTo(ic); err != nil {
				return err
			}
			signal(inRead)
			<-pre.Done()
			// The younger item is blocked behind this one in MoveTo(write).
			if !awaitTrue(func() bool { return parkedOf(c) == 1 }) {
				return errors.New("item 2 never blocked")
			}
			return fmt.Errorf("fetch: %w", context.Canceled)
		case 2:
			<-inRead
			if err := read.MoveTo(ic); err != nil {
				return err
			}
			err := write.MoveTo(ic)
			if err == nil {
				youngerWrote.Store(true)
			}
			youngerErr <- err
			return err
		}
		<-ic.Done()
		return context.Cause(ic)
	})
	<-inRead
	waitFor(t, "item 2 blocked in MoveTo(write)", func() bool { return parkedOf(c) == 1 })
	cancel(cause)
	assertShutdownCause(t, "younger item", recvErr(t, "item 2", youngerErr), cause)
	err := recvErr(t, "Run", done)
	assertShutdownCause(t, "Run", err, cause)
	if re := err.(RunError); len(re.ItemErrors()) != 0 {
		t.Fatalf("ItemErrors = %v, want none", re.ItemErrors())
	}
	if youngerWrote.Load() {
		t.Fatal("younger item entered write after an older one was aborted")
	}
}

// TestUntilShutdownNoGap: items move into write with their UntilShutdown context and on with their own. Shutdown
// begins at a random moment. No item enters write after shutdown has begun, and the items that enter it are always
// the oldest ones, with no gap; all of them commit.
func TestUntilShutdownNoGap(t *testing.T) {
	iterations := 200
	if testing.Short() {
		iterations = 30
	}
	var mixed int // iterations where some items entered write and some were refused: the interesting ones
	defer func() {
		if mixed == 0 && !t.Failed() {
			t.Error("no iteration had both entered and refused items; the scenarios tested nothing")
		}
	}()
	for i := 0; i < iterations; i++ {
		seed := int64(7_000 + i)
		rnd := rand.New(rand.NewSource(seed))
		c := NewConveyor()
		read := c.AddStage(OptName("read")).SetLimit(4)
		write := c.AddStage(OptName("write")).SetQueueSize(rnd.Intn(2))
		commit := c.AddStage(OptName("commit")).SetLimit(2)
		writeIdx := write.unit().index

		var mu sync.Mutex
		var entered, committed []int64
		var failure atomic.Value
		var refused atomic.Int64
		var delays sync.Map
		delay := func(no int64) time.Duration {
			if d, ok := delays.Load(no); ok {
				return d.(time.Duration)
			}
			return 0
		}
		for no := int64(1); no <= 200; no++ {
			delays.Store(no, time.Duration(rnd.Intn(150))*time.Microsecond)
		}
		started := make(chan struct{})
		var startOnce sync.Once
		cancel, done := runAsync(c, func(ic context.Context) error {
			no := itemNo(ic)
			startOnce.Do(func() { close(started) })
			pre := UntilShutdown(ic)
			if err := read.MoveTo(ic); err != nil {
				return err
			}
			time.Sleep(delay(no))
			if err := write.MoveTo(pre); err != nil {
				refused.Add(1)
				return err
			}
			mu.Lock()
			entered = append(entered, no)
			mu.Unlock()
			if err := commit.MoveTo(ic); err != nil {
				failure.CompareAndSwap(nil, fmt.Errorf("item %d: MoveTo(commit, ctx) after write = %v", no, err))
				return nil
			}
			mu.Lock()
			committed = append(committed, no)
			mu.Unlock()
			return nil
		})
		<-started
		time.Sleep(time.Duration(rnd.Intn(2000)) * time.Microsecond)

		// Begin the shutdown under the run's lock and record which items had entered write by then: the live items
		// that entered it, plus the recorded ones (a finished item recorded its entry before returning).
		cause := errors.New("stop")
		before := map[int64]bool{}
		r := implOf(c).currentRun.Load()
		r.mu.Lock()
		r.markShutdownLocked(cause)
		r.cond.Broadcast()
		for it := r.scopes[0].head; it != nil; it = it.next {
			if it.entered[writeIdx] {
				before[it.no] = true
			}
		}
		mu.Lock()
		for _, no := range entered {
			before[no] = true
		}
		mu.Unlock()
		r.mu.Unlock()
		cancel(cause)

		err := recvErr(t, "Run", done)
		assertShutdownCause(t, fmt.Sprintf("seed %d: Run", seed), err, cause)
		if re := err.(RunError); len(re.ItemErrors()) != 0 {
			t.Fatalf("seed %d: ItemErrors = %v, want none", seed, re.ItemErrors())
		}
		if e := failure.Load(); e != nil {
			t.Fatalf("seed %d: %v", seed, e)
		}
		assertStrictlyIncreasing(t, entered, fmt.Sprintf("seed %d: entered write", seed))
		assertStrictlyIncreasing(t, committed, fmt.Sprintf("seed %d: committed", seed))
		if len(committed) != len(entered) {
			t.Fatalf("seed %d: entered %v, committed %v", seed, entered, committed)
		}
		for _, no := range entered {
			if !before[no] {
				t.Fatalf("seed %d: item %d entered write after shutdown began", seed, no)
			}
		}
		if len(entered) > 0 && refused.Load() > 0 {
			mixed++
		}
	}
}

// TestUntilShutdownPoolTask: a pool task's context works with UntilShutdown: it keeps the item's number, is done on
// shutdown with a ShutdownError, and Schedule with it then fails. The task returns nil, so the item goes on.
func TestUntilShutdownPoolTask(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	write := c.AddStage(OptName("write"))

	running := make(chan struct{})
	res := make(chan error, 1)
	var wrote atomic.Bool
	cancel, done := runAsync(c, func(ic context.Context) error {
		if itemNo(ic) != 1 {
			<-UntilShutdown(ic).Done()
			return nil
		}
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		if err := fo.Schedule(ic, pool.NewTask(func(tctx context.Context) error {
			pre := UntilShutdown(tctx)
			if no, ok := ItemNoFromContext(pre); !ok || no != 1 {
				res <- fmt.Errorf("ItemNoFromContext(task pre) = %d, %v", no, ok)
				return nil
			}
			signal(running)
			<-pre.Done()
			if !isShutdown(context.Cause(pre)) {
				res <- fmt.Errorf("task pre cause = %v, want a ShutdownError", context.Cause(pre))
				return nil
			}
			if err := fo.Schedule(pre, pool.NewTask(func(context.Context) error { return nil })); !isShutdown(err) {
				res <- fmt.Errorf("Schedule(task pre) = %v, want a ShutdownError", err)
				return nil
			}
			res <- nil
			return nil
		})); err != nil {
			return err
		}
		if err := write.MoveTo(ic); err != nil {
			return err
		}
		wrote.Store(true)
		return nil
	})
	<-running
	cancel(cause)
	if err := recvErr(t, "task", res); err != nil {
		t.Fatal(err)
	}
	assertShutdownCause(t, "Run", recvErr(t, "Run", done), cause)
	if !wrote.Load() {
		t.Fatal("item did not go on after its task returned nil")
	}
}

// TestUntilShutdownLaneChild: a lane child's context works with UntilShutdown: a child blocked in MoveTo with it gets
// a ShutdownError, while the child inside the node, using its own context, finishes.
func TestUntilShutdownLaneChild(t *testing.T) {
	cause := errors.New("stop")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	lane := fo.AddLane(OptName("lane"))
	laneStage := lane.AddStage(OptName("lane stage"))

	inLane, release := make(chan struct{}), make(chan struct{})
	res := make(chan error, 2)
	cancel, done := runAsync(c, func(ic context.Context) error {
		if itemNo(ic) != 1 {
			<-UntilShutdown(ic).Done()
			return nil
		}
		if err := fo.MoveTo(ic); err != nil {
			return err
		}
		return fo.Schedule(ic, lane.NewTasks(2, func(cctx context.Context, i int) error {
			if i == 0 {
				if err := laneStage.MoveTo(cctx); err != nil {
					res <- err
					return nil
				}
				signal(inLane)
				<-release
				res <- context.Cause(cctx)
				return nil
			}
			<-inLane
			pre := UntilShutdown(cctx)
			if no, ok := ItemNoFromContext(pre); !ok || no != 1 {
				res <- fmt.Errorf("ItemNoFromContext(child pre) = %d, %v", no, ok)
				return nil
			}
			err := laneStage.MoveTo(pre) // blocked: the first child is inside
			if !isShutdown(err) {
				err = fmt.Errorf("MoveTo(lane stage, child pre) = %v, want a ShutdownError", err)
			}
			res <- err
			return nil
		}))
	})
	<-inLane
	waitFor(t, "second child blocked", func() bool { return parkedOf(c) >= 1 })
	cancel(cause)
	assertShutdownCause(t, "blocked child", recvErr(t, "second child", res), cause)
	close(release)
	if err := recvErr(t, "first child", res); err != nil {
		t.Fatalf("child inside the lane stage was canceled: %v", err)
	}
	assertShutdownCause(t, "Run", recvErr(t, "Run", done), cause)
}

// TestUntilShutdownGracePeriodItems: with a grace period, items stopped through UntilShutdown end at once; items
// using their own context are canceled only when the grace period ends.
func TestUntilShutdownGracePeriodItems(t *testing.T) {
	cause := errors.New("stop")
	graceCtx, endGrace := context.WithCancel(context.Background())
	defer endGrace()
	var graceOver atomic.Bool
	c := NewConveyor(OptGracePeriodFunc(func(error) (context.Context, context.CancelFunc) {
		return graceCtx, nil
	}))
	s := c.AddStage(OptName("s")).SetLimit(2)

	inS, waiting := make(chan struct{}), make(chan struct{})
	preErr, ctxErr := make(chan error, 1), make(chan error, 1)
	var earlyCancel atomic.Bool
	cancel, done := runAsync(c, func(ic context.Context) error {
		switch itemNo(ic) {
		case 1:
			if err := s.MoveTo(ic); err != nil {
				return err
			}
			signal(inS)
			<-ic.Done()
			if !graceOver.Load() {
				earlyCancel.Store(true)
			}
			ctxErr <- context.Cause(ic)
			return context.Cause(ic)
		case 2:
			<-inS
			pre := UntilShutdown(ic)
			signal(waiting)
			<-pre.Done()
			preErr <- context.Cause(pre)
			return context.Cause(pre)
		}
		<-ic.Done()
		return context.Cause(ic)
	})
	<-waiting
	cancel(cause)
	assertShutdownCause(t, "pre item", recvErr(t, "pre item", preErr), cause)
	graceOver.Store(true)
	endGrace()
	assertShutdownCause(t, "ctx item", recvErr(t, "ctx item", ctxErr), cause)
	if earlyCancel.Load() {
		t.Fatal("item using its own context was canceled before the grace period ended")
	}
	assertShutdownCause(t, "Run", recvErr(t, "Run", done), cause)
}

// TestOptGracePeriod: items are canceled once the grace period ends (at once for d <= 0), and DrainError reports it.
func TestOptGracePeriod(t *testing.T) {
	for _, d := range []time.Duration{-time.Second, 0, 50 * time.Millisecond} {
		t.Run(d.String(), func(t *testing.T) {
			cause := errors.New("stop")
			c := NewConveyor(OptGracePeriod(d))
			ready := make(chan struct{})
			var canceledAfter time.Duration
			var begun time.Time
			cancel, done := runAsync(c, func(ic context.Context) error {
				if itemNo(ic) != 1 {
					<-ic.Done()
					return context.Cause(ic)
				}
				signal(ready)
				<-ic.Done()
				canceledAfter = time.Since(begun)
				return context.Cause(ic)
			})
			<-ready
			begun = time.Now()
			cancel(cause)
			err := recvErr(t, "Run", done)
			assertShutdownCause(t, "Run", err, cause)
			if de := err.(RunError).DrainError(); !errors.Is(de, context.DeadlineExceeded) {
				t.Fatalf("DrainError = %v, want %v", de, context.DeadlineExceeded)
			}
			if d > 0 && canceledAfter < d {
				t.Fatalf("item canceled after %v, before the grace period of %v", canceledAfter, d)
			}
		})
	}
}

// TestOptGracePeriodLastWins: OptGracePeriod and OptGracePeriodFunc override each other; the last one wins.
func TestOptGracePeriodLastWins(t *testing.T) {
	noLimit := OptGracePeriodFunc(func(error) (context.Context, context.CancelFunc) { return nil, nil })
	cases := []struct {
		name         string
		opts         []Option
		wantCanceled bool
	}{
		{"func then duration", []Option{noLimit, OptGracePeriod(0)}, true},
		{"duration then func", []Option{OptGracePeriod(0), noLimit}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cause := errors.New("stop")
			c := NewConveyor(tc.opts...)
			ready := make(chan struct{})
			got := make(chan error, 1)
			cancel, done := runAsync(c, func(ic context.Context) error {
				if itemNo(ic) != 1 {
					<-ic.Done()
					return context.Cause(ic)
				}
				signal(ready)
				if !awaitTrue(func() bool { return shutdownBegun(c) }) {
					got <- errors.New("shutdown never began")
					return nil
				}
				select {
				case <-ic.Done():
				case <-time.After(50 * time.Millisecond):
				}
				got <- context.Cause(ic)
				return nil
			})
			<-ready
			cancel(cause)
			err := recvErr(t, "item", got)
			if tc.wantCanceled {
				assertShutdownCause(t, "item", err, cause)
			} else if err != nil {
				t.Fatalf("item canceled: %v, want no limit", err)
			}
			assertShutdownCause(t, "Run", recvErr(t, "Run", done), cause)
		})
	}
}

// TestUntilShutdownNoLeak: many calls for one item, with its own context and with derived ones, leave no goroutine
// or watch behind once the run is over.
func TestUntilShutdownNoLeak(t *testing.T) {
	base := runtime.NumGoroutine()
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	err := runOnce(t, c, func(ic context.Context) error {
		first := UntilShutdown(ic)
		for i := 0; i < 10_000; i++ {
			if UntilShutdown(ic) != first {
				return errors.New("the item context's UntilShutdown context is not reused")
			}
			if pre := UntilShutdown(context.WithValue(ic, ctxKey(100), i)); pre.Err() != nil {
				return fmt.Errorf("derived pre done: %v", context.Cause(pre))
			}
		}
		return write.MoveTo(first)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	waitFor(t, "goroutines to return to the baseline", func() bool { return runtime.NumGoroutine() <= base })
}
