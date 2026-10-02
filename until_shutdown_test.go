package conveyor

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"slices"
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
	if de := err.(RunError).DrainError(); de != nil {
		t.Fatalf("DrainError = %v, want nil", de)
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
			if de := err.(RunError).DrainError(); de != nil {
				t.Fatalf("DrainError = %v, want nil", de)
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
	if de := err.(RunError).DrainError(); de != nil {
		t.Fatalf("DrainError = %v, want nil", de)
	}
	if !committed.Load() || batchLen.Load() != 2 {
		t.Fatalf("committed = %v with %d values, want a partial batch of 2", committed.Load(), batchLen.Load())
	}
}

// TestUntilShutdownWrappedCanceled: a read canceled through UntilShutdown whose error (wrapping context.Canceled)
// is returned as is fails the item, for either trigger: it is the DrainError. Returning context.Cause(pre) instead is
// an abort. An item the conveyor canceled is aborted whatever it returns. Before any shutdown such an error is a
// failure too.
func TestUntilShutdownWrappedCanceled(t *testing.T) {
	errFetch := errors.New("fetch")
	fetch := func(ctx context.Context) error {
		<-ctx.Done()
		return fmt.Errorf("%w: %w", errFetch, context.Canceled)
	}
	read := func(ic context.Context, returnCause bool) error {
		pre := UntilShutdown(ic)
		err := fetch(pre)
		if returnCause && pre.Err() != nil {
			return context.Cause(pre)
		}
		return err
	}
	// assertDrain fails unless DrainError is nil (returnCause) or an ItemError of item 1 in unit u with the fetch error.
	assertDrain := func(t *testing.T, err error, returnCause bool, u Unit) {
		t.Helper()
		de := err.(RunError).DrainError()
		if returnCause {
			if de != nil {
				t.Fatalf("DrainError = %v, want nil", de)
			}
			return
		}
		var ie ItemError
		if !errors.As(de, &ie) || !errors.Is(ie.Unwrap(), errFetch) || !errors.Is(de, context.Canceled) ||
			ie.Unit() != u {
			t.Fatalf("DrainError = %v, want an ItemError in %v with the fetch error", de, u)
		}
	}
	for _, returnCause := range []bool{false, true} {
		t.Run(fmt.Sprintf("run canceled/return cause=%v", returnCause), func(t *testing.T) {
			cause := errors.New("stop")
			c := NewConveyor()
			ready := make(chan struct{})
			cancel, done := runAsync(c, func(ic context.Context) error {
				if itemNo(ic) != 1 {
					<-ic.Done()
					return context.Cause(ic)
				}
				signal(ready)
				return read(ic, returnCause)
			})
			<-ready
			cancel(cause)
			err := recvErr(t, "Run", done)
			assertShutdownCause(t, "Run", err, cause)
			assertDrain(t, err, returnCause, c.StartingStage())
		})
		t.Run(fmt.Sprintf("item failed/return cause=%v", returnCause), func(t *testing.T) {
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
					return read(ic, returnCause)
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
			if !errors.As(err, &ie) || ie.Unwrap() != boom {
				t.Fatalf("Run = %v, want an ItemError with %v", err, boom)
			}
			assertDrain(t, err, returnCause, s)
		})
	}
	t.Run("canceled by the conveyor", func(t *testing.T) {
		boom := errors.New("boom")
		c := NewConveyor()
		s := c.AddStage(OptName("s")).SetLimit(2)
		w := c.AddStage(OptName("w"))
		inW := make(chan struct{})
		moveErr := make(chan error, 1)
		_, done := runAsync(c, func(ic context.Context) error {
			switch itemNo(ic) {
			case 1:
				if err := s.MoveTo(ic); err != nil {
					return err
				}
				if err := w.MoveTo(ic); err != nil {
					return err
				}
				signal(inW)
				// Fail only once item 2 is blocked behind this one, so the cascade cancels it.
				if !awaitTrue(func() bool { return parkedOf(c) == 1 }) {
					return errors.New("item 2 never blocked")
				}
				return boom
			case 2:
				<-inW
				if err := s.MoveTo(ic); err != nil {
					return err
				}
				moveErr <- w.MoveTo(ic)
				return errors.New("own error after the cancellation") // an abort all the same
			}
			<-ic.Done()
			return context.Cause(ic)
		})
		err := recvErr(t, "Run", done)
		var ie ItemError
		if !errors.As(err, &ie) || ie.Unwrap() != boom {
			t.Fatalf("Run = %v, want an ItemError with %v", err, boom)
		}
		if de := ie.DrainError(); de != nil {
			t.Fatalf("DrainError = %v, want nil", de)
		}
		assertShutdownCause(t, "item 2 MoveTo", recvErr(t, "item 2", moveErr), boom)
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
// that uses its own context, so the younger one never enters a later node. If it returns context.Cause(pre), it is
// an abort and DrainError is nil; if it returns the read's wrapped context.Canceled, it is a failure and the
// DrainError, and the younger item is canceled all the same.
func TestUntilShutdownAbortCascade(t *testing.T) {
	for _, returnCause := range []bool{true, false} {
		t.Run(fmt.Sprintf("return cause=%v", returnCause), func(t *testing.T) {
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
					if returnCause {
						return context.Cause(pre)
					}
					return fmt.Errorf("fetch: %w", pre.Err())
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
			de := err.(RunError).DrainError()
			if returnCause && de != nil {
				t.Fatalf("DrainError = %v, want nil", de)
			}
			var ie ItemError
			if !returnCause && (!errors.As(de, &ie) || !errors.Is(de, context.Canceled) || ie.Unit() != Unit(read)) {
				t.Fatalf("DrainError = %v, want an ItemError in %v with the read's error", de, read)
			}
			if youngerWrote.Load() {
				t.Fatal("younger item entered write after an older one stopped")
			}
		})
	}
}

// TestUntilShutdownCascadeSameStage: an older item that stops through UntilShutdown inside a stage and returns
// context.Cause(pre) cancels a younger item in the same stage that already works with its own context, with no drain
// timeout. If the older item returns nil instead, the younger one is not canceled.
func TestUntilShutdownCascadeSameStage(t *testing.T) {
	for _, returnErr := range []bool{true, false} {
		t.Run(fmt.Sprintf("return error=%v", returnErr), func(t *testing.T) {
			cause := errors.New("stop")
			c := NewConveyor()
			s := c.AddStage(OptName("s")).SetLimit(2)

			inS, youngerInS, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			youngerErr := make(chan error, 1)
			cancel, done := runAsync(c, func(ic context.Context) error {
				switch itemNo(ic) {
				case 1:
					if err := s.MoveTo(ic); err != nil {
						return err
					}
					signal(inS)
					pre := UntilShutdown(ic)
					<-pre.Done()
					if !returnErr {
						return nil
					}
					return context.Cause(pre) // an abort
				case 2:
					<-inS
					if err := s.MoveTo(ic); err != nil {
						return err
					}
					signal(youngerInS)
					select {
					case <-ic.Done():
					case <-release:
					}
					youngerErr <- context.Cause(ic)
					return context.Cause(ic)
				}
				<-UntilShutdown(ic).Done()
				return nil
			})
			<-youngerInS
			cancel(cause)
			if returnErr {
				err, ok := recvErrWithin(youngerErr, 5*time.Second)
				if !ok {
					close(release)
					t.Fatal("younger item in the same stage not canceled after the older one was aborted")
				}
				assertShutdownCause(t, "younger item", err, cause)
			} else {
				// Item 1 has freed its slot: its completion, where the cascade would cancel item 2, is over.
				waitFor(t, "item 1 done", func() bool { return occupancyOf(c, s) <= 1 })
				close(release)
				if err := recvErr(t, "younger item", youngerErr); err != nil {
					t.Fatalf("younger item canceled: %v", err)
				}
			}
			err := recvErr(t, "Run", done)
			assertShutdownCause(t, "Run", err, cause)
			if de := err.(RunError).DrainError(); de != nil {
				t.Fatalf("DrainError = %v, want nil", de)
			}
		})
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
		var entered, committed, refused []int64
		var failure atomic.Value
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
				mu.Lock()
				refused = append(refused, no)
				mu.Unlock()
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
		if de := err.(RunError).DrainError(); de != nil {
			t.Fatalf("seed %d: DrainError = %v, want nil", seed, de)
		}
		if e := failure.Load(); e != nil {
			t.Fatalf("seed %d: %v", seed, e)
		}
		// No gap: entered and committed are exactly 1..m, and every item refused at write is younger than all of them.
		// Two items inside commit (limit 2) may record in either order, so committed is compared as a set.
		assertStrictlyIncreasing(t, entered, fmt.Sprintf("seed %d: entered write", seed))
		slices.Sort(committed)
		assertStrictlyIncreasing(t, committed, fmt.Sprintf("seed %d: committed", seed))
		if len(committed) != len(entered) {
			t.Fatalf("seed %d: entered %v, committed %v", seed, entered, committed)
		}
		for _, no := range refused {
			if no <= int64(len(entered)) {
				t.Fatalf("seed %d: item %d refused at write, but entered %v", seed, no, entered)
			}
		}
		for _, no := range entered {
			if !before[no] {
				t.Fatalf("seed %d: item %d entered write after shutdown began", seed, no)
			}
		}
		if len(entered) > 0 && len(refused) > 0 {
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

// TestUntilShutdownWorkReturn: a pool task or lane child whose read stops through UntilShutdown fails fast if it
// returns context.Cause(pre): the item and a sibling using its own context are canceled with a ShutdownError. If it
// returns nil instead, the sibling finishes and the item goes on, with no drain timeout.
func TestUntilShutdownWorkReturn(t *testing.T) {
	for _, kind := range []string{"pool task", "lane child"} {
		for _, returnErr := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/return error=%v", kind, returnErr), func(t *testing.T) {
				cause := errors.New("stop")
				c := NewConveyor()
				fo := c.AddFanOut(OptName("fo"))
				pool := fo.AddPool(OptName("pool")).SetLimit(2)
				lane := fo.AddLane(OptName("lane"))
				laneStage := lane.AddStage(OptName("lane stage")).SetLimit(2) // past the lane's start, both run at once
				write := c.AddStage(OptName("write"))

				reading, siblingRunning, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
				siblingErr, itemErr := make(chan error, 1), make(chan error, 1)
				var wrote atomic.Bool
				var it atomic.Pointer[item]
				read := func(wctx context.Context) error {
					pre := UntilShutdown(wctx)
					signal(reading)
					<-pre.Done()
					if !returnErr {
						return nil
					}
					return context.Cause(pre) // an abort
				}
				sibling := func(wctx context.Context) error {
					signal(siblingRunning)
					select {
					case <-wctx.Done():
					case <-release:
					}
					siblingErr <- context.Cause(wctx)
					return nil
				}
				cancel, done := runAsync(c, func(ic context.Context) error {
					if itemNo(ic) != 1 {
						<-UntilShutdown(ic).Done()
						return nil
					}
					it.Store(itemOf(ic))
					if err := fo.MoveTo(ic); err != nil {
						return err
					}
					var err error
					if kind == "pool task" {
						err = fo.Schedule(ic, pool.NewTask(read), pool.NewTask(sibling))
					} else {
						err = fo.Schedule(ic, lane.NewTasks(2, func(cctx context.Context, i int) error {
							if err := laneStage.MoveTo(cctx); err != nil {
								return err
							}
							if i == 0 {
								return read(cctx)
							}
							return sibling(cctx)
						}))
					}
					if err != nil {
						return err
					}
					err = write.MoveTo(ic)
					if err == nil {
						wrote.Store(true)
					}
					itemErr <- context.Cause(ic)
					return err
				})
				<-reading
				<-siblingRunning
				cancel(cause)
				if returnErr {
					err, ok := recvErrWithin(siblingErr, 5*time.Second)
					if !ok {
						close(release)
						t.Fatal("sibling not canceled after the reader returned its error")
					}
					assertShutdownCause(t, "sibling", err, cause)
					assertShutdownCause(t, "item", recvErr(t, "item", itemErr), cause)
				} else {
					waitFor(t, "reader done", func() bool {
						r := it.Load().run
						r.mu.Lock()
						defer r.mu.Unlock()
						return it.Load().pending.running <= 1
					})
					close(release)
					if err := recvErr(t, "sibling", siblingErr); err != nil {
						t.Fatalf("sibling canceled: %v", err)
					}
					if err := recvErr(t, "item", itemErr); err != nil {
						t.Fatalf("item canceled: %v", err)
					}
				}
				err := recvErr(t, "Run", done)
				assertShutdownCause(t, "Run", err, cause)
				if de := err.(RunError).DrainError(); de != nil {
					t.Fatalf("DrainError = %v, want nil", de)
				}
				if wrote.Load() == returnErr {
					t.Fatalf("item entered write = %v, want %v", wrote.Load(), !returnErr)
				}
			})
		}
	}
}

// TestUntilShutdownDrainTimeoutItems: with a drain timeout, items stopped through UntilShutdown end at once; items
// using their own context are canceled only when the drain times out.
func TestUntilShutdownDrainTimeoutItems(t *testing.T) {
	cause := errors.New("stop")
	drainCtx, endDrain := context.WithCancel(context.Background())
	defer endDrain()
	var drainOver atomic.Bool
	c := NewConveyor(OptDrainContextFunc(func(error) (context.Context, context.CancelFunc) {
		return drainCtx, nil
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
			if !drainOver.Load() {
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
	drainOver.Store(true)
	endDrain()
	assertShutdownCause(t, "ctx item", recvErr(t, "ctx item", ctxErr), cause)
	if earlyCancel.Load() {
		t.Fatal("item using its own context was canceled before the drain timed out")
	}
	assertShutdownCause(t, "Run", recvErr(t, "Run", done), cause)
}

// TestOptDrainTimeout: items are canceled once the drain times out (at once for d <= 0), and DrainError reports it.
func TestOptDrainTimeout(t *testing.T) {
	for _, d := range []time.Duration{-time.Second, 0, 50 * time.Millisecond} {
		t.Run(d.String(), func(t *testing.T) {
			cause := errors.New("stop")
			c := NewConveyor(OptDrainTimeout(d))
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
				t.Fatalf("item canceled after %v, before the drain timeout of %v", canceledAfter, d)
			}
		})
	}
}

// TestOptDrainTimeoutLastWins: OptDrainTimeout and OptDrainContextFunc override each other; the last one wins.
func TestOptDrainTimeoutLastWins(t *testing.T) {
	for _, funcLast := range []bool{false, true} {
		t.Run(fmt.Sprintf("func last=%v", funcLast), func(t *testing.T) {
			asked := make(chan struct{})
			noLimit := OptDrainContextFunc(func(error) (context.Context, context.CancelFunc) {
				signal(asked)
				return nil, nil
			})
			opts := []Option{noLimit, OptDrainTimeout(0)}
			if funcLast {
				opts = []Option{OptDrainTimeout(0), noLimit}
			}
			cause := errors.New("stop")
			c := NewConveyor(opts...)
			ready := make(chan struct{})
			got := make(chan error, 1)
			cancel, done := runAsync(c, func(ic context.Context) error {
				if itemNo(ic) != 1 {
					<-ic.Done()
					return context.Cause(ic)
				}
				signal(ready)
				select {
				case <-asked:
					if !funcLast {
						got <- errors.New("the drain context func was asked although OptDrainTimeout came last")
						return nil
					}
					// The func declined a limit, so the watcher cancels nothing after that.
					got <- context.Cause(ic)
				case <-ic.Done():
					got <- context.Cause(ic)
				case <-time.After(testTimeout):
					got <- errors.New("the item was neither canceled nor did the watcher ask the func")
				}
				return nil
			})
			<-ready
			cancel(cause)
			err := recvErr(t, "item", got)
			if !funcLast {
				assertShutdownCause(t, "item", err, cause)
			} else if err != nil {
				t.Fatalf("item canceled: %v, want no limit", err)
			}
			assertShutdownCause(t, "Run", recvErr(t, "Run", done), cause)
		})
	}
}

// TestUntilShutdownNoLeak: many calls for one item reuse the cached context for its own context, and with derived
// ones leave no goroutine behind once the run is over. TestUntilShutdownWatchFreed checks the watches.
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

// TestUntilShutdownWatchFreed: an UntilShutdown context whose parent is canceled removes its watch on the
// shutdown, so in a live item the watches do not grow with the number of such contexts.
func TestUntilShutdownWatchFreed(t *testing.T) {
	const n = 1000
	c := NewConveyor()
	var watches atomic.Int64
	implOf(c).watchHook = func(delta int) { watches.Add(int64(delta)) }
	err := runOnce(t, c, func(ic context.Context) error {
		base := watches.Load()
		cancels := make([]context.CancelFunc, 0, n)
		pres := make([]context.Context, 0, n)
		for i := 0; i < n; i++ {
			parent, cancelParent := context.WithCancel(ic)
			cancels = append(cancels, cancelParent)
			pres = append(pres, UntilShutdown(parent))
		}
		if got := watches.Load() - base; got != n {
			return fmt.Errorf("live watches = %d with %d live contexts, want %d", got, n, n)
		}
		for _, cancelParent := range cancels {
			cancelParent()
		}
		for _, pre := range pres {
			if !errors.Is(context.Cause(pre), context.Canceled) || isShutdown(context.Cause(pre)) {
				return fmt.Errorf("pre cause = %v, want the parent's cancel", context.Cause(pre))
			}
		}
		// A done context removes its watch on a goroutine of its own.
		if !awaitTrue(func() bool { return watches.Load() == base }) {
			return fmt.Errorf("live watches = %d after the contexts are done, want %d", watches.Load(), base)
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
}

// TestUntilShutdownFastPath: before shutdown, a call with an UntilShutdown context (or a value derivation of it)
// returns that context without taking the run's lock.
func TestUntilShutdownFastPath(t *testing.T) {
	c := NewConveyor()
	err := runOnce(t, c, func(ic context.Context) error {
		r := itemOf(ic).run
		own := UntilShutdown(ic)
		derived := UntilShutdown(context.WithValue(ic, ctxKey(100), 1))
		for _, pre := range []context.Context{own, derived, context.WithValue(own, ctxKey(101), 2)} {
			locked, returned := make(chan struct{}), make(chan struct{})
			var timedOut atomic.Bool
			go func() {
				r.mu.Lock()
				defer r.mu.Unlock() // released in all paths, so a call that waits for it does not hang the test
				signal(locked)
				select {
				case <-returned:
				case <-time.After(5 * time.Second):
					timedOut.Store(true)
				}
			}()
			<-locked
			got := UntilShutdown(pre)
			signal(returned)
			if timedOut.Load() {
				return errors.New("UntilShutdown waited for the run's lock")
			}
			if got != pre {
				return errors.New("UntilShutdown returned a new context, want the same one")
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
}

// TestUntilShutdownAbortCancelsOwnWork: an item that aborts (returns a ShutdownError while its own context is alive)
// cancels its own running pool task or RetainFor work. If it returns nil instead, that work finishes.
func TestUntilShutdownAbortCancelsOwnWork(t *testing.T) {
	for _, kind := range []string{"pool task", "RetainFor"} {
		for _, abort := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/abort=%v", kind, abort), func(t *testing.T) {
				cause := errors.New("stop")
				c := NewConveyor()
				hold := c.AddStage(OptName("hold"))
				fo := c.AddFanOut(OptName("fo"))
				pool := fo.AddPool(OptName("pool"))
				write := c.AddStage(OptName("write"))

				running, release := make(chan struct{}), make(chan struct{})
				workCause := make(chan error, 1)
				work := func(ctx context.Context) error {
					signal(running)
					select {
					case <-ctx.Done():
					case <-release:
					}
					workCause <- context.Cause(ctx)
					return nil
				}
				var it atomic.Pointer[item]
				cancel, done := runAsync(c, func(ic context.Context) error {
					if itemNo(ic) != 1 {
						<-UntilShutdown(ic).Done()
						return nil
					}
					it.Store(itemOf(ic))
					pre := UntilShutdown(ic)
					if kind == "pool task" {
						if err := fo.MoveTo(ic); err != nil {
							return err
						}
						if err := fo.Schedule(ic, pool.NewTask(work)); err != nil {
							return err
						}
					} else {
						if err := hold.MoveTo(ic); err != nil {
							return err
						}
						hold.RetainFor(ic, func() error { return work(ic) })
					}
					<-pre.Done()
					if !abort {
						return nil
					}
					return write.MoveTo(pre) // a ShutdownError while the item's context is alive
				})
				<-running
				cancel(cause)
				if abort {
					err, ok := recvErrWithin(workCause, 5*time.Second)
					if !ok {
						close(release)
						t.Fatal("own work not canceled after the item aborted")
					}
					assertShutdownCause(t, "own work", err, cause)
				} else {
					// The item has returned and its completion, where an abort would cancel the work, is done.
					waitFor(t, "item 1 returned", func() bool {
						r := it.Load().run
						r.mu.Lock()
						defer r.mu.Unlock()
						return it.Load().returned
					})
					close(release)
					if err := recvErr(t, "own work", workCause); err != nil {
						t.Fatalf("own work canceled: %v", err)
					}
				}
				err := recvErr(t, "Run", done)
				assertShutdownCause(t, "Run", err, cause)
				if de := err.(RunError).DrainError(); de != nil {
					t.Fatalf("DrainError = %v, want nil", de)
				}
			})
		}
	}
}
