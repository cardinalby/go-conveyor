package conveyor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestWaveStartedThenFinished: Started closes when the work has all been handed out, Finished when it has all
// completed, and Started never closes after Finished.
func TestWaveStartedThenFinished(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	var startedBeforeFinished atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTasks(4, func(_ context.Context, i int) error { return nil })})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		<-w.Started()
		select {
		case <-w.Finished():
			// Finished may already be closed by now; what matters is Started closed no later than it.
		default:
		}
		startedBeforeFinished.Store(true)
		<-w.Finished()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if !startedBeforeFinished.Load() {
		t.Fatalf("Started did not close")
	}
}

// TestWaveStartedWaitsForStreamingSource: with a streaming source, Started is the signal that the conveyor has
// stopped pulling user code — the documented moment after which the generator's state may be mutated.
func TestWaveStartedWaitsForStreamingSource(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool")).SetLimit(2)

	var pulls, pullsAtStarted atomic.Int64
	err := runOnce(t, c, func(ctx context.Context) error {
		err := fo.MoveTo(ctx, Tasks{pool.NewTasksGen(func(yield func(TaskFunc) bool) {
			for i := 0; i < 5; i++ {
				pulls.Add(1)
				if !yield(func(context.Context) error { return nil }) {
					return
				}
			}
		})})
		if err != nil {
			return err
		}
		w := fo.Detach(ctx)
		<-w.Started()
		pullsAtStarted.Store(pulls.Load())
		<-w.Finished()
		return w.Err()
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
	if got := pullsAtStarted.Load(); got != 5 {
		t.Fatalf("generator had produced %d items when Started closed, want all 5", got)
	}
}

// TestWaveErrIsFinalAfterFinished checks Err reports the work's error once the wave is finished.
func TestWaveErrIsFinalAfterFinished(t *testing.T) {
	boom := errors.New("wave boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	var got error
	err := runOnce(t, c, func(ctx context.Context) error {
		ferr := fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error { return boom })})
		if ferr != nil {
			return ferr
		}
		w := fo.Detach(ctx)
		<-w.Finished()
		got = w.Err()
		return nil // the error was observed, so it must not fail the run again
	})
	if !errors.Is(got, boom) {
		t.Fatalf("Wave.Err = %v, want %v", got, boom)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("observing the error should not fail the run, got %v", err)
	}
}

// TestUnobservedWaveErrorFailsTheRun is the safety net: a wave nobody joined and nobody read still reports its
// failure — delayed, never lost.
func TestUnobservedWaveErrorFailsTheRun(t *testing.T) {
	boom := errors.New("unobserved boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		var tasks Tasks
		if no == 1 {
			tasks.Add(pool.NewTask(func(context.Context) error { return boom }))
		} else {
			tasks.Add(pool.NewTask(func(context.Context) error { return nil }))
		}
		ferr := fo.MoveTo(ic, tasks)
		return ferr // the wave is never joined
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want the unobserved %v", err, boom)
	}
}

// TestWaveJoinedAtLaterNodeSurfacesThere: the error appears at the MoveTo that names the wave, before that node's
// work runs.
func TestWaveJoinedAtLaterNodeSurfacesThere(t *testing.T) {
	boom := errors.New("joined boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	mid := c.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	var midReached, commitReached atomic.Bool
	var joinErr error
	// The task must not fail before the item has passed the un-joined stage: its error is fail-fast, so it would
	// otherwise be free to cancel the item first and make the assertion below racy.
	pastMid := make(chan struct{})
	err := runOnce(t, c, func(ctx context.Context) error {
		ferr := fo.MoveTo(ctx, Tasks{pool.NewTask(func(context.Context) error {
			<-pastMid
			return boom
		})})
		if ferr != nil {
			return ferr
		}
		// Detached: passing `mid` must not wait for the work, so the wave is the item's to carry to `commit`.
		w := fo.Detach(ctx)
		if err := mid.MoveTo(ctx); err != nil { // not joined here
			return err
		}
		midReached.Store(true)
		close(pastMid)
		joinErr = commit.MoveTo(ctx, w) // joined here
		if joinErr != nil {
			return joinErr
		}
		commitReached.Store(true)
		return nil
	})

	if !midReached.Load() {
		t.Fatalf("the un-joined stage should have been reached")
	}
	if commitReached.Load() {
		t.Fatalf("the joining stage ran its work despite the failed wave")
	}
	if !errors.Is(joinErr, boom) {
		t.Fatalf("join error = %v, want %v", joinErr, boom)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
}

// TestJoinSeveralWavesReportsFirstFailure: joins are processed in the order listed.
func TestJoinSeveralWavesReportsFirstFailure(t *testing.T) {
	first := errors.New("first boom")
	second := errors.New("second boom")
	c := NewConveyor()
	foA := c.AddFanOut(OptName("foA"))
	poolA := foA.AddPool(OptName("poolA"))
	foB := c.AddFanOut(OptName("foB"))
	poolB := foB.AddPool(OptName("poolB"))
	commit := c.AddStage(OptName("commit"))

	var joinErr error
	_ = runOnce(t, c, func(ctx context.Context) error {
		err := foA.MoveTo(ctx, Tasks{poolA.NewTask(func(context.Context) error { return first })})
		if err != nil {
			return err
		}
		wa := foA.Detach(ctx)
		err = foB.MoveTo(ctx, Tasks{poolB.NewTask(func(context.Context) error { return second })})
		if err != nil {
			// foB's move joins nothing, but the item's ctx may already be poisoned by foA's failure.
			return err
		}
		wb := foB.Detach(ctx)
		joinErr = commit.MoveTo(ctx, wa, wb)
		return joinErr
	})
	if joinErr != nil && !errors.Is(joinErr, first) && !errors.Is(joinErr, second) {
		t.Fatalf("join error = %v, want one of the two task errors", joinErr)
	}
}

// TestJoinForeignWavePanics: a wave is only meaningful to the item that created it.
func TestJoinForeignWavePanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	waves := make(chan Wave, 1)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	var checked atomic.Bool

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		switch no {
		case 1:
			err := fo.MoveTo(ic, Tasks{pool.NewTask(func(context.Context) error { return nil })})
			if err != nil {
				return err
			}
			w := fo.Detach(ic)
			waves <- w
			return commit.MoveTo(ic)
		case 2:
			// Item 2 tries to join item 1's wave.
			foreign := <-waves
			assertPanics(t, errForeignWave, func() {
				_ = commit.MoveTo(ic, foreign)
			})
			checked.Store(true)
			cancel()
			return nil
		default:
			return nil
		}
	})
	if !checked.Load() {
		t.Fatalf("the foreign-wave check did not run")
	}
}

// TestWaveResolvesOnShutdown: a wave always resolves — on cancellation its work stops and the wave finishes.
func TestWaveResolvesOnShutdown(t *testing.T) {
	c := NewConveyor(optCancelItemsOnShutdown()) // cancel in-flight items at once on shutdown
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	ctx, cancel := context.WithCancel(context.Background())
	resolved := make(chan struct{})
	var once atomic.Bool

	go func() {
		_ = c.Run(ctx, func(ic context.Context) error {
			err := fo.MoveTo(ic, Tasks{pool.NewTasks(3, func(_ context.Context, i int) error {
				<-ic.Done() // never completes until shutdown cancels the item
				return nil
			})})
			if err != nil {
				return err
			}
			w := fo.Detach(ic)
			if once.CompareAndSwap(false, true) {
				cancel()
				<-w.Finished() // must resolve thanks to the cancellation
				close(resolved)
			}
			return nil
		})
	}()

	select {
	case <-resolved:
	case <-time.After(testTimeout):
		t.Fatalf("the wave never resolved after shutdown")
	}
}

// TestRetainWaveJoined: Stage.Retain hands back the same currency, joined the same way.
func TestRetainWaveJoined(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))

	var order recorder
	runNOK(t, c, 6, func(ctx context.Context, no int64) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.Retain(ctx, func() error {
			order.add("bg-%d", no)
			return nil
		})
		if err := commit.MoveTo(ctx, w); err != nil {
			return err
		}
		order.add("commit-%d", no)
		return nil
	})

	events := order.all()
	// Every commit-N must be preceded by its bg-N.
	seen := map[string]bool{}
	for _, ev := range events {
		seen[ev] = true
		if len(ev) > 7 && ev[:7] == "commit-" {
			if !seen["bg-"+ev[7:]] {
				t.Fatalf("%s ran before its retained work: %v", ev, events)
			}
		}
	}
}

// TestWaveNeverReportsCleanFinishForSkippedWork: whether an item may keep working is decided per item, not per
// task, so the shape a task set came in must not change what the wave reports. An item still allowed to continue
// runs its whole set (Err() == nil means every task ran); an item that lost permission drops the rest and the wave
// must carry the cancellation cause rather than settling clean — a callback that was never invoked returns no error
// of its own, so without this the wave would look exactly like full success.
func TestWaveNeverReportsCleanFinishForSkippedWork(t *testing.T) {
	const tasks = 12

	shapes := map[string]func(b Branch, fn TaskFunc) Task{
		"counted": func(b Branch, fn TaskFunc) Task {
			return b.NewTasks(tasks, func(ctx context.Context, _ int) error { return fn(ctx) })
		},
		"generator": func(b Branch, fn TaskFunc) Task {
			return b.NewTasksGen(func(yield func(TaskFunc) bool) {
				for range tasks {
					if !yield(fn) {
						return
					}
				}
			})
		},
		"channel": func(b Branch, fn TaskFunc) Task {
			ch := make(chan TaskFunc, tasks)
			for range tasks {
				ch <- fn
			}
			close(ch)
			return b.NewTasksChan(ch)
		},
	}

	for _, tc := range []struct {
		name    string
		opts    []Option
		wantAll bool // the item keeps permission, so every task must run and the wave must be clean
	}{
		{name: "allowed to continue", wantAll: true},
		{name: "canceled at once", opts: []Option{optCancelItemsOnShutdown()}},
	} {
		for shape, build := range shapes {
			t.Run(tc.name+"/"+shape, func(t *testing.T) {
				c := NewConveyor(tc.opts...)
				fo := c.AddFanOut(OptName("fo"))
				pool := fo.AddPool(OptName("pool")) // limit 1: strictly one task at a time, so there is a tail to cut

				// Several items may be created before the shutdown lands; all assertions are about the first one,
				// so its tasks are counted apart from any later item's.
				var ran atomic.Int64
				var once atomic.Bool
				firstDone := make(chan error, 1) // the first item's wave error

				ctx, cancel := context.WithCancel(context.Background())
				err := c.Run(ctx, func(ic context.Context) error {
					no, _ := ItemNoFromContext(ic)
					if no != 1 {
						return nil // later items take no part
					}
					err := fo.MoveTo(ic, Tasks{build(pool, func(context.Context) error {
						if once.CompareAndSwap(false, true) {
							cancel() // shut down with the rest of the set still queued
							// Hold the pool until the shutdown has landed on this item, so the rest of the set is
							// still waiting when it does. An item that keeps its permission is never canceled, so
							// there is nothing to wait for in that case.
							for i := 0; !tc.wantAll && i < 200 && context.Cause(ic) == nil; i++ {
								time.Sleep(time.Millisecond)
							}
						}
						ran.Add(1)
						return nil
					})})
					if err != nil {
						return err
					}
					w := fo.Detach(ic)
					<-w.Finished()
					firstDone <- w.Err()
					return nil
				})
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("run failed: %v", err)
				}
				var waveErr error
				select {
				case waveErr = <-firstDone:
				default:
					t.Fatal("the first item never observed its wave")
				}

				switch got := ran.Load(); {
				case tc.wantAll:
					if got != tasks {
						t.Errorf("ran %d/%d tasks: an item allowed to continue must run its whole set", got, tasks)
					}
					if waveErr != nil {
						t.Errorf("wave error = %v, want nil: every task ran", waveErr)
					}
				default:
					if got >= tasks {
						t.Skip("the whole set ran before the cancellation landed; nothing was skipped to report")
					}
					if waveErr == nil {
						t.Fatalf("wave reported a clean finish after only %d/%d tasks ran: skipped work must not "+
							"look like success", got, tasks)
					}
					if !isShutdown(waveErr) {
						t.Errorf("wave error = %v, want the item's shutdown cause", waveErr)
					}
				}
			})
		}
	}
}
