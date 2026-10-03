package conveyor

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestTaskGroupWaitIsFinalAfterFinished: once the task group is finished, its recorded outcome is final, and Wait
// reports it as a TaskError and joins it. A processor that handles it and returns nil ends the item clean.
func TestTaskGroupWaitIsFinalAfterFinished(t *testing.T) {
	boom := errors.New("task group boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	var recorded, got error
	var joined bool
	err := runOnce(t, c, func(ctx context.Context) error {
		ferr := fo.MoveTo(ctx)
		if ferr == nil {
			ferr = fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return boom }))
		}
		if ferr != nil {
			return ferr
		}
		w := fo.Retain(ctx)
		<-w.Finished()
		recorded = groupErr(w)
		got = w.Wait(ctx)
		joined = joinedOf(ctx, w)
		return nil // the error was joined, so it must not fail the run again
	})
	if !errors.Is(recorded, boom) {
		t.Fatalf("recorded outcome after Finished = %v, want %v", recorded, boom)
	}
	if got != recorded { //nolint:errorlint // identity: Wait reports the recorded value
		t.Fatalf("TaskGroup.Wait = %v, want the recorded %v", got, recorded)
	}
	if !joined {
		t.Fatalf("Wait on a finished task group did not join it")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("a joined error should not fail the run, got %v", err)
	}
}

// TestReadingTaskGroupOutcomeDoesNotJoin: there is no join by reading. A monitor goroutine that sees Finished and reads
// the recorded outcome does not join the task group, so the processor's nil does not hide the failure: the item still
// fails with the TaskError.
func TestReadingTaskGroupOutcomeDoesNotJoin(t *testing.T) {
	boom := errors.New("monitored boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	read := make(chan error, 1)
	err := runOnce(t, c, func(ctx context.Context) error {
		ferr := fo.MoveTo(ctx)
		if ferr == nil {
			ferr = fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return boom }))
		}
		if ferr != nil {
			return ferr
		}
		w := fo.Retain(ctx)
		go func() {
			<-w.Finished()
			read <- groupErr(w)
		}()
		if err := <-read; !errors.Is(err, boom) {
			t.Errorf("monitor read %v, want %v", err, boom)
		}
		if joinedOf(ctx, w) {
			t.Errorf("reading the outcome joined the task group")
		}
		return nil
	})
	if te := runTaskError(err); te == nil || !errors.Is(te, boom) {
		t.Fatalf("Run = %v, want the item to fail with the TaskError of %v", err, boom)
	}
}

// runTaskError finds the TaskError an item failed with in a Run result: as the trigger, or, when the Run context was
// canceled first (runOnce cancels it once a processor returns nil), in RunError.DrainError. nil if there is none.
func runTaskError(err error) TaskError {
	var te TaskError
	if errors.As(err, &te) {
		return te
	}
	var re RunError
	if errors.As(err, &re) && errors.As(re.DrainError(), &te) {
		return te
	}
	return nil
}

// TestTaskGroupWaitReturnsTaskError: Wait reports a task's failure as a TaskError. errors.As finds it, Unwrap gives the
// task's own error, and Unit is where the task ran: the stage for RetainFor, the pool for a fan-out task.
func TestTaskGroupWaitReturnsTaskError(t *testing.T) {
	boom := errors.New("task boom")
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	check := func(what string, err error, want Unit) {
		t.Helper()
		var te TaskError
		if !errors.As(err, &te) {
			t.Errorf("%s: Wait = %v (%T), want a TaskError", what, err, err)
			return
		}
		if te.Unwrap() != boom { //nolint:errorlint // identity: Unwrap gives the task's error unchanged
			t.Errorf("%s: Unwrap = %v, want %v", what, te.Unwrap(), boom)
		}
		if te.Unit() != want {
			t.Errorf("%s: Unit = %v, want %v", what, te.Unit(), want)
		}
		if wantText := want.String() + " task: " + boom.Error(); te.Error() != wantText {
			t.Errorf("%s: Error = %q, want %q", what, te.Error(), wantText)
		}
	}

	err := runOnce(t, c, func(ctx context.Context) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		rw := write.RetainFor(ctx, func(context.Context) error { return boom })
		check("RetainFor", rw.Wait(ctx), write)

		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return boom })); err != nil {
			return err
		}
		fw := fo.Retain(ctx)
		check("FanOut.Retain", fw.Wait(ctx), pool)
		return nil // both errors were joined
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v; both errors were joined, completion must not raise them", err)
	}
}

// TestUnjoinedTaskGroupErrorFailsTheRun is the safety net: a task group nobody joined still reports its failure —
// delayed, never lost.
func TestUnjoinedTaskGroupErrorFailsTheRun(t *testing.T) {
	boom := errors.New("unjoined boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err := c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		var tasks []Task
		if no == 1 {
			tasks = append(tasks, pool.NewTask(func(context.Context) error { return boom }))
		} else {
			tasks = append(tasks, pool.NewTask(func(context.Context) error { return nil }))
		}
		ferr := fo.MoveTo(ic)
		if ferr == nil {
			ferr = fo.Schedule(ic, tasks...)
		}
		return ferr // the task group is never joined
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want the unjoined %v", err, boom)
	}
}

// TestTaskGroupJoinedAtLaterNodeSurfacesThere: a retained task group's failure does not stop the item. The moves after
// it work, and the error appears at the Wait in the later node, before that node's work runs.
func TestTaskGroupJoinedAtLaterNodeSurfacesThere(t *testing.T) {
	boom := errors.New("joined boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	mid := c.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	var midReached, commitReached atomic.Bool
	var joinErr error
	// The task fails only once the item has passed the un-joined stage, so the failure lands while the item moves on.
	pastMid := make(chan struct{})
	err := runOnce(t, c, func(ctx context.Context) error {
		ferr := fo.MoveTo(ctx)
		if ferr == nil {
			ferr = fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
				<-pastMid
				return boom
			}))
		}
		if ferr != nil {
			return ferr
		}
		// Retained: passing `mid` must not wait for the work, so the task group is the item's to carry to `commit`.
		w := fo.Retain(ctx)
		if err := mid.MoveTo(ctx); err != nil { // not joined here
			return err
		}
		midReached.Store(true)
		close(pastMid)
		// A task failure does not cancel the item, so this move works whenever the task fails.
		if err := commit.MoveTo(ctx); err != nil {
			t.Errorf("commit.MoveTo = %v, want nil: a task failure does not cancel the item", err)
			return err
		}
		<-w.Finished()
		joinErr = w.Wait(ctx) // waited for here
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
		t.Fatalf("the stage ran its work despite the failed task group")
	}
	if !errors.Is(joinErr, boom) {
		t.Fatalf("join error = %v, want %v", joinErr, boom)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want %v", err, boom)
	}
}

// TestWaitSeveralTaskGroupsEachReportsItsOwnFailure: every task group answers for its own work. Two retained task
// groups fail; each Wait reports its own task group's TaskError, named after the pool the task ran on, and joins only
// that task group. Once both were waited for, the processor may end the item clean.
func TestWaitSeveralTaskGroupsEachReportsItsOwnFailure(t *testing.T) {
	first := errors.New("first boom")
	second := errors.New("second boom")
	c := NewConveyor()
	foA := c.AddFanOut(OptName("foA"))
	poolA := foA.AddPool(OptName("poolA"))
	foB := c.AddFanOut(OptName("foB"))
	poolB := foB.AddPool(OptName("poolB"))
	commit := c.AddStage(OptName("commit"))

	release := make(chan struct{}) // both tasks fail only once both task groups exist and the item is in commit
	err := runOnce(t, c, func(ctx context.Context) error {
		err := foA.MoveTo(ctx)
		if err == nil {
			err = foA.Schedule(ctx, poolA.NewTask(func(context.Context) error { <-release; return first }))
		}
		if err != nil {
			return err
		}
		wa := foA.Retain(ctx)
		err = foB.MoveTo(ctx)
		if err == nil {
			err = foB.Schedule(ctx, poolB.NewTask(func(context.Context) error { <-release; return second }))
		}
		if err != nil {
			return err
		}
		wb := foB.Retain(ctx)
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		close(release)
		<-wa.Finished()
		<-wb.Finished()

		var te TaskError
		if err := wa.Wait(ctx); !errors.Is(err, first) {
			t.Errorf("wa.Wait = %v, want %v", err, first)
		} else if !errors.As(err, &te) || te.Unit() != Unit(poolA) || !strings.HasPrefix(err.Error(), "poolA task: ") {
			t.Errorf("wa.Wait = %q, want a TaskError named after poolA", err)
		}
		if joinedOf(ctx, wb) {
			t.Errorf("wb was joined by waiting for wa")
		}
		if err := wb.Wait(ctx); !errors.Is(err, second) {
			t.Errorf("wb.Wait = %v, want %v", err, second)
		} else if !errors.As(err, &te) || te.Unit() != Unit(poolB) || !strings.HasPrefix(err.Error(), "poolB task: ") {
			t.Errorf("wb.Wait = %q, want a TaskError named after poolB", err)
		}
		return nil // both errors were reported to us
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v; both task group errors were joined, completion must not raise them", err)
	}
}

// joinedOf reports whether w's outcome was joined. Caller passes the task group's item context.
func joinedOf(ctx context.Context, w TaskGroup) bool {
	r := itemOf(ctx).run
	r.mu.Lock()
	defer r.mu.Unlock()
	return w.(*taskGroup).joined
}

// TestWaitForeignTaskGroupPanics: a task group is only meaningful to the item that created it.
func TestWaitForeignTaskGroupPanics(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	commit := c.AddStage(OptName("commit"))

	groups := make(chan TaskGroup, 1)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	var checked atomic.Bool

	_ = c.Run(ctx, func(ic context.Context) error {
		no, _ := ItemNoFromContext(ic)
		switch no {
		case 1:
			err := fo.MoveTo(ic)
			if err == nil {
				err = fo.Schedule(ic, pool.NewTask(func(context.Context) error { return nil }))
			}
			if err != nil {
				return err
			}
			w := fo.Retain(ic)
			groups <- w
			return commit.MoveTo(ic)
		case 2:
			// Item 2 tries to wait for item 1's task group.
			foreign := <-groups
			assertPanics(t, errForeignTaskGroup, func() {
				_ = foreign.Wait(ic)
			})
			checked.Store(true)
			cancel()
			return nil
		default:
			return nil
		}
	})
	if !checked.Load() {
		t.Fatalf("the foreign-task-group check did not run")
	}
}

// TestTaskGroupResolvesOnShutdown: a task group always resolves — on cancellation its work stops and the task group
// finishes.
func TestTaskGroupResolvesOnShutdown(t *testing.T) {
	c := NewConveyor(OptDrainTimeout(0)) // cancel in-flight items at once on shutdown
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))

	ctx, cancel := context.WithCancel(context.Background())
	resolved := make(chan struct{})
	var once atomic.Bool

	go func() {
		_ = c.Run(ctx, func(ic context.Context) error {
			err := fo.MoveTo(ic)
			if err == nil {
				err = fo.Schedule(ic, pool.NewTasks(3, func(_ context.Context, i int) error {
					<-ic.Done() // never completes until shutdown cancels the item
					return nil
				}))
			}
			if err != nil {
				return err
			}
			w := fo.Retain(ic)
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
		t.Fatalf("the task group never resolved after shutdown")
	}
}

// TestRetainTaskGroupJoined: Stage.Retain hands back the same currency, joined the same way.
func TestRetainTaskGroupJoined(t *testing.T) {
	c := NewConveyor()
	write := c.AddStage(OptName("write"))
	commit := c.AddStage(OptName("commit"))

	var order recorder
	runNOK(t, c, 6, func(ctx context.Context, no int64) error {
		if err := write.MoveTo(ctx); err != nil {
			return err
		}
		w := write.RetainFor(ctx, func(context.Context) error {
			order.add("bg-%d", no)
			return nil
		})
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		if err := w.Wait(ctx); err != nil {
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

// TestTaskGroupNeverReportsCleanFinishForSkippedWork: whether an item may keep working is decided per item, not per
// task, so the shape a task set came in must not change what the task group reports. An item still allowed to continue
// runs its whole set (a nil outcome means every task ran); an item that lost permission drops the rest and the task
// group must carry the cancellation cause rather than settling clean — a callback that was never invoked returns no
// error of its own, so without this the task group would look exactly like full success.
func TestTaskGroupNeverReportsCleanFinishForSkippedWork(t *testing.T) {
	const tasks = 12

	shapes := map[string]func(b Branch, fn TaskFunc) []Task{
		"counted": func(b Branch, fn TaskFunc) []Task {
			return []Task{b.NewTasks(tasks, func(ctx context.Context, _ int) error { return fn(ctx) })}
		},
		"singles": func(b Branch, fn TaskFunc) []Task {
			out := make([]Task, tasks)
			for i := range out {
				out[i] = b.NewTask(fn)
			}
			return out
		},
	}

	for _, tc := range []struct {
		name    string
		opts    []Option
		wantAll bool // the item keeps permission, so every task must run and the task group must be clean
	}{
		{name: "allowed to continue", wantAll: true},
		{name: "canceled at once", opts: []Option{OptDrainTimeout(0)}},
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
				firstDone := make(chan error, 1) // the first item's task group error

				ctx, cancel := context.WithCancel(context.Background())
				err := c.Run(ctx, func(ic context.Context) error {
					no, _ := ItemNoFromContext(ic)
					if no != 1 {
						return nil // later items take no part
					}
					err := fo.MoveTo(ic)
					if err == nil {
						err = fo.Schedule(ic, build(pool, func(context.Context) error {
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
						})...)
					}
					if err != nil {
						return err
					}
					w := fo.Retain(ic)
					<-w.Finished()
					// Not Wait: on a canceled item it gives the cancellation cause for a clean task group too, which
					// would hide the very bug this test looks for. The recorded outcome is what must not be nil.
					firstDone <- groupErr(w)
					return nil
				})
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("run failed: %v", err)
				}
				var tgErr error
				select {
				case tgErr = <-firstDone:
				default:
					t.Fatal("the first item never observed its task group")
				}

				switch got := ran.Load(); {
				case tc.wantAll:
					if got != tasks {
						t.Errorf("ran %d/%d tasks: an item allowed to continue must run its whole set", got, tasks)
					}
					if tgErr != nil {
						t.Errorf("task group error = %v, want nil: every task ran", tgErr)
					}
				default:
					if got >= tasks {
						t.Skip("the whole set ran before the cancellation landed; nothing was skipped to report")
					}
					if tgErr == nil {
						t.Fatalf("task group reported a clean finish after only %d/%d tasks ran: skipped work must not "+
							"look like success", got, tasks)
					}
					if !isShutdown(tgErr) {
						t.Errorf("task group error = %v, want the item's shutdown cause", tgErr)
					}
				}
			})
		}
	}
}

// TestWaitOnFailedTaskGroupJoinsIt: the failing task does not cancel the item, so the moves work and Wait blocks until
// the task group is finished, then reports its TaskError and joins it. A processor that handles the reported error and
// returns nil has dealt with it; completion must not fail the item with it a second time.
func TestWaitOnFailedTaskGroupJoinsIt(t *testing.T) {
	boom := errors.New("joined boom")
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo"))
	pool := fo.AddPool(OptName("pool"))
	mid := c.AddStage(OptName("mid"))
	commit := c.AddStage(OptName("commit"))

	var handled atomic.Bool
	err := runOnce(t, c, func(ctx context.Context) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error {
			// Fail only once the item is inside commit: the move succeeds and the Wait is what hears the failure.
			waitFor(t, "the item to enter commit", func() bool { return occupancyOf(c, commit) == 1 })
			return boom
		}))
		if err != nil {
			return err
		}
		w := fo.Retain(ctx)
		if err := mid.MoveTo(ctx); err != nil {
			return err
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		if err := w.Wait(ctx); !errors.Is(err, boom) {
			t.Errorf("Wait = %v, want %v", err, boom)
		}
		if !joinedOf(ctx, w) {
			t.Errorf("Wait on the failed task group did not join it")
		}
		handled.Store(true)
		return nil // the error was reported to us and we handled it
	})
	if !handled.Load() {
		t.Fatalf("the item never waited for the task group")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v; the joined error was already reported to the item, completion must not raise it again", err)
	}
}
