package conveyor

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// inBodyOf reports the item numbers holding a slot in u right now, sorted, for assertions on who is where.
func inBodyOf(c Conveyor, u Unit) []int64 {
	for _, occ := range c.DebugUnitOccupants() {
		if occ.Unit == u {
			out := slices.Clone(occ.InBody)
			slices.Sort(out)
			return out
		}
	}
	return nil
}

// inQueueOf reports the item numbers waiting in front of u right now, in arrival order.
func inQueueOf(c Conveyor, u Unit) []int64 {
	for _, occ := range c.DebugUnitOccupants() {
		if occ.Unit == u {
			return slices.Clone(occ.InQueue)
		}
	}
	return nil
}

// heldItems reports the numbers of the items that carry an upstream hold right now, sorted. White-box: the hold is
// internal bookkeeping, and this is the one direct way to tell "still holding" from "inside the fan-out".
func heldItems(c Conveyor) []int64 {
	r := implOf(c).currentRun.Load()
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []int64
	for _, scope := range r.scopes {
		for it := scope.head; it != nil; it = it.next {
			if len(it.holds) > 0 {
				out = append(out, it.no)
			}
		}
	}
	slices.Sort(out)
	return out
}

// holdCount reports how many upstream holds item no carries right now. White-box, like heldItems.
func holdCount(c Conveyor, no int64) int {
	r := implOf(c).currentRun.Load()
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, scope := range r.scopes {
		for it := scope.head; it != nil; it = it.next {
			if it.no == no {
				return len(it.holds)
			}
		}
	}
	return 0
}

// blockingTasks hands out task callbacks that block until released, keyed by "<item>/<pool>", and records which ones
// have started.
type blockingTasks struct {
	mu      sync.Mutex
	started map[string]bool
	release map[string]chan struct{}
}

func newBlockingTasks() *blockingTasks {
	return &blockingTasks{started: map[string]bool{}, release: map[string]chan struct{}{}}
}

func (b *blockingTasks) gate(key string) chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch, ok := b.release[key]
	if !ok {
		ch = make(chan struct{})
		b.release[key] = ch
	}
	return ch
}

func (b *blockingTasks) task(p Pool, no int64) Task {
	key := fmt.Sprintf("%d/%s", no, p)
	ch := b.gate(key)
	return p.NewTask(func(context.Context) error {
		b.mu.Lock()
		b.started[key] = true
		b.mu.Unlock()
		<-ch
		return nil
	})
}

func (b *blockingTasks) hasStarted(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.started[key]
}

func (b *blockingTasks) releaseAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.release {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}

// holdingModes are the modes that open an upstream hold; tests that pin the shared hold mechanics run under both.
var holdingModes = []FanOutBackpressure{BackpressureBalanced, BackpressureStrict}

// allModes lists every mode, for tests that pin what is the same across them.
var allModes = []FanOutBackpressure{BackpressureBuffered, BackpressureBalanced, BackpressureStrict}

// twoPoolFanOut is the fixture most hold tests share: read -> f (roomy, in the given mode) with pools p1 and p2 (limit
// 1 each) -> commit. Item 1 blocks one or both pools; a younger item that schedules on a blocked pool enters (entry
// ignores pool capacity) and keeps its read slot until its work starts.
type twoPoolFanOut struct {
	c              Conveyor
	read, commit   Stage
	f              FanOut
	p1, p2         Pool
	bt             *blockingTasks
	processorGates map[int64]chan struct{}
}

func newTwoPoolFanOut(mode FanOutBackpressure, items int64) *twoPoolFanOut {
	x := &twoPoolFanOut{c: NewConveyor(), bt: newBlockingTasks(), processorGates: map[int64]chan struct{}{}}
	x.read = x.c.AddStage(OptName("read"))
	x.f = x.c.AddFanOut(OptName("f")).SetLimit(100).SetBackpressure(mode)
	x.p1 = x.f.AddPool(OptName("p1"))
	x.p2 = x.f.AddPool(OptName("p2"))
	x.commit = x.c.AddStage(OptName("commit")).SetLimit(10)
	for no := int64(1); no <= items; no++ {
		x.bt.gate(fmt.Sprintf("%d/p1", no))
		x.bt.gate(fmt.Sprintf("%d/p2", no))
		x.processorGates[no] = make(chan struct{})
	}
	return x
}

// enterAndBlock is item 1's part: enter f, block the given pools, then leave (the leave waits for the tasks).
func (x *twoPoolFanOut) enterAndBlock(ctx context.Context, pools ...Pool) error {
	return x.enterAndBlockWith(ctx, false, pools...)
}

// enterAndBlockWith is enterAndBlock with the option to retain the blocked work first, so item 1 reaches commit (and
// publishes its rank for the items behind) while the pools stay busy.
func (x *twoPoolFanOut) enterAndBlockWith(ctx context.Context, retain bool, pools ...Pool) error {
	if err := x.read.MoveTo(ctx); err != nil {
		return err
	}
	if err := x.f.MoveTo(ctx); err != nil {
		return err
	}
	tasks := make([]Task, 0, len(pools))
	for _, p := range pools {
		tasks = append(tasks, x.bt.task(p, 1))
	}
	if err := x.f.Schedule(ctx, tasks...); err != nil {
		return err
	}
	if !retain {
		return x.commit.MoveTo(ctx)
	}
	w := x.f.Retain(ctx)
	if err := x.commit.MoveTo(ctx); err != nil {
		return err
	}
	return w.Wait(ctx)
}

// itemReturned reports whether item no's processor has returned (white-box: completion waits on the plain cond, so
// parkedOf cannot see it).
func itemReturned(c Conveyor, no int64) bool {
	r := implOf(c).currentRun.Load()
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, scope := range r.scopes {
		for it := scope.head; it != nil; it = it.next {
			if it.no == no {
				return it.returned
			}
		}
	}
	return false
}

// TestBackpressure_SameItemCeilingWhenEveryPoolIsBusy: entry is by item slot and turn under every mode. With the one
// pool full, item 2 enters (its work queues) and item 3 waits in read for an item slot — not for the pool. Under
// Buffered item 2 releases read on entry; under Balanced and Strict it keeps read until its task starts.
func TestBackpressure_SameItemCeilingWhenEveryPoolIsBusy(t *testing.T) {
	for _, mode := range allModes {
		t.Run(mode.String(), func(t *testing.T) {
			c := NewConveyor()
			read := c.AddStage(OptName("read")).SetLimit(3)
			f := c.AddFanOut(OptName("f")).SetLimit(2).SetBackpressure(mode)
			p := f.AddPool(OptName("p"))
			commit := c.AddStage(OptName("commit")).SetLimit(10)
			bt := newBlockingTasks()
			for _, k := range []string{"1/p", "2/p", "3/p"} {
				bt.gate(k)
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				waitFor(t, "item 1 started", func() bool { return bt.hasStarted("1/p") })
				waitFor(t, "item 2 inside f", func() bool { return slices.Contains(inBodyOf(c, f), 2) })
				waitFor(t, "item 2's task queued", func() bool { return queueOccupancy(c, p) == 1 })
				waitFor(t, "item 3 in read", func() bool { return slices.Contains(inBodyOf(c, read), 3) })
				waitFor(t, "item 3 parked", func() bool { return parkedOf(c) >= 2 }) // item 1's leave and item 3's move
				if got := inBodyOf(c, f); !slices.Equal(got, []int64{1, 2}) {
					t.Errorf("f = %v, want [1 2]: item 3 is kept out by the item limit", got)
				}
				if mode == BackpressureBuffered {
					if got := inBodyOf(c, read); !slices.Equal(got, []int64{3}) {
						t.Errorf("read = %v, want [3]: Buffered releases read on entry", got)
					}
					if held := heldItems(c); len(held) != 0 {
						t.Errorf("items holding upstream = %v under Buffered, want none", held)
					}
				} else {
					if got := inBodyOf(c, read); !slices.Equal(got, []int64{2, 3}) {
						t.Errorf("read = %v, want [2 3]: item 2 holds read while its task waits for p", got)
					}
					if held := heldItems(c); !slices.Equal(held, []int64{2}) {
						t.Errorf("items holding upstream = %v, want [2]", held)
					}
				}
				close(bt.gate("1/p")) // item 1 leaves: item 2's task starts (hold ends), item 3 gets the item slot
				waitFor(t, "item 2 started", func() bool { return bt.hasStarted("2/p") })
				waitFor(t, "item 3 inside f", func() bool { return slices.Contains(inBodyOf(c, f), 3) })
				close(bt.gate("2/p")) // item 3's task starts: its hold (if any) ends too
				waitFor(t, "item 3 started", func() bool { return bt.hasStarted("3/p") })
				waitFor(t, "read empty", func() bool { return len(inBodyOf(c, read)) == 0 })
				bt.releaseAll()
			}()

			runNOK(t, c, 3, func(ctx context.Context, no int64) error {
				if err := read.MoveTo(ctx); err != nil {
					return err
				}
				if err := f.MoveTo(ctx); err != nil {
					return err
				}
				if err := f.Schedule(ctx, bt.task(p, no)); err != nil {
					return err
				}
				return commit.MoveTo(ctx)
			})
			<-done
		})
	}
}

// TestBackpressure_OneFastOneBusyPoolTable replays the mode table of the design: pool A is free, pool B is busy (item 1
// blocks it). Whether item 2 releases read before B frees depends on the mode and on which pools its first Schedule
// touches. The "B only" rows also pin that an unrelated idle pool (A) does not satisfy Balanced or Strict.
func TestBackpressure_OneFastOneBusyPoolTable(t *testing.T) {
	type row struct {
		work     string
		useA     bool
		useB     bool
		released map[FanOutBackpressure]bool // read released before B frees?
	}
	rows := []row{
		{"A and B", true, true, map[FanOutBackpressure]bool{BackpressureBuffered: true, BackpressureBalanced: true, BackpressureStrict: false}},
		{"B only", false, true, map[FanOutBackpressure]bool{BackpressureBuffered: true, BackpressureBalanced: false, BackpressureStrict: false}},
		{"A only", true, false, map[FanOutBackpressure]bool{BackpressureBuffered: true, BackpressureBalanced: true, BackpressureStrict: true}},
	}
	for _, mode := range allModes {
		for _, tc := range rows {
			t.Run(mode.String()+"/"+tc.work, func(t *testing.T) {
				c := NewConveyor()
				read := c.AddStage(OptName("read"))
				f := c.AddFanOut(OptName("f")).SetLimit(100).SetBackpressure(mode)
				a := f.AddPool(OptName("A"))
				b := f.AddPool(OptName("B"))
				commit := c.AddStage(OptName("commit")).SetLimit(10)
				bt := newBlockingTasks()
				for _, k := range []string{"1/B", "2/A", "2/B"} {
					bt.gate(k)
				}

				done := make(chan struct{})
				go func() {
					defer close(done)
					waitFor(t, "item 1 blocks B", func() bool { return bt.hasStarted("1/B") })
					waitFor(t, "item 2 inside f", func() bool { return slices.Contains(inBodyOf(c, f), 2) })
					if tc.useA {
						waitFor(t, "item 2 started on A", func() bool { return bt.hasStarted("2/A") })
					}
					if tc.useB {
						waitFor(t, "item 2 queued on B", func() bool { return queueOccupancy(c, b) == 1 })
					}
					if tc.released[mode] {
						waitFor(t, "read released", func() bool { return len(inBodyOf(c, read)) == 0 })
					} else {
						// Nothing but B freeing can end the hold from here, so the state is stable.
						if got := inBodyOf(c, read); !slices.Equal(got, []int64{2}) {
							t.Errorf("read = %v, want [2] (held until B starts)", got)
						}
						if held := heldItems(c); !slices.Equal(held, []int64{2}) {
							t.Errorf("items holding upstream = %v, want [2]", held)
						}
						close(bt.gate("1/B"))
						waitFor(t, "item 2 started on B", func() bool { return bt.hasStarted("2/B") })
						waitFor(t, "read released", func() bool { return len(inBodyOf(c, read)) == 0 })
					}
					if held := heldItems(c); len(held) != 0 {
						t.Errorf("items holding upstream = %v, want none", held)
					}
					bt.releaseAll()
				}()

				runNOK(t, c, 2, func(ctx context.Context, no int64) error {
					if err := read.MoveTo(ctx); err != nil {
						return err
					}
					if err := f.MoveTo(ctx); err != nil {
						return err
					}
					var tasks []Task
					if no == 1 {
						tasks = []Task{bt.task(b, no)}
					} else {
						if tc.useA {
							tasks = append(tasks, bt.task(a, no))
						}
						if tc.useB {
							tasks = append(tasks, bt.task(b, no))
						}
					}
					if err := f.Schedule(ctx, tasks...); err != nil {
						return err
					}
					return commit.MoveTo(ctx)
				})
				<-done
			})
		}
	}
}

// TestBackpressureStrict_WalkThrough: an item whose work waits for a saturated pool keeps its read slot, a younger
// item whose work can start passes it on the branches, and the stage before the fan-out fills with stuck items until
// the saturated pool frees a slot. Downstream order is unchanged.
func TestBackpressureStrict_WalkThrough(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read")).SetLimit(2)
	f := c.AddFanOut(OptName("dbsWrite")).SetLimit(100).SetBackpressure(BackpressureStrict)
	db1 := f.AddPool(OptName("db1")).SetLimit(4)
	db2 := f.AddPool(OptName("db2")).SetLimit(1)
	commit := c.AddStage(OptName("commit")) // exclusive, so the recorded commit order is the admission order
	bt := newBlockingTasks()
	var commitOrder numbers

	// Pre-create every gate so the driver can wait on and release them without racing the tasks.
	for _, no := range []int64{1, 2, 3, 4, 5} {
		bt.gate(fmt.Sprintf("%d/db1", no))
		bt.gate(fmt.Sprintf("%d/db2", no))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// 1. Item 1 starts on both pools and releases read.
		waitFor(t, "item 1 started", func() bool { return bt.hasStarted("1/db1") && bt.hasStarted("1/db2") })
		waitFor(t, "item 1 released read", func() bool { return !slices.Contains(inBodyOf(c, read), 1) })
		// 2. Item 2's db2 task queues; item 2 keeps read.
		waitFor(t, "item 2 started on db1", func() bool { return bt.hasStarted("2/db1") })
		// 3. Item 3 needs only db1: it passes item 2 on the branches and releases read.
		waitFor(t, "item 3 started on db1", func() bool { return bt.hasStarted("3/db1") })
		waitFor(t, "item 3 released read", func() bool { return !slices.Contains(inBodyOf(c, read), 3) })
		// Item 3's work is done from here: the only thing keeping it out of commit is item 2 at the ordering gate.
		close(bt.gate("3/db1"))
		waitFor(t, "item 3's task to finish", func() bool { return occupancyOf(c, db1) == 2 })
		if got := inBodyOf(c, read); !slices.Contains(got, 2) {
			t.Errorf("item 2 should keep its read slot while its db2 task is queued; read = %v", got)
		}
		// 4. Item 4 needs db2: it enters, queues behind item 2, keeps read. read is full now.
		waitFor(t, "item 4 admitted", func() bool { return slices.Contains(inBodyOf(c, f), 4) })
		// 5. Item 5 blocks in read.MoveTo, on the start stage.
		waitFor(t, "item 5 waits for read", func() bool { return slices.Contains(inBodyOf(c, c.StartingStage()), 5) })
		if got := inBodyOf(c, read); !slices.Equal(got, []int64{2, 4}) {
			t.Errorf("read should hold the two stuck items; read = %v", got)
		}
		if got := inBodyOf(c, f); !slices.Equal(got, []int64{1, 2, 3, 4}) {
			t.Errorf("fan-out should hold items 1-4; got %v", got)
		}
		if got := inBodyOf(c, commit); len(got) != 0 {
			t.Errorf("commit = %v, want empty: item 3's work is done but it waits at commit's gate for item 2", got)
		}
		// 6. Item 1's db2 task finishes: the slot goes to item 2, item 2 releases read, item 5 gets in. Item 5 may
		// pass read and start on db1 before a poll sees it in read, so its task is what is observed.
		close(bt.gate("1/db2"))
		waitFor(t, "item 2 started on db2", func() bool { return bt.hasStarted("2/db2") })
		waitFor(t, "item 2 released read", func() bool { return !slices.Contains(inBodyOf(c, read), 2) })
		waitFor(t, "item 5 passed read", func() bool { return bt.hasStarted("5/db1") })
		bt.releaseAll()
	}()

	runNOK(t, c, 5, func(ctx context.Context, no int64) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := f.MoveTo(ctx); err != nil {
			return err
		}
		var tasks []Task
		switch no {
		case 1, 2:
			tasks = []Task{bt.task(db1, no), bt.task(db2, no)}
		case 3, 5:
			tasks = []Task{bt.task(db1, no)}
		case 4:
			tasks = []Task{bt.task(db2, no)}
		}
		if err := f.Schedule(ctx, tasks...); err != nil {
			return err
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		commitOrder.add(no)
		return nil
	})
	<-done
	assertStrictlyIncreasing(t, commitOrder.all(), "commit order under Strict backpressure")
}

// TestBackpressureStrict_HoldEndsAtFirstStartPerBranch: the hold ends when every touched branch has had one start, not
// when the batch is dispatched. Item 2 enters with NewTasks(100) on p (limit 2) and one task on q, where item 1's
// task blocks: the hold survives p's first two starts and ends at q's first, with 98 tasks still queued on p.
func TestBackpressureStrict_HoldEndsAtFirstStartPerBranch(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetBackpressure(BackpressureStrict)
	p := f.AddPool(OptName("p")).SetLimit(2)
	q := f.AddPool(OptName("q"))
	commit := c.AddStage(OptName("commit")).SetLimit(10)
	bt := newBlockingTasks()
	bt.gate("1/q")
	bt.gate("2/q")
	release := make(chan struct{})
	var started atomic.Int64

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "two p tasks to start", func() bool { return started.Load() == 2 })
		if got := inBodyOf(c, read); !slices.Equal(got, []int64{2}) {
			t.Errorf("read = %v, want [2]: q has not started, so the hold must survive p's starts", got)
		}
		if held := heldItems(c); !slices.Equal(held, []int64{2}) {
			t.Errorf("items holding upstream = %v, want [2]", held)
		}
		close(bt.gate("1/q")) // q frees: item 2's q task is the first start on its last awaited branch
		waitFor(t, "item 2 started on q", func() bool { return bt.hasStarted("2/q") })
		waitFor(t, "read released", func() bool { return len(inBodyOf(c, read)) == 0 })
		if held := heldItems(c); len(held) != 0 {
			t.Errorf("items holding upstream = %v, want none", held)
		}
		if got := queueOccupancy(c, p); got != 1 {
			t.Errorf("p backlog = %d, want 1 (98 tasks still queued in one collection)", got)
		}
		if got := started.Load(); got != 2 {
			t.Errorf("p tasks started = %d, want 2: the hold ended before the batch was dispatched", got)
		}
		close(release)
		bt.releaseAll()
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := f.MoveTo(ctx); err != nil {
			return err
		}
		tasks := []Task{bt.task(q, no)}
		if no == 2 {
			tasks = append(tasks, p.NewTasks(100, func(context.Context, int) error {
				started.Add(1)
				<-release
				return nil
			}))
		}
		if err := f.Schedule(ctx, tasks...); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	<-done
}

// TestBackpressureBalanced_HoldEndsAtFirstStartAnywhere: the same setup under Balanced. Item 2's first start on p ends
// the hold while its q task is still queued behind item 1.
func TestBackpressureBalanced_HoldEndsAtFirstStartAnywhere(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetBackpressure(BackpressureBalanced)
	p := f.AddPool(OptName("p"))
	q := f.AddPool(OptName("q"))
	commit := c.AddStage(OptName("commit")).SetLimit(10)
	bt := newBlockingTasks()
	for _, k := range []string{"1/q", "2/p", "2/q"} {
		bt.gate(k)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2 started on p", func() bool { return bt.hasStarted("2/p") })
		waitFor(t, "read released", func() bool { return len(inBodyOf(c, read)) == 0 })
		if held := heldItems(c); len(held) != 0 {
			t.Errorf("items holding upstream = %v, want none after the first start", held)
		}
		if got := queueOccupancy(c, q); got != 1 {
			t.Errorf("q backlog = %d, want 1 (item 2's q task still waits for item 1)", got)
		}
		bt.releaseAll()
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := f.MoveTo(ctx); err != nil {
			return err
		}
		tasks := []Task{bt.task(q, no)}
		if no == 2 {
			tasks = append(tasks, bt.task(p, no))
		}
		if err := f.Schedule(ctx, tasks...); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	<-done
}

// TestBackpressure_EmptyEnteringSubmissionDischarges: a first Schedule with no tasks, or only statically empty ones,
// releases the previous node at once while the item stays inside.
func TestBackpressure_EmptyEnteringSubmissionDischarges(t *testing.T) {
	for _, mode := range holdingModes {
		for _, tc := range []struct {
			name  string
			tasks func(p Pool) []Task
		}{
			{"no tasks", func(Pool) []Task { return nil }},
			{"statically empty", func(p Pool) []Task {
				return []Task{p.NewTasks(0, func(context.Context, int) error { return nil }), p.NewTasksGen(nil)}
			}},
		} {
			t.Run(mode.String()+"/"+tc.name, func(t *testing.T) {
				c := NewConveyor()
				read := c.AddStage(OptName("read"))
				f := c.AddFanOut(OptName("f")).SetBackpressure(mode)
				p := f.AddPool(OptName("p"))
				commit := c.AddStage(OptName("commit"))
				release := make(chan struct{})

				done := make(chan struct{})
				go func() {
					defer close(done)
					waitFor(t, "item 1 inside f", func() bool { return slices.Contains(inBodyOf(c, f), 1) })
					waitFor(t, "read released", func() bool { return len(inBodyOf(c, read)) == 0 })
					if held := heldItems(c); len(held) != 0 {
						t.Errorf("items holding upstream = %v, want none", held)
					}
					close(release)
				}()

				runNOK(t, c, 1, func(ctx context.Context, no int64) error {
					if err := read.MoveTo(ctx); err != nil {
						return err
					}
					if err := f.MoveTo(ctx); err != nil {
						return err
					}
					if err := f.Schedule(ctx, tc.tasks(p)...); err != nil {
						return err
					}
					<-release
					return commit.MoveTo(ctx)
				})
				<-done
			})
		}
	}
}

// TestBackpressure_ExhaustionWithoutStartIsNotAStart: item 2's first Schedule touches p (blocked by item 1) and q with
// a streaming source that ends without a task. q running out is not a start: under both modes the hold stays until
// p starts. Once q has run out and p starts, both milestones are met.
func TestBackpressure_ExhaustionWithoutStartIsNotAStart(t *testing.T) {
	for _, mode := range holdingModes {
		t.Run(mode.String(), func(t *testing.T) {
			c := NewConveyor()
			read := c.AddStage(OptName("read"))
			f := c.AddFanOut(OptName("f")).SetLimit(100).SetBackpressure(mode)
			p := f.AddPool(OptName("p"))
			q := f.AddPool(OptName("q"))
			commit := c.AddStage(OptName("commit")).SetLimit(10)
			bt := newBlockingTasks()
			bt.gate("1/p")
			bt.gate("2/p")
			feed := make(chan TaskFunc) // item 2's q source: closed empty by the driver

			done := make(chan struct{})
			go func() {
				defer close(done)
				waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
				waitFor(t, "the q pull to reserve a slot", func() bool { return occupancyOf(c, q) == 1 })
				close(feed) // q proves empty: its branch is settled without a start
				waitFor(t, "q released", func() bool { return occupancyOf(c, q) == 0 && queueOccupancy(c, q) == 0 })
				if got := inBodyOf(c, read); !slices.Equal(got, []int64{2}) {
					t.Errorf("read = %v after q ran out, want [2] (p has not started)", got)
				}
				if held := heldItems(c); !slices.Equal(held, []int64{2}) {
					t.Errorf("items holding upstream = %v, want [2]", held)
				}
				close(bt.gate("1/p"))
				waitFor(t, "item 2 started on p", func() bool { return bt.hasStarted("2/p") })
				waitFor(t, "read released", func() bool { return len(inBodyOf(c, read)) == 0 })
				bt.releaseAll()
			}()

			runNOK(t, c, 2, func(ctx context.Context, no int64) error {
				if err := read.MoveTo(ctx); err != nil {
					return err
				}
				if err := f.MoveTo(ctx); err != nil {
					return err
				}
				tasks := []Task{bt.task(p, no)}
				if no == 2 {
					tasks = append(tasks, q.NewTasksChan(feed))
				}
				if err := f.Schedule(ctx, tasks...); err != nil {
					return err
				}
				return commit.MoveTo(ctx)
			})
			<-done
		})
	}
}

// TestBackpressure_EveryBranchRunsOutDischarges: a first Schedule whose only source is a stream that ends without a
// task releases the previous node once it has run out — the batch proved empty — under both modes.
func TestBackpressure_EveryBranchRunsOutDischarges(t *testing.T) {
	for _, mode := range holdingModes {
		t.Run(mode.String(), func(t *testing.T) {
			c := NewConveyor()
			read := c.AddStage(OptName("read"))
			f := c.AddFanOut(OptName("f")).SetBackpressure(mode)
			p := f.AddPool(OptName("p"))
			commit := c.AddStage(OptName("commit"))
			feed := make(chan TaskFunc)

			done := make(chan struct{})
			go func() {
				defer close(done)
				waitFor(t, "item 1 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{1}) })
				waitFor(t, "the pull to reserve a slot", func() bool { return occupancyOf(c, p) == 1 })
				if got := inBodyOf(c, read); !slices.Equal(got, []int64{1}) {
					t.Errorf("read = %v while the pull is in flight, want [1] (a pull is not a start)", got)
				}
				close(feed)
				waitFor(t, "read released", func() bool { return len(inBodyOf(c, read)) == 0 })
				if held := heldItems(c); len(held) != 0 {
					t.Errorf("items holding upstream = %v, want none", held)
				}
			}()

			runNOK(t, c, 1, func(ctx context.Context, no int64) error {
				if err := read.MoveTo(ctx); err != nil {
					return err
				}
				if err := f.MoveTo(ctx); err != nil {
					return err
				}
				if err := f.Schedule(ctx, p.NewTasksChan(feed)); err != nil {
					return err
				}
				return commit.MoveTo(ctx)
			})
			<-done
		})
	}
}

// TestBackpressure_LaterRoundsAndFollowUpsNeverHold: only the first Schedule holds upstream. A second round queued
// behind the pool, and a follow-up a task schedules, leave the previous node released.
func TestBackpressure_LaterRoundsAndFollowUpsNeverHold(t *testing.T) {
	for _, mode := range holdingModes {
		t.Run(mode.String(), func(t *testing.T) {
			c := NewConveyor()
			read := c.AddStage(OptName("read"))
			f := c.AddFanOut(OptName("f")).SetBackpressure(mode)
			p := f.AddPool(OptName("p"))
			commit := c.AddStage(OptName("commit"))
			release := make(chan struct{})
			roundTwo := make(chan struct{})
			var followUpQueued atomic.Bool

			done := make(chan struct{})
			go func() {
				defer close(done)
				waitFor(t, "the follow-up to be queued", func() bool { return followUpQueued.Load() })
				waitFor(t, "round two to be queued", func() bool { return queueOccupancy(c, p) == 2 })
				if got := inBodyOf(c, read); len(got) != 0 {
					t.Errorf("read = %v with two collections queued, want empty (the hold ended at the first start)", got)
				}
				if held := heldItems(c); len(held) != 0 {
					t.Errorf("items holding upstream = %v, want none", held)
				}
				close(release)
			}()

			runNOK(t, c, 1, func(ctx context.Context, no int64) error {
				if err := read.MoveTo(ctx); err != nil {
					return err
				}
				if err := f.MoveTo(ctx); err != nil {
					return err
				}
				err := f.Schedule(ctx, p.NewTask(func(tctx context.Context) error {
					// A follow-up from the running task: queued behind this one on the limit-1 pool.
					if err := f.Schedule(tctx, p.NewTask(func(context.Context) error { return nil })); err != nil {
						return err
					}
					followUpQueued.Store(true)
					close(roundTwo)
					<-release
					return nil
				}))
				if err != nil {
					return err
				}
				<-roundTwo
				if err := f.Schedule(ctx, p.NewTask(func(context.Context) error { return nil })); err != nil {
					return err
				}
				return commit.MoveTo(ctx)
			})
			<-done
		})
	}
}

// TestBackpressureStrict_UnrelatedRetainCompletionKeepsTheHeldToken: the sweep a finishing Retain triggers frees the
// retained stage but leaves the token the hold protects.
func TestBackpressureStrict_UnrelatedRetainCompletionKeepsTheHeldToken(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	read := c.AddStage(OptName("read"))
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetBackpressure(BackpressureStrict)
	p1 := f.AddPool(OptName("p1"))
	commit := c.AddStage(OptName("commit")).SetLimit(10)
	bt := newBlockingTasks()
	bt.gate("1/p1")
	bt.gate("2/p1")
	bgDone := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
		if got := inBodyOf(c, a); !slices.Equal(got, []int64{2}) {
			t.Errorf("a = %v, want [2] (the Retain holds it)", got)
		}
		close(bgDone) // the Retain finishes: its sweep must free a but not read
		waitFor(t, "a released", func() bool { return len(inBodyOf(c, a)) == 0 })
		if got := inBodyOf(c, read); !slices.Equal(got, []int64{2}) {
			t.Errorf("read = %v after the unrelated Retain finished, want [2] (still held)", got)
		}
		close(bt.gate("1/p1"))
		waitFor(t, "item 2's task to start", func() bool { return bt.hasStarted("2/p1") })
		waitFor(t, "read released", func() bool { return len(inBodyOf(c, read)) == 0 })
		bt.releaseAll()
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := a.MoveTo(ctx); err != nil {
			return err
		}
		var w Wave
		if no == 2 {
			w = a.Retain(ctx, func() error { <-bgDone; return nil })
		}
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := f.MoveTo(ctx); err != nil {
			return err
		}
		if err := f.Schedule(ctx, bt.task(p1, no)); err != nil { // p1 saturated by item 1
			return err
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		if w != nil {
			return w.Wait(ctx)
		}
		return nil
	})
	<-done
}

// TestBackpressureStrict_RetainOnPreviousStagePlusHold: a Retain on the stage the hold protects. The slot is freed
// only when both have ended, in either order.
func TestBackpressureStrict_RetainOnPreviousStagePlusHold(t *testing.T) {
	for _, retainFirst := range []bool{true, false} {
		name := "hold ends first"
		if retainFirst {
			name = "retain ends first"
		}
		t.Run(name, func(t *testing.T) {
			x := newTwoPoolFanOut(BackpressureStrict, 2)
			c := x.c
			bgDone := make(chan struct{})
			waveCh := make(chan Wave, 1)

			done := make(chan struct{})
			go func() {
				defer close(done)
				waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
				if retainFirst {
					close(bgDone)
					<-(<-waveCh).Finished() // the finishing sweep has run by the time the channel closes
					if got := inBodyOf(c, x.read); !slices.Equal(got, []int64{2}) {
						t.Errorf("read = %v after the Retain ended, want [2] (the hold still protects it)", got)
					}
					close(x.bt.gate("1/p1"))
					waitFor(t, "read released", func() bool { return len(inBodyOf(c, x.read)) == 0 })
				} else {
					close(x.bt.gate("1/p1"))
					waitFor(t, "the hold to end", func() bool { return len(heldItems(c)) == 0 })
					if got := inBodyOf(c, x.read); !slices.Equal(got, []int64{2}) {
						t.Errorf("read = %v after the hold ended, want [2] (the Retain still holds it)", got)
					}
					close(bgDone)
					waitFor(t, "read released", func() bool { return len(inBodyOf(c, x.read)) == 0 })
				}
				x.bt.releaseAll()
			}()

			runNOK(t, c, 2, func(ctx context.Context, no int64) error {
				if no == 1 {
					return x.enterAndBlock(ctx, x.p1)
				}
				if err := x.read.MoveTo(ctx); err != nil {
					return err
				}
				w := x.read.Retain(ctx, func() error { <-bgDone; return nil })
				waveCh <- w
				if err := x.f.MoveTo(ctx); err != nil {
					return err
				}
				if err := x.f.Schedule(ctx, x.bt.task(x.p1, no)); err != nil {
					return err
				}
				if err := x.commit.MoveTo(ctx); err != nil {
					return err
				}
				return w.Wait(ctx)
			})
			<-done
		})
	}
}

// TestBackpressure_WaitingRoomTokenIsHeld: an item admitted from the fan-out's waiting room keeps its queued token
// (the previous node was released when it stepped aside) until its work starts. Item 1 retains a blocked p1 task and
// moves on, so its body keeps one of f's two slots while p1 stays busy; item 2 takes the other slot and later leaves;
// item 3 waits in the room, is admitted when item 2 leaves, and queues on p1 behind item 1's task.
func TestBackpressure_WaitingRoomTokenIsHeld(t *testing.T) {
	for _, mode := range holdingModes {
		t.Run(mode.String(), func(t *testing.T) {
			x := newTwoPoolFanOut(mode, 3)
			c := x.c
			x.f.SetLimit(2).SetQueueSize(1)

			done := make(chan struct{})
			go func() {
				defer close(done)
				waitFor(t, "item 3 in the waiting room", func() bool { return slices.Equal(inQueueOf(c, x.f), []int64{3}) })
				if got := inBodyOf(c, x.read); len(got) != 0 {
					t.Errorf("read = %v while item 3 waits in the room, want empty", got)
				}
				close(x.processorGates[2]) // item 2 leaves f: item 3 is admitted from the room
				waitFor(t, "item 3 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{3}) })
				waitFor(t, "item 3's task queued on p1", func() bool { return queueOccupancy(c, x.p1) == 1 })
				if got := inQueueOf(c, x.f); !slices.Equal(got, []int64{3}) {
					t.Errorf("f waiting room = %v after admission, want [3] (the queued token is held)", got)
				}
				if q := queueOccupancy(c, x.f); q != 1 {
					t.Errorf("f queued = %d, want 1", q)
				}
				close(x.bt.gate("1/p1"))
				waitFor(t, "the queued token released", func() bool { return queueOccupancy(c, x.f) == 0 })
				x.bt.releaseAll()
			}()

			runNOK(t, c, 3, func(ctx context.Context, no int64) error {
				if err := x.read.MoveTo(ctx); err != nil {
					return err
				}
				if err := x.f.MoveTo(ctx); err != nil {
					return err
				}
				var tasks []Task
				if no != 2 {
					tasks = []Task{x.bt.task(x.p1, no)}
				}
				if err := x.f.Schedule(ctx, tasks...); err != nil {
					return err
				}
				var w Wave
				switch no {
				case 1:
					w = x.f.Retain(ctx)
				case 2:
					<-x.processorGates[2]
				}
				if err := x.commit.MoveTo(ctx); err != nil {
					return err
				}
				if w != nil {
					return w.Wait(ctx)
				}
				return nil
			})
			<-done
		})
	}
}

// TestBackpressure_StartStageTokenThrottlesItemCreation: entering from the start stage holds the start slot, so no
// new item is created until the held item's work starts.
func TestBackpressure_StartStageTokenThrottlesItemCreation(t *testing.T) {
	for _, mode := range holdingModes {
		t.Run(mode.String(), func(t *testing.T) {
			c := NewConveyor()
			f := c.AddFanOut(OptName("f")).SetLimit(100).SetBackpressure(mode)
			p1 := f.AddPool(OptName("p1"))
			commit := c.AddStage(OptName("commit")).SetLimit(10)
			bt := newBlockingTasks()
			for _, k := range []string{"1/p1", "2/p1", "3/p1"} {
				bt.gate(k)
			}
			start := c.StartingStage()

			done := make(chan struct{})
			go func() {
				defer close(done)
				waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
				waitFor(t, "item 2's task queued", func() bool { return queueOccupancy(c, p1) == 1 })
				if got := inBodyOf(c, start); !slices.Equal(got, []int64{2}) {
					t.Errorf("start = %v while item 2 holds it, want [2]", got)
				}
				if s := c.Stats(); s.InFlight.Last != 2 {
					t.Errorf("InFlight = %d while the start slot is held, want 2 (no item 3 yet)", s.InFlight.Last)
				}
				close(bt.gate("1/p1"))
				waitFor(t, "item 3 created", func() bool { return slices.Contains(inBodyOf(c, start), 3) })
				bt.releaseAll()
			}()

			runNOK(t, c, 3, func(ctx context.Context, no int64) error {
				if err := f.MoveTo(ctx); err != nil {
					return err
				}
				if err := f.Schedule(ctx, bt.task(p1, no)); err != nil {
					return err
				}
				return commit.MoveTo(ctx)
			})
			<-done
		})
	}
}

// TestBackpressureStrict_CancellationDropsEnteringCollectionAndDischarges: a canceled item's queued entering
// collection is dropped when the pool next looks at its queue, which ends the hold while the processor is still
// inside. After the run nothing is left occupied.
func TestBackpressureStrict_CancellationDropsEnteringCollectionAndDischarges(t *testing.T) {
	x := newTwoPoolFanOut(BackpressureStrict, 2)
	c := x.c
	boom := errors.New("boom")
	var rn *run

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
		// The hold exists from admission; the cancel must come after the Schedule, or the Schedule itself fails.
		waitFor(t, "item 2's collection queued", func() bool { return queueOccupancy(c, x.p1) == 1 })
		rn = implOf(c).currentRun.Load()
		// Cancel item 2's own context only: nothing wakes anyone, the collection stays queued.
		rn.mu.Lock()
		for it := rn.scopes[0].head; it != nil; it = it.next {
			if it.no == 2 {
				it.cancel(boom)
			}
		}
		rn.mu.Unlock()
		close(x.bt.gate("1/p1")) // the freed slot looks at the queue: item 2's collection is dropped
		waitFor(t, "the hold to end", func() bool { return len(heldItems(c)) == 0 })
		if got := inBodyOf(c, x.read); len(got) != 0 {
			t.Errorf("read = %v after the drop, want empty", got)
		}
		if got := inBodyOf(c, x.f); !slices.Contains(got, 2) {
			t.Errorf("f = %v, want item 2 still inside (its processor has not returned)", got)
		}
		close(x.processorGates[2])
		x.bt.releaseAll()
	}()

	err := runUntil(t, c, 2, func(ctx context.Context, no int64) error {
		if no == 1 {
			return x.enterAndBlock(ctx, x.p1)
		}
		if err := x.read.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.Schedule(ctx, x.bt.task(x.p1, no)); err != nil {
			return err
		}
		<-x.processorGates[2]
		return x.commit.MoveTo(ctx)
	})
	<-done
	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want %v", err, boom)
	}
	rn.mu.Lock()
	defer rn.mu.Unlock()
	for j, u := range rn.conveyor.units {
		if occ := rn.occupancy[j].val; occ != 0 {
			t.Errorf("%s still holds %d after the run", u, occ)
		}
		if q := rn.queued[j].val; q != 0 {
			t.Errorf("%s still has %d queued after the run", u, q)
		}
	}
}

// TestBackpressureStrict_ProcessorReturnWithNoSubmissionDischarges: a processor that returns without any Schedule
// has its body sealed by completion as an empty batch, which ends the hold — and wakes the item parked on the freed
// slot. Item 3 is that waiter: it must get into read on item 2's return alone, with nothing else in the run able to
// broadcast (item 1 blocked in its leave).
func TestBackpressureStrict_ProcessorReturnWithNoSubmissionDischarges(t *testing.T) {
	x := newTwoPoolFanOut(BackpressureStrict, 3)
	c := x.c
	var passed3 atomic.Bool

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
		waitFor(t, "item 3 to wait for read", func() bool { return slices.Contains(inBodyOf(c, c.StartingStage()), 3) })
		waitFor(t, "item 1's leave and item 3's move to park", func() bool { return parkedOf(c) == 2 })
		// From here the only broadcast left in the run is the one item 2's completion must make.
		close(x.processorGates[2])
		waitFor(t, "item 3 to pass read", passed3.Load)
		waitFor(t, "the hold to end", func() bool { return len(heldItems(c)) == 0 })
		x.bt.releaseAll()
	}()

	runNOK(t, c, 3, func(ctx context.Context, no int64) error {
		switch no {
		case 1:
			return x.enterAndBlock(ctx, x.p1)
		case 3:
			if err := x.read.MoveTo(ctx); err != nil {
				return err
			}
			passed3.Store(true)
			return nil
		}
		if err := x.read.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.MoveTo(ctx); err != nil {
			return err
		}
		<-x.processorGates[2]
		return nil
	})
	<-done
}

// TestBackpressureStrict_ProcessorReturnKeepsPendingBatchHold: a processor that returns with its entering work still
// queued keeps the hold: sealing does not bypass the backpressure. Item 3 stays out of read until item 2's task
// starts; item 2 itself stays inside f waiting for that work.
func TestBackpressureStrict_ProcessorReturnKeepsPendingBatchHold(t *testing.T) {
	x := newTwoPoolFanOut(BackpressureStrict, 3)
	c := x.c
	var passed3 atomic.Bool

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
		waitFor(t, "item 2's collection queued", func() bool { return queueOccupancy(c, x.p1) == 1 })
		waitFor(t, "item 3 to wait for read", func() bool { return slices.Contains(inBodyOf(c, c.StartingStage()), 3) })
		close(x.processorGates[2]) // item 2 returns; its body is sealed with the batch still queued
		waitFor(t, "item 2 to return", func() bool { return itemReturned(c, 2) })
		waitFor(t, "item 1's leave and item 3's move to park", func() bool { return parkedOf(c) == 2 })
		if got := heldItems(c); !slices.Equal(got, []int64{2}) {
			t.Errorf("items holding upstream = %v after the return, want [2]", got)
		}
		if got := inBodyOf(c, x.read); !slices.Equal(got, []int64{2}) {
			t.Errorf("read = %v after the return, want [2] (still held)", got)
		}
		if passed3.Load() {
			t.Error("item 3 passed read while item 2's batch was still queued")
		}
		close(x.bt.gate("1/p1")) // item 2's task starts: the hold ends, item 3 gets read
		waitFor(t, "the hold to end", func() bool { return len(heldItems(c)) == 0 })
		waitFor(t, "item 3 to pass read", passed3.Load)
		x.bt.releaseAll()
	}()

	runNOK(t, c, 3, func(ctx context.Context, no int64) error {
		switch no {
		case 1:
			return x.enterAndBlock(ctx, x.p1)
		case 3:
			if err := x.read.MoveTo(ctx); err != nil {
				return err
			}
			passed3.Store(true)
			return nil
		}
		if err := x.read.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.Schedule(ctx, x.bt.task(x.p1, no)); err != nil {
			return err
		}
		<-x.processorGates[2]
		return nil
	})
	<-done
}

// TestBackpressureStrict_RetainThenMoveKeepsTheHold: Retain ends the item's own path into the body but not the hold,
// and neither does the move to commit: read stays held until the retained work starts.
func TestBackpressureStrict_RetainThenMoveKeepsTheHold(t *testing.T) {
	x := newTwoPoolFanOut(BackpressureStrict, 2)
	c := x.c

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
		waitFor(t, "item 2's task queued", func() bool { return queueOccupancy(c, x.p1) == 1 })
		close(x.processorGates[2]) // item 2 retains and moves to commit (item 1 is there already)
		waitFor(t, "item 2 in commit", func() bool { return slices.Contains(inBodyOf(c, x.commit), 2) })
		if got := heldItems(c); !slices.Equal(got, []int64{2}) {
			t.Errorf("items holding upstream = %v after Retain and move, want [2]", got)
		}
		if got := inBodyOf(c, x.read); !slices.Equal(got, []int64{2}) {
			t.Errorf("read = %v after Retain and move, want [2] (still held)", got)
		}
		if got := inBodyOf(c, x.f); !slices.Equal(got, []int64{1, 2}) {
			t.Errorf("f = %v, want [1 2] (the retained body keeps its slot)", got)
		}
		close(x.bt.gate("1/p1")) // item 2's task starts: the hold ends
		waitFor(t, "the hold to end", func() bool { return len(heldItems(c)) == 0 })
		waitFor(t, "read released", func() bool { return len(inBodyOf(c, x.read)) == 0 })
		x.bt.releaseAll()
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if no == 1 {
			return x.enterAndBlockWith(ctx, true, x.p1)
		}
		if err := x.read.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.Schedule(ctx, x.bt.task(x.p1, no)); err != nil {
			return err
		}
		<-x.processorGates[2]
		w := x.f.Retain(ctx)
		if err := x.commit.MoveTo(ctx); err != nil {
			return err
		}
		return w.Wait(ctx)
	})
	<-done
}

// TestBackpressureStrict_TwoRetainedFanOutsHoldTwoSlots: two fan-outs in a row, both retained with their batches
// queued. The first hold keeps read; the second keeps the first fan-out's slot (also kept by the retained body).
// Each ends on its own batch's start, and the first fan-out's slot is freed only once both its body and the hold are
// done.
func TestBackpressureStrict_TwoRetainedFanOutsHoldTwoSlots(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	f1 := c.AddFanOut(OptName("f1")).SetLimit(100).SetBackpressure(BackpressureStrict)
	p := f1.AddPool(OptName("p"))
	f2 := c.AddFanOut(OptName("f2")).SetLimit(100).SetBackpressure(BackpressureStrict)
	q := f2.AddPool(OptName("q"))
	commit := c.AddStage(OptName("commit")).SetLimit(10)
	bt := newBlockingTasks()
	for _, k := range []string{"1/p", "1/q", "2/p", "2/q"} {
		bt.gate(k)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2 in commit with two holds", func() bool {
			return slices.Contains(inBodyOf(c, commit), 2) && holdCount(c, 2) == 2
		})
		if got := inBodyOf(c, read); !slices.Equal(got, []int64{2}) {
			t.Errorf("read = %v, want [2] (held by the f1 admission)", got)
		}
		if got := inBodyOf(c, f1); !slices.Equal(got, []int64{1, 2}) {
			t.Errorf("f1 = %v, want [1 2] (item 2's slot: retained body and the f2 admission's hold)", got)
		}
		close(bt.gate("1/p")) // item 2's p task starts: the first hold ends, read is freed
		waitFor(t, "item 2 started on p", func() bool { return bt.hasStarted("2/p") })
		waitFor(t, "read released", func() bool { return len(inBodyOf(c, read)) == 0 })
		if n := holdCount(c, 2); n != 1 {
			t.Errorf("item 2 holds = %d after the first start, want 1", n)
		}
		close(bt.gate("2/p")) // item 2's f1 body finishes: its slot stays for the second hold
		waitFor(t, "item 1's f1 body done", func() bool { return occupancyOf(c, p) == 0 })
		if got := inBodyOf(c, f1); !slices.Equal(got, []int64{2}) {
			t.Errorf("f1 = %v after item 2's body finished, want [2] (the f2 hold keeps it)", got)
		}
		close(bt.gate("1/q")) // item 2's q task starts: the second hold ends, f1 is freed
		waitFor(t, "item 2 started on q", func() bool { return bt.hasStarted("2/q") })
		waitFor(t, "f1 released", func() bool { return len(inBodyOf(c, f1)) == 0 })
		if n := holdCount(c, 2); n != 0 {
			t.Errorf("item 2 holds = %d, want 0", n)
		}
		bt.releaseAll()
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := f1.MoveTo(ctx); err != nil {
			return err
		}
		if err := f1.Schedule(ctx, bt.task(p, no)); err != nil {
			return err
		}
		w1 := f1.Retain(ctx)
		if err := f2.MoveTo(ctx); err != nil {
			return err
		}
		if err := f2.Schedule(ctx, bt.task(q, no)); err != nil {
			return err
		}
		w2 := f2.Retain(ctx)
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		if err := w1.Wait(ctx); err != nil {
			return err
		}
		return w2.Wait(ctx)
	})
	<-done
}

// TestBackpressure_HeldWaitingRoomTokenSurvivesALaterWaitingRoom: the token an item keeps in f's waiting room is
// owned by the hold, so stepping into another node's waiting room later (which gives back any queued slot the item
// stands in) leaves it in place. Same cast as TestBackpressure_WaitingRoomTokenIsHeld, plus stage s (occupied by
// item 1) that item 3 waits in front of after retaining.
func TestBackpressure_HeldWaitingRoomTokenSurvivesALaterWaitingRoom(t *testing.T) {
	x := newTwoPoolFanOut(BackpressureStrict, 3)
	c := x.c
	x.f.SetLimit(2).SetQueueSize(1)
	s := c.AddStage(OptName("s")).SetQueueSize(2) // built after f: rank between f and commit
	commit := c.AddStage(OptName("commit2")).SetLimit(10)

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 3 in f's waiting room", func() bool { return slices.Equal(inQueueOf(c, x.f), []int64{3}) })
		close(x.processorGates[2]) // item 2 leaves f: item 3 is admitted from the room
		waitFor(t, "item 3 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{3}) })
		waitFor(t, "item 3 in s's waiting room", func() bool { return slices.Contains(inQueueOf(c, s), 3) })
		if got := inQueueOf(c, x.f); !slices.Equal(got, []int64{3}) {
			t.Errorf("f waiting room = %v while item 3 waits in front of s, want [3] (the held token stays)", got)
		}
		if q := queueOccupancy(c, x.f); q != 1 {
			t.Errorf("f queued = %d, want 1", q)
		}
		close(x.bt.gate("1/p1")) // item 3's task starts: the hold ends and the token is given back
		waitFor(t, "the held token released", func() bool { return queueOccupancy(c, x.f) == 0 })
		if got := heldItems(c); len(got) != 0 {
			t.Errorf("items holding upstream = %v, want none", got)
		}
		close(x.processorGates[1])
		x.bt.releaseAll()
	}()

	runNOK(t, c, 3, func(ctx context.Context, no int64) error {
		if err := x.read.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.MoveTo(ctx); err != nil {
			return err
		}
		var tasks []Task
		if no != 2 {
			tasks = []Task{x.bt.task(x.p1, no)}
		}
		if err := x.f.Schedule(ctx, tasks...); err != nil {
			return err
		}
		var w Wave
		switch no {
		case 1, 3:
			w = x.f.Retain(ctx)
		case 2:
			<-x.processorGates[2]
		}
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		if no == 1 {
			<-x.processorGates[1]
		}
		if err := commit.MoveTo(ctx); err != nil {
			return err
		}
		if w != nil {
			return w.Wait(ctx)
		}
		return nil
	})
	<-done
}

// TestBackpressureStrict_FailedLeaveKeepsTheHold: a leave whose call context expires with work still running leaves
// the body open and the hold in place. The hold ends when the queued branch starts, and the retried leave then
// succeeds with nothing held.
func TestBackpressureStrict_FailedLeaveKeepsTheHold(t *testing.T) {
	x := newTwoPoolFanOut(BackpressureStrict, 2)
	c := x.c
	leaveFailed := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-leaveFailed
		if got := heldItems(c); !slices.Equal(got, []int64{2}) {
			t.Errorf("items holding upstream = %v after the failed leave, want [2]", got)
		}
		if got := inBodyOf(c, x.read); !slices.Equal(got, []int64{2}) {
			t.Errorf("read = %v after the failed leave, want [2]", got)
		}
		close(x.bt.gate("1/p1"))
		waitFor(t, "the hold to end", func() bool { return len(heldItems(c)) == 0 })
		x.bt.releaseAll()
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if no == 1 {
			return x.enterAndBlock(ctx, x.p1) // p1 saturated; p2 stays free
		}
		if err := x.read.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.MoveTo(ctx); err != nil {
			return err
		}
		// p2 starts (running work); p1 is queued behind item 1, so the hold stays.
		if err := x.f.Schedule(ctx, x.bt.task(x.p1, no), x.bt.task(x.p2, no)); err != nil {
			return err
		}
		waitFor(t, "item 2's p2 task to start", func() bool { return x.bt.hasStarted("2/p2") })
		dctx, dcancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer dcancel()
		if err := x.commit.MoveTo(dctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("leave with an expired call context = %v, want DeadlineExceeded", err)
		}
		if st := bodyStateOf(ctx, x.f); st != bodyOpen {
			t.Errorf("body state after the failed leave = %d, want open (%d)", st, bodyOpen)
		}
		close(leaveFailed)
		if err := x.commit.MoveTo(ctx); err != nil {
			return err
		}
		if got := heldItems(c); len(got) != 0 {
			t.Errorf("items holding upstream = %v after the successful leave, want none", got)
		}
		return nil
	})
	<-done
}

// TestBackpressure_TryMoveToEntersWithBusyPools: TryMoveTo is never declined for busy pools. It enters, and under
// Balanced/Strict the item then keeps read until its work starts; under Buffered read is released on entry.
func TestBackpressure_TryMoveToEntersWithBusyPools(t *testing.T) {
	for _, mode := range allModes {
		t.Run(mode.String(), func(t *testing.T) {
			x := newTwoPoolFanOut(mode, 2)
			c := x.c
			entered := make(chan struct{})

			done := make(chan struct{})
			go func() {
				defer close(done)
				<-entered
				waitFor(t, "item 2's task queued", func() bool { return queueOccupancy(c, x.p1) == 1 })
				if got := inBodyOf(c, x.f); !slices.Equal(got, []int64{1, 2}) {
					t.Errorf("f = %v after the try, want [1 2]", got)
				}
				if mode == BackpressureBuffered {
					waitFor(t, "read released", func() bool { return len(inBodyOf(c, x.read)) == 0 })
				} else {
					if got := inBodyOf(c, x.read); !slices.Equal(got, []int64{2}) {
						t.Errorf("read = %v after the try, want [2] (held)", got)
					}
					if held := heldItems(c); !slices.Equal(held, []int64{2}) {
						t.Errorf("items holding upstream = %v, want [2]", held)
					}
				}
				close(x.bt.gate("1/p1"))
				waitFor(t, "item 2 started", func() bool { return x.bt.hasStarted("2/p1") })
				waitFor(t, "read released", func() bool { return len(inBodyOf(c, x.read)) == 0 })
				x.bt.releaseAll()
			}()

			runNOK(t, c, 2, func(ctx context.Context, no int64) error {
				if no == 1 {
					return x.enterAndBlock(ctx, x.p1, x.p2)
				}
				waitFor(t, "both pools busy", func() bool { return x.bt.hasStarted("1/p1") && x.bt.hasStarted("1/p2") })
				if err := x.read.MoveTo(ctx); err != nil {
					return err
				}
				if ok, err := x.f.TryMoveTo(ctx); err != nil || !ok {
					t.Errorf("TryMoveTo = (%v, %v) with every pool full, want (true, nil)", ok, err)
				}
				if err := x.f.Schedule(ctx, x.bt.task(x.p1, no)); err != nil {
					return err
				}
				close(entered)
				return x.commit.MoveTo(ctx)
			})
			<-done
		})
	}
}

// TestBackpressureStrict_PoolLimitLoweredUnderAHold: lowering a pool's limit while a held item's work is queued on it
// evicts nothing; the work starts, and the hold ends, once occupancy has fallen below the new limit.
func TestBackpressureStrict_PoolLimitLoweredUnderAHold(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetBackpressure(BackpressureStrict)
	p1 := f.AddPool(OptName("p1")).SetLimit(2)
	commit := c.AddStage(OptName("commit")).SetLimit(10)
	bt := newBlockingTasks()
	firstOf1 := bt.gate("1/p1")
	secondOf1 := make(chan struct{})
	bt.gate("2/p1")
	var startedOf1 atomic.Int64

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
		waitFor(t, "item 2's task queued", func() bool { return queueOccupancy(c, p1) == 1 })
		p1.SetLimit(1)
		if occ := occupancyOf(c, p1); occ != 2 {
			t.Errorf("p1 occupancy = %d after the lower, want 2 (running work is not evicted)", occ)
		}
		close(firstOf1) // one of item 1's tasks ends: occupancy 1 = the new limit, so item 2's task still waits
		waitFor(t, "p1 down to 1", func() bool { return occupancyOf(c, p1) == 1 })
		if got := heldItems(c); !slices.Equal(got, []int64{2}) {
			t.Errorf("items holding upstream = %v at the new limit, want [2]", got)
		}
		close(secondOf1)
		waitFor(t, "item 2's task to start", func() bool { return bt.hasStarted("2/p1") })
		waitFor(t, "read released", func() bool { return len(inBodyOf(c, read)) == 0 })
		bt.releaseAll()
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := f.MoveTo(ctx); err != nil {
			return err
		}
		var tasks []Task
		if no == 1 {
			tasks = []Task{p1.NewTasks(2, func(context.Context, int) error {
				if startedOf1.Add(1) == 1 {
					<-firstOf1
				} else {
					<-secondOf1
				}
				return nil
			})}
		} else {
			tasks = []Task{bt.task(p1, no)}
		}
		if err := f.Schedule(ctx, tasks...); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	<-done
}

// TestBackpressure_LaneHoldEndsAtChildCreation: on a lane the hold ends when the child is created at the lane's start
// gate, even while that child still waits for an interior stage.
func TestBackpressure_LaneHoldEndsAtChildCreation(t *testing.T) {
	for _, mode := range holdingModes {
		t.Run(mode.String(), func(t *testing.T) {
			c := NewConveyor()
			read := c.AddStage(OptName("read"))
			f := c.AddFanOut(OptName("f")).SetLimit(100).SetBackpressure(mode)
			lane := f.AddLane(OptName("lane"))
			mid := lane.AddStage(OptName("mid")) // exclusive: item 2's child waits here
			commit := c.AddStage(OptName("commit")).SetLimit(10)
			release := make(chan struct{})

			done := make(chan struct{})
			go func() {
				defer close(done)
				waitFor(t, "item 2's child at the lane's entrance", func() bool {
					return slices.Equal(inBodyOf(c, lane), []int64{2})
				})
				if got := inBodyOf(c, mid); !slices.Equal(got, []int64{1}) {
					t.Errorf("mid = %v, want [1] (item 2's child has not entered it)", got)
				}
				waitFor(t, "read released", func() bool { return len(inBodyOf(c, read)) == 0 })
				if held := heldItems(c); len(held) != 0 {
					t.Errorf("items holding upstream = %v, want none once the child exists", held)
				}
				close(release)
			}()

			runNOK(t, c, 2, func(ctx context.Context, no int64) error {
				if err := read.MoveTo(ctx); err != nil {
					return err
				}
				if err := f.MoveTo(ctx); err != nil {
					return err
				}
				if err := f.Schedule(ctx, lane.NewTask(func(cctx context.Context) error {
					if err := mid.MoveTo(cctx); err != nil {
						return err
					}
					<-release
					return nil
				})); err != nil {
					return err
				}
				return commit.MoveTo(ctx)
			})
			<-done
		})
	}
}

// TestBackpressure_SetBackpressureOnALiveConveyor: the mode applies to later admissions only. Item 1 enters under
// Buffered and releases read; the switch to Strict gives item 2 a hold (both pools full); the switch to Balanced gives
// item 3 one too (its only pool is full); the switch back to Buffered lets item 4 release read at once while the two
// existing holds end on their own, each by its own mode's milestone.
func TestBackpressure_SetBackpressureOnALiveConveyor(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read")).SetLimit(3)
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetBackpressure(BackpressureBuffered)
	p1 := f.AddPool(OptName("p1"))
	p2 := f.AddPool(OptName("p2"))
	commit := c.AddStage(OptName("commit")).SetLimit(10)
	bt := newBlockingTasks()
	for _, k := range []string{"1/p1", "1/p2", "2/p1", "2/p2", "3/p1", "4/p1"} {
		bt.gate(k)
	}
	gates := map[int64]chan struct{}{2: make(chan struct{}), 3: make(chan struct{}), 4: make(chan struct{})}
	inside := make(chan int64, 4)

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-inside // item 1, Buffered
		waitFor(t, "item 1 released read", func() bool { return !slices.Contains(inBodyOf(c, read), 1) })
		f.SetBackpressure(BackpressureStrict)
		if got := f.Backpressure(); got != BackpressureStrict {
			t.Errorf("Backpressure() = %v, want Strict", got)
		}
		close(gates[2])
		<-inside // item 2, Strict: p1 and p2 both queued behind item 1
		waitFor(t, "item 2's work queued", func() bool { return queueOccupancy(c, p1) == 1 && queueOccupancy(c, p2) == 1 })
		if held := heldItems(c); !slices.Equal(held, []int64{2}) {
			t.Errorf("items holding upstream = %v, want [2]", held)
		}
		f.SetBackpressure(BackpressureBalanced)
		close(gates[3])
		<-inside // item 3, Balanced: p1 queued behind items 1 and 2
		waitFor(t, "item 3's work queued", func() bool { return queueOccupancy(c, p1) == 2 })
		if held := heldItems(c); !slices.Equal(held, []int64{2, 3}) {
			t.Errorf("items holding upstream = %v, want [2 3]", held)
		}
		f.SetBackpressure(BackpressureBuffered)
		close(gates[4])
		<-inside // item 4, Buffered: released at once, the existing holds stay
		waitFor(t, "item 4 released read", func() bool { return slices.Equal(inBodyOf(c, read), []int64{2, 3}) })
		if held := heldItems(c); !slices.Equal(held, []int64{2, 3}) {
			t.Errorf("items holding upstream = %v after the switch back, want [2 3] (left to end naturally)", held)
		}
		close(bt.gate("1/p1")) // item 2's p1 task starts: Strict still awaits p2
		waitFor(t, "item 2 started on p1", func() bool { return bt.hasStarted("2/p1") })
		if held := heldItems(c); !slices.Equal(held, []int64{2, 3}) {
			t.Errorf("items holding upstream = %v with item 2's p2 task still queued, want [2 3]", held)
		}
		close(bt.gate("1/p2"))
		waitFor(t, "item 2's hold to end", func() bool { return slices.Equal(heldItems(c), []int64{3}) })
		close(bt.gate("2/p1")) // item 3's p1 task starts: Balanced ends at the first start
		waitFor(t, "item 3's hold to end", func() bool { return len(heldItems(c)) == 0 })
		waitFor(t, "read empty", func() bool { return len(inBodyOf(c, read)) == 0 })
		bt.releaseAll()
	}()

	runNOK(t, c, 4, func(ctx context.Context, no int64) error {
		if g := gates[no]; g != nil {
			<-g
		}
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := f.MoveTo(ctx); err != nil {
			return err
		}
		tasks := []Task{bt.task(p1, no)}
		if no <= 2 {
			tasks = append(tasks, bt.task(p2, no))
		}
		if err := f.Schedule(ctx, tasks...); err != nil {
			return err
		}
		inside <- no
		return commit.MoveTo(ctx)
	})
	<-done
}

// TestFanOutBackpressure_ValuesAndString pins the getters: String for every mode, Balanced as the default, and an
// unknown value stored as Balanced.
func TestFanOutBackpressure_ValuesAndString(t *testing.T) {
	for m, want := range map[FanOutBackpressure]string{
		BackpressureBuffered:   "BackpressureBuffered",
		BackpressureBalanced:   "BackpressureBalanced",
		BackpressureStrict:     "BackpressureStrict",
		FanOutBackpressure(7):  "FanOutBackpressure(7)",
		FanOutBackpressure(-1): "FanOutBackpressure(-1)",
	} {
		if got := m.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
	f := NewConveyor().AddFanOut()
	if got := f.Backpressure(); got != BackpressureBalanced {
		t.Errorf("default Backpressure() = %v, want Balanced", got)
	}
	for _, m := range allModes {
		if got := f.SetBackpressure(m).Backpressure(); got != m {
			t.Errorf("Backpressure() = %v after SetBackpressure(%v)", got, m)
		}
	}
	if got := f.SetBackpressure(FanOutBackpressure(7)).Backpressure(); got != BackpressureBalanced {
		t.Errorf("Backpressure() = %v after an unknown value, want Balanced", got)
	}
}
