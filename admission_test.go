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

// TestAdmitByPoolsStrict_WalkThrough replays the walk-through of design_pool_aware_admission.md §1.3: an item whose work
// waits for a saturated pool keeps its read slot, a younger item whose work can start passes it, and the stage
// before the fan-out fills with stuck items until the saturated pool frees a slot.
func TestAdmitByPoolsStrict_WalkThrough(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read")).SetLimit(2)
	f := c.AddFanOut(OptName("dbsWrite")).SetLimit(100).SetAdmission(AdmitByPoolsStrict)
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
		// 4. Item 4 needs db2: admitted (db1 has room), queued behind item 2, keeps read. read is full now.
		waitFor(t, "item 4 admitted", func() bool { return slices.Contains(inBodyOf(c, f), 4) })
		// 5. Item 5 blocks in read.MoveTo, on the start stage.
		waitFor(t, "item 5 waits for read", func() bool { return slices.Contains(inBodyOf(c, c.StartUnit()), 5) })
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
	assertStrictlyIncreasing(t, commitOrder.all(), "commit order under AdmitByPoolsStrict")
}

// TestAdmitByPoolsStrict_AllPoolsFullBlocksAdmission pins the admission predicate: with every pool full the next item stays
// in the previous stage even though the fan-out has free item slots, and is admitted once a pool slot frees with an
// empty queue.
func TestAdmitByPoolsStrict_AllPoolsFullBlocksAdmission(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetAdmission(AdmitByPoolsStrict)
	p := f.AddPool(OptName("p"))
	commit := c.AddStage(OptName("commit"))
	bt := newBlockingTasks()
	bt.gate("1/p")
	bt.gate("2/p")

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 1 started", func() bool { return bt.hasStarted("1/p") })
		waitFor(t, "item 2 in read", func() bool { return slices.Contains(inBodyOf(c, read), 2) })
		waitFor(t, "item 2 parked", func() bool { return parkedOf(c) >= 2 }) // item 1's leave and item 2's MoveTo
		if got := inBodyOf(c, f); !slices.Equal(got, []int64{1}) {
			t.Errorf("item 2 must not be admitted while p is full; fan-out = %v", got)
		}
		close(bt.gate("1/p"))
		waitFor(t, "item 2 admitted", func() bool { return slices.Contains(inBodyOf(c, f), 2) })
		waitFor(t, "item 2 started", func() bool { return bt.hasStarted("2/p") })
		if got := inBodyOf(c, read); len(got) != 0 {
			t.Errorf("read should be released once item 2's task started; read = %v", got)
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
		if err := f.Schedule(ctx, bt.task(p, no)); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	<-done
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
			if it.hold != nil {
				out = append(out, it.no)
			}
		}
	}
	slices.Sort(out)
	return out
}

// twoPoolFanOut is the fixture most hold tests share: read -> f (AdmitByPoolsStrict, roomy) with pools p1 and p2 (limit 1
// each) -> commit. Item 1 blocks both pools; a younger item that schedules on p1 alone is admitted (p2 is free) and
// keeps its read slot until p1 frees.
type twoPoolFanOut struct {
	c              Conveyor
	read, commit   Stage
	f              FanOut
	p1, p2         Pool
	bt             *blockingTasks
	processorGates map[int64]chan struct{}
}

func newTwoPoolFanOut(items int64) *twoPoolFanOut {
	return newTwoPoolFanOutWith(AdmitByPoolsStrict, items)
}

// newTwoPoolFanOutWith is the fixture with the given policy.
func newTwoPoolFanOutWith(a FanOutAdmission, items int64) *twoPoolFanOut {
	x := &twoPoolFanOut{c: NewConveyor(), bt: newBlockingTasks(), processorGates: map[int64]chan struct{}{}}
	x.read = x.c.AddStage(OptName("read"))
	x.f = x.c.AddFanOut(OptName("f")).SetLimit(100).SetAdmission(a)
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
// Blocking p1 alone leaves p2 as the capacity that admits item 2; blocking both keeps item 2 out.
func (x *twoPoolFanOut) enterAndBlock(ctx context.Context, pools ...Pool) error {
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
	return x.commit.MoveTo(ctx)
}

// TestAdmitByPoolsStrict_FreeSlotWithQueuedWorkBlocksAdmission: a pool with a free slot and a non-empty queue has a
// streaming pull in flight on its head, so its capacity is spoken for and admission stays closed until the queue
// empties.
func TestAdmitByPoolsStrict_FreeSlotWithQueuedWorkBlocksAdmission(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read")).SetLimit(2) // item 1 holds read while its pull is in flight
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetAdmission(AdmitByPoolsStrict)
	p := f.AddPool(OptName("p")).SetLimit(2)
	commit := c.AddStage(OptName("commit"))
	feed := make(chan TaskFunc) // item 1's streaming source: the pull blocks here, holding one reserved slot
	admitted2 := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "the pull to reserve a slot", func() bool { return occupancyOf(c, p) == 1 })
		waitFor(t, "item 2 in read", func() bool { return slices.Contains(inBodyOf(c, read), 2) })
		waitFor(t, "item 1's leave and item 2's move to park", func() bool { return parkedOf(c) == 2 })
		if got := inBodyOf(c, f); !slices.Equal(got, []int64{1}) {
			t.Errorf("fan-out = %v while p has a free slot but a queued head, want [1]", got)
		}
		close(feed) // the source is exhausted: the reserved slot is given back and the queue empties
		<-admitted2
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := f.MoveTo(ctx); err != nil {
			return err
		}
		if no == 1 {
			if err := f.Schedule(ctx, p.NewTasksChan(feed)); err != nil {
				return err
			}
		} else {
			close(admitted2)
			if err := f.Schedule(ctx); err != nil {
				return err
			}
		}
		return commit.MoveTo(ctx)
	})
	<-done
}

// TestAdmitByPoolsStrict_HoldEndsAtFirstAssignmentPerBranch: the hold ends when every touched branch has had one assignment,
// not when the batch is dispatched. Item 2 enters with NewTasks(100) on p (limit 2) and one task on q, where item 1's
// task blocks: the hold survives p's first two assignments and ends at q's first, with 98 tasks still queued on p.
func TestAdmitByPoolsStrict_HoldEndsAtFirstAssignmentPerBranch(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetAdmission(AdmitByPoolsStrict)
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
			t.Errorf("read = %v, want [2]: q has not started, so the hold must survive p's assignments", got)
		}
		if held := heldItems(c); !slices.Equal(held, []int64{2}) {
			t.Errorf("items holding upstream = %v, want [2]", held)
		}
		close(bt.gate("1/q")) // q frees: item 2's q task is the first assignment on its last awaited branch
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

// TestAdmitByPoolsStrict_EmptyEnteringSubmissionDischarges: a first Schedule with no tasks, or only statically empty ones,
// releases the previous node at once while the item stays inside.
func TestAdmitByPoolsStrict_EmptyEnteringSubmissionDischarges(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tasks func(p Pool) []Task
	}{
		{"no tasks", func(Pool) []Task { return nil }},
		{"statically empty", func(p Pool) []Task {
			return []Task{p.NewTasks(0, func(context.Context, int) error { return nil }), p.NewTasksGen(nil)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewConveyor()
			read := c.AddStage(OptName("read"))
			f := c.AddFanOut(OptName("f")).SetAdmission(AdmitByPoolsStrict)
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

// TestAdmitByPoolsStrict_LaterRoundsAndFollowUpsNeverHold: only the first Schedule holds upstream. A second round queued
// behind the pool, and a follow-up a task schedules, leave the previous node released.
func TestAdmitByPoolsStrict_LaterRoundsAndFollowUpsNeverHold(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	f := c.AddFanOut(OptName("f")).SetAdmission(AdmitByPoolsStrict)
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
			t.Errorf("read = %v with two collections queued, want empty (the hold ended at the first assignment)", got)
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
}

// TestAdmitByPoolsStrict_UnrelatedRetainCompletionKeepsTheHeldToken: the sweep a finishing Retain triggers frees the
// retained stage but leaves the token the hold protects.
func TestAdmitByPoolsStrict_UnrelatedRetainCompletionKeepsTheHeldToken(t *testing.T) {
	c := NewConveyor()
	a := c.AddStage(OptName("a"))
	read := c.AddStage(OptName("read"))
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetAdmission(AdmitByPoolsStrict)
	p1 := f.AddPool(OptName("p1"))
	f.AddPool(OptName("p2")) // never used: the free capacity that admits item 2
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
		if err := f.Schedule(ctx, bt.task(p1, no)); err != nil { // p1 saturated by item 1; p2 stays free
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

// TestAdmitByPoolsStrict_RetainOnPreviousStagePlusHold: a Retain on the stage the hold protects. The slot is freed only
// when both have ended, in either order.
func TestAdmitByPoolsStrict_RetainOnPreviousStagePlusHold(t *testing.T) {
	for _, retainFirst := range []bool{true, false} {
		name := "hold ends first"
		if retainFirst {
			name = "retain ends first"
		}
		t.Run(name, func(t *testing.T) {
			x := newTwoPoolFanOut(2)
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

// TestAdmitByPoolsStrict_WaitingRoomTokenIsHeld: an item admitted from the fan-out's waiting room keeps its queued token
// (the previous node was released when it stepped aside) until its work starts.
func TestAdmitByPoolsStrict_WaitingRoomTokenIsHeld(t *testing.T) {
	x := newTwoPoolFanOut(2)
	c := x.c
	x.f.SetQueueSize(1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2 in the waiting room", func() bool { return slices.Equal(inQueueOf(c, x.f), []int64{2}) })
		if got := inBodyOf(c, x.read); len(got) != 0 {
			t.Errorf("read = %v while item 2 waits in the room, want empty", got)
		}
		close(x.bt.gate("1/p2")) // p2 frees with an empty queue: item 2 is admitted from the room
		waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
		if got := inQueueOf(c, x.f); !slices.Equal(got, []int64{2}) {
			t.Errorf("f waiting room = %v after admission, want [2] (the queued token is held)", got)
		}
		if q := queueOccupancy(c, x.f); q != 1 {
			t.Errorf("f queued = %d, want 1", q)
		}
		close(x.bt.gate("1/p1"))
		waitFor(t, "the queued token released", func() bool { return queueOccupancy(c, x.f) == 0 })
		x.bt.releaseAll()
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if no == 1 {
			return x.enterAndBlock(ctx, x.p1, x.p2)
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
		return x.commit.MoveTo(ctx)
	})
	<-done
}

// TestAdmitByPoolsStrict_StartStageTokenThrottlesItemCreation: entering from the start stage holds the start slot, so no
// new item is created until the held item's work starts.
func TestAdmitByPoolsStrict_StartStageTokenThrottlesItemCreation(t *testing.T) {
	c := NewConveyor()
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetAdmission(AdmitByPoolsStrict)
	p1 := f.AddPool(OptName("p1"))
	p2 := f.AddPool(OptName("p2"))
	commit := c.AddStage(OptName("commit")).SetLimit(10)
	bt := newBlockingTasks()
	for _, k := range []string{"1/p1", "1/p2", "2/p1", "3/p1"} {
		bt.gate(k)
	}
	start := c.StartUnit()

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2 on start, blocked", func() bool { return slices.Equal(inBodyOf(c, start), []int64{2}) })
		close(bt.gate("1/p2"))
		waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
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
		tasks := []Task{bt.task(p1, no)}
		if no == 1 {
			tasks = append(tasks, bt.task(p2, no))
		}
		if err := f.Schedule(ctx, tasks...); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	<-done
}

// TestAdmitByPoolsStrict_CancellationDropsEnteringCollectionAndDischarges: a canceled item's queued entering collection is
// dropped when the pool next looks at its queue, which ends the hold while the processor is still inside. After the
// run nothing is left occupied.
func TestAdmitByPoolsStrict_CancellationDropsEnteringCollectionAndDischarges(t *testing.T) {
	x := newTwoPoolFanOut(2)
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

// TestAdmitByPoolsStrict_ProcessorReturnSealsAndDischarges: a processor that returns with its entering work still queued
// has its body sealed by completion, which ends the hold while the item waits for that work — and wakes the item
// parked on the freed slot. Item 3 is that waiter: it must get into read on item 2's return alone, with nothing else
// in the run able to broadcast (item 1 blocked in its leave, item 2 waiting for its work).
func TestAdmitByPoolsStrict_ProcessorReturnSealsAndDischarges(t *testing.T) {
	x := newTwoPoolFanOut(3)
	c := x.c
	var passed3 atomic.Bool

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
		waitFor(t, "item 2's collection queued", func() bool { return queueOccupancy(c, x.p1) == 1 })
		waitFor(t, "item 3 to wait for read", func() bool { return slices.Contains(inBodyOf(c, c.StartUnit()), 3) })
		waitFor(t, "item 1's leave and item 3's move to park", func() bool { return parkedOf(c) == 2 })
		// From here the only broadcast left in the run is the one item 2's completion must make.
		close(x.processorGates[2])
		waitFor(t, "item 3 to pass read", passed3.Load)
		waitFor(t, "the hold to end", func() bool { return len(heldItems(c)) == 0 })
		if got := inBodyOf(c, x.f); !slices.Contains(got, 2) {
			t.Errorf("f = %v, want item 2 still inside (its work is queued)", got)
		}
		if q := queueOccupancy(c, x.p1); q != 1 {
			t.Errorf("p1 backlog = %d, want 1", q)
		}
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

// TestAdmitByPoolsStrict_DetachDischarges: Detach ends the item's own path into the body, and with it the hold.
func TestAdmitByPoolsStrict_DetachDischarges(t *testing.T) {
	x := newTwoPoolFanOut(2)
	c := x.c

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 2 admitted with a hold", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
		close(x.processorGates[2]) // item 2 detaches
		waitFor(t, "the hold to end", func() bool { return len(heldItems(c)) == 0 })
		waitFor(t, "read released", func() bool { return len(inBodyOf(c, x.read)) == 0 })
		if q := queueOccupancy(c, x.p1); q != 1 {
			t.Errorf("p1 backlog = %d after Detach, want 1 (the work is still queued)", q)
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
		if err := x.f.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.Schedule(ctx, x.bt.task(x.p1, no)); err != nil {
			return err
		}
		<-x.processorGates[2]
		w := x.f.Detach(ctx)
		if err := x.commit.MoveTo(ctx); err != nil {
			return err
		}
		return w.Wait(ctx)
	})
	<-done
}

// TestAdmitByPoolsStrict_FailedLeaveKeepsTheHold: a leave whose call context expires with work still running leaves the
// body open and the hold in place. The hold ends when the queued branch starts, and the retried leave then succeeds
// with nothing held.
func TestAdmitByPoolsStrict_FailedLeaveKeepsTheHold(t *testing.T) {
	x := newTwoPoolFanOut(2)
	c := x.c
	x.bt.gate("2/p2")
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
			if err := x.read.MoveTo(ctx); err != nil {
				return err
			}
			if err := x.f.MoveTo(ctx); err != nil {
				return err
			}
			if err := x.f.Schedule(ctx, x.bt.task(x.p1, 1)); err != nil { // p1 saturated; p2 stays free
				return err
			}
			return x.commit.MoveTo(ctx)
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

// TestAdmitByPoolsStrict_TryMoveToDeclinesWithoutCapacity: TryMoveTo under AdmitByPoolsStrict is declined while no pool has
// capacity, and the declined item stays where it was, with nothing marked.
func TestAdmitByPoolsStrict_TryMoveToDeclinesWithoutCapacity(t *testing.T) {
	x := newTwoPoolFanOut(2)
	c := x.c
	x.bt.gate("2/p1")
	declined := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-declined
		if got := inBodyOf(c, x.f); !slices.Equal(got, []int64{1}) {
			t.Errorf("f = %v after the declined try, want [1]", got)
		}
		if got := inBodyOf(c, x.read); !slices.Equal(got, []int64{2}) {
			t.Errorf("read = %v after the declined try, want [2]", got)
		}
		if held := heldItems(c); len(held) != 0 {
			t.Errorf("items holding upstream = %v after the declined try, want none", held)
		}
		close(x.processorGates[2])
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
		if entered, err := x.f.TryMoveTo(ctx); err != nil || entered {
			t.Errorf("TryMoveTo = (%v, %v) with every pool full, want (false, nil)", entered, err)
		}
		close(declined)
		<-x.processorGates[2]
		if err := x.f.MoveTo(ctx); err != nil { // the plain move works once capacity appears
			return err
		}
		if err := x.f.Schedule(ctx, x.bt.task(x.p1, no)); err != nil {
			return err
		}
		return x.commit.MoveTo(ctx)
	})
	<-done
}

// TestAdmitByPoolsStrict_PoolLimitLoweredUnderAHold: lowering a pool's limit while a held item's work is queued on it evicts
// nothing; the work starts, and the hold ends, once occupancy has fallen below the new limit.
func TestAdmitByPoolsStrict_PoolLimitLoweredUnderAHold(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetAdmission(AdmitByPoolsStrict)
	p1 := f.AddPool(OptName("p1")).SetLimit(2)
	f.AddPool(OptName("p2")) // never used: the free capacity that admits item 2
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

// TestAdmitByPoolsStrict_LaneHoldEndsAtChildCreation: on a lane the hold ends when the child is created at the lane's start
// gate, even while that child still waits for an interior stage.
func TestAdmitByPoolsStrict_LaneHoldEndsAtChildCreation(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read"))
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetAdmission(AdmitByPoolsStrict)
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
}

// TestAdmitByPoolsStrict_SetAdmissionOnALiveConveyor: switching to AdmitByPoolsStrict blocks the next admission while the pools are
// full and gives it a hold, leaving the items already inside as they were; switching back admits waiters at once and
// leaves existing holds to end on their own. The switches here happen while the affected items are parked; a switch
// racing one admission decision has no deterministic hook and is left to the live policy jitter of the property test
// (the dials are stored under run.mu, and enterUnit re-checks before stepping into the waiting room).
func TestAdmitByPoolsStrict_SetAdmissionOnALiveConveyor(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read")).SetLimit(2)
	f := c.AddFanOut(OptName("f")).SetLimit(100) // AdmitByLimit to start with
	p1 := f.AddPool(OptName("p1"))
	p2 := f.AddPool(OptName("p2"))
	commit := c.AddStage(OptName("commit")).SetLimit(10)
	bt := newBlockingTasks()
	for _, k := range []string{"1/p1", "1/p2", "2/p1", "2/p2"} {
		bt.gate(k)
	}
	inside1 := make(chan struct{})
	go2 := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-inside1
		if got := inBodyOf(c, read); len(got) != 0 {
			t.Errorf("read = %v after a by-limit admission, want empty", got)
		}
		f.SetAdmission(AdmitByPoolsStrict)
		if got := f.Admission(); got != AdmitByPoolsStrict {
			t.Errorf("Admission() = %v, want AdmitByPoolsStrict", got)
		}
		close(go2)
		waitFor(t, "item 2 in read", func() bool { return slices.Contains(inBodyOf(c, read), 2) })
		waitFor(t, "item 2 and item 1's leave parked", func() bool { return parkedOf(c) >= 2 })
		if got := inBodyOf(c, f); !slices.Equal(got, []int64{1}) {
			t.Errorf("f = %v with every pool full, want [1]", got)
		}
		if held := heldItems(c); len(held) != 0 {
			t.Errorf("items holding upstream = %v, want none (item 1 was admitted by limit)", held)
		}
		close(bt.gate("1/p2")) // p2 frees: item 2 is admitted, starts on p2, queues on p1 -> holds read
		waitFor(t, "item 2 holds", func() bool { return slices.Equal(heldItems(c), []int64{2}) })
		waitFor(t, "item 2 started on p2", func() bool { return bt.hasStarted("2/p2") })
		// Item 3: every pool is full again (p1 by item 1 with a queue, p2 by item 2), so it waits in read.
		waitFor(t, "item 3 in read", func() bool { return slices.Equal(inBodyOf(c, read), []int64{2, 3}) })
		waitFor(t, "item 3 parked", func() bool { return parkedOf(c) >= 3 })
		f.SetAdmission(AdmitByLimit)
		waitFor(t, "item 3 admitted at once", func() bool { return slices.Contains(inBodyOf(c, f), 3) })
		waitFor(t, "item 3 released read", func() bool { return slices.Equal(inBodyOf(c, read), []int64{2}) })
		if got := heldItems(c); !slices.Equal(got, []int64{2}) {
			t.Errorf("items holding upstream = %v after the switch back, want [2] (left to end naturally)", got)
		}
		close(bt.gate("1/p1"))
		waitFor(t, "item 2's hold to end", func() bool { return len(heldItems(c)) == 0 })
		bt.releaseAll()
	}()

	runNOK(t, c, 3, func(ctx context.Context, no int64) error {
		if no == 2 {
			<-go2
		}
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := f.MoveTo(ctx); err != nil {
			return err
		}
		var tasks []Task
		switch no {
		case 1, 2:
			tasks = []Task{bt.task(p1, no), bt.task(p2, no)}
		}
		if err := f.Schedule(ctx, tasks...); err != nil {
			return err
		}
		if no == 1 {
			close(inside1)
		}
		return commit.MoveTo(ctx)
	})
	<-done
}

// --- AdmitByPools (the gate alone: pool capacity to enter, no upstream hold) ---

// TestAdmitByPools_AllPoolsFullBlocksAdmission: with every pool full the next item waits in read even though the fan-out
// has free item slots; a freed slot with an empty queue lets it in, and it leaves read at once — no hold.
func TestAdmitByPools_AllPoolsFullBlocksAdmission(t *testing.T) {
	x := newTwoPoolFanOutWith(AdmitByPools, 2)
	c := x.c

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "both pools busy", func() bool { return x.bt.hasStarted("1/p1") && x.bt.hasStarted("1/p2") })
		waitFor(t, "item 2 in read", func() bool { return slices.Contains(inBodyOf(c, x.read), 2) })
		waitFor(t, "item 1's leave and item 2's move to park", func() bool { return parkedOf(c) == 2 })
		if got := inBodyOf(c, x.f); !slices.Equal(got, []int64{1}) {
			t.Errorf("f = %v with every pool full, want [1] (free item slots do not admit)", got)
		}
		close(x.bt.gate("1/p2")) // p2 frees with nothing queued: item 2 may enter
		waitFor(t, "item 2 admitted", func() bool { return slices.Contains(inBodyOf(c, x.f), 2) })
		waitFor(t, "read released", func() bool { return len(inBodyOf(c, x.read)) == 0 })
		if held := heldItems(c); len(held) != 0 {
			t.Errorf("items holding upstream = %v, want none under AdmitByPools", held)
		}
		waitFor(t, "item 2 started on p2", func() bool { return x.bt.hasStarted("2/p2") })
		x.bt.releaseAll()
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if no == 1 {
			return x.enterAndBlock(ctx, x.p1, x.p2)
		}
		if err := x.read.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.Schedule(ctx, x.bt.task(x.p2, no)); err != nil {
			return err
		}
		return x.commit.MoveTo(ctx)
	})
	<-done
}

// TestAdmitByPools_FreeSlotWithQueuedWorkBlocksAdmission: a free slot next to a non-empty queue (a streaming pull in
// flight on the head) is spoken for, so it does not admit; the queue emptying does.
func TestAdmitByPools_FreeSlotWithQueuedWorkBlocksAdmission(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read")).SetLimit(2)
	f := c.AddFanOut(OptName("f")).SetLimit(100).SetAdmission(AdmitByPools)
	p := f.AddPool(OptName("p")).SetLimit(2)
	commit := c.AddStage(OptName("commit"))
	feed := make(chan TaskFunc)
	admitted2 := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "the pull to reserve a slot", func() bool { return occupancyOf(c, p) == 1 })
		waitFor(t, "item 2 in read", func() bool { return slices.Contains(inBodyOf(c, read), 2) })
		waitFor(t, "item 1's leave and item 2's move to park", func() bool { return parkedOf(c) == 2 })
		if got := inBodyOf(c, f); !slices.Equal(got, []int64{1}) {
			t.Errorf("fan-out = %v while p has a free slot but a queued head, want [1]", got)
		}
		close(feed) // exhausted: the reserved slot is given back and the queue empties
		<-admitted2
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := f.MoveTo(ctx); err != nil {
			return err
		}
		if no == 1 {
			if err := f.Schedule(ctx, p.NewTasksChan(feed)); err != nil {
				return err
			}
		} else {
			close(admitted2)
			if err := f.Schedule(ctx); err != nil {
				return err
			}
		}
		return commit.MoveTo(ctx)
	})
	<-done
}

// TestAdmitByPools_MixedWorkloadKeepsThePoolsBusy: item 1 blocks p2; items 2 and 3 need only p1 and both enter and run
// while item 1 is stuck, releasing read at once (under the strict policy read would fill with held items). Item 4 needs
// p2: it enters (p1 has room) and queues; item 5 then waits, kept out by the item limit of 4 — the bound on the p2
// queue — although p1 still has room.
func TestAdmitByPools_MixedWorkloadKeepsThePoolsBusy(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read")).SetLimit(2)
	f := c.AddFanOut(OptName("f")).SetLimit(4).SetAdmission(AdmitByPools)
	p1 := f.AddPool(OptName("p1")).SetLimit(3)
	p2 := f.AddPool(OptName("p2"))
	commit := c.AddStage(OptName("commit")).SetLimit(10)
	bt := newBlockingTasks()
	for _, k := range []string{"1/p2", "2/p1", "3/p1", "4/p2", "5/p1"} {
		bt.gate(k)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "items 2 and 3 running on p1", func() bool { return bt.hasStarted("2/p1") && bt.hasStarted("3/p1") })
		waitFor(t, "items 2 and 3 released read", func() bool {
			got := inBodyOf(c, read)
			return !slices.Contains(got, 2) && !slices.Contains(got, 3)
		})
		if held := heldItems(c); len(held) != 0 {
			t.Errorf("items holding upstream = %v, want none", held)
		}
		waitFor(t, "item 4 admitted with its p2 task queued", func() bool {
			return slices.Contains(inBodyOf(c, f), 4) && queueOccupancy(c, p2) == 1
		})
		waitFor(t, "item 5 in read", func() bool { return slices.Contains(inBodyOf(c, read), 5) })
		waitFor(t, "item 5 parked", func() bool { return parkedOf(c) >= 4 }) // leaves of 1, 2, 3 and item 5's move
		if got := inBodyOf(c, f); !slices.Equal(got, []int64{1, 2, 3, 4}) {
			t.Errorf("f = %v, want [1 2 3 4]: item 5 is kept out by the item limit", got)
		}
		if got := inBodyOf(c, read); !slices.Equal(got, []int64{5}) {
			t.Errorf("read = %v, want [5]", got)
		}
		close(bt.gate("1/p2")) // item 1 leaves f: item 4's p2 task starts and item 5 gets the freed item slot
		waitFor(t, "item 4 started on p2", func() bool { return bt.hasStarted("4/p2") })
		waitFor(t, "item 5 started on p1", func() bool { return bt.hasStarted("5/p1") })
		bt.releaseAll()
	}()

	runNOK(t, c, 5, func(ctx context.Context, no int64) error {
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := f.MoveTo(ctx); err != nil {
			return err
		}
		var task Task
		switch no {
		case 1, 4:
			task = bt.task(p2, no)
		default:
			task = bt.task(p1, no)
		}
		if err := f.Schedule(ctx, task); err != nil {
			return err
		}
		return commit.MoveTo(ctx)
	})
	<-done
}

// TestAdmitByPools_TryMoveToFollowsCapacity: TryMoveTo is declined while no pool has capacity and mutates nothing; once
// a pool frees it enters and releases read at once.
func TestAdmitByPools_TryMoveToFollowsCapacity(t *testing.T) {
	x := newTwoPoolFanOutWith(AdmitByPools, 2)
	c := x.c
	declined := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-declined
		if got := inBodyOf(c, x.f); !slices.Equal(got, []int64{1}) {
			t.Errorf("f = %v after the declined try, want [1]", got)
		}
		if got := inBodyOf(c, x.read); !slices.Equal(got, []int64{2}) {
			t.Errorf("read = %v after the declined try, want [2]", got)
		}
		close(x.bt.gate("1/p2"))
		waitFor(t, "p2 free", func() bool { return occupancyOf(c, x.p2) == 0 })
		close(x.processorGates[2])
		waitFor(t, "item 2 inside", func() bool { return slices.Contains(inBodyOf(c, x.f), 2) })
		waitFor(t, "read released", func() bool { return len(inBodyOf(c, x.read)) == 0 })
		if held := heldItems(c); len(held) != 0 {
			t.Errorf("items holding upstream = %v, want none", held)
		}
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
		if entered, err := x.f.TryMoveTo(ctx); err != nil || entered {
			t.Errorf("TryMoveTo = (%v, %v) with every pool full, want (false, nil)", entered, err)
		}
		close(declined)
		<-x.processorGates[2]
		if entered, err := x.f.TryMoveTo(ctx); err != nil || !entered {
			t.Errorf("TryMoveTo = (%v, %v) with p2 free, want (true, nil)", entered, err)
		}
		if err := x.f.Schedule(ctx, x.bt.task(x.p2, no)); err != nil {
			return err
		}
		return x.commit.MoveTo(ctx)
	})
	<-done
}

// TestAdmitByPools_SetAdmissionAcrossAllPolicies walks a live fan-out through the policies: by limit -> by pools blocks
// the next item while the pools are full; by pools -> strict gives the next admitted item a hold while the earlier
// ones have none; strict -> by pools lets a waiter that passes the gate in without a hold and leaves the existing hold
// to end on its own.
func TestAdmitByPools_SetAdmissionAcrossAllPolicies(t *testing.T) {
	c := NewConveyor()
	read := c.AddStage(OptName("read")).SetLimit(3)
	f := c.AddFanOut(OptName("f")).SetLimit(100) // AdmitByLimit to start with
	p1 := f.AddPool(OptName("p1"))
	p2 := f.AddPool(OptName("p2"))
	commit := c.AddStage(OptName("commit")).SetLimit(10)
	bt := newBlockingTasks()
	for _, k := range []string{"1/p1", "1/p2", "2/p2", "3/p2", "4/p1", "4/p2", "5/p2"} {
		bt.gate(k)
	}
	inside1 := make(chan struct{})
	go2 := make(chan struct{})
	go3 := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-inside1 // item 1 blocks both pools, admitted by limit
		f.SetAdmission(AdmitByPools)
		close(go2)
		waitFor(t, "item 2 in read", func() bool { return slices.Contains(inBodyOf(c, read), 2) })
		waitFor(t, "item 1's leave and item 2's move parked", func() bool { return parkedOf(c) >= 2 })
		if got := inBodyOf(c, f); !slices.Equal(got, []int64{1}) {
			t.Errorf("f = %v after the switch to AdmitByPools with full pools, want [1]", got)
		}
		close(bt.gate("1/p2")) // p2 frees: item 2 enters by pools, no hold, runs on p2 and blocks it again
		waitFor(t, "item 2 started on p2", func() bool { return bt.hasStarted("2/p2") })
		waitFor(t, "item 2 released read", func() bool { return !slices.Contains(inBodyOf(c, read), 2) })
		if held := heldItems(c); len(held) != 0 {
			t.Errorf("items holding upstream = %v under AdmitByPools, want none", held)
		}
		f.SetAdmission(AdmitByPoolsStrict)
		close(go3)
		waitFor(t, "item 3 in read", func() bool { return slices.Contains(inBodyOf(c, read), 3) })
		waitFor(t, "item 3 parked", func() bool { return parkedOf(c) >= 3 })
		close(bt.gate("2/p2")) // p2 frees: item 3 enters strict, its p2 task starts -> its hold ends at once
		waitFor(t, "item 3 started on p2", func() bool { return bt.hasStarted("3/p2") })
		waitFor(t, "item 3 released read", func() bool { return !slices.Contains(inBodyOf(c, read), 3) })
		// Item 4 needs p1 (blocked by item 1) and p2: with p2 full it waits in read.
		waitFor(t, "item 4 in read", func() bool { return slices.Contains(inBodyOf(c, read), 4) })
		waitFor(t, "item 4 parked", func() bool { return parkedOf(c) >= 4 })
		close(bt.gate("3/p2")) // p2 frees: item 4 enters strict, starts on p2, queues on p1 -> holds read
		waitFor(t, "item 4 holds read", func() bool { return slices.Equal(heldItems(c), []int64{4}) })
		waitFor(t, "item 4 started on p2", func() bool { return bt.hasStarted("4/p2") })
		f.SetAdmission(AdmitByPools) // back: the existing hold stays until its work starts
		// Item 5 needs p2: both pools are full, so it waits in read next to the held item 4.
		waitFor(t, "item 5 in read", func() bool { return slices.Equal(inBodyOf(c, read), []int64{4, 5}) })
		waitFor(t, "item 5 parked", func() bool { return parkedOf(c) >= 5 })
		if got := inBodyOf(c, f); slices.Contains(got, 5) {
			t.Errorf("f = %v, want item 5 kept out while every pool is full", got)
		}
		close(bt.gate("4/p2")) // p2 frees: item 5 passes the gate without a hold; item 4 keeps its hold
		waitFor(t, "item 5 admitted without a hold", func() bool {
			return slices.Contains(inBodyOf(c, f), 5) && slices.Equal(heldItems(c), []int64{4})
		})
		waitFor(t, "item 5 released read", func() bool { return slices.Equal(inBodyOf(c, read), []int64{4}) })
		close(bt.gate("1/p1")) // item 4's p1 task starts: the hold ends
		waitFor(t, "item 4's hold to end", func() bool { return len(heldItems(c)) == 0 })
		bt.releaseAll()
	}()

	runNOK(t, c, 5, func(ctx context.Context, no int64) error {
		switch no {
		case 2:
			<-go2
		case 3:
			<-go3
		}
		if err := read.MoveTo(ctx); err != nil {
			return err
		}
		if err := f.MoveTo(ctx); err != nil {
			return err
		}
		var tasks []Task
		switch no {
		case 1:
			tasks = []Task{bt.task(p1, no), bt.task(p2, no)}
		case 2, 3, 5:
			tasks = []Task{bt.task(p2, no)}
		case 4:
			tasks = []Task{bt.task(p1, no), bt.task(p2, no)}
		}
		if err := f.Schedule(ctx, tasks...); err != nil {
			return err
		}
		if no == 1 {
			close(inside1)
		}
		return commit.MoveTo(ctx)
	})
	<-done
}

// TestFanOutAdmission_ValuesAndString pins the getters: String for every policy, and an unknown value stored as
// AdmitByLimit.
func TestFanOutAdmission_ValuesAndString(t *testing.T) {
	for a, want := range map[FanOutAdmission]string{
		AdmitByLimit:        "AdmitByLimit",
		AdmitByPools:        "AdmitByPools",
		AdmitByPoolsStrict:  "AdmitByPoolsStrict",
		FanOutAdmission(7):  "FanOutAdmission(7)",
		FanOutAdmission(-1): "FanOutAdmission(-1)",
	} {
		if got := a.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
	f := NewConveyor().AddFanOut()
	if got := f.Admission(); got != AdmitByLimit {
		t.Errorf("default Admission() = %v, want AdmitByLimit", got)
	}
	for _, a := range []FanOutAdmission{AdmitByPools, AdmitByPoolsStrict, AdmitByLimit} {
		if got := f.SetAdmission(a).Admission(); got != a {
			t.Errorf("Admission() = %v after SetAdmission(%v)", got, a)
		}
	}
	if got := f.SetAdmission(FanOutAdmission(7)).Admission(); got != AdmitByLimit {
		t.Errorf("Admission() = %v after an unknown value, want AdmitByLimit", got)
	}
}

// TestAdmitByPools_SwitchToLimitAdmitsWaiters: a switch from AdmitByPools to AdmitByLimit while every pool is full lets
// the item parked at the door in at once, by the item limit alone.
func TestAdmitByPools_SwitchToLimitAdmitsWaiters(t *testing.T) {
	x := newTwoPoolFanOutWith(AdmitByPools, 2)
	c := x.c

	done := make(chan struct{})
	go func() {
		defer close(done)
		waitFor(t, "item 1 started on both pools", func() bool { return x.bt.hasStarted("1/p1") && x.bt.hasStarted("1/p2") })
		waitFor(t, "item 2 in read", func() bool { return slices.Equal(inBodyOf(c, x.read), []int64{2}) })
		waitFor(t, "item 1's leave and item 2's move to park", func() bool { return parkedOf(c) == 2 })
		if got := inBodyOf(c, x.f); !slices.Equal(got, []int64{1}) {
			t.Errorf("f = %v with every pool full, want [1]", got)
		}
		x.f.SetAdmission(AdmitByLimit)
		waitFor(t, "item 2 admitted by limit", func() bool { return slices.Contains(inBodyOf(c, x.f), 2) })
		waitFor(t, "read released", func() bool { return len(inBodyOf(c, x.read)) == 0 })
		if held := heldItems(c); len(held) != 0 {
			t.Errorf("items holding upstream = %v, want none", held)
		}
		close(x.processorGates[2])
		x.bt.releaseAll()
	}()

	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if no == 1 {
			return x.enterAndBlock(ctx, x.p1, x.p2)
		}
		if err := x.read.MoveTo(ctx); err != nil {
			return err
		}
		if err := x.f.MoveTo(ctx); err != nil {
			return err
		}
		<-x.processorGates[2]
		if err := x.f.Schedule(ctx, x.bt.task(x.p1, no)); err != nil {
			return err
		}
		return x.commit.MoveTo(ctx)
	})
	<-done
}
