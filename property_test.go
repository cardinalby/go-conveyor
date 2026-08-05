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

// This file is a randomized invariant checker: each seed builds a small random topology, pushes a random number of
// items through it with random per-item choices, and asserts the package's structural guarantees — commit order,
// capacity, completion, no leaks and the documented Run result. Seeds are explicit and reported on every failure, so
// a failing scenario can be reproduced by running the named subtest.

// propSeeds is how many scenarios each randomized test runs. Each one is small on purpose: many cheap topologies
// find more than one big one.
const propSeeds = 24

// errPropBoom is the failure the fail-fast scenarios inject.
var errPropBoom = errors.New("property boom")

// injection kinds — where a fail-fast scenario plants errPropBoom.
const (
	injProc   = iota // the ItemProcessor returns it at a stage
	injRetain        // a Stage.Retain bgOp returns it
	injTask          // a task on a lane without interior nodes returns it
	injChild         // a child item on a lane with interior nodes returns it
)

// propInjection names the single failure a fail-fast scenario plants: which item, which root node, and (for lane
// work) which lane of that node.
type propInjection struct {
	kind int
	node int
	lane int
	item int64
}

func (inj propInjection) String() string {
	kinds := [...]string{"processor error", "retain error", "task error", "child error"}
	return fmt.Sprintf("%s at item %d, node %d, lane %d", kinds[inj.kind], inj.item, inj.node, inj.lane)
}

// propGauge tracks the live occupancy of one instrumented unit against the limit it was declared with. Every
// instrumented region lies strictly inside the window in which its unit's slot is held, so the peak can never
// legitimately exceed the limit.
// Its limit is a CEILING rather than a constant, because a scenario may resize the unit while items are inside it
// (see propJitter). A raise is recorded here before it is applied to the conveyor, so the ceiling is never lower than
// what the conveyor is currently willing to admit, and "peak <= ceiling" stays a real bound: over-admission beyond
// anything ever configured still fails.
type propGauge struct {
	name string

	mu    sync.Mutex
	limit int
	cur   int
	peak  int
}

// raiseCeiling records a new configured limit. Only raises matter: a lower is admission-only, so the peak already
// reached under the higher limit remains legitimate.
func (p *propGauge) raiseCeiling(limit int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if limit > p.limit {
		p.limit = limit
	}
}

func (p *propGauge) enter() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cur++
	if p.cur > p.peak {
		p.peak = p.cur
	}
}

func (p *propGauge) leave() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cur--
}

func (p *propGauge) snapshot() (cur, peak, ceiling int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cur, p.peak, p.limit
}

// propStage is a stage plus the gauge watching its inline region.
type propStage struct {
	st Stage
	g  *propGauge
}

// propFanOut is a fan-out plus its instrumented branches.
type propFanOut struct {
	fo    FanOut
	lanes []*propLane
}

// propLane is one branch: its entrance gauge, and (when it is a Lane) the nodes its children travel. branch is the
// task-construction surface either kind offers; lane is set only for the travelling kind, so the two are what tell a
// pool from a lane here.
type propLane struct {
	branch  Branch     // AddPool or AddLane result — always set
	lane    Lane       // the same handle, set only when this branch travels
	g       *propGauge // the branch's entrance
	travels bool
	inner   *propFanOut // a rare interior fan-out, entered first
	stages  []*propStage
}

// propNode is one root node of the generated topology: either a stage or a fan-out.
type propNode struct {
	stage  *propStage
	fanOut *propFanOut
}

// propJitter is one node a scenario may resize while the conveyor runs. setQueue is nil for a lane, which has no
// waiting room of its own; g is nil for a fan-out node, whose occupancy no instrumented region measures.
type propJitter struct {
	name     string
	setLimit func(int)
	setQueue func(int)
	g        *propGauge
}

// propTopology is a generated conveyor plus everything the invariants are checked against.
type propTopology struct {
	c       *Conveyor
	seed    int64
	nodes   []propNode
	commit  Stage
	commitG *propGauge
	gauges  []*propGauge
	// jitters lists every resizable node EXCEPT commit: the order invariant is asserted at commit, and it holds only
	// while commit stays exclusive.
	jitters []propJitter

	commitOrder numbers
	scheduled   atomic.Int64 // pieces of background work handed to the conveyor
	ran         atomic.Int64 // pieces that actually ran
}

func (top *propTopology) newGauge(name string, limit int) *propGauge {
	g := &propGauge{name: name, limit: limit}
	top.gauges = append(top.gauges, g)
	return g
}

// buildPropTopology draws a small random topology: 2-6 root nodes (stages with optional queues, or fan-outs with
// 1-3 lanes, some of which have interior nodes of their own), always ending in an exclusive commit stage.
func buildPropTopology(rnd *rand.Rand, seed int64) *propTopology {
	top := &propTopology{c: NewConveyor(), seed: seed}
	for i, n := 0, 2+rnd.Intn(5); i < n; i++ {
		if rnd.Intn(2) == 0 {
			name := fmt.Sprintf("stage%d", i)
			limit := 1 + rnd.Intn(3)
			st := top.c.AddStage(OptName(name)).SetLimit(limit)
			if rnd.Intn(3) == 0 {
				st.SetQueueSize(1 + rnd.Intn(3))
			}
			g := top.newGauge(name, limit)
			top.jitters = append(top.jitters, propJitter{
				name: name, g: g,
				setLimit: func(n int) { st.SetLimit(n) },
				setQueue: func(n int) { st.SetQueueSize(n) },
			})
			top.nodes = append(top.nodes, propNode{stage: &propStage{st: st, g: g}})
			continue
		}
		name := fmt.Sprintf("fo%d", i)
		fo := top.c.AddFanOut(OptName(name)).SetLimit(1 + rnd.Intn(3))
		if rnd.Intn(4) == 0 {
			fo.SetQueueSize(1 + rnd.Intn(3))
		}
		top.jitters = append(top.jitters, propJitter{
			name:     name,
			setLimit: func(n int) { fo.SetLimit(n) },
			setQueue: func(n int) { fo.SetQueueSize(n) },
		})
		pf := &propFanOut{fo: fo}
		for j, nl := 0, 1+rnd.Intn(3); j < nl; j++ {
			pf.lanes = append(pf.lanes, top.buildLane(rnd, fo, fmt.Sprintf("%s.l%d", name, j), true))
		}
		top.nodes = append(top.nodes, propNode{fanOut: pf})
	}
	top.commit = top.c.AddStage(OptName("commit")) // exclusive: the order-preserving end of the flow
	top.commitG = top.newGauge("commit", 1)
	return top
}

// buildLane draws one branch of fo. The shape is drawn *first*, because it decides the kind: a branch with interior
// nodes must be a Lane (its work travels as child items), and one without must be a Pool (which is the only kind with
// an entrance limit to tune). With interiors allowed it sometimes gets 1-2 stages and rarely an interior fan-out.
func (top *propTopology) buildLane(rnd *rand.Rand, fo FanOut, name string, interiors bool) *propLane {
	stages, innerFanOut := 0, false
	if interiors {
		stages = rnd.Intn(3)  // 0, 1 or 2 interior stages
		if rnd.Intn(6) == 0 { // rarely, an interior fan-out — entered before the stages so its wave can be joined
			innerFanOut = true
			if stages == 0 {
				stages = 1
			}
		}
	}
	pl := &propLane{travels: stages > 0 || innerFanOut}

	if !pl.travels {
		limit := 1 + rnd.Intn(3)
		p := fo.AddPool(OptName(name)).SetLimit(limit)
		pl.branch, pl.g = p, top.newGauge(name, limit)
		top.jitters = append(top.jitters, propJitter{
			name: name, g: pl.g,
			setLimit: func(n int) { p.SetLimit(n) }, // a pool has no waiting room to resize
		})
		return pl
	}

	// A lane's entrance is fixed at one child at a time, so there is no limit to draw and nothing to jitter here —
	// the tunable capacity lives on the interior nodes below.
	l := fo.AddLane(OptName(name))
	pl.branch, pl.lane = l, l
	pl.g = top.newGauge(name, 1)

	if innerFanOut {
		inner := l.AddFanOut(OptName(name + ".ifo")).SetLimit(1 + rnd.Intn(2))
		pf := &propFanOut{fo: inner}
		for j, nl := 0, 1+rnd.Intn(2); j < nl; j++ {
			pf.lanes = append(pf.lanes, top.buildLane(rnd, inner, fmt.Sprintf("%s.ifo.l%d", name, j), false))
		}
		pl.inner = pf
	}
	for k := 0; k < stages; k++ {
		sname := fmt.Sprintf("%s.s%d", name, k)
		slimit := 1 + rnd.Intn(3)
		st := l.AddStage(OptName(sname)).SetLimit(slimit)
		sg := top.newGauge(sname, slimit)
		top.jitters = append(top.jitters, propJitter{
			name: sname, g: sg,
			setLimit: func(n int) { st.SetLimit(n) },
			setQueue: func(n int) { st.SetQueueSize(n) },
		})
		pl.stages = append(pl.stages, &propStage{st: st, g: sg})
	}
	return pl
}

// spin hands the scheduler a few chances to interleave, so the instrumented regions actually overlap in time and the
// capacity assertions have something to catch. It must stay cheap: no sleeping.
func spin() {
	for i := 0; i < 3; i++ {
		runtime.Gosched()
	}
}

// process is the ItemProcessor of a generated scenario: it walks the root nodes, skipping some, scheduling random
// work on the fan-outs' lanes, joining waves either at the next node or later, and finally committing. With inj set
// it instead plants exactly one failure (and takes no random shortcuts, so the failure is always reached).
func (top *propTopology) process(ctx context.Context, no int64, inj *propInjection) error {
	rnd := rand.New(rand.NewSource(top.seed*7919 + no*31))
	failing := inj != nil && inj.item == no
	var pending []Wave

	for i := range top.nodes {
		nd := top.nodes[i]
		if inj == nil && rnd.Intn(6) == 0 {
			continue // skip this node entirely
		}
		var joins []Wave
		if inj == nil && len(pending) > 0 && rnd.Intn(2) == 0 {
			joins, pending = pending, nil // join here; otherwise the join is deferred to a later node
		}

		if nd.stage != nil {
			if err := nd.stage.st.MoveTo(ctx, joins...); err != nil {
				return err
			}
			// The instrumented region lies between two moves, so it is strictly inside the stage's slot.
			nd.stage.g.enter()
			spin()
			injectRetain := failing && inj.kind == injRetain && inj.node == i
			if injectRetain || rnd.Intn(4) == 0 {
				top.scheduled.Add(1)
				pending = append(pending, nd.stage.st.Retain(ctx, func() error {
					top.ran.Add(1)
					if injectRetain {
						return errPropBoom
					}
					return nil
				}))
			}
			nd.stage.g.leave()
			if failing && inj.kind == injProc && inj.node == i {
				return errPropBoom
			}
		} else {
			err := nd.fanOut.fo.MoveTo(ctx, top.fanOutTasks(ctx, nd.fanOut, rnd, failing, inj, i), joins...)
			if err != nil {
				return err
			}
			w := nd.fanOut.fo.Detach(ctx)
			pending = append(pending, w)
		}

		if inj == nil && rnd.Intn(14) == 0 {
			return nil // return early; whatever was scheduled is still joined at completion
		}
	}

	if err := top.commit.MoveTo(ctx, pending...); err != nil {
		return err
	}
	top.commitG.enter()
	spin()
	top.commitG.leave()
	top.commitOrder.add(no)
	return nil
}

// fanOutTasks draws 0-3 tasks per lane of one fan-out, planting the injected failure on the chosen lane. The source
// KIND is drawn too, so a scenario exercises the lazy eager source and both streaming ones — whose pulls run user code
// outside the conveyor's lock, single-flight per source — under the same random topologies and interleavings as
// everything else.
func (top *propTopology) fanOutTasks(
	ctx context.Context, pf *propFanOut, rnd *rand.Rand, failing bool, inj *propInjection, nodeIdx int,
) Tasks {
	var tasks Tasks
	for li, pl := range pf.lanes {
		inject := failing && (inj.kind == injTask || inj.kind == injChild) && inj.node == nodeIdx && inj.lane == li
		n := rnd.Intn(4)
		if inject && n == 0 {
			n = 1 // the failure must actually be scheduled
		}
		if n == 0 {
			continue
		}
		top.scheduled.Add(int64(n))
		tasks = append(tasks, top.laneTask(ctx, pl, rnd.Intn(3), n, inject))
	}
	return tasks
}

// laneTask wraps n pieces of a lane's work in one of the three source kinds. All three produce exactly n callbacks
// when the item is allowed to finish, so the COMPLETION invariant (scheduled == ran) is the same whichever is drawn.
func (top *propTopology) laneTask(ctx context.Context, pl *propLane, kind, n int, inject bool) Task {
	work := top.laneWork(pl, inject)
	fn := func(i int) TaskFunc {
		return func(cctx context.Context) error { return work(cctx, i) }
	}
	switch kind {
	case 0:
		return pl.branch.NewTasks(n, work)
	case 1:
		return pl.branch.NewTasksGen(func(yield func(TaskFunc) bool) {
			for i := 0; i < n; i++ {
				if !yield(fn(i)) {
					return
				}
			}
		})
	default:
		// The producer must respect the item's ctx and must close, or a canceled item would leave it blocked on a
		// channel nobody reads — which assertNoLeaks would catch.
		ch := make(chan TaskFunc)
		go func() {
			defer close(ch)
			for i := 0; i < n; i++ {
				select {
				case ch <- fn(i):
				case <-ctx.Done():
					return
				}
			}
		}()
		return pl.branch.NewTasksChan(ch)
	}
}

// laneWork is one piece of a branch's work. On a pool it just runs; on a lane it is a child item that travels the
// lane's interior fan-out and stages.
func (top *propTopology) laneWork(pl *propLane, inject bool) func(context.Context, int) error {
	return func(cctx context.Context, _ int) error {
		pl.g.enter()
		if !pl.travels {
			defer pl.g.leave()
			spin()
			top.ran.Add(1)
			if inject {
				return errPropBoom
			}
			return nil
		}
		// A child gives up the lane's entrance on its first move, so the instrumented region ends before it.
		spin()
		pl.g.leave()

		var iw Wave
		if pl.inner != nil {
			if err := pl.inner.fo.MoveTo(cctx, top.innerTasks(pl.inner)); err != nil {
				return err
			}
			iw = pl.inner.fo.Detach(cctx)
		}
		for k, si := range pl.stages {
			var joins []Wave
			if k == 0 && iw != nil {
				joins = append(joins, iw)
			}
			if err := si.st.MoveTo(cctx, joins...); err != nil {
				return err
			}
			si.g.enter()
			spin()
			si.g.leave()
		}
		top.ran.Add(1)
		if inject {
			return errPropBoom
		}
		return nil
	}
}

// innerTasks schedules a fixed amount of work on an interior fan-out's lanes (fixed, because this runs on many child
// goroutines at once and must not share a random source).
func (top *propTopology) innerTasks(pf *propFanOut) Tasks {
	var tasks Tasks
	for _, pl := range pf.lanes {
		top.scheduled.Add(2)
		tasks = append(tasks, pl.branch.NewTasks(2, top.laneWork(pl, false)))
	}
	return tasks
}

// jitterCapacities resizes random nodes for as long as the run lasts, so every scenario meets live SetLimit and
// SetQueueSize calls landing at arbitrary moments — including on nodes that are saturated, that have items waiting in
// front of them, or that are mid-fan-out. It runs on its own goroutine, which is the point: the resize races the
// admission path rather than being sequenced with it.
//
// A raise is recorded on the gauge BEFORE the conveyor is told about it, so the ceiling the capacity invariant checks
// is never behind what the conveyor will admit.
func (top *propTopology) jitterCapacities(stop <-chan struct{}, seed int64) {
	rnd := rand.New(rand.NewSource(seed * 104729))
	for {
		select {
		case <-stop:
			return
		default:
		}
		j := top.jitters[rnd.Intn(len(top.jitters))]
		limit := 1 + rnd.Intn(4)
		if j.g != nil {
			j.g.raiseCeiling(limit)
		}
		j.setLimit(limit)
		if j.setQueue != nil {
			j.setQueue(rnd.Intn(4)) // 0 included: taking a waiting room away entirely
		}
		time.Sleep(50 * time.Microsecond) // often enough to interleave, cheap enough not to dominate the run
	}
}

// pickInjection chooses one place in the topology to plant errPropBoom, and one of the first few items to plant it
// in.
func (top *propTopology) pickInjection(rnd *rand.Rand) *propInjection {
	var cands []propInjection
	for i := range top.nodes {
		if top.nodes[i].stage != nil {
			cands = append(cands, propInjection{kind: injProc, node: i}, propInjection{kind: injRetain, node: i})
			continue
		}
		for li, pl := range top.nodes[i].fanOut.lanes {
			kind := injTask
			if pl.travels {
				kind = injChild
			}
			cands = append(cands, propInjection{kind: kind, node: i, lane: li})
		}
	}
	inj := cands[rnd.Intn(len(cands))]
	inj.item = int64(1 + rnd.Intn(4))
	return &inj
}

// --- assertions ---

// assertCommitOrder pins the ORDER invariant: the exclusive commit stage saw the items it saw in strictly increasing
// order (items that skipped it or failed simply do not appear).
func (top *propTopology) assertCommitOrder(t *testing.T) {
	t.Helper()
	got := top.commitOrder.all()
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("seed %d: commit saw %v, want strictly increasing item numbers", top.seed, got)
		}
	}
}

// assertCapacity pins the CAPACITY invariant: no unit's instrumented occupancy ever exceeded the limit it was
// declared with, and nothing is left inside one.
func (top *propTopology) assertCapacity(t *testing.T) {
	t.Helper()
	for _, g := range top.gauges {
		cur, peak, ceiling := g.snapshot()
		if peak > ceiling {
			t.Errorf("seed %d: %s peaked at %d, above its limit %d", top.seed, g.name, peak, ceiling)
		}
		if cur != 0 {
			t.Errorf("seed %d: %s still has %d occupants after the run", top.seed, g.name, cur)
		}
	}
}

// assertNoLeaks pins the NO LEAKS invariant: the run state is gone (Stats reports the zero value once the run is
// over) and the goroutines it spawned are gone too.
func (top *propTopology) assertNoLeaks(t *testing.T, baseGoroutines int) {
	t.Helper()
	if s := top.c.Stats(); len(s.Units) != 0 || s.InFlight != (Gauge{}) || s.LiveWorkers != (Gauge{}) {
		t.Errorf("seed %d: Stats after the run = %+v, want the zero Stats", top.seed, s)
	}
	waitFor(t, sprintf("seed %d: goroutines to settle back to %d", top.seed, baseGoroutines), func() bool {
		return runtime.NumGoroutine() <= baseGoroutines+4
	})
}

// --- the randomized tests ---

// TestPropertyRandomTopologiesHoldInvariants is the main invariant checker: over many random topologies and random
// per-item behavior, commit order, capacity, completion, the absence of leaks and Run's result all hold.
func TestPropertyRandomTopologiesHoldInvariants(t *testing.T) {
	for i := 0; i < propSeeds; i++ {
		seed := int64(1_000 + i*7)
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rnd := rand.New(rand.NewSource(seed))
			top := buildPropTopology(rnd, seed)
			items := int64(10 + rnd.Intn(31)) // 10..40

			base := runtime.NumGoroutine()
			err := runUntil(t, top.c, items, func(ctx context.Context, no int64) error {
				return top.process(ctx, no, nil)
			})
			// RUN RESULT: nothing failed, so Run reports the run context's cancellation cause.
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("seed %d: Run = %v, want the run context's cause", seed, err)
			}
			top.assertCommitOrder(t)
			top.assertCapacity(t)
			// COMPLETION: everything scheduled ran, since nothing was ever cancelled.
			if sched, ran := top.scheduled.Load(), top.ran.Load(); sched != ran {
				t.Errorf("seed %d: %d pieces of work were scheduled but %d ran", seed, sched, ran)
			}
			if len(top.commitOrder.all()) == 0 {
				t.Errorf("seed %d: no item ever committed; the scenario tested nothing", seed)
			}
			top.assertNoLeaks(t, base)
		})
	}
}

// TestPropertyRandomFailFast injects exactly one failure at a random point of a random topology and pins the
// fail-fast contract: Run reports that error, no item at or after the failing one commits, and the run drains.
func TestPropertyRandomFailFast(t *testing.T) {
	for i := 0; i < propSeeds; i++ {
		seed := int64(5_000 + i*13)
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rnd := rand.New(rand.NewSource(seed))
			top := buildPropTopology(rnd, seed)
			inj := top.pickInjection(rnd)

			base := runtime.NumGoroutine()
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()
			// A hang shows up as the timeout's cause instead of the injected error.
			err := top.c.Run(ctx, func(ic context.Context) error {
				no, _ := ItemNoFromContext(ic)
				return top.process(ic, no, inj)
			})
			if !errors.Is(err, errPropBoom) {
				t.Fatalf("seed %d: Run = %v, want the injected %v (%s)", seed, err, errPropBoom, inj)
			}
			for _, committed := range top.commitOrder.all() {
				if committed >= inj.item {
					t.Errorf("seed %d: item %d committed although item %d failed (%s)",
						seed, committed, inj.item, inj)
				}
			}
			top.assertCommitOrder(t)
			top.assertCapacity(t)
			top.assertNoLeaks(t, base)
		})
	}
}

// TestPropertyCapacityJitterHoldsInvariants is the invariant checker again, with every node's limit and waiting room
// being resized from another goroutine throughout the run. It is a separate test rather than an option on the base
// one so a failure says whether live resizing is implicated.
//
// Capacity changes are admission-only in both directions, so none of the invariants may bend: order still holds at the
// exclusive commit stage (which is the one node never resized), no instrumented region ever exceeds the highest limit
// it was ever given, and — since nothing is cancelled — everything scheduled still runs.
func TestPropertyCapacityJitterHoldsInvariants(t *testing.T) {
	for i := 0; i < propSeeds; i++ {
		seed := int64(9_000 + i*11)
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rnd := rand.New(rand.NewSource(seed))
			top := buildPropTopology(rnd, seed)
			items := int64(10 + rnd.Intn(31))

			base := runtime.NumGoroutine()
			stop := make(chan struct{})
			jitterDone := make(chan struct{})
			go func() {
				defer close(jitterDone)
				top.jitterCapacities(stop, seed)
			}()

			err := runUntil(t, top.c, items, func(ctx context.Context, no int64) error {
				return top.process(ctx, no, nil)
			})
			close(stop)
			<-jitterDone

			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("seed %d: Run = %v, want the run context's cause", seed, err)
			}
			top.assertCommitOrder(t)
			top.assertCapacity(t)
			if sched, ran := top.scheduled.Load(), top.ran.Load(); sched != ran {
				t.Errorf("seed %d: %d pieces of work were scheduled but %d ran", seed, sched, ran)
			}
			if len(top.commitOrder.all()) == 0 {
				t.Errorf("seed %d: no item ever committed; the scenario tested nothing", seed)
			}
			top.assertNoLeaks(t, base)
		})
	}
}
