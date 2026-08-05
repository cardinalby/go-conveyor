package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
)

// titem is one item flowing through the traditional pipeline; no is its 1-based
// number, used both as its ordering key and its Observer index.
type titem struct{ no int }

// traditionalPipeline is the hand-rolled comparison: long-lived goroutines per
// stage (or per slot for Shared/FanOut) connected by channels, exactly the shape
// a developer would write by hand. It restores item order after each Shared or
// FanOut section (a sequence-numbered reorder buffer) so its output order matches
// the conveyor's, making the two functionally equivalent.
type traditionalPipeline struct {
	spec Spec
	obs  *Observer
}

// NewTraditional builds a Pipeline backed by channels and goroutines from spec.
func NewTraditional(spec Spec, obs *Observer) Pipeline {
	return &traditionalPipeline{spec: spec, obs: obs}
}

// Run starts every stage's goroutines, feeds items 1..n through the head with
// backpressure, and returns once all n have exited the tail. Canceling ctx (done
// or caller abort) stops all goroutines; they are joined before Run returns so no
// goroutine leaks between runs.
func (p *traditionalPipeline) Run(ctx context.Context, n int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	feed := make(chan *titem)
	last := p.build(ctx, &wg, feed)

	// Sink: record completions in item order; stop once all n have exited.
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		got := 0
		for {
			select {
			case it := <-last:
				p.obs.itemFinished(it.no)
				got++
				if got == n {
					close(done)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Feeder: submit items 1..n. The unbuffered feed channel gives one-deep head
	// backpressure, mirroring the conveyor's start-stage gate.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= n; i++ {
			select {
			case feed <- &titem{no: i}:
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-done: // all n items finished: normal completion
		cancel()
		wg.Wait()
		return nil
	case <-ctx.Done(): // caller aborted before completion
		wg.Wait()
		return ctx.Err()
	}
}

// build wires the topology, starting all stage goroutines on wg, and returns the
// channel carrying finished items to the sink. Shared and FanOut nodes lose order
// across their concurrent workers, so each is followed by a reorder buffer that
// restores ascending item order before the next stage — matching the conveyor,
// whose next exclusive stage re-serializes items.
func (p *traditionalPipeline) build(ctx context.Context, wg *sync.WaitGroup, feed chan *titem) <-chan *titem {
	var prev <-chan *titem = feed
	for _, s := range p.spec.Stages {
		switch s.Kind {
		case Exclusive:
			next := make(chan *titem)
			rt := p.obs.runtime(s.Name, s.Work)
			in := prev // capture: prev is reassigned each iteration
			spawn(wg, func() { stageWorker(ctx, rt, in, next) })
			prev = next

		case Shared:
			raw := make(chan *titem)
			rt := p.obs.runtime(s.Name, s.Work)
			for w := 0; w < s.Limit; w++ {
				in := prev
				spawn(wg, func() { stageWorker(ctx, rt, in, raw) })
			}
			next := make(chan *titem)
			spawn(wg, func() { reorder(ctx, in2(raw), next) })
			prev = next

		case FanOut:
			raw := make(chan *titem)
			branchIns := make([]chan branchTask, len(s.Branches))
			branchRts := make([]*stageRuntime, len(s.Branches))
			for bi, b := range s.Branches {
				branchIns[bi] = make(chan branchTask)
				branchRts[bi] = p.obs.runtime(b.Name, b.Work)
				for w := 0; w < b.Limit; w++ {
					in := branchIns[bi]
					spawn(wg, func() { branchWorker(ctx, in) })
				}
			}
			dispIn := prev
			spawn(wg, func() { fanoutDispatch(ctx, dispIn, branchIns, branchRts, raw) })
			next := make(chan *titem)
			spawn(wg, func() { reorder(ctx, in2(raw), next) })
			prev = next
		}
	}
	return prev
}

// spawn runs fn as a tracked goroutine.
func spawn(wg *sync.WaitGroup, fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		fn()
	}()
}

// in2 narrows a bidirectional channel to receive-only (readability helper).
func in2(ch chan *titem) <-chan *titem { return ch }

// stageWorker processes items from in and forwards them to out, one at a time.
// A pool of these on a shared in/out is a Shared stage. It exits when ctx is
// canceled (channels are never closed; cancellation is the sole stop signal).
func stageWorker(ctx context.Context, rt *stageRuntime, in <-chan *titem, out chan<- *titem) {
	for {
		select {
		case it := <-in:
			rt.process(ctx, it.no)
			select {
			case out <- it:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// reorder emits items in ascending item order, buffering out-of-order arrivals.
// It restores the order that a Shared/FanOut section's concurrent workers lose.
func reorder(ctx context.Context, in <-chan *titem, out chan<- *titem) {
	pending := make(map[int]*titem)
	next := 1
	for {
		select {
		case it := <-in:
			pending[it.no] = it
			for {
				nx, ok := pending[next]
				if !ok {
					break
				}
				select {
				case out <- nx:
				case <-ctx.Done():
					return
				}
				delete(pending, next)
				next++
			}
		case <-ctx.Done():
			return
		}
	}
}

// branchTask is one item's work for one fan-out branch, plus the shared join
// state that forwards the item once every branch has finished it.
type branchTask struct {
	it   *titem
	rt   *stageRuntime
	left *atomic.Int32 // remaining branches for this item; last one forwards
	out  chan<- *titem
}

// branchWorker runs one branch's tasks. A pool of these per branch gives the
// branch its concurrency limit.
func branchWorker(ctx context.Context, in <-chan branchTask) {
	for {
		select {
		case t := <-in:
			t.rt.process(ctx, t.it.no)
			if t.left.Add(-1) == 0 {
				select {
				case t.out <- t.it:
				case <-ctx.Done():
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// fanoutDispatch reads each item and hands one task to every branch's pool,
// tagged with shared join state. Per-branch unbuffered channels backpressure the
// dispatcher so at most Limit of each branch's tasks are in flight, which is what
// saturates the branches. The item is forwarded (to reorder) by whichever branch
// worker finishes it last.
func fanoutDispatch(ctx context.Context, in <-chan *titem, branchIns []chan branchTask, branchRts []*stageRuntime, out chan<- *titem) {
	for {
		select {
		case it := <-in:
			left := &atomic.Int32{}
			left.Store(int32(len(branchIns)))
			for i := range branchIns {
				t := branchTask{it: it, rt: branchRts[i], left: left, out: out}
				select {
				case branchIns[i] <- t:
				case <-ctx.Done():
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
