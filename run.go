package conveyor

import (
	"context"
	"sync"
)

// run holds the mutable state of a single Run invocation. A fresh run is allocated per Run so state never leaks
// between invocations. All fields below mu are guarded by mu; the state transitions and gate logic that operate on
// them live in state.go.
type run struct {
	conveyor    *Conveyor
	proc        ItemProcessor
	itemsCtx    context.Context         // parent of every root item context; canceled when Run returns
	cancelItems context.CancelCauseFunc // cancels itemsCtx (and thus every item context) with a cause

	mu sync.Mutex
	// cond is broadcast (never signaled) on every state mutation: waiters block on different conditions, so a
	// targeted wake is not possible and all of them must re-check. This is O(waiters) wake-ups per event.
	cond       *sync.Cond
	occupancy  []windowedInt       // slots of the node itself in use per unit index, windowed for Stats
	queued     []windowedInt       // items waiting in front of the node, per unit index (see unit.queueSize)
	taskQueues [][]*taskCollection // per-branch FIFO queues (by unit index) of items' task collections, in item order
	scopes     []scopeList         // in-flight items per scope (0 = the root series, one per branch)

	// occupancy (per unit), inFlight and liveWorkers each carry their own min/max window since the last Stats
	// read; Stats reports and resets it (see windowedInt / Stats), capturing transients a point-in-time sample
	// would miss. All guarded by mu.
	inFlight    windowedInt
	liveWorkers windowedInt

	nextItemNo int64 // last number assigned to a root item this run; a child inherits its parent's (see item.no)
	idle       int   // workers parked in acquireItem waiting for the start stage (0 or 1; extras retire)
	// spawning counts workers that have been started but have not yet reached acquireItem. Together with idle it
	// answers "is a worker already on its way to take the next item", which is what the replacement decision in
	// acquireItem needs. Counting only idle would ignore a worker that exists but has not been scheduled yet, so a
	// fast ItemProcessor would spawn a fresh goroutine per item — unboundedly, since the pool never gets a chance
	// to park (worst on a single-P runtime, where the new goroutines simply pile up on the run queue).
	spawning     int
	stopCreating bool           // once set, no new root items are created and idle workers exit
	runErr       error          // first non-shutdown item error; becomes Run's result
	workers      sync.WaitGroup // all root-item worker goroutines

	// shutdownCh is closed once, when shutdown begins from either trigger (caller-context cancellation or an
	// item error). watchShutdown waits on it to bound how long the in-flight items may keep running.
	shutdownOnce sync.Once
	shutdownCh   chan struct{}
}

// ItemProcessor processes one item, moving it through the conveyor's nodes with Stage.MoveTo / FanOut.MoveTo,
// using the context it receives. It need not enter every node and may return early.
//
// Returning an error shuts the conveyor down: no new items are created, later items are canceled, and earlier ones
// are allowed to finish.
type ItemProcessor func(ctx context.Context) error

// Run starts the conveyor, creating one item per itemProcessor invocation until ctx is canceled or an item fails.
// It blocks until every in-flight item has finished, then returns the first item error, or else ctx's cancellation
// cause.
//
// Build all nodes before calling Run; the topology is frozen from the first Run on. Run may be called again after
// it returns, but a concurrent second call returns ErrConveyorAlreadyRunning.
func (c *Conveyor) Run(ctx context.Context, itemProcessor ItemProcessor) error {
	if err := c.tryRun(); err != nil {
		return err
	}
	defer c.stopRun()

	r := c.newRun()
	r.proc = itemProcessor
	r.itemsCtx, r.cancelItems = context.WithCancelCause(context.Background())
	defer r.cancelItems(nil) // release any item contexts still lingering when Run returns

	c.currentRun.Store(r)
	defer c.currentRun.Store(nil)

	// Watch the caller's context; its cancellation begins a graceful shutdown.
	stopWatch := make(chan struct{})
	watcherDone := make(chan struct{})
	go r.watchShutdown(ctx, stopWatch, watcherDone)

	r.mu.Lock()
	r.spawnWorker() // the pool grows on demand from here (see acquireItem)
	r.mu.Unlock()
	r.workers.Wait()

	close(stopWatch)
	<-watcherDone

	r.mu.Lock()
	runErr := r.runErr
	r.mu.Unlock()
	if runErr != nil {
		return runErr
	}
	return context.Cause(ctx)
}

func (c *Conveyor) tryRun() error {
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if c.isRunning {
		return ErrConveyorAlreadyRunning
	}
	c.finalize() // assign scopes and ranks once, under runMu; the topology is frozen from here on
	c.isRunning = true
	return nil
}

func (c *Conveyor) stopRun() {
	c.runMu.Lock()
	c.isRunning = false
	c.runMu.Unlock()
}

// watchShutdown waits for shutdown to begin — from either trigger: the caller cancels ctx, or an item error
// closes shutdownCh — and then bounds how long the in-flight items may keep running: it asks the configured
// ShutdownContextFactory for the shutdown context (see OptShutdownContext) and cancels the items once that context
// is done. No factory, or a nil context from it, leaves the items to finish on their own.
//
// It exits early (without touching in-flight items, and without asking the factory) if the run drains before any
// shutdown, which the closing of stopWatch signals.
func (r *run) watchShutdown(ctx context.Context, stopWatch <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	select {
	case <-stopWatch:
		return // run finished without a shutdown; nothing to do
	case <-ctx.Done():
		r.beginShutdown()
	case <-r.shutdownCh:
		// shutdown already begun by an item error
	}

	factory := r.conveyor.shutdownCtxFactory
	if factory == nil {
		return // no limit: in-flight items are left to finish
	}
	shutdownCtx, cancel := factory(r.shutdownCause(ctx))
	if cancel != nil {
		// Release the shutdown context (a timer, typically) as soon as it is out of use: the watcher outlives the
		// items it bounds by nothing, and Run joins it before returning.
		defer cancel()
	}
	if shutdownCtx == nil {
		return // the caller declined a limit for this shutdown
	}
	select {
	case <-shutdownCtx.Done():
		r.cancelInFlight(ctx) // the items outlived the shutdown context
	case <-stopWatch:
	}
}

// beginShutdown stops new items from being created and wakes all waiters so idle workers exit and blocked node
// calls re-evaluate. In-flight items are otherwise left running.
func (r *run) beginShutdown() {
	r.mu.Lock()
	r.markShutdownLocked()
	r.cond.Broadcast()
	r.mu.Unlock()
}

// markShutdownLocked records that shutdown has begun: it stops new items from being created and signals the
// watcher (which applies shutdownTimeout). Idempotent. The caller holds r.mu and must broadcast.
func (r *run) markShutdownLocked() {
	r.stopCreating = true
	r.shutdownOnce.Do(func() { close(r.shutdownCh) })
}

// cancelInFlight cancels every in-flight item's context with a ShutdownError by canceling their common parent, so
// items blocked in a node method return promptly (child items are canceled with them, their contexts descending
// from their parents'). Code inside an ItemProcessor that ignores its context (e.g. a bare channel receive) is not
// forcibly interrupted — only node calls unblock. Items already canceled individually (the error cascade in
// completeItem) keep their more specific cause, since the first cancellation of a context wins.
func (r *run) cancelInFlight(ctx context.Context) {
	r.cancelItems(&shutdownError{cause: r.shutdownCause(ctx)})
	r.mu.Lock()
	r.cond.Broadcast()
	r.mu.Unlock()
}

// shutdownCause is why the shutdown happened: the first item error if one was recorded, else the Run context's
// cancellation cause. It is what the canceled items see as their ShutdownError's cause, and what the
// ShutdownContextFactory is told. Deliberately not the shutdown context's own cause: an expired grace period says
// nothing an item does not already know, while the trigger — which SIGTERM, which item error — does. It is read
// afresh at each use, so an item error recorded after the factory was asked still wins.
func (r *run) shutdownCause(ctx context.Context) error {
	r.mu.Lock()
	cause := r.runErr
	r.mu.Unlock()
	if cause == nil {
		cause = context.Cause(ctx)
	}
	return cause
}

// spawnWorker starts one worker goroutine. Workers self-propagate via acquireItem, so the pool grows to match the
// number of in-flight items. The worker maintains the live-worker count itself (under mu). Caller holds mu (so the
// spawning count and the decision that led here are one atomic step).
func (r *run) spawnWorker() {
	r.spawning++
	r.workers.Add(1)
	go r.worker()
}

func (r *run) worker() {
	r.mu.Lock()
	r.liveWorkers.add(1)
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.liveWorkers.add(-1)
		r.mu.Unlock()
		r.workers.Done()
	}()
	// arrived tells acquireItem to clear this worker's spawning reservation; only its first call does, since from
	// then on the worker is accounted for by what it is doing (running an item, parked, or gone).
	for arrived := true; ; arrived = false {
		it := r.acquireItem(arrived)
		if it == nil {
			return
		}
		r.completeItem(it, r.proc(it.ctx))
	}
}

// acquireItem blocks until this worker may create the next root item — the start stage has room and the
// items-in-flight cap (SetItemsLimit), if any, allows it — and returns it, or returns nil when the worker should
// exit: the conveyor has stopped creating items, or another worker is already standing by. At most one worker
// waits idle — extra workers retire, so the pool shrinks back after a burst instead of keeping a herd of idle
// waiters that every cond.Broadcast wakes. When creating an item leaves nobody standing by, it spawns a
// replacement so a worker is always ready for the following item.
//
// "Standing by" means idle+spawning: a worker that has been spawned but not yet scheduled is already on its way
// here and must not be duplicated (see run.spawning). arrived is set by a worker's first call, which is where its
// own spawning reservation is cleared.
func (r *run) acquireItem(arrived bool) *item {
	r.mu.Lock()
	defer r.mu.Unlock()
	if arrived {
		r.spawning--
	}
	for {
		if r.stopCreating {
			return nil
		}
		if r.unitHasFreeSlot(0) && r.hasItemsRoom() { // room at the start stage, and under the items cap -> create
			it := r.newRootItem()
			if r.idle+r.spawning == 0 {
				r.spawnWorker()
			}
			return it
		}
		if r.idle+r.spawning > 0 {
			return nil // a worker is already standing by for the start stage; this one retires
		}
		r.idle++
		r.cond.Wait()
		r.idle--
	}
}

// completeItem runs after an item's processor returns — for a root item's ItemProcessor and for a child item's
// ItemFunc alike. It joins the item's outstanding background work, computes the item's effective error, releases
// its slots, cancels its context and wakes waiters.
//
// The effective error is the processor's own error, else the first error from a wave whose outcome nobody observed
// (see Wave). Where it goes depends on the kind of item:
//   - a child reports it to the wave that created it, which cancels its parent (fail-fast) and surfaces there;
//   - a root item's real (non-shutdown) error triggers error-shutdown: record the first error, stop creating, and
//     cancel every later item so they abort while earlier items keep finishing.
func (r *run) completeItem(it *item, procErr error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// A real processor error aborts the item's own still-running background work; a graceful return lets it
	// finish (a live task owns its slot and cannot be force-freed).
	if procErr != nil && !isShutdown(procErr) {
		it.poison(procErr)
	}
	// Join all outstanding background work before releasing slots.
	for it.hasLiveWaves() {
		r.cond.Wait()
	}

	effErr := procErr
	isShutdownErr := isShutdown(effErr)
	if effErr == nil || isShutdownErr {
		if e := it.firstUnackedWaveErr(); e != nil {
			effErr = e
			isShutdownErr = isShutdown(effErr)
		}
	}
	if it.parentWave == nil && effErr != nil && !isShutdownErr {
		if r.runErr == nil {
			r.runErr = effErr
		}
		// Cancel every later item immediately (error semantics), with this error as the shutdown cause; earlier
		// items are left to finish, bounded by shutdownTimeout via the watcher that markShutdownLocked wakes.
		for later := it.next; later != nil; later = later.next {
			later.cancel(&shutdownError{cause: effErr})
		}
		r.markShutdownLocked()
	}
	r.finishItem(it)
	if it.cancel != nil {
		it.cancel(nil) // release the context; a child has none of its own (it shares its parent's)
	}
	if it.parentWave != nil {
		// Report the child's outcome to the wave that scheduled it — after its slots are freed, so a parent
		// joining the wave sees the branch already released.
		it.parentWave.workDone(effErr)
	}
	r.cond.Broadcast()
}
