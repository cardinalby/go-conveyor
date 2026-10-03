package conveyor

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// run holds the mutable state of a single Run invocation. A fresh run is allocated per Run so state never leaks
// between invocations. All fields below mu are guarded by mu; the state transitions and gate logic that operate on
// them live in state.go.
type run struct {
	conveyor    *conveyor
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
	nextSeq    int64 // last creation sequence number handed to any item of this run, root or child (see item.seq)
	idle       int   // workers parked in acquireItem waiting for the start stage (0 or 1; extras retire)
	parked     int   // callers blocked in waitUntil (a node method or TaskGroup.Wait); read by tests to sync on a wait
	// spawning counts workers that have been started but have not yet reached acquireItem. Together with idle it
	// answers "is a worker already on its way to take the next item", which is what the replacement decision in
	// acquireItem needs. Counting only idle would ignore a worker that exists but has not been scheduled yet, so a
	// fast ItemProcessor would spawn a fresh goroutine per item — unboundedly, since the pool never gets a chance
	// to park (worst on a single-P runtime, where the new goroutines simply pile up on the run queue).
	spawning     int
	stopCreating bool // once set, no new root items are created and idle workers exit
	// trigger is why the shutdown began, set once by the first event: the Run context's cancellation cause, or the
	// *itemError of the first failed item. It is Run's result (see result) and the cause of the ShutdownError that
	// aborted items see.
	trigger error
	// shutdownErr is the ShutdownError with the trigger as cause, set with it. Its presence is the "shutdown has begun"
	// flag that UntilShutdown contexts and the abort rules check under mu.
	shutdownErr *shutdownError
	// shutdownBegun mirrors shutdownErr != nil for the lock-free fast path of UntilShutdown. Set with it, never
	// cleared.
	shutdownBegun atomic.Bool
	shutdownAt    time.Time // when shutdownErr was set; OptDrainTimeout counts from it
	// shutdownCtx is canceled with shutdownErr in the same step that sets it, and when Run returns. UntilShutdown
	// contexts watch it.
	shutdownCtx    context.Context
	cancelShutdown context.CancelCauseFunc
	drainErr       error          // first problem of the drain: an *itemError after the trigger, or the drain timeout cause
	workers        sync.WaitGroup // all root-item worker goroutines

	// shutdownCh is closed once, when shutdown begins from either trigger (caller-context cancellation or an
	// item error). watchShutdown waits on it to bound how long the in-flight items may keep running.
	shutdownOnce sync.Once
	shutdownCh   chan struct{}
}

// ItemProcessor processes one item, moving it through the conveyor's nodes with Stage.MoveTo / FanOut.MoveTo using
// the context it receives. It need not enter every node and may return early.
//
// Returning an error shuts the conveyor down: no new items are created, later items are canceled, and earlier ones
// are allowed to finish. An item aborted during the shutdown cancels all younger items too. Use UntilShutdown for the
// part of the path that should stop at once when shutdown begins.
//
// A task error does not cancel the item: the join that reports it (TaskGroup.Wait, FanOut.Wait, or the MoveTo that
// leaves a fan-out) returns a TaskError, and the processor decides. Return it, or any error, to fail the item; return
// nil to go on, e.g. after sending the message to a dead-letter queue. An error of tasks the processor never joined
// fails the item when the processor returns.
//
// Cancellation is judged by the item's own context. Once it is canceled (a shutdown, or for a lane child the failure
// of its fan-out body), every node method returns the cause, even when called with a context that hides the
// cancellation (context.WithoutCancel). A derived context with its own deadline still works. Code that must run after
// cancellation can still run in plain Go once the node method has returned; it just cannot run inside a node.
type ItemProcessor func(ctx context.Context) error

// Run drives items through the conveyor until ctx is canceled or an item fails (see the Conveyor interface).
func (c *conveyor) Run(ctx context.Context, itemProcessor ItemProcessor) error {
	r, err := c.tryRun(ctx, itemProcessor)
	if err != nil {
		return err
	}
	defer c.stopRun()
	defer r.cancelItems(nil)    // release any item contexts still lingering when Run returns
	defer r.cancelShutdown(nil) // and the UntilShutdown watches

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

	return r.result(ctx)
}

// result builds the error Run returns from the trigger of the shutdown and the drain outcome. The trigger decides the
// kind: an ItemError when an item failed first, else a ShutdownError with the Run context's cause. It is nil if the
// Run context was never canceled and nothing failed.
func (r *run) result(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ie, ok := r.trigger.(*itemError); ok {
		// A copy: items keep ie as their cancellation cause and must not see the run-level data.
		return &itemError{unit: ie.unit, err: ie.err, drain: r.drainErr}
	}
	cause := r.trigger
	if cause == nil {
		cause = context.Cause(ctx)
	}
	if cause == nil {
		return nil
	}
	return &shutdownError{cause: cause, drain: r.drainErr}
}

// tryRun creates the run and publishes it as currentRun, under runMu. Item contexts carry ctx's values but not its
// cancellation: the caller's cancellation begins a graceful shutdown instead (see watchShutdown).
func (c *conveyor) tryRun(ctx context.Context, itemProcessor ItemProcessor) (*run, error) {
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if c.isRunning {
		return nil, ErrConveyorAlreadyRunning
	}
	c.finalize() // assign scopes and ranks once, under runMu; the topology is frozen from here on
	c.isRunning = true
	r := c.newRun()
	r.proc = itemProcessor
	r.itemsCtx, r.cancelItems = context.WithCancelCause(context.WithoutCancel(ctx))
	r.shutdownCtx, r.cancelShutdown = context.WithCancelCause(context.Background())
	c.currentRun.Store(r)
	return r, nil
}

func (c *conveyor) stopRun() {
	c.runMu.Lock()
	c.currentRun.Store(nil)
	c.isRunning = false
	c.runMu.Unlock()
}

// watchShutdown waits for shutdown to begin — from either trigger: the caller cancels ctx, or an item error
// closes shutdownCh — and then bounds how long the in-flight items may keep running: it asks the configured
// DrainContextFunc for the drain context (see OptDrainContextFunc) and cancels the items once that context is
// done. No func, or a nil context from it, leaves the items to finish on their own.
//
// It exits early (without touching in-flight items, and without asking the func) if the run drains before any
// shutdown, which the closing of stopWatch signals. Once a shutdown has begun the func is always asked, even when the
// run has drained meanwhile.
func (r *run) watchShutdown(ctx context.Context, stopWatch <-chan struct{}, done chan<- struct{}) {
	defer close(done)

	select {
	case <-stopWatch:
		select {
		case <-r.shutdownCh: // both were ready: an item error began the shutdown before the run drained
		default:
			return // run finished without a shutdown; nothing to do
		}
	case <-ctx.Done():
		r.beginShutdown(context.Cause(ctx))
	case <-r.shutdownCh:
		// shutdown already begun by an item error
	}

	drainFunc := r.conveyor.drainContext
	if drainFunc == nil {
		return // no limit: in-flight items are left to finish
	}
	start, cause := r.shutdownCause()
	drainCtx, cancel := drainFunc(start, cause)
	if cancel != nil {
		// Release the drain context (a timer, typically) as soon as it is out of use: the watcher outlives the
		// items it bounds by nothing, and Run joins it before returning.
		defer cancel()
	}
	if drainCtx == nil {
		return // the caller declined a limit for this shutdown
	}
	select {
	case <-drainCtx.Done():
		r.mu.Lock()
		// Run joins this watcher only after the items have finished, so a drain context that is done with or after them
		// (always, if it was born done) must not count as an overrun.
		if r.inFlight.val > 0 {
			if r.drainErr == nil { // an item failure that came first is kept
				r.drainErr = context.Cause(drainCtx)
			}
			r.cancelInFlight() // the items outlived the drain timeout
		}
		r.mu.Unlock()
	case <-stopWatch:
	}
}

// beginShutdown stops new items from being created and wakes all waiters so idle workers exit and blocked node
// calls re-evaluate. In-flight items are otherwise left running.
func (r *run) beginShutdown(cause error) {
	r.mu.Lock()
	r.markShutdownLocked(cause)
	r.cond.Broadcast()
	r.mu.Unlock()
}

// markShutdownLocked records that shutdown has begun: it stops new items from being created, cancels the
// UntilShutdown contexts, and signals the watcher (which applies the drain timeout). cause becomes the trigger unless
// an earlier one is kept. Idempotent. The caller holds r.mu and must broadcast.
func (r *run) markShutdownLocked(cause error) {
	r.stopCreating = true
	if r.trigger == nil {
		r.trigger = cause
		r.shutdownErr = &shutdownError{cause: cause}
		r.shutdownAt = time.Now()
		r.shutdownBegun.Store(true)
		// Under mu: node methods check shutdownErr in the same lock hold as an entry, so an UntilShutdown context
		// never enters a node after this point, even before its Done channel is closed.
		r.cancelShutdown(r.shutdownErr)
	}
	r.shutdownOnce.Do(func() { close(r.shutdownCh) })
}

// cancelInFlight cancels every in-flight item's context with a ShutdownError by canceling their common parent, so
// items blocked in a node method return promptly (child items are canceled with them, their contexts descending
// from their parents'). Code inside an ItemProcessor that ignores its context (e.g. a bare channel receive) is not
// forcibly interrupted — only node calls unblock. Items already canceled individually (the error cascade in
// completeItem) keep their more specific cause, since the first cancellation of a context wins. Caller holds mu.
func (r *run) cancelInFlight() {
	r.cancelItems(r.shutdownErr)
	r.cond.Broadcast()
}

// shutdownCause is when the shutdown began, and why, as the DrainContextFunc is told: the first failed item's own
// error, or the Run context's cancellation cause. Deliberately not the drain context's own cause: an expired
// drain timeout says nothing an item does not already know, while the trigger does (it is reported by
// RunError.DrainError instead).
func (r *run) shutdownCause() (time.Time, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ie, ok := r.trigger.(*itemError); ok {
		return r.shutdownAt, ie.err
	}
	return r.shutdownAt, r.trigger
}

// cancelLater cancels every root item younger than it with the shutdown error. Caller holds mu and must broadcast.
func (r *run) cancelLater(it *item) {
	for later := it.next; later != nil; later = later.next {
		later.cancel(r.shutdownErr)
	}
}

// itemUnit is the node a root item occupies or waits in front of now, for ItemError.Unit. Caller holds mu.
func (r *run) itemUnit(it *item) Unit {
	for _, u := range r.conveyor.units {
		if u.scope == 0 && (u.rank == it.reachedRank || u.queueRank() == it.reachedRank) {
			return u.handle()
		}
	}
	return r.conveyor.units[0].handle()
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
// callback alike. It decides the item's outcome, joins the item's outstanding background work, releases its slots,
// cancels its context and wakes waiters.
//
// The item's error is the processor's own error, else the first error of a task group the processor did not join (see
// taskGroup.joined). It is final from here on — nobody can join a task group for the item any more — so a failure
// counts now, not once the join below is over (see itemFailed), and so does a failure of unjoined work while the join
// is running (see taskGroup.recordErr). An abort of a root item during a shutdown cancels every later item too (the
// abort cascade), so no later item gets past it.
func (r *run) completeItem(it *item, procErr error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	procErr = it.asAbort(procErr)
	it.returned = true
	r.dropDormant(it, math.MaxInt) // work prepared for a fan-out never entered: nothing waits for it

	// A root processor's error, an abort too, cancels the item's own still-running background work; a nil return lets
	// it finish. A child's failure cancels its body, and with it the child's own work, in itemFailed.
	if procErr != nil && it.cancel != nil {
		it.cancel(procErr)
	}
	// Called now for its effects only: a failure is reported and an abort cascades without waiting for the join. The
	// outcome may still change during the join, so it is taken from the second call below.
	_ = r.settleOutcome(it, procErr)
	r.cond.Broadcast()
	// An open fan-out body is sealed: the processor's path into it is over, and an idle body finishes only once
	// sealed, so the wait below would otherwise never end. It still grows from its own running work.
	if w := it.pending; w != nil {
		it.sealBody(w, bodyClosed)
		// Sealing may have ended an upstream hold (see dischargeHold) and freed a slot upstream waiters are parked on;
		// the wait below would otherwise leave them asleep until an unrelated broadcast.
		r.cond.Broadcast()
	}
	// Join all outstanding background work before releasing slots.
	for it.hasLiveTaskGroups() {
		r.cond.Wait()
	}

	// Work that failed during the join has reported itself already; an abort found only now still cascades. Before
	// finishItem, so no younger item can pass this one through the ordering gate first.
	effErr := r.settleOutcome(it, procErr)
	r.finishItem(it)
	if it.cancel != nil {
		it.cancel(nil) // release the context; a child has none of its own (it shares its body's)
	}
	if it.parentGroup != nil {
		// Report the child's outcome to the task group that scheduled it — after its slots are freed, so a parent
		// joining the task group sees the branch already released.
		it.parentGroup.workDone(effErr, it.lane.start)
	}
	r.cond.Broadcast()
}

// settleOutcome computes the outcome of an item whose processor returned procErr — that error if it is a real
// failure, else the first error of a task group the processor did not join, else procErr (nil or an abort) — and acts
// on it: a failure is reported (itemFailed, once), and a root item's abort during a shutdown cancels every later item
// (the abort cascade). Idempotent. Caller holds mu and must broadcast.
func (r *run) settleOutcome(it *item, procErr error) error {
	err := procErr
	if err == nil || isShutdown(err) {
		if e := it.firstUnjoinedErr(); e != nil {
			err = e
		}
	}
	if err != nil && !isShutdown(err) {
		r.itemFailed(it, err)
	} else if it.parentGroup == nil && err != nil && r.shutdownErr != nil {
		r.cancelLater(it)
	}
	return err
}

// itemFailed reports the final failure of an item, once. A root item's failure goes to the run (failRoot); a child's
// fails the body that created it, which cancels the body's other work and becomes its outcome. Caller holds mu and
// must broadcast.
func (r *run) itemFailed(it *item, err error) {
	if it.failReported {
		return
	}
	it.failReported = true
	if it.parentGroup == nil {
		r.failRoot(it, err)
		return
	}
	it.parentGroup.recordErr(err, it.lane.start)
}

// failRoot handles the real error of a root item: the first one is the trigger of the shutdown, a later one the first
// failure of the drain. It cancels every later item at once (error semantics), with the trigger as the shutdown
// cause. Earlier items are left to finish, bounded by the drain timeout via the watcher that markShutdownLocked
// wakes. Caller holds mu and must broadcast.
func (r *run) failRoot(it *item, err error) {
	ie := &itemError{unit: r.itemUnit(it), err: err}
	if r.trigger != nil && r.drainErr == nil {
		r.drainErr = ie
	}
	r.markShutdownLocked(ie)
	r.cancelLater(it)
}
