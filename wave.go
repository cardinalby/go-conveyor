package conveyor

import "context"

// Wave is a handle to background work an item started: a Stage.Retain operation, or work at a fan-out taken over
// with FanOut.Detach. Join it by naming it in a later MoveTo — commit.MoveTo(ctx, wave) — which waits for it and
// returns its error.
//
// A wave whose error nobody observes still fails the item when it completes.
type Wave interface {
	// Started is closed once all the wave's work has been handed out — every task started, or the item canceled.
	// Relevant mainly for streaming sources (NewTasksGen, NewTasksChan): state they read must not be mutated until
	// it closes.
	Started() <-chan struct{}

	// Finished is closed once every task of this wave has finished, or was skipped because the item was canceled.
	// After it is closed, Err reports the outcome.
	Finished() <-chan struct{}

	// Err returns the first error the wave's work produced, or nil. It is only final once Finished is closed.
	// Reading it counts as observing the outcome.
	Err() error
}

// wave is the Wave implementation. All fields are guarded by run.mu (the channels are created up front, so
// reading them needs no lock); the channels are closed under the lock exactly once.
type wave struct {
	run *run
	// it is the item that created the wave and is charged for its work. nil only for a wave returned by a
	// Retain that could not even resolve an item (a foreign context), which is born finished.
	it *item

	// retainUnit is the unit whose slot this wave holds until its work is done: the stage a Stage.Retain was called
	// on, or the fan-out a FanOut.Detach handed over. nil while a fan-out's work is still the node's body — then the
	// item itself holds the slot, because it cannot leave until the work is done.
	retainUnit *unit

	// atNode is the fan-out node whose body this wave is (nil for a Retain or standalone wave). It names the node in
	// the error of an implicit join, and it is what lets Detach tell this wave apart from one the item detached at an
	// earlier fan-out and is still carrying.
	atNode *unit

	// unexhausted is the number of this wave's task collections (one per branch touched by the FanOut.MoveTo
	// call) that may still produce work. Started closes when it reaches zero.
	unexhausted int
	// running is the number of this wave's tasks (or child items) that have started but not finished.
	running int

	// err is the first error produced by the wave's work; it is also set as the cancellation cause of the
	// owning item's context (fail-fast).
	err error
	// acked records that the error was observed — by a join in MoveTo, or by an Err call after the wave
	// finished. An unacked error fails the item at completion.
	acked bool

	startedCh   chan struct{}
	finishedCh  chan struct{}
	startedSet  bool // startedCh has been closed
	finishedSet bool // finishedCh has been closed
}

// newWave creates a wave owned by it, with no work registered yet. The caller holds run.mu (except for the
// foreign-context path, which has no run state to touch) and must register the work before releasing the lock,
// or the wave would look finished too early.
func newWave(r *run, it *item) *wave {
	w := &wave{
		run:        r,
		it:         it,
		startedCh:  make(chan struct{}),
		finishedCh: make(chan struct{}),
	}
	if it != nil {
		it.waves = append(it.waves, w)
	}
	return w
}

// finishedWave returns a wave of it that is already complete, carrying err. It is used for the degenerate cases
// that must still hand back a usable handle: a MoveTo that could not enter, and a Retain that declined to run its
// bgOp. The error is registered on the item, so ignoring the handle still fails the item (see Wave). Caller holds
// run.mu.
func finishedWave(r *run, it *item, err error) *wave {
	w := newWave(r, it)
	w.err = err
	w.closeStarted()
	w.closeFinished()
	return w
}

// standaloneWave returns a finished wave belonging to no item, for the calls that could not even resolve one (a
// foreign context). It needs no lock and touches no run state.
func standaloneWave(err error) *wave {
	return &wave{
		err:         err,
		startedCh:   closedChan(),
		finishedCh:  closedChan(),
		startedSet:  true,
		finishedSet: true,
	}
}

// closedChan returns an already-closed channel, for waves that are born finished.
func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (w *wave) Started() <-chan struct{}  { return w.startedCh }
func (w *wave) Finished() <-chan struct{} { return w.finishedCh }

// Err reports the first error of the wave's work; reading it after the wave finished marks the error observed
// (see the Wave interface).
func (w *wave) Err() error {
	if w.run == nil {
		return w.err // standalone wave (foreign context): immutable, no lock needed
	}
	w.run.mu.Lock()
	defer w.run.mu.Unlock()
	if w.finishedSet {
		w.acked = true
	}
	return w.err
}

// --- state transitions (all called under run.mu) ---

// closeStarted marks the wave's work fully handed out (idempotent).
func (w *wave) closeStarted() {
	if !w.startedSet {
		w.startedSet = true
		close(w.startedCh)
	}
}

// closeFinished marks the wave complete (idempotent).
func (w *wave) closeFinished() {
	if !w.finishedSet {
		w.finishedSet = true
		close(w.finishedCh)
	}
}

// isFinished reports whether the wave has completed. Caller holds run.mu.
func (w *wave) isFinished() bool { return w.finishedSet }

// addSource registers one more collection that may still produce work.
func (w *wave) addSource() { w.unexhausted++ }

// sourceExhausted records that one collection can produce no more work, and settles the wave if that was the
// last outstanding thing. The caller must broadcast.
func (w *wave) sourceExhausted() {
	w.unexhausted--
	w.settle()
}

// workStarted / workDone track one task (or child item) of the wave. workDone records the work's error
// fail-fast: the first error poisons the owning item's context so siblings and the ItemProcessor abort promptly.
// The caller must broadcast.
func (w *wave) workStarted() { w.running++ }

func (w *wave) workDone(err error) {
	w.running--
	if err != nil {
		w.recordErr(err)
	}
	w.settle()
}

// recordErr keeps the first error and poisons the owning item with it (fail-fast), so the ItemProcessor and any
// sibling work abort promptly.
func (w *wave) recordErr(err error) {
	if w.err != nil || err == nil {
		return
	}
	w.err = err
	if w.it != nil {
		w.it.poison(err)
	}
}

// recordAbandoned reports that some of the wave's work was dropped without running, because its item was canceled
// and so lost permission to keep working. cause is the item's cancellation cause.
//
// Without this a wave would settle clean whenever its work was skipped rather than failed — nothing ever returns an
// error for a callback that was never invoked — so a caller could not tell "all my work ran" from "most of it was
// thrown away". That is the one thing a wave must never be ambiguous about: it is the signal a pipeline uses to
// decide whether the item's effects are complete (whether to commit the broker offset).
//
// It does not poison the item: the item is already canceled by definition, and its cause is what we are recording.
// First-error-wins is preserved, so a real failure from the wave's own work is never masked by a shutdown cause.
func (w *wave) recordAbandoned(cause error) {
	if w.err != nil || cause == nil {
		return
	}
	w.err = cause
}

// settle closes Started / Finished once the corresponding counters have drained.
func (w *wave) settle() {
	if w.unexhausted == 0 {
		w.closeStarted()
		if w.running == 0 {
			w.closeFinished()
			w.releaseRetained()
		}
	}
}

// releaseRetained gives back the slot this wave was holding on its item's behalf — the stage of a Stage.Retain, or the
// fan-out of a FanOut.Detach — now that its work is done. It only frees it if the item has already moved past that
// node; if the item is still in it, the item's next move does the freeing (releaseBelow no longer skips the unit once
// the wave has finished). Together those two are the "whichever happens last" half of the contract: an item never sits
// in a node holding nothing, and a slot never outlives the work it was kept for.
//
// Caller holds run.mu and must broadcast.
func (w *wave) releaseRetained() {
	if w.retainUnit == nil || w.run == nil || w.it == nil || w.it.finished {
		return
	}
	w.run.releaseBelow(w.it, w.it.reachedRank)
}

// join waits for every listed wave (in order), marks their errors observed and returns the first error. The
// caller holds run.mu; on return the lock is still held. A wave from another item is misuse and panics — a wave
// is only meaningful to the item that created it.
func (r *run) join(ctx context.Context, it *item, waves []Wave) error {
	for _, jw := range waves {
		w, ok := jw.(*wave)
		if !ok || w == nil {
			panic(errForeignWave)
		}
		// A standalone wave (it == nil) is one a failed call handed back instead of real work: it is already
		// finished, so joining it just surfaces the reason rather than punishing a caller who ignored the error.
		if w.it != nil && w.it != it {
			panic(errForeignWave)
		}
		if err := r.waitUntil(ctx, w.isFinished); err != nil {
			return err
		}
		w.acked = true
		if w.err != nil {
			return w.err
		}
	}
	return nil
}
