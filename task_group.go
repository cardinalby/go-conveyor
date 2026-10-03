package conveyor

import (
	"context"
	"fmt"
)

// TaskGroup is a handle to tasks an item started in the background: a RetainFor task, or the fan-out tasks handed over
// with FanOut.Retain. Join them with Wait, before or after the next MoveTo (the choice decides which node's slot the
// item holds meanwhile). Finished can be used in a select.
//
// The tasks' first error is reported by Wait as a TaskError, and the processor decides what it means for the item.
// An error of tasks the item never joined fails the item when it completes.
type TaskGroup interface {
	// Finished is closed once every task has finished, including work those tasks scheduled themselves, or was
	// skipped because the item was canceled, and the item has stopped adding to it.
	Finished() <-chan struct{}

	// Wait blocks until every task has finished and returns their first error as a TaskError, or nil. From then on
	// the outcome is the processor's: what it returns decides the item's result. If the conveyor canceled the item,
	// the tasks it stopped are reported with that ShutdownError instead. Only the item that started the tasks may
	// call it, with its own context or one derived from it.
	//
	// If the item or the call context is canceled first, Wait returns that cause at once and the tasks stay
	// unjoined. A canceled item never gets nil.
	//
	// It returns ErrForeignContext for a context without an item and ErrStaleContext once the item has finished. It
	// panics on misuse: a task group of another item (a lane child may wait only on its own task groups), or a task's
	// context (a task must never wait for other work).
	Wait(ctx context.Context) error
}

// taskGroup is the TaskGroup implementation. All fields are guarded by run.mu (the channels are created up front, so
// reading them needs no lock); the channels are closed under the lock exactly once.
type taskGroup struct {
	run *run
	// it is the item that created the task group and is charged for its work. nil only for a task group returned by a
	// RetainFor that could not even resolve an item (a foreign context), which is born finished.
	it *item

	// retainUnit is the unit whose slot this task group holds until its work is done: the stage a RetainFor was called
	// on, or the fan-out a FanOut.Retain handed over. nil while a fan-out's work is still the node's body — then the
	// item itself holds the slot, because it cannot leave until the work is done.
	retainUnit *unit

	// atNode is the fan-out node whose body this task group is (nil for a RetainFor or standalone task group). It is
	// what lets FanOut.Retain tell this task group apart from one the item retained at an earlier fan-out and is still
	// carrying.
	atNode *unit

	// ctx is the context the task group's tasks run with. A fan-out body has its own, derived from the item's: the
	// body's first real error cancels it (cancel), so sibling tasks stop and queued work is dropped, while the item
	// goes on (errgroup-style). Other task groups use the item's context and have no cancel.
	ctx    context.Context
	cancel context.CancelCauseFunc

	// sealed records that nothing more can be added to the task group through its item's own path; only the task
	// group's own running work may still add to it. The channels close only once the task group is sealed (see settle).
	// Every task group is born sealed except a fan-out body, which is sealed when its item leaves the node, retains, or
	// completes.
	sealed bool
	// unexhausted is the number of this task group's task collections (one per branch touched by a submission) that may
	// still produce work. Together with running it decides idle.
	unexhausted int
	// running is the number of this task group's tasks (or child items) that have started but not finished.
	running int
	// rootSubmitted records that a root submission has reached this body, so only the first one is the entering
	// submission of a Balanced/Strict admission (see run.markEntering).
	rootSubmitted bool
	// hold is the upstream hold the admission that opened this body deferred, nil under Buffered (see upstreamHold).
	// It lives on the task group, not the item, so the body's own progress finds it after the item has moved on.
	hold *upstreamHold

	// err is the outcome a join reports: the first real error of the task group's tasks as a *taskError, or, when the
	// conveyor canceled the item, that cause as is (an abort).
	err error
	// joined records that a join reported the settled outcome to the processor — Wait on a finished task group,
	// FanOut.Wait on an idle body, or the leave that closes it. From then on the processor decides; an error of a
	// task group that is not joined fails the item. New root work in a body clears it.
	joined bool
	// abandoned records that err is a cancellation cause (an abort, or work dropped because the item or the body was
	// canceled), not a failure of the task group's own tasks. A later real failure replaces it (see recordErr); a task
	// error is never replaced.
	abandoned bool

	finishedCh  chan struct{}
	finishedSet bool // finishedCh has been closed
}

// newTaskGroup creates a sealed task group owned by it, with no work registered yet. The caller holds run.mu (except
// for the foreign-context path, which has no run state to touch) and must register the work before releasing the lock,
// or the task group would look finished too early. A task group that must stay open for further submissions clears
// sealed itself.
func newTaskGroup(r *run, it *item) *taskGroup {
	w := &taskGroup{
		run:        r,
		it:         it,
		sealed:     true,
		finishedCh: make(chan struct{}),
	}
	if it != nil {
		w.ctx = it.ctx
		it.taskGroups = append(it.taskGroups, w)
	}
	return w
}

// finishedTaskGroup returns a task group of it that is already complete, carrying err. It is used for the degenerate
// cases that must still hand back a usable handle: a MoveTo that could not enter, and a RetainFor that declined to run
// its task. The error is registered on the item, so ignoring the handle still fails the item (see TaskGroup). Caller
// holds run.mu.
func finishedTaskGroup(r *run, it *item, err error) *taskGroup {
	w := newTaskGroup(r, it)
	w.err = err
	w.closeFinished()
	return w
}

// standaloneTaskGroup returns a finished task group belonging to no item, for the calls that could not even resolve one
// (a foreign context). It needs no lock and touches no run state.
func standaloneTaskGroup(err error) *taskGroup {
	return &taskGroup{
		err:         err,
		sealed:      true,
		finishedCh:  closedChan(),
		finishedSet: true,
	}
}

// closedChan returns an already-closed channel, for task groups that are born finished.
func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (w *taskGroup) Finished() <-chan struct{} { return w.finishedCh }

// --- state transitions (all called under run.mu) ---

// closeFinished marks the task group complete (idempotent).
func (w *taskGroup) closeFinished() {
	if !w.finishedSet {
		w.finishedSet = true
		close(w.finishedCh)
	}
}

// isFinished reports whether the task group has completed. Caller holds run.mu.
func (w *taskGroup) isFinished() bool { return w.finishedSet }

// idle reports whether the task group has nothing outstanding: no collection may still produce work and no task is
// running. An unsealed idle task group may become busy again; a sealed idle one is finished. Caller holds run.mu.
func (w *taskGroup) idle() bool { return w.unexhausted == 0 && w.running == 0 }

// seal closes the task group to additions through its item's own path and settles it, so a task group sealed while idle
// finishes at once. Idempotent. The caller must broadcast.
func (w *taskGroup) seal() {
	w.sealed = true
	w.settle()
}

// addSource registers one more collection that may still produce work.
func (w *taskGroup) addSource() { w.unexhausted++ }

// sourceExhausted records that one collection can produce no more work, and settles the task group if that was the
// last outstanding thing. The caller must broadcast.
func (w *taskGroup) sourceExhausted() {
	w.unexhausted--
	w.settle()
}

// workStarted / workDone track one task (or child item) of the task group; u is the unit the task ran on (a branch's
// start gate, or the stage of RetainFor). workDone records the task's error (see recordErr). The caller must
// broadcast.
func (w *taskGroup) workStarted() { w.running++ }

func (w *taskGroup) workDone(err error, u *unit) {
	w.running--
	w.recordErr(err, u)
	w.settle()
}

// recordErr records a task's error. An error after the conveyor canceled the item is an abort (see item.asAbort): it is
// kept as is, and only if nothing else is recorded. The first real error becomes the task group's outcome as a
// TaskError and cancels the body's context (fail-fast within the body, not the item). An abandonment cause stored
// earlier is replaced: the task group's own task failed, and that is the truer outcome. The task group cannot be
// finished here (this task was still running), so nothing has read the earlier value as final.
//
// If the processor has already returned without joining the task group, nobody can take the error any more: the item
// fails with it now (see run.itemFailed). The caller must broadcast.
func (w *taskGroup) recordErr(err error, u *unit) {
	if w.it != nil {
		err = w.it.asAbort(err)
	}
	if err == nil || (w.err != nil && !w.abandoned) {
		return
	}
	if isShutdown(err) {
		w.err, w.abandoned = err, true
		return
	}
	te := &taskError{unit: u.handle(), err: err}
	w.err, w.abandoned = te, false
	if w.cancel != nil {
		w.cancel(te)
	}
	if it := w.it; it != nil && it.returned && !w.joined {
		w.run.itemFailed(it, te)
	}
}

// recordAbandoned reports that some of the task group's work was dropped without running, because its item (or its
// body) was canceled and so lost permission to keep working. cause is that cancellation cause.
//
// Without this a task group would settle clean whenever its work was skipped rather than failed — nothing ever
// returns an error for a callback that was never invoked — so a caller could not tell "all my work ran" from "most of
// it was thrown away". That is the one thing a task group must never be ambiguous about: it is the signal a pipeline
// uses to decide whether the item's effects are complete (whether to commit the broker offset).
//
// A real failure from the task group's own work is never masked by it: an earlier one is kept, a later one replaces it
// (see recordErr).
func (w *taskGroup) recordAbandoned(cause error) {
	if w.err != nil || cause == nil {
		return
	}
	w.err = cause
	w.abandoned = true
}

// settle closes Finished once the task group is sealed and idle, and releases the body's context. An unsealed task
// group never closes it: its item may still add work, so "all done" cannot be final.
func (w *taskGroup) settle() {
	if !w.sealed || !w.idle() || w.finishedSet {
		return
	}
	w.closeFinished()
	if w.cancel != nil {
		w.cancel(nil) // nothing runs with it any more
	}
	w.releaseRetained()
}

// releaseRetained gives back the slot this task group was holding on its item's behalf — the stage of a RetainFor, or
// the fan-out of a FanOut.Retain — now that its work is done. It only frees it if the item has already moved past
// that node; if the item is still in it, the item's next move does the freeing (releaseBelow no longer skips the unit
// once the task group has finished). Together those two are the "whichever happens last" half of the contract: an item
// never sits in a node holding nothing, and a slot never outlives the work it was kept for.
//
// Caller holds run.mu and must broadcast.
func (w *taskGroup) releaseRetained() {
	if w.retainUnit == nil || w.run == nil || w.it == nil || w.it.finished {
		return
	}
	w.run.releaseBelow(w.it, w.it.reachedRank)
}

// owner describes the task group for messages: "the task group of <node>" for a body or a RetainFor, "the task group"
// otherwise.
func (w *taskGroup) owner() string {
	u := w.atNode
	if u == nil {
		u = w.retainUnit
	}
	if u == nil {
		return "the task group"
	}
	return fmt.Sprintf("the task group of %s", u.owner)
}

// Wait blocks until the task group is finished and reports its outcome (see the TaskGroup interface). Only the task
// group's own item may wait for it — a task group is only meaningful to the item that created it.
func (w *taskGroup) Wait(ctx context.Context) error {
	if w == nil {
		panic(fmt.Errorf("cannot wait for a nil task group: %w", errForeignTaskGroup))
	}
	// A standalone task group is one a failed call handed back instead of real work: it is already finished and belongs
	// to no item, so waiting just surfaces the reason rather than punishing a caller who ignored the error.
	if w.run == nil {
		return w.err
	}
	checkTaskCtx(ctx, "wait for", w.owner())
	it, err := itemFromContext(ctx)
	if err != nil {
		return err
	}
	if it != w.it {
		panic(fmt.Errorf("cannot wait for %s: %w", w.owner(), errForeignTaskGroup))
	}
	r := w.run
	r.mu.Lock()
	defer r.mu.Unlock()
	if it.finished {
		return ErrStaleContext // a context decoupled from cancellation; normally waitUntil catches the cancel first
	}
	// A task group that is finished when the wait is canceled is still joined: its outcome is settled. A clean one
	// leaves the cancellation cause as the answer, so a canceled item never gets nil.
	if err := r.waitUntil(ctx, it, w.isFinished); err != nil && (!w.isFinished() || w.err == nil) {
		return err
	}
	w.joined = true
	return w.err
}
