package conveyor

import (
	"errors"
	"fmt"
)

// Returned errors, inspectable with errors.Is on the value returned by Run and the node methods (MoveTo, TryMoveTo,
// FanOut.Schedule, FanOut.Wait), or on Wave.Err.
var (
	// ErrConveyorAlreadyRunning is returned by Run when the conveyor is already running.
	ErrConveyorAlreadyRunning = errors.New("conveyor is already running")

	// ErrInvalidContext classifies a context that cannot drive a node transition. Match it, or one of the two
	// variants below, with errors.Is.
	ErrInvalidContext = errors.New("invalid context")

	// ErrForeignContext indicates the context was never derived from an item's ctx.
	ErrForeignContext = fmt.Errorf("%w: not derived from a conveyor item's ctx", ErrInvalidContext)

	// ErrStaleContext indicates the context's owner has already finished: the item, a lane child whose callback
	// returned, or a pool task whose wave has finished.
	ErrStaleContext = fmt.Errorf("%w: the item or work it belongs to has finished", ErrInvalidContext)
)

// RunError describes why a run shut down and how the shutdown went. Run returns one of its two kinds: a
// ShutdownError when the Run context was canceled first, or an ItemError when an item failed first. Only the
// event that came first decides the kind. Failures of other items that happen during the shutdown are listed by
// ItemErrors.
//
// Items see a ShutdownError too, as the cancellation cause of their context when the conveyor aborts them. There,
// DrainError and ItemErrors are always nil: they describe the whole shutdown and are known only when Run returns.
//
// The interface is sealed: only the conveyor creates these errors.
type RunError interface {
	error
	// Unwrap returns the trigger of the shutdown, never nil: the Run context's cancellation cause for a
	// ShutdownError, the failed item's own error for an ItemError.
	Unwrap() error
	// DrainError is nil if all in-flight items finished on their own. If the grace period (see OptGracePeriod)
	// ended first, it is the cause of its context (e.g. context.DeadlineExceeded) and the remaining items were
	// canceled.
	DrainError() error
	// ItemErrors returns the failures of other items during the shutdown, in completion order. It does not repeat
	// the trigger and does not include items aborted by the conveyor. Errors that a canceled item returns
	// because of its cancellation, but not as a plain context.Canceled, are listed too.
	ItemErrors() []error
	sealedRunError()
}

// ShutdownError is returned by Run when the Run context was canceled and no item had failed before. Its Unwrap
// gives the context's cancellation cause, so errors.Is(err, ErrSignalReceived{}) works.
//
// It is also the cancellation cause of an item's context when the conveyor aborts the item: because the Run
// context was canceled, an earlier item failed (Unwrap gives its ItemError) or was aborted, or the grace period is
// over. And it is the cause of an UntilShutdown context once shutdown begins. Recover it with errors.As.
// errors.Is(err, context.Canceled) reaches the wrapped trigger.
type ShutdownError interface {
	RunError
	sealedShutdownError()
}

// ItemError is returned by Run when an item failed and the Run context was not canceled before. Its Unwrap gives
// the item's own error, so errors.Is and errors.As work with it. The item's error can come from the ItemProcessor,
// from a task, or from RetainFor.
type ItemError interface {
	RunError
	// Unit is the node the item was in when it failed. For an item waiting in the queue in front of a node, it is
	// that node.
	Unit() Unit
	sealedItemError()
}

// shutdownError is the only ShutdownError implementation.
type shutdownError struct {
	cause error
	// drain and items are set only on the value Run returns.
	drain error
	items []error
}

func (e *shutdownError) sealedRunError()      {}
func (e *shutdownError) sealedShutdownError() {}
func (e *shutdownError) Unwrap() error        { return e.cause }
func (e *shutdownError) DrainError() error    { return e.drain }
func (e *shutdownError) ItemErrors() []error  { return e.items }

func (e *shutdownError) Error() string {
	msg := "conveyor is shutting down"
	if e.cause != nil {
		msg += ": " + e.cause.Error()
	}
	return msg + runErrorSuffix(e)
}

// itemError is the only ItemError implementation.
type itemError struct {
	unit Unit
	err  error
	// drain and items are set only on the value Run returns.
	drain error
	items []error
}

func (e *itemError) sealedRunError()     {}
func (e *itemError) sealedItemError()    {}
func (e *itemError) Unit() Unit          { return e.unit }
func (e *itemError) Unwrap() error       { return e.err }
func (e *itemError) DrainError() error   { return e.drain }
func (e *itemError) ItemErrors() []error { return e.items }
func (e *itemError) Error() string       { return e.err.Error() + runErrorSuffix(e) }

// runErrorSuffix tells in the error text that the shutdown had more to report, so it shows up in logs.
func runErrorSuffix(e RunError) string {
	var s string
	if n := len(e.ItemErrors()); n > 0 {
		s += fmt.Sprintf(" (and %d more item errors)", n)
	}
	if d := e.DrainError(); d != nil {
		s += fmt.Sprintf(" (drain: %v)", d)
	}
	return s
}

// isShutdown reports whether err is, or wraps, a shutdown cancellation.
func isShutdown(err error) bool {
	var e ShutdownError
	return errors.As(err, &e)
}

// Panic sentinels — programmer errors, never returned. They flag misuse of the conveyor as a synchronization
// primitive: calls that are meaningless or violate its usage contract, with no benign occurrence and no meaningful
// recovery. Like unlocking an unlocked sync.Mutex, these panic (loudly, immediately) rather than returning —
// whether the misuse is static wiring or a dynamic per-item contract violation. They are unexported on purpose: a
// caller must not branch on them (there is nothing to recover); they exist only so the package's own tests can
// assert the panic via errors.Is.
//
// Genuine runtime conditions that a correct program legitimately hits — ctx cancellation, ShutdownError,
// fail-fast work errors, and a stale/unknown context (benign, like a closed-channel receive) — are returned, not
// panicked.
var (
	// errInvalidUnit is panicked with when a node handle (Stage / FanOut / Branch / Task) is used on a conveyor or
	// fan-out it does not belong to — including a handle used with an item context from a different conveyor.
	errInvalidUnit = errors.New("invalid node")

	// errWrongScope is panicked with when an item tries to move to a node of another series: a child item may only
	// move through the nodes of the lane it runs in, and a root item may not reach into a lane.
	errWrongScope = errors.New("node belongs to another series")

	// errCannotMove is panicked with when a context handed to a Pool's work is used to move or wait: there is nowhere
	// to go, the work holds a slot it must not wait on, and the context carries the item that scheduled the work,
	// which must not be moved from a task goroutine. Such a context may only Schedule at its own fan-out.
	errCannotMove = errors.New("pool work cannot move or wait")

	// errConveyorRunning is panicked with when the topology is extended while the conveyor is running.
	errConveyorRunning = errors.New("cannot change the topology while the conveyor is running")

	// errConveyorFinalized is panicked with when the topology is extended after the conveyor has run (it is frozen
	// from the first Run on).
	errConveyorFinalized = errors.New("cannot change the topology after the conveyor has run")

	// errStageNotEntered is panicked with by Retain, RetainFor and FanOut.Retain when the current item is not in the
	// node it is trying to hand its slot to, and by FanOut.Schedule / FanOut.Wait at a fan-out the item never entered.
	// Its message reads as a trailing clause of the wrapped panic.
	errStageNotEntered = errors.New("the item does not currently occupy it")

	// errNothingToRetain is panicked with by FanOut.Retain when the item has no body to hand over at this fan-out:
	// the body is closed (the item left, or tried to and failed), or it was already retained.
	errNothingToRetain = errors.New("the item has no work to retain here")

	// errBodyClosed is panicked with when the ItemProcessor adds to or waits for its body at a fan-out after closing
	// it: by leaving (a leave that fails after the body was joined still closes it), or by returning.
	errBodyClosed = errors.New("the item's body at this fan-out is closed")

	// errWorkRetained is panicked with when the ItemProcessor adds to or waits for its body at a fan-out where it
	// retained the work: from Retain on, the body belongs to the returned wave.
	errWorkRetained = errors.New("the item's work at this fan-out was retained")

	// errWrongEnterOrder is panicked with by MoveTo when the target is behind the item's furthest rank (items move
	// forward only).
	errWrongEnterOrder = errors.New("items move forward only")

	// errNodeAlreadyEntered is panicked with by MoveTo when the current item has already entered this node (nodes
	// are entered once per item).
	errNodeAlreadyEntered = errors.New("nodes are entered once per item")

	// errNilTaskFunc flags a nil callback. The eager constructors (NewTask, NewTasks) panic with it;
	// a nil callback produced later by a streaming source (NewTasksGen, NewTasksChan)
	// instead fails the item with it (fail-fast), since by then the misuse surfaces on an internal goroutine.
	errNilTaskFunc = errors.New("nil callback")

	// errTaskReused is panicked with by FanOut.Schedule when a Task is submitted twice (tasks are lazy, stateful and
	// single-use).
	errTaskReused = errors.New("tasks are single-use")

	// errForeignWave is panicked with when Wave.Wait is called with the context of an item that did not create the
	// wave (or on a nil wave). A wave is only meaningful to its own item.
	errForeignWave = errors.New("wave belongs to another item")
)
