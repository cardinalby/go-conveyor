package conveyor

import (
	"errors"
	"fmt"
)

// Returned errors, inspectable with errors.Is on the value returned by Run and the node methods (Stage.MoveTo /
// FanOut.MoveTo), or on Wave.Err.
var (
	// ErrConveyorAlreadyRunning is returned by Run when the conveyor is already running.
	ErrConveyorAlreadyRunning = errors.New("conveyor is already running")

	// ErrInvalidContext classifies a context that cannot drive a node transition. Match it, or one of the two
	// variants below, with errors.Is.
	ErrInvalidContext = errors.New("invalid context")

	// ErrForeignContext indicates the context was never derived from an item's ctx.
	ErrForeignContext = fmt.Errorf("%w: not derived from a conveyor item's ctx", ErrInvalidContext)

	// ErrStaleContext indicates the context's item has already finished.
	ErrStaleContext = fmt.Errorf("%w: item is finished", ErrInvalidContext)
)

// ShutdownError is the cancellation cause of an item's context when the conveyor cancels it during a shutdown.
// Recover it with errors.As, or reach the wrapped cause with errors.Is (e.g. against context.Canceled).
//
// The interface is sealed: only the conveyor creates ShutdownErrors, so matching one reliably means the conveyor
// canceled the item.
type ShutdownError interface {
	error
	// Cause returns the shutdown reason: the first item error, or the Run context's cancellation cause.
	Cause() error
	// sealedShutdownError restricts implementations to this package.
	sealedShutdownError()
}

// shutdownError is the only ShutdownError implementation (see the sealed interface above).
type shutdownError struct {
	cause error
}

func (e *shutdownError) sealedShutdownError() {}

func (e *shutdownError) Cause() error { return e.cause }

func (e *shutdownError) Error() string {
	if e.cause == nil {
		return "conveyor is shutting down"
	}
	return "conveyor is shutting down: " + e.cause.Error()
}

// Unwrap exposes the cause so errors.Is / errors.As can reach it (e.g. errors.Is(err, context.Canceled)).
func (e *shutdownError) Unwrap() error { return e.cause }

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

	// errCannotMove is panicked with when a context handed to a Pool's work is used to move: there is nowhere to go,
	// and the context carries the item that scheduled the work, which must not be moved from a task goroutine.
	errCannotMove = errors.New("this work cannot move")

	// errConveyorRunning is panicked with when the topology is extended while the conveyor is running.
	errConveyorRunning = errors.New("cannot change the topology while the conveyor is running")

	// errConveyorFinalized is panicked with when the topology is extended after the conveyor has run (it is frozen
	// from the first Run on).
	errConveyorFinalized = errors.New("cannot change the topology after the conveyor has run")

	// errStageNotEntered is panicked with by Stage.Retain and FanOut.Detach when the current item does not occupy the
	// node it is trying to hand its slot to. Its message reads as a trailing clause of the wrapped panic.
	errStageNotEntered = errors.New("the item does not currently occupy it")

	// errNothingToDetach is panicked with by FanOut.Detach when the item has no work outstanding at this fan-out to
	// detach — it never scheduled any here, or it has already detached it.
	errNothingToDetach = errors.New("the item has no work to detach here")

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

	// errTaskReused is panicked with by FanOut.MoveTo when a Task is submitted twice (tasks are lazy, stateful and
	// single-use).
	errTaskReused = errors.New("tasks are single-use")

	// errForeignWave is panicked with when a wave is passed as a join target of an item that did not create it (or
	// a nil/foreign Wave implementation). A wave is only meaningful to its own item.
	errForeignWave = errors.New("wave belongs to another item")
)
