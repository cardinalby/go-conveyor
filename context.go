package conveyor

import (
	"context"
	"errors"
	"fmt"
)

type ctxKey int

const (
	// itemCtxKey is the private key under which the acting *item is stored in an item's context. A single key is
	// enough: the handle carries the item number and a back-reference to its run.
	itemCtxKey ctxKey = iota
	// poolWorkCtxKey marks a context handed to work on a Pool. Such work has nowhere to move, and the context it
	// holds still carries its scheduling item — so the marker is what turns an attempted move into a clear panic
	// instead of silently moving that item from a task goroutine.
	poolWorkCtxKey
	// retainTaskCtxKey marks a context handed to a RetainFor task. Like pool work, the task runs in the background
	// with the item's context and must not drive the item: the marker turns a node call into a clear panic.
	retainTaskCtxKey
	// untilShutdownCtxKey marks a context made by UntilShutdown (see untilShutdownMark).
	untilShutdownCtxKey
)

// untilShutdownMark is the value under untilShutdownCtxKey: the run whose shutdown ends the context, and the
// context's Done channel, which tells the marked context itself from a derivation that drops its cancellation.
type untilShutdownMark struct {
	r    *run
	done <-chan struct{}
}

// poolWorkMarker identifies the collection whose work holds the context. Through it the runtime reaches the pool
// (to name it when the work tries to move), and the task group and owning item the work is charged to.
type poolWorkMarker struct {
	col *taskCollection
}

// retainTaskMarker names the stage (or lane) a RetainFor task holds, for the panic of a node call made with its
// context.
type retainTaskMarker struct {
	u *unit
}

// withItem derives an item context from parent, carrying the *item handle.
func withItem(parent context.Context, it *item) context.Context {
	return context.WithValue(parent, itemCtxKey, it)
}

// withPoolWork derives the context handed to a Pool's work: the scheduling item's context (so cancellation and
// deadlines are shared, with nothing extra to release), marked as non-movable and tied to its collection.
func withPoolWork(parent context.Context, col *taskCollection) context.Context {
	return context.WithValue(parent, poolWorkCtxKey, poolWorkMarker{col: col})
}

// withRetainTask derives the context handed to a RetainFor task holding u: its item's context, marked so that node
// calls with it panic.
func withRetainTask(parent context.Context, u *unit) context.Context {
	return context.WithValue(parent, retainTaskCtxKey, retainTaskMarker{u: u})
}

// checkTaskCtx panics (errCannotMove) if ctx belongs to a background task — a pool task or a RetainFor task — which
// must not move or wait for its item. verb names the refused call, target the node or task group it was about.
func checkTaskCtx(ctx context.Context, verb string, target any) {
	if m, ok := ctx.Value(poolWorkCtxKey).(poolWorkMarker); ok {
		panic(poolWorkPanic(verb, target, m.col.branch))
	}
	if m, ok := ctx.Value(retainTaskCtxKey).(retainTaskMarker); ok {
		panic(fmt.Errorf("cannot %s %s with the context of a RetainFor task on %s: only the ItemProcessor may: %w",
			verb, target, m.u, errCannotMove))
	}
}

// ItemNoFromContext returns the number of the item this context belongs to, for logging and tracing. A child
// item's work reports the number of the item that scheduled it. ok is false if ctx did not originate from a
// conveyor item.
func ItemNoFromContext(ctx context.Context) (no int64, ok bool) {
	if it, ok := ctx.Value(itemCtxKey).(*item); ok && it != nil {
		return it.no, true
	}
	return 0, false
}

// UntilShutdown returns a context derived from ctx, an item's context, that is also done once shutdown of the run
// begins: the Run context is canceled, or an item fails. Its cause is then a ShutdownError. If shutdown has already
// begun, the context is born done. Its Done channel may close a moment after shutdown begins, but node methods
// called with it fail from the moment shutdown begins.
//
// Use it for the part of the path that may stop at once, such as reading the input and moving into the first node
// with side effects; use ctx after that:
//
//	pre := conveyor.UntilShutdown(ctx)
//	msg, err := reader.Fetch(pre)
//	if err != nil {
//		return conveyor.ShutdownCause(pre, err) // an abort if pre stopped the fetch, not a failure
//	}
//	if err := write.MoveTo(pre); err != nil {
//		return err // a ShutdownError once shutdown has begun
//	}
//	// from here on, use ctx
//
// The error of a call canceled through it, e.g. one wrapping context.Canceled, is a failure if returned as is (see
// Conveyor.Run): pass it through ShutdownCause. An item may also go on with ctx after its UntilShutdown context is
// done, e.g. to finish a partial batch.
//
// Calls with the item's own ctx return the same context. Any other ctx gets a new context that lives until ctx or
// the item is done, so call it once per item and derive from the result (context.WithValue(pre, ...)) instead of
// calling UntilShutdown(context.WithValue(ctx, ...)) in a loop.
//
// A ctx that does not belong to a conveyor item is returned unchanged.
func UntilShutdown(ctx context.Context) context.Context {
	it, ok := ctx.Value(itemCtxKey).(*item)
	if !ok || it == nil {
		return ctx
	}
	r := it.run
	// Already one: no lock needed. A shutdown that begins right after the load is the same as one that begins right
	// after the return, and node methods check shutdownErr under mu.
	m, ok := ctx.Value(untilShutdownCtxKey).(untilShutdownMark)
	if ok && m.r == r && m.done == ctx.Done() && (!r.shutdownBegun.Load() || ctx.Err() != nil) {
		return ctx
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx != it.ctx {
		out, _ := r.newUntilShutdown(ctx)
		return out
	}
	if it.untilShutdown == nil {
		it.untilShutdown, it.cancelUntilShutdown = r.newUntilShutdown(ctx)
	} else if r.shutdownErr != nil {
		it.cancelUntilShutdown(r.shutdownErr) // done now, not only once its watch has run
	}
	return it.untilShutdown
}

// ShutdownCause maps err, returned by a call made with pre, to the reason pre stopped it: if pre is done and err is or
// wraps pre's own error (context.Canceled, context.DeadlineExceeded), it returns context.Cause(pre) — a ShutdownError
// once shutdown has begun, so returning it is an abort, not a failure. Any other err, nil included, is returned as is.
//
// Use it in an ItemProcessor or a task for every error of a call that got an UntilShutdown context:
//
//	pre := conveyor.UntilShutdown(ctx)
//	if err := db.Exec(pre, query); err != nil {
//		return conveyor.ShutdownCause(pre, err)
//	}
func ShutdownCause(pre context.Context, err error) error {
	if err == nil {
		return nil
	}
	if perr := pre.Err(); perr != nil && errors.Is(err, perr) {
		return context.Cause(pre)
	}
	return err
}

// newUntilShutdown derives the UntilShutdown context of ctx. The watch on shutdownCtx is stopped once the context is
// done, so it lives no longer than ctx (and at most until Run returns). Caller holds mu.
func (r *run) newUntilShutdown(ctx context.Context) (context.Context, context.CancelCauseFunc) {
	d, cancel := context.WithCancelCause(ctx)
	out := context.WithValue(d, untilShutdownCtxKey, untilShutdownMark{r: r, done: d.Done()})
	if r.shutdownErr != nil {
		cancel(r.shutdownErr)
		return out, cancel
	}
	stop := context.AfterFunc(r.shutdownCtx, func() { cancel(context.Cause(r.shutdownCtx)) })
	h := r.conveyor.watchHook
	if h != nil {
		h(1)
	}
	context.AfterFunc(d, func() {
		if stop() && h != nil {
			h(-1)
		}
	})
	return out, cancel
}

// isUntilShutdown reports whether ctx is, or derives from, an UntilShutdown context of run r.
func isUntilShutdown(ctx context.Context, r *run) bool {
	m, ok := ctx.Value(untilShutdownCtxKey).(untilShutdownMark)
	return ok && m.r == r
}

// itemFromContext resolves the item a node method is being called for. It returns ErrForeignContext if the context
// does not carry an item handle (it was never derived from an item's ctx).
//
// It intentionally does not check here that the item belongs to the currently active run: the conveyor cancels an
// item's context when the item finishes and when Run returns, so a context left over from a finished item or a
// previous run is normally already canceled and the caller's cancellation check neutralizes it. The remaining
// case — a stale context decoupled from cancellation (e.g. context.WithoutCancel) — is caught by the callers'
// it.finished guard, which returns ErrStaleContext.
func itemFromContext(ctx context.Context) (*item, error) {
	it, ok := ctx.Value(itemCtxKey).(*item)
	if !ok || it == nil {
		return nil, ErrForeignContext
	}
	return it, nil
}

// resolveItem returns the item acting under ctx for a node method of this conveyor. It combines the checks every
// such method needs:
//   - it panics (errCannotMove) if ctx belongs to a Pool's work or a RetainFor task, which must not move or wait —
//     the context carries the scheduling item, so without this check the task would move that item from under its
//     own ItemProcessor; verb names the refused call ("move to", "wait at", ...) so the hint fits it;
//   - it returns ErrForeignContext if ctx carries no conveyor item;
//   - it panics (errInvalidUnit) if the item belongs to a different conveyor — a handle from this conveyor may only
//     be used with one of its own items.
//
// Callers still hold their own it.finished / cancellation handling.
func (c *conveyor) resolveItem(ctx context.Context, verb string, u *unit) (*item, error) {
	checkTaskCtx(ctx, verb, u)
	it, err := itemFromContext(ctx)
	if err != nil {
		return nil, err
	}
	c.validateItemConveyor(it)
	return it, nil
}

// poolWorkPanic builds the errCannotMove panic for a node method called with a pool task's context. A move gets the
// AddLane hint; a wait gets the hold-and-wait reason and the continuation hint; anything else is refused plainly.
// target is the node or the task group the call was about.
func poolWorkPanic(verb string, target any, pool fmt.Stringer) error {
	at := fmt.Sprintf(" %s", target)
	switch verb {
	case "move to", "try move to":
		return fmt.Errorf("cannot %s%s with the context of a task on %s: a task has no nodes of its own to move "+
			"through (use AddLane for a branch whose work travels): %w", verb, at, pool, errCannotMove)
	case "wait at", "wait for":
		return fmt.Errorf("cannot %s%s with the context of a task on %s: a running task holds a slot and must not "+
			"wait for other work (schedule the follow-up as a new task instead): %w", verb, at, pool, errCannotMove)
	default:
		return fmt.Errorf("cannot %s%s with the context of a task on %s: only the ItemProcessor may: %w",
			verb, at, pool, errCannotMove)
	}
}

// resolveCaller returns who acts under ctx for FanOut.Schedule, the one node method open to a pool's work: the
// collection whose work holds the context (nil when ctx is an item's own), and the acting item — that work's owner,
// or the item the context carries. It returns ErrForeignContext if ctx carries neither, and panics (errInvalidUnit)
// if the item belongs to a different conveyor, or (errCannotMove) for a RetainFor task's context.
func (c *conveyor) resolveCaller(ctx context.Context) (*taskCollection, *item, error) {
	if m, ok := ctx.Value(poolWorkCtxKey).(poolWorkMarker); ok {
		c.validateItemConveyor(m.col.it)
		return m.col, m.col.it, nil
	}
	if m, ok := ctx.Value(retainTaskCtxKey).(retainTaskMarker); ok {
		panic(fmt.Errorf("cannot schedule with the context of a RetainFor task on %s: only the ItemProcessor may: %w",
			m.u, errCannotMove))
	}
	it, err := itemFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	c.validateItemConveyor(it)
	return nil, it, nil
}

// actingItem is the preamble every node method shares: it validates the handle and the acting item, then takes the
// run's lock. On success it returns the item and its run WITH r.mu HELD — the caller must unlock it (the alternative,
// a closure, does not fit the four different return shapes of the node methods).
//
// checkCancel additionally declines a canceled item — canceled on the call context or on its own context, see
// item.cancelCause. The blocking paths leave it false: they get the cancellation check from waitUntil, which tests it
// before admissibility. The non-blocking ones (TryMoveTo) set it, having no wait to piggyback on — without it a
// canceled item would be reported as "no room". RetainFor leaves it false too, because it answers cancellation
// with a task group of its own rather than an error, which needs the item and the lock this call has just obtained.
//
// It panics on the same static misuse the individual methods used to check inline: a foreign handle
// (errInvalidUnit), the context of a background task (errCannotMove), or a node outside the item's series
// (errWrongScope). verb names the call in those panics ("move to", "wait at", ...). Errors — a foreign or stale
// context — are returned for the caller to wrap with its own operation prefix.
func (c *conveyor) actingItem(ctx context.Context, verb string, u *unit, checkCancel bool) (*item, *run, error) {
	c.validateUnit(u)
	it, err := c.resolveItem(ctx, verb, u)
	if err != nil {
		return nil, nil, err
	}
	c.validateScope(it, verb, u)
	r := it.run
	r.mu.Lock()
	if it.finished {
		// The item already finished; its context must no longer drive transitions. Normally the context is canceled
		// on finish and caught by waitUntil — this guards a caller that decoupled its ctx from cancellation (e.g.
		// context.WithoutCancel).
		r.mu.Unlock()
		return nil, nil, ErrStaleContext
	}
	if checkCancel {
		if err := it.cancelCause(ctx); err != nil {
			r.dropDormantIfCanceled(it)
			r.mu.Unlock()
			return nil, nil, err
		}
	}
	return it, r, nil
}
