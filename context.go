package conveyor

import (
	"context"
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
)

// poolWorkMarker names the pool whose work holds the context, for the panic message.
type poolWorkMarker struct {
	pool *branch
}

// withItem derives an item context from parent, carrying the *item handle.
func withItem(parent context.Context, it *item) context.Context {
	return context.WithValue(parent, itemCtxKey, it)
}

// withPoolWork derives the context handed to a Pool's work: the scheduling item's context (so cancellation and
// deadlines are shared, with nothing extra to release), marked as non-movable.
func withPoolWork(parent context.Context, b *branch) context.Context {
	return context.WithValue(parent, poolWorkCtxKey, poolWorkMarker{pool: b})
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

// itemFromContext resolves the item a node method is being called for. It returns ErrForeignContext if the context
// does not carry an item handle (it was never derived from an item's ctx).
//
// It intentionally does not check here that the item belongs to the currently active run: Conveyor cancels an
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
//   - it panics (errCannotMove) if ctx belongs to a Pool's work, which has nowhere to move — the context
//     carries the scheduling item, so without this check the work would move that item from under its own
//     ItemProcessor;
//   - it returns ErrForeignContext if ctx carries no conveyor item;
//   - it panics (errInvalidUnit) if the item belongs to a different conveyor — a handle from this conveyor may only
//     be used with one of its own items.
//
// Callers still hold their own it.finished / cancellation handling.
func (c *Conveyor) resolveItem(ctx context.Context) (*item, error) {
	if m, ok := ctx.Value(poolWorkCtxKey).(poolWorkMarker); ok {
		panic(fmt.Errorf("this context belongs to work on %s, which has no nodes of its own to move through "+
			"(use AddLane for a branch whose work travels): %w", m.pool, errCannotMove))
	}
	it, err := itemFromContext(ctx)
	if err != nil {
		return nil, err
	}
	c.validateItemConveyor(it)
	return it, nil
}

// actingItem is the preamble every node method shares: it validates the handle and the acting item, then takes the
// run's lock. On success it returns the item and its run WITH r.mu HELD — the caller must unlock it (the package
// already returns under a held lock in run.join; the alternative, a closure, does not fit the four different return
// shapes of the node methods).
//
// checkCancel additionally declines a canceled item. The blocking paths leave it false: they get the cancellation
// check from waitUntil, which tests it before admissibility. The non-blocking ones (TryMoveTo) set it, having no
// wait to piggyback on — without it a canceled item would be reported as "no room". Stage.Retain leaves it false
// too, because it answers cancellation with a wave of its own rather than an error, which needs the item and the
// lock this call has just obtained.
//
// It panics on the same static misuse the individual methods used to check inline: a foreign handle (errInvalidUnit),
// a context belonging to a pool's non-movable work (errCannotMove), or a node outside the item's series (errWrongScope).
// Errors — a foreign or stale context — are returned for the caller to wrap with its own operation prefix.
func (c *Conveyor) actingItem(ctx context.Context, u *unit, checkCancel bool) (*item, *run, error) {
	c.validateUnit(u)
	it, err := c.resolveItem(ctx)
	if err != nil {
		return nil, nil, err
	}
	c.validateScope(it, u)
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
		if err := context.Cause(ctx); err != nil {
			r.mu.Unlock()
			return nil, nil, err
		}
	}
	return it, r, nil
}
