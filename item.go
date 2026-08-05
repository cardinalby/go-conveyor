package conveyor

import "context"

// item identifies one journey through a series and tracks its position and its outstanding background work. Both
// kinds of journey are items:
//
//   - a root item — one ItemProcessor call, moving through the conveyor's own nodes (scope 0);
//   - a child item — one piece of work on a Lane, moving through the lane's interior nodes (the lane's scope).
//     Its work is charged to the wave that created it, and it shares its parent's cancellation.
//
// All fields are guarded by run.mu (see state.go for the transitions that mutate them), except no/scope/ctx which
// are set once at creation and then read-only.
type item struct {
	// no identifies the conveyor item this journey belongs to, exposed via ItemNoFromContext: a child inherits its
	// parent's number, so the number always answers "which item am I part of". It is NOT used for any ordering
	// decision (the in-flight list carries order) — do not reintroduce no comparisons.
	no  int64
	run *run

	// scope is the id of the series this item travels through: 0 for a root item, the lane's scope for a child. It
	// bounds where the item may move: only nodes of the same scope.
	scope int

	// ctx is the context passed to this item's processor (it carries this item handle). A root item's context
	// derives from run.itemsCtx and owns cancel; a child's derives from its parent's item context and has no cancel
	// of its own — cancellation is item-wide, so a child's failure escalates to its parent instead (see poison).
	ctx    context.Context
	cancel context.CancelCauseFunc // nil for a child item

	// occupied[k] is how many slots this item currently holds in unit k (by index). An item may hold several units
	// at once (a node plus the queue it came through, briefly), and a pool's unit may be held with several slots by
	// one item when it runs several of that item's tasks there.
	occupied []int
	// entered[k] records whether this item has ever entered unit k. It is set on occupy and never cleared, so a
	// node stays "entered once ever" even after the item leaves it. Waiting in front of a node does not count as
	// entering it (see run.takeQueue).
	entered []bool
	// queuedAt is the index of the node this item is currently waiting in front of, or -1 when it is not waiting.
	// A single field suffices because an item can be in at most one waiting room at a time, and only in front of
	// the node it is trying to enter next — which is why the waiting room needs no unit of its own, and so costs
	// nothing per item beyond this int (see the capacity discussion in unit.go).
	queuedAt int
	// maxRank is the highest rank this item has published within its scope (its high-water mark). It only ever
	// increases and drives the ordering gate for the items behind it. A fan-out publishes it at enqueue, not at
	// admission, so an item that is inside the node but has not yet scheduled its work still blocks the next item's
	// enqueue — which is what keeps each branch's work in item order.
	maxRank int
	// reachedRank is the highest rank this item has actually occupied, whether or not it has been published yet. It
	// says where the item *is*, while maxRank says what the items behind it are allowed to see — the two differ only
	// between a fan-out's admission and its enqueue. Background work uses it to decide whether the item has moved on
	// (see wave.releaseRetained).
	reachedRank int
	// finished is set once the item's processor has returned and all its slots are released.
	finished bool

	// prev/next link the in-flight items of this scope in creation order (see run.scopeList). prev is the item's
	// binding predecessor for the ordering gate; both are nil once the item is finished (unlinked).
	prev, next *item

	// waves are the background-work handles this item created (FanOut.MoveTo, Stage.Retain). They are joined when
	// the item completes, and an error none of them had observed fails the item.
	waves []*wave
	// pending is the work the item scheduled at the fan-out it currently occupies and has not detached: the node's
	// body, which the item must see finish before it may leave (see run.joinPending). nil when the item is not in a
	// fan-out, or has handed its work over with FanOut.Detach.
	pending *wave

	// parentWave is the wave whose work created this child item (nil for a root item). The child's outcome is
	// reported to it.
	parentWave *wave
}

// poison cancels this item's context with cause (fail-fast). A child has no context of its own to cancel, so it
// escalates to its parent: cancellation is item-wide, and a child's failure fails its parent anyway. Caller holds
// run.mu.
func (it *item) poison(cause error) {
	if it.cancel != nil {
		it.cancel(cause)
		return
	}
	if it.parentWave != nil && it.parentWave.it != nil {
		it.parentWave.it.poison(cause)
	}
}

// hasLiveWaves reports whether any background work of this item is still outstanding. Caller holds run.mu.
func (it *item) hasLiveWaves() bool {
	for _, w := range it.waves {
		if !w.finishedSet {
			return true
		}
	}
	return false
}

// firstUnackedWaveErr returns the first error from a wave whose outcome nobody observed (no join, no Err call after
// it finished). Caller holds run.mu, after every wave has finished.
func (it *item) firstUnackedWaveErr() error {
	for _, w := range it.waves {
		if !w.acked && w.err != nil {
			return w.err
		}
	}
	return nil
}

// consumePending takes the item's pending wave: it marks the outcome observed (the item is about to be told about
// it) and clears it, so the item is free to leave the fan-out. The wave must have finished. Caller holds run.mu.
func (it *item) consumePending() *wave {
	w := it.pending
	it.pending = nil
	w.acked = true
	return w
}

// isRetaining reports whether a live wave of this item holds unit j — a Stage.Retain's stage or a FanOut.Detach's
// node. Such a slot must not be taken away when the item moves on; it is freed when the work returns. Caller holds
// run.mu.
func (it *item) isRetaining(j int) bool {
	for _, w := range it.waves {
		if !w.finishedSet && w.retainUnit != nil && w.retainUnit.index == j {
			return true
		}
	}
	return false
}
