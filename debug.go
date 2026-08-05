package conveyor

// UnitOccupants is a point-in-time snapshot of exactly which items occupy one unit right now. See
// DebugUnitOccupants.
type UnitOccupants struct {
	Unit Unit

	// InBody lists the item numbers holding a slot in the node itself, in arrival order. An item holding several
	// slots appears once per slot.
	InBody []int64

	// InQueue lists the item numbers waiting in front of the node, in arrival order.
	InQueue []int64
}

// DebugUnitOccupants reports, for every unit, exactly which items occupy its body and its waiting room right now.
// It exists for debugging and visualization; use Stats for production observability.
//
// Safe to call at any time, from any goroutine; outside a run it reports nil.
func (c *Conveyor) DebugUnitOccupants() []UnitOccupants {
	r := c.currentRun.Load()
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	n := len(c.units)
	inBody := make([][]int64, n)
	inQueue := make([][]int64, n)
	for _, scope := range r.scopes {
		for it := scope.head; it != nil; it = it.next {
			for unitIdx, count := range it.occupied {
				for k := 0; k < count; k++ {
					inBody[unitIdx] = append(inBody[unitIdx], it.no)
				}
			}
			if it.queuedAt >= 0 {
				inQueue[it.queuedAt] = append(inQueue[it.queuedAt], it.no)
			}
		}
	}

	result := make([]UnitOccupants, n)
	for _, u := range c.units {
		q := inQueue[u.index]
		if u.kind == kindStart && u.branchSeries != nil {
			// A branch's backlog is not the queuedAt admission gate (a branch has no waiting room of its own — its
			// work is born already queued): it is the collections not yet handed out, one per scheduled batch.
			q = nil
			for _, col := range r.taskQueues[u.index] {
				q = append(q, col.it.no)
			}
		}
		result[u.index] = UnitOccupants{Unit: u.handle(), InBody: inBody[u.index], InQueue: q}
	}
	return result
}
