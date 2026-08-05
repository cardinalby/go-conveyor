package conveyor

// Stats is a snapshot of a conveyor's runtime state, for metrics and observability. Its gauge fields report the
// range observed since the previous Stats call, not just a point sample.
//
// Reading resets the windows, so wire exactly one consumer to Stats.
type Stats struct {
	// InFlight counts the conveyor's own items — one per live ItemProcessor call. Child items of a lane's work are
	// not counted; their work shows up as branch occupancy instead.
	InFlight    Gauge
	LiveWorkers Gauge      // worker goroutines alive for root items
	Units       []UnitStat // one entry per node and branch, in creation order (index 0 is the implicit start stage)
}

// Gauge is a windowed view of an integer quantity: the minimum and maximum it reached since the previous Stats
// read, plus its value at that read.
type Gauge struct {
	Min  int
	Max  int
	Last int
}

// windowedInt is an integer together with the min/max it has reached since the window was last reset. Every
// mutation goes through add, so the window is always accurate; Stats reads and resets it via snapshot. All access
// is under run.mu.
type windowedInt struct {
	val, min, max int
}

// add changes the value by delta and widens the window to include the new value.
func (w *windowedInt) add(delta int) {
	w.val += delta
	if w.val > w.max {
		w.max = w.val
	}
	if w.val < w.min {
		w.min = w.val
	}
}

// snapshot returns the window as a Gauge and resets min/max to the current value for the next interval.
func (w *windowedInt) snapshot() Gauge {
	g := Gauge{Min: w.min, Max: w.max, Last: w.val}
	w.min, w.max = w.val, w.val
	return g
}

// UnitStat is the per-node portion of Stats. Unit is the handle (a Stage, a FanOut, a Pool, a Lane, or
// Conveyor.StartUnit) that produced it, for matching a stat back to what was built.
//
// Occupied and Limit describe the node itself: items running its code, with work outstanding, or occupying a
// branch's entrance. Queued describes what is waiting in front of it — a stage's or fan-out's waiting room, or a
// branch's accepted-but-not-started work.
type UnitStat struct {
	Unit     Unit
	Occupied Gauge // slots of the node in use since the previous Stats read
	Limit    int   // the node's capacity — the denominator for Occupied
	Queued   Gauge // work waiting in front of the node since the previous Stats read
}

// Stats returns a snapshot of the active run's state and resets the gauge windows. Safe to call at any time, from
// any goroutine; outside a run it reports the zero Stats.
func (c *Conveyor) Stats() Stats {
	r := c.currentRun.Load()
	if r == nil {
		return Stats{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	s := Stats{
		InFlight:    r.inFlight.snapshot(),
		LiveWorkers: r.liveWorkers.snapshot(),
		Units:       make([]UnitStat, 0, len(c.units)),
	}
	for _, u := range c.units {
		s.Units = append(s.Units, UnitStat{
			Unit:     u.handle(),
			Occupied: r.occupancy[u.index].snapshot(),
			Limit:    int(u.limit.Load()),
			Queued:   r.queued[u.index].snapshot(),
		})
	}
	return s
}

// handle returns the public handle for this unit: the Stage, FanOut or Branch that owns it (or the start-stage
// handle), so a UnitStat can be matched back to what the caller built.
func (u *unit) handle() Unit {
	if h, ok := u.owner.(Unit); ok {
		return h
	}
	return startHandle{u} // the implicit start stage, whose owner is not a public handle
}
