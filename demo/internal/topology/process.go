package topology

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	conveyor "github.com/cardinalby/go-conveyor"
)

// Delays holds live-adjustable processing delays, keyed by node/lane id. The running ItemProcessor reads them on
// every pass and the UI's slider writes them, both in real time, on a running conveyor — mirroring how Limit and
// QueueSize are already live on the conveyor side, for the one per-node dial the library itself knows nothing
// about.
type Delays struct {
	mu     sync.Mutex
	msByID map[string]int
}

// NewDelays seeds a Delays from a Spec's configured DelayMs, one entry per stage and per branch (a fan-out itself
// runs no code of its own, so it gets none) plus StartID for the implicit start stage's own delay. Recurses into
// every lane branch's own interior nodes, to any depth.
func NewDelays(spec Spec) *Delays {
	d := &Delays{msByID: make(map[string]int)}
	d.msByID[StartID] = spec.StartDelayMs
	seedDelays(d, spec.Nodes)
	return d
}

func seedDelays(d *Delays, nodes []NodeSpec) {
	for _, n := range nodes {
		d.msByID[n.ID] = n.DelayMs
		for _, br := range n.Branches {
			d.msByID[br.ID] = br.DelayMs
			if br.Kind == KindLane {
				seedDelays(d, br.Nodes)
			}
		}
	}
}

// Get returns the current delay for id as a Duration (0 for an unknown id).
func (d *Delays) Get(id string) time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	return time.Duration(d.msByID[id]) * time.Millisecond
}

// Set updates the live delay for id. An id this Delays was not seeded with is ignored rather than silently
// accepted, since it can never be read back by a running ItemProcessor built from the same Spec.
func (d *Delays) Set(id string, ms int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.msByID[id]; ok {
		d.msByID[id] = ms
	}
}

// TaskCounts holds live-adjustable tasks-per-item counts, keyed by branch id: how many tasks/children (see
// conveyor.Branch.NewTasks) one item schedules on a pool or a lane. Mirrors Delays: the running ItemProcessor reads
// it once per item, at the point that item reaches the fan-out and creates its tasks, so a live change never
// touches tasks an item has already created — only later items pick up the new count — and the UI's slider writes
// it in real time.
type TaskCounts struct {
	mu         sync.Mutex
	byBranchID map[string]int
}

// NewTaskCounts seeds a TaskCounts from a Spec's configured TasksPerItem, one entry per branch. Recurses into every
// lane branch's own interior nodes, to any depth, since a nested fan-out's branches need seeding too.
func NewTaskCounts(spec Spec) *TaskCounts {
	t := &TaskCounts{byBranchID: make(map[string]int)}
	seedTaskCounts(t, spec.Nodes)
	return t
}

func seedTaskCounts(t *TaskCounts, nodes []NodeSpec) {
	for _, n := range nodes {
		for _, br := range n.Branches {
			t.byBranchID[br.ID] = br.TasksPerItem
			if br.Kind == KindLane {
				seedTaskCounts(t, br.Nodes)
			}
		}
	}
}

// Get returns the current tasks-per-item count for branchID, floored at 1 (a branch not seeded with a positive
// count — an unknown id, or a Spec that predates this field — schedules one task per item, same as before
// TasksPerItem existed).
func (t *TaskCounts) Get(branchID string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if n := t.byBranchID[branchID]; n > 0 {
		return n
	}
	return 1
}

// Set updates the live tasks-per-item count for branchID. An id this TaskCounts was not seeded with is ignored,
// same as Delays.Set.
func (t *TaskCounts) Set(branchID string, count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.byBranchID[branchID]; ok {
		t.byBranchID[branchID] = count
	}
}

// FanOutEntry tracks, per fan-out node, which items are currently admitted into it but have not yet had their
// tasks dispatched — the narrow window between entering the node's waiting room/body and FanOut.MoveTo actually
// returning (join + task scheduling). DebugUnitOccupants cannot see this distinction on its own: an item counts as
// occupying the fan-out's body from the moment it is admitted, which happens *inside* MoveTo, before our own code
// gets control back. The UI uses this to show an item's circle with a striped fill while pending, solid once
// confirmed — see runtime.NodeState.PendingEntry.
//
// Entries are never explicitly removed mid-run: once an item leaves the fan-out (its next move releases the slot),
// DebugUnitOccupants simply stops reporting it there, so a stale "confirmed" entry left behind is inert — nothing
// ever looks it up again. NewFanOutEntry is called fresh per Run, which is the only cleanup this needs.
type FanOutEntry struct {
	mu      sync.Mutex
	pending map[string]map[int64]bool // nodeID -> itemNo -> pending
}

func NewFanOutEntry() *FanOutEntry {
	return &FanOutEntry{pending: make(map[string]map[int64]bool)}
}

// MarkPending records that itemNo has been admitted into nodeID's fan-out but MoveTo has not returned yet.
func (e *FanOutEntry) MarkPending(nodeID string, itemNo int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pending[nodeID] == nil {
		e.pending[nodeID] = make(map[int64]bool)
	}
	e.pending[nodeID][itemNo] = true
}

// MarkConfirmed records that itemNo's tasks have been dispatched — MoveTo returned successfully.
func (e *FanOutEntry) MarkConfirmed(nodeID string, itemNo int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.pending[nodeID], itemNo)
}

// Pending reports the item numbers currently pending at nodeID, for one State snapshot.
func (e *FanOutEntry) Pending(nodeID string) []int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]int64, 0, len(e.pending[nodeID]))
	for no := range e.pending[nodeID] {
		out = append(out, no)
	}
	return out
}

// BlockedEntries tracks, per start/stage node, which (item, path) occurrences have finished that node's own work
// (its configured delay) and are now attempting to advance into the next node — the window DebugUnitOccupants
// cannot see on its own, since a stage holds an item's slot identically throughout both phases: it is only released
// once the next node admits the item (see Stage.MoveTo), not when the item's own work finishes. The UI uses this to
// switch an item's circle from its normal filled look (still doing this node's own work) to an outlined one (done,
// waiting for room ahead) — see runtime.NodeState.BlockedLeaving. A fan-out needs no entry here: whether its own
// work is still running is already visible per-lane (InBody/InQueue), which is what the UI derives the same
// distinction from for a fan-out's entry slot.
//
// Keyed by (item number, path) rather than item number alone: a node outside any branch holds a given item number
// at most once, ever, so the item number alone used to be enough to identify an entry — but a node reachable
// through a branch can hold the very same item number again, and again, across distinct children/tasks the same
// item scheduled there (TasksPerItem > 1 on a Lane or a Pool, see LanePathEntry), sequentially or at once. Path is
// what tells those occurrences apart, exactly as it does everywhere else this package uses it. Keying by item
// number alone here made an entry meant for one child leak onto every later occurrence of that item number at the
// same node — reporting a freshly-admitted second child as already "finished, waiting to leave" from the instant it
// arrived, before it had even started its own delay.
//
// Entries are never removed mid-run, and unlike FanOutEntry there is no MarkConfirmed-style counterpart that could
// remove them: a stage's slot is released from inside the *next* node's MoveTo, so no point in the loop below knows
// that an occurrence has actually vacated the node it was marked at. A mark therefore means "this occurrence has
// finished nodeID's work at some point", not "is still standing in nodeID waiting to leave" — narrowing it to the
// latter needs the node's current occupants, which only the caller has: see runtime.Manager.State, which narrows via
// StillBlocked. Reading a node's raw marks on their own would report occurrences that left long ago. Growth is
// bounded by items x nodes for one run, and NewBlockedEntries is called fresh per Run.
type BlockedEntries struct {
	mu      sync.Mutex
	blocked map[string]map[blockedKey]bool
}

// blockedKey identifies one occurrence at one node: an item number plus its disambiguating path (see
// LanePathEntry), stringified because a slice cannot be a map key. pathKey("") is a root item's own top-level walk,
// where the item number alone is already unique.
type blockedKey struct {
	itemNo int64
	path   string
}

// pathKey stringifies a lane-child/pool-task ordinal path into a value comparable by ==, the way blockedKey needs
// (a []int cannot be a map key directly). nil/empty becomes "", one value for every path-less (root item) call.
func pathKey(path []int) string {
	if len(path) == 0 {
		return ""
	}
	b := make([]byte, 0, len(path)*4)
	for i, p := range path {
		if i > 0 {
			b = append(b, '.')
		}
		b = strconv.AppendInt(b, int64(p), 10)
	}
	return string(b)
}

func NewBlockedEntries() *BlockedEntries {
	return &BlockedEntries{blocked: make(map[string]map[blockedKey]bool)}
}

// Mark records that the occurrence of itemNo at path has finished nodeID's own work and is now trying to leave it.
// path is nil for a root item's own top-level walk, where the item number alone already identifies the occurrence.
func (e *BlockedEntries) Mark(nodeID string, itemNo int64, path []int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.blocked[nodeID] == nil {
		e.blocked[nodeID] = make(map[blockedKey]bool)
	}
	e.blocked[nodeID][blockedKey{itemNo: itemNo, path: pathKey(path)}] = true
}

// StillBlocked narrows this node's own marks down to whichever of occupants actually match one, preserving
// occupants' own order. occupants is the node's current occupants, one entry per occurrence — LanePaths itself for
// a node reachable through some branch, or one nil-path entry per raw InBody item otherwise (see
// runtime.Manager.State, the only caller). This is the occurrence-aware replacement for a bare item-number
// membership check: exact for a node outside any branch (item number alone already identifies the occurrence
// there), and what keeps a stale mark from one occurrence leaking onto a later, unrelated one sharing the same item
// number at a node reachable through a branch.
func (e *BlockedEntries) StillBlocked(nodeID string, occupants []LanePathEntry) []int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	marks := e.blocked[nodeID]
	if len(marks) == 0 {
		return []int64{}
	}
	out := make([]int64, 0, len(occupants))
	for _, o := range occupants {
		if marks[blockedKey{itemNo: o.ItemNo, path: pathKey(o.Path)}] {
			out = append(out, o.ItemNo)
		}
	}
	return out
}

// LanePathEntry is one lane child's, or one pool task's, presence at one node it currently occupies (its own
// interior node, or a node nested deeper still): ItemNo is the (possibly repeated) item number
// conveyor.DebugUnitOccupants reports there — a child/task always inherits its parent item's number, see item.go —
// and Path is the ordinal chain that identifies which one: [2] for the second child a top-level lane spawned for
// this item (or the second of its tasks running concurrently on a top-level pool, TasksPerItem > 1), [2, 1] for
// that child's own first child (or task) one branch deeper. The UI zips this against a node's InBody/InQueue
// occurrences of the same item number to label them "42.2", "42.2.1" instead of an ambiguous repeated "42" — see
// runtime.NodeState.LanePaths.
type LanePathEntry struct {
	ItemNo int64
	Path   []int
}

// LanePaths tracks, per node reachable through some branch (a lane's interior, or a pool's own tasks), which
// children/tasks currently occupy it. Unlike BlockedEntries/FanOutEntry this reports current occupancy, not "ever
// seen": an entry is added when a child's walk (or a pool task's run) reaches a node and removed once it moves on
// (see runNodes' withPath) or finishes, so a stale entry never lingers to mislabel a later, unrelated occupant of
// the same node. NewLanePaths is called fresh per Run.
type LanePaths struct {
	mu   sync.Mutex
	byID map[string][]LanePathEntry
}

func NewLanePaths() *LanePaths {
	return &LanePaths{byID: make(map[string][]LanePathEntry)}
}

// Enter records that itemNo's child or task at path has reached nodeID.
func (l *LanePaths) Enter(nodeID string, itemNo int64, path []int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.byID[nodeID] = append(l.byID[nodeID], LanePathEntry{ItemNo: itemNo, Path: path})
}

// Leave removes one entry matching itemNo and path exactly from nodeID — the counterpart to Enter, called once
// that child has moved past nodeID.
func (l *LanePaths) Leave(nodeID string, itemNo int64, path []int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entries := l.byID[nodeID]
	for i, e := range entries {
		if e.ItemNo == itemNo && samePath(e.Path, path) {
			l.byID[nodeID] = append(entries[:i], entries[i+1:]...)
			return
		}
	}
}

func samePath(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Snapshot returns every entry currently at nodeID, for one State read. The returned slice is a copy, safe to hold
// onto after the call.
func (l *LanePaths) Snapshot(nodeID string) []LanePathEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]LanePathEntry, len(l.byID[nodeID]))
	copy(out, l.byID[nodeID])
	return out
}

// ErrInjectedFailure is what a UI-injected item/task failure returns, wrapped with what was asked to fail. It is a
// plain error, deliberately not a shutdown one, so the library treats it as a genuine item failure — which is the
// entire point of injecting it (see Failures).
var ErrInjectedFailure = errors.New("failure injected from the UI")

func itemFailure(itemNo int64) error {
	return fmt.Errorf("item %d: %w", itemNo, ErrInjectedFailure)
}

func taskFailure(laneID string, itemNo int64) error {
	return fmt.Errorf("item %d's task on pool %q: %w", itemNo, laneID, ErrInjectedFailure)
}

// laneTask identifies one item's work on one lane — exactly what a task circle in the UI stands for. An item runs at
// most one task per lane of a fan-out at a time, so the pair is unique for as long as that task exists.
type laneTask struct {
	laneID string
	itemNo int64
}

// Failures records synthetic, UI-requested failures for individual in-flight items and lane tasks: Ctrl/⌘-clicking
// one item's rectangle or one task's circle asks that one item (or that one task) to fail. It exists so the demo can
// show what the library does when a single item's ItemProcessor returns a real error — error-shutdown: the first
// error is recorded as the run's result, no new items are created, later items are canceled, and earlier ones are
// left to finish (see run.completeItem) — without failing everything at once.
//
// A request is recorded and never consumed: an item passes a given node once, so there is nothing to un-mark, and
// the predicates are only ever asked about an item or task that is still running. Growth is bounded by one entry per
// click, and NewFailures is called fresh per Run.
//
// Changed is the wake-up side, and the reason this is not just two maps: without it a click would only take effect
// once the delay it landed in had elapsed anyway, which is precisely the wait it is meant to cut short. One shared
// broadcast channel is enough — clicks are rare, so the spurious wake-ups every waiter gets from someone else's
// click cost nothing, and it avoids keeping a channel per in-flight (item, node) pair.
type Failures struct {
	mu      sync.Mutex
	items   map[int64]bool
	tasks   map[laneTask]bool
	changed chan struct{}
}

func NewFailures() *Failures {
	return &Failures{items: make(map[int64]bool), tasks: make(map[laneTask]bool), changed: make(chan struct{})}
}

// FailItem requests that itemNo's ItemProcessor fail. It interrupts whatever delay the item is sleeping out right
// now; if the item is instead blocked inside a MoveTo (queued, or holding a finished node while it waits for room
// ahead) there is nothing here to interrupt — the library owns that wait — so the request lands the moment that
// MoveTo returns, before the item starts the next node's work. It also fails any lane task the item currently has
// outstanding, which is what makes a click on an item parked in a fan-out's entry slot do something: its tasks are
// the only part of it still under this package's control.
func (f *Failures) FailItem(itemNo int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items[itemNo] = true
	f.broadcastLocked()
}

// FailTask requests that itemNo's task on lane laneID fail. That error poisons the owning item fail-fast (see Wave),
// so its processor aborts too and the run ends up in the same error-shutdown; the only difference from FailItem is
// where the error originates, which is the interesting part to watch for a fan-out.
func (f *Failures) FailTask(laneID string, itemNo int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tasks[laneTask{laneID: laneID, itemNo: itemNo}] = true
	f.broadcastLocked()
}

// ItemFailed reports whether a failure has been requested for itemNo itself.
func (f *Failures) ItemFailed(itemNo int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.items[itemNo]
}

// TaskFailed reports whether a failure has been requested for itemNo's task on laneID specifically (an item-level
// request is not reported here — see the task predicate in ItemProcessor, which honors both).
func (f *Failures) TaskFailed(laneID string, itemNo int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tasks[laneTask{laneID: laneID, itemNo: itemNo}]
}

// Changed returns a channel closed by the next FailItem/FailTask call, whichever item or task it targets. Take it
// *before* testing ItemFailed/TaskFailed and select on it afterwards: that order is what makes a request landing
// between the two wake the waiter, instead of being missed until some later, unrelated click.
func (f *Failures) Changed() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changed
}

// broadcastLocked wakes every waiter currently parked on Changed and installs a fresh channel for the next ones.
// Caller holds mu.
func (f *Failures) broadcastLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}

// sleep parks for d, returning early if ctx is canceled (so a stop lands immediately instead of waiting out whatever's
// left of the current step) or as soon as failed reports a UI-injected failure for this step. It reports which of the
// two ended it: true means "fail this step now".
//
// The loop is there for the failure side: a click can land at any point during d, and it wakes every waiter, not just
// this one, so the wait has to be re-armed for the remaining time around each broadcast rather than being one select.
func sleep(ctx context.Context, d time.Duration, failures *Failures, failed func() bool) bool {
	deadline := time.Now().Add(d)
	for {
		// Order matters: taking the channel before testing failed() closes the window a request landing between the
		// two would otherwise fall into — see Failures.Changed.
		changed := failures.Changed()
		if failed() {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		t := time.NewTimer(remaining)
		select {
		case <-t.C:
			t.Stop()
			return failed() // one last look, so a click landing right on the deadline still counts
		case <-changed:
			t.Stop()
		case <-ctx.Done():
			t.Stop()
			return failed()
		}
	}
}

// ItemProcessor builds the generic conveyor.ItemProcessor for spec: it sleeps for the start stage's own delay, then
// hands off to runNodes for spec.Nodes — see its own doc for the walk shared with every lane's interior.
//
// built must come from Build(spec) — the handle lookups below assume every id in spec resolves to the matching
// Stage/FanOut/Pool/Lane kind, which Build guarantees for a spec it accepted.
func ItemProcessor(
	spec Spec,
	built *Built,
	delays *Delays,
	entries *FanOutEntry,
	blocked *BlockedEntries,
	failures *Failures,
	taskCounts *TaskCounts,
	lanePaths *LanePaths,
) conveyor.ItemProcessor {
	return func(ctx context.Context) error {
		no, _ := conveyor.ItemNoFromContext(ctx)
		itemFailed := func() bool { return failures.ItemFailed(no) }

		if sleep(ctx, delays.Get(StartID), failures, itemFailed) {
			return itemFailure(no)
		}
		blocked.Mark(StartID, no, nil) // a root item's own walk, no path to disambiguate — see BlockedEntries.Mark
		return runNodes(ctx, no, nil, spec.Nodes, built, delays, entries, blocked, failures, taskCounts, lanePaths, noRelease)
	}
}

// noRelease is the "nothing precedes this walk" releasePrev for the root item's own walk — see runNodes.
func noRelease() {}

// runNodes walks nodes in order, calling MoveTo on each stage in turn, or scheduling taskCounts.Get(branch) tasks
// per branch and calling MoveTo on the fan-out, and sleeps for the target's configured delay — read from delays on
// every pass, so a live slider change takes effect for the next item/child to reach that node (a pool's task reads
// it once, when the task actually starts running, for the same reason). Every sleep is followed by a
// BlockedEntries.Mark for the node just finished, before the loop goes on to attempt the next node's MoveTo — see
// BlockedEntries.
//
// It is called once for a root item, walking spec.Nodes with path == nil, and once per child a KindLane branch
// spawns, walking that lane's own interior nodes with path set to the child's ordinal chain (see LanePathEntry) —
// the recursion is what lets a lane's interior contain a fan-out whose own branches may again be lanes, to any
// depth.
//
// releasePrev releases whatever LanePaths entry preceded this walk (the lane entrance's own, for a child's first
// call here; noRelease for the root item, which has nothing to disambiguate). It is deliberately not called until
// *after* admission to the first node in nodes actually succeeds — release is the same rule applied node to node
// as the walk proceeds — because releasing it eagerly (right after the previous node's own work, before the next
// one admits) would leave a window, however brief, where nothing in LanePaths accounts for a child that
// DebugUnitOccupants still genuinely reports at the previous node (its real slot is held until the *next* node's
// MoveTo succeeds, exactly like everywhere else in the library). A same-item sibling whose own Enter lands inside
// that window would find no entry to reuse and fall back to an ambiguous bare item number — which collides with
// that item's own top-level slot key and made the UI flicker an item's rectangle into a lane's entrance and back.
// Keeping the previous node's entry alive until the new one is confirmed closes that window: for as long as the
// walk is in progress, path != nil, and something in nodes is actually occupied, exactly one LanePaths entry for
// it is alive, so there is always something for keyAssigner (../pipeline/itemPositions.ts) to find.
func runNodes(
	ctx context.Context,
	no int64,
	path []int,
	nodes []NodeSpec,
	built *Built,
	delays *Delays,
	entries *FanOutEntry,
	blocked *BlockedEntries,
	failures *Failures,
	taskCounts *TaskCounts,
	lanePaths *LanePaths,
	releasePrev func(),
) error {
	itemFailed := func() bool { return failures.ItemFailed(no) }
	release := releasePrev

	for _, n := range nodes {
		if path != nil {
			lanePaths.Enter(n.ID, no, path)
		}
		nodeID := n.ID
		releaseThis := func() {
			if path != nil {
				lanePaths.Leave(nodeID, no, path)
			}
		}

		switch n.Kind {
		case KindStage:
			st := built.Handles[n.ID].(conveyor.Stage)
			if err := st.MoveTo(ctx); err != nil {
				release()
				releaseThis()
				return err
			}
			release() // admitted here — whatever the child held before can finally let go
			release = releaseThis
			if itemFailed() {
				release()
				return itemFailure(no)
			}
			if sleep(ctx, delays.Get(n.ID), failures, itemFailed) {
				release()
				return itemFailure(no)
			}
			blocked.Mark(n.ID, no, path) // path disambiguates this occurrence from another child's own visit here
		case KindFanOut:
			fo := built.Handles[n.ID].(conveyor.FanOut)
			var tasks conveyor.Tasks
			for _, br := range n.Branches {
				id := br.ID
				switch br.Kind {
				case KindPool:
					pool := built.Handles[id].(conveyor.Pool)
					// Read once per item, right as its tasks are created: a live slider change (see
					// TaskCounts.Set) takes effect for the next item to reach this branch, never for one
					// already running with a count it already committed to — mirrors how a delay change only
					// affects a step not yet slept out.
					tasks.Add(pool.NewTasks(taskCounts.Get(id), func(tctx context.Context, i int) error {
						// Registered in lanePaths exactly like a lane child (see the KindLane case below), even
						// though a pool task never walks any further nodes: two of the same item's tasks can run on
						// this pool at once (TasksPerItem > 1), and without a path they would be indistinguishable
						// "42"s — see runtime.NodeState.LanePaths and ../../web/src/components/shared/TaskStrip,
						// which is what actually renders these as "42.1", "42.2" badges. A fresh backing array per
						// call, same reasoning as childPath below: concurrent siblings must never alias one
						// another's path.
						taskPath := append(append(make([]int, 0, len(path)+1), path...), i+1)
						lanePaths.Enter(id, no, taskPath)
						defer lanePaths.Leave(id, no, taskPath)
						// An item-level request fails this task too: it is the item's own work, and while the
						// item is parked in the fan-out's entry slot waiting for it, this is the only part of
						// the item still running any of this package's code — see Failures.FailItem.
						if sleep(tctx, delays.Get(id), failures, func() bool {
							return failures.TaskFailed(id, no) || failures.ItemFailed(no)
						}) {
							if failures.TaskFailed(id, no) {
								return taskFailure(id, no)
							}
							return itemFailure(no)
						}
						return nil
					}))
				case KindLane:
					lane := built.Handles[id].(conveyor.Lane)
					laneNodes := br.Nodes
					tasks.Add(lane.NewTasks(taskCounts.Get(id), func(tctx context.Context, i int) error {
						// A fresh backing array per call: concurrent siblings must never alias one another's
						// path, and append below must not silently reuse (and corrupt) another call's slice.
						childPath := append(append(make([]int, 0, len(path)+1), path...), i+1)
						// The lane's own entrance behaves exactly like the conveyor's own implicit start: every
						// child sleeps out its own configured delay here before moving on, whether or not the
						// lane has any interior nodes to move on to — see runtime.NodeState's doc on why an
						// empty lane looks and behaves like a pool.
						lanePaths.Enter(id, no, childPath)
						releaseEntrance := func() { lanePaths.Leave(id, no, childPath) }
						if sleep(tctx, delays.Get(id), failures, func() bool { return failures.ItemFailed(no) }) {
							releaseEntrance()
							return itemFailure(no)
						}
						blocked.Mark(id, no, childPath) // this child's own path — see BlockedEntries' own doc on why
						if len(laneNodes) == 0 {
							releaseEntrance() // nothing to move on to — release now, same as a stage's last node
							return nil
						}
						// releaseEntrance is not called here: see runNodes' own doc on why the entrance's entry
						// must outlive its own work until the first interior node's admission actually succeeds.
						return runNodes(tctx, no, childPath, laneNodes, built, delays, entries, blocked, failures, taskCounts, lanePaths, releaseEntrance)
					}))
				}
			}
			entries.MarkPending(n.ID, no)
			if err := fo.MoveTo(ctx, tasks); err != nil {
				release()
				releaseThis()
				return err
			}
			release()
			release = releaseThis
			entries.MarkConfirmed(n.ID, no)
			if itemFailed() {
				release()
				return itemFailure(no)
			}
		}
	}
	release() // the last node's own entry — this walk (the child's journey through nodes) is over
	return nil
}
