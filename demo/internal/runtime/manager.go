// Package runtime owns the demo's single conveyor instance across its whole lifetime and exposes the handful of
// operations the WASM/JS boundary needs: start it from a topology.Spec, cancel or force-stop it, poll its live
// state, and adjust a node's limit, queue size or delay while it runs.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	conveyor "github.com/cardinalby/go-conveyor"
	"github.com/cardinalby/go-conveyor/demo/internal/topology"
)

// LanePathEntry is the JSON twin of topology.LanePathEntry.
type LanePathEntry struct {
	ItemNo int64 `json:"itemNo"`
	Path   []int `json:"path"`
}

// NodeState is one node's live state: its current dials and exactly who occupies it, straight from
// conveyor.DebugUnitOccupants. InBody/InQueue are nil once the run has drained.
type NodeState struct {
	ID        string `json:"id"`
	Limit     int    `json:"limit"`
	QueueSize int    `json:"queueSize"`
	DelayMs   int    `json:"delayMs"`
	// TasksPerItem is how many tasks/children (see conveyor.Branch.NewTasks) one item schedules on this branch.
	// Always 0 for anything but a pool or a lane — see topology.TaskCounts.
	TasksPerItem int     `json:"tasksPerItem"`
	InBody       []int64 `json:"inBody"`
	InQueue      []int64 `json:"inQueue"`
	// PendingEntry lists the InBody items of a fan-out node that have been admitted but whose FanOut.MoveTo call
	// has not returned yet (tasks not dispatched). Always empty for a stage, a pool or the start stage — see
	// topology.FanOutEntry.
	PendingEntry []int64 `json:"pendingEntry"`
	// BlockedLeaving lists the InBody items of a start/stage node — or a lane's own entrance, which behaves exactly
	// like one — that have finished this node's own work and are now trying to advance into the next one. Always
	// empty for a fan-out or a pool — see topology.BlockedEntries.
	BlockedLeaving []int64 `json:"blockedLeaving"`
	// LanePaths disambiguates concurrent InBody/InQueue occurrences of the same item number for a node reachable
	// through some branch — a lane's interior, or a pool's own tasks (TasksPerItem > 1 running several of one
	// item's tasks at once) — since a child/task always inherits its parent item's number, so without this, two of
	// the same item's occupants sitting at the same node would be indistinguishable. Always empty for a node outside
	// any branch — see topology.LanePaths.
	LanePaths []LanePathEntry `json:"lanePaths"`
}

// State is a full snapshot returned by Manager.State, polled by the UI while in run mode.
type State struct {
	Running     bool        `json:"running"`
	Stopping    bool        `json:"stopping"`
	Forced      bool        `json:"forced"`
	Error       string      `json:"error,omitempty"`
	WorkerCount int         `json:"workerCount"`
	Nodes       []NodeState `json:"nodes"`
}

// Manager owns the demo's single conveyor instance across its whole lifetime: build-mode edits never reach it (the
// UI's topology.Spec is plain JS/TS data until Run is pressed), and each Run press builds a brand-new Conveyor from
// scratch, per the library's build-once-run-once contract — the topology is frozen forever after Run starts, so
// there is no such thing as "add a stage" on a live Manager, only CancelCtx/Stop, edit the Spec in the UI, and Run
// again.
type Manager struct {
	mu         sync.Mutex
	built      *topology.Built
	delays     *topology.Delays
	taskCounts *topology.TaskCounts
	entries    *topology.FanOutEntry
	blocked    *topology.BlockedEntries
	failures   *topology.Failures
	lanePaths  *topology.LanePaths
	// cancel stops the run's context: no new items are created from that point on (see CancelCtx).
	cancel context.CancelFunc
	// forceCancel is the shutdown context's own cancel func — see OptShutdownContext — that Stop uses to cancel
	// every item still in flight at once. It is wired up fresh per Run and owned here rather than by the conveyor
	// (the factory below hands back a nil CancelFunc) because it must be reachable from Stop, called independently
	// of - and possibly long after - the shutdown that begins it.
	forceCancel context.CancelFunc
	running     bool
	stopping    bool // true once CancelCtx or Stop has been called for the active run
	forced      bool // true once Stop has been called for the active run
	runErr      error
}

func New() *Manager { return &Manager{} }

// Run builds spec into a fresh conveyor and starts it in the background, returning as soon as it has started (Run
// never blocks on the conveyor draining). It fails if a previous run is still active — Stop it first — or if spec
// is malformed.
func (m *Manager) Run(spec topology.Spec) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return errors.New("already running: stop it first")
	}

	// forceCtx is the shutdown context Stop uses to cancel in-flight items at once (see OptShutdownContext). It
	// starts out live: CancelCtx alone never touches it, so items left running after a graceful shutdown keep going
	// exactly as before, unbounded, until Stop cancels it — immediately if the run isn't shutting down yet, or as
	// an escalation if CancelCtx already started a graceful one.
	forceCtx, forceCancel := context.WithCancel(context.Background())
	built, err := topology.Build(spec, conveyor.OptShutdownContext(func(error) (context.Context, context.CancelFunc) {
		return forceCtx, nil // Manager owns forceCancel directly; nothing for the conveyor to release itself
	}))
	if err != nil {
		forceCancel()
		return err
	}
	delays := topology.NewDelays(spec)
	taskCounts := topology.NewTaskCounts(spec)
	entries := topology.NewFanOutEntry()
	blocked := topology.NewBlockedEntries()
	failures := topology.NewFailures()
	lanePaths := topology.NewLanePaths()
	ctx, cancel := context.WithCancel(context.Background())

	m.built = built
	m.delays = delays
	m.taskCounts = taskCounts
	m.entries = entries
	m.blocked = blocked
	m.failures = failures
	m.lanePaths = lanePaths
	m.cancel = cancel
	m.forceCancel = forceCancel
	m.running = true
	m.stopping = false
	m.forced = false
	m.runErr = nil

	proc := topology.ItemProcessor(spec, built, delays, entries, blocked, failures, taskCounts, lanePaths)
	go func() {
		runErr := built.Conveyor.Run(ctx, proc)
		forceCancel() // release the shutdown context's resources now that the run has fully drained
		m.mu.Lock()
		defer m.mu.Unlock()
		m.running = false
		m.stopping = false
		m.forced = false
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			m.runErr = runErr
		}
	}()
	return nil
}

// CancelCtx cancels the active run's context — the graceful trigger: no new items are created from that point on,
// but every item already in flight keeps running and is left to finish its own journey on its own schedule, exactly
// as if Stop were never called (see Run's OptShutdownContext wiring). State.Running only drops once they all have.
// It is a no-op if nothing is running or a shutdown (graceful or forced) is already under way.
func (m *Manager) CancelCtx() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running || m.stopping {
		return
	}
	m.stopping = true
	m.cancel()
}

// Stop force-stops the active run: on top of everything CancelCtx does, it also cancels the shutdown context handed
// to the conveyor via OptShutdownContext, which cancels every item still in flight at once instead of leaving it to
// finish on its own — whether that shutdown started right now or was already under way from an earlier CancelCtx.
// It is a no-op if nothing is running or Stop was already called.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running || m.forced {
		return
	}
	m.stopping = true
	m.forced = true
	m.forceCancel() // cancel in-flight items first, so the run ctx cancellation below finds it already done
	m.cancel()
}

// State reports the current run's status and, for every node, its live dials plus exactly who occupies it (nil
// occupants once nothing is running). Safe to call at any time, from any goroutine.
//
// WorkerCount is the conveyor's own worker goroutines, not runtime.NumGoroutine() (which would also count the WASM
// runtime's own bookkeeping goroutines and anything else sharing the process): Stats().LiveWorkers is the pool that
// runs root items, and every unit outside the root scope — a branch's own entrance, and every node inside a lane's
// interior, at any depth (see topology.Built.Depth) — has its Occupied count summed in on top, since a lane child's
// branchWorker goroutine runs the child's *entire* journey inline and so is invisible to LiveWorkers (a pool's work
// never travels either, so its Occupied is exactly its running branchWorker goroutines the same way). The two sums
// together account for every goroutine currently doing conveyor work. This is the single consumer of Stats — see
// its own docs on why that matters — so its Min/Max gauge windows are safe to leave unread here in favor of Last,
// the live count the UI wants.
func (m *Manager) State() State {
	m.mu.Lock()
	built, delays, taskCounts, entries, blocked, lanePaths := m.built, m.delays, m.taskCounts, m.entries, m.blocked, m.lanePaths
	s := State{Running: m.running, Stopping: m.stopping, Forced: m.forced}
	if m.runErr != nil {
		s.Error = m.runErr.Error()
	}
	m.mu.Unlock()

	if built == nil {
		return s
	}

	idByUnit := make(map[conveyor.Unit]string, len(built.Handles))
	for id, h := range built.Handles {
		idByUnit[h] = id
	}

	stats := built.Conveyor.Stats()
	s.WorkerCount = stats.LiveWorkers.Last
	for _, u := range stats.Units {
		if built.Depth[idByUnit[u.Unit]] > 0 {
			s.WorkerCount += u.Occupied.Last
		}
	}

	occByUnit := make(map[conveyor.Unit]conveyor.UnitOccupants)
	for _, o := range built.Conveyor.DebugUnitOccupants() {
		occByUnit[o.Unit] = o
	}

	s.Nodes = make([]NodeState, 0, len(built.Handles))
	for id, u := range built.Handles {
		ns := NodeState{
			ID:           id,
			Limit:        unitLimit(u),
			QueueSize:    unitQueueSize(u),
			DelayMs:      int(delays.Get(id).Milliseconds()),
			PendingEntry: []int64{},
			LanePaths:    []LanePathEntry{},
		}
		if o, ok := occByUnit[u]; ok {
			ns.InBody, ns.InQueue = o.InBody, o.InQueue
		}
		reachableThroughBranch := built.Depth[id] > 0
		if reachableThroughBranch {
			for _, e := range lanePaths.Snapshot(id) {
				ns.LanePaths = append(ns.LanePaths, LanePathEntry{ItemNo: e.ItemNo, Path: e.Path})
			}
		}
		// Narrowed to who is actually in the node right now, rather than taken raw: BlockedEntries never unmarks, so
		// a node's own marks include every occurrence that has ever finished its work there, including ones that
		// moved on long ago (or, for a node reachable through a branch, a wholly different child/task that merely
		// shares this occurrence's item number — see BlockedEntries' own doc). StillBlocked matches by (item number,
		// path) for such a node — LanePaths is exactly that, one entry per current occurrence — and falls back to
		// item number alone for a node outside any branch, which can never hold the same item number twice at once.
		ns.BlockedLeaving = stillBlocked(blocked, id, ns.InBody, ns.LanePaths, reachableThroughBranch)
		if _, ok := u.(conveyor.FanOut); ok {
			ns.PendingEntry = entries.Pending(id)
		}
		if _, ok := u.(conveyor.Branch); ok {
			ns.TasksPerItem = taskCounts.Get(id)
		}
		s.Nodes = append(s.Nodes, ns)
	}
	return s
}

// stillBlocked narrows blocked's own marks for nodeID down to whichever of the node's current occupants they
// actually belong to, preserving occupants' own order. This is exact rather than best-effort: takeUnit occupies the
// next unit and releases the previous one in a single critical section, and DebugUnitOccupants reads under that
// same mutex, so no snapshot can ever show one occurrence occupying two stage bodies at once.
//
// throughBranch selects how occupants are identified: LanePaths itself (one entry per current occurrence, already
// in the exact shape BlockedEntries.Mark was called with — see topology.LanePathEntry) for a node reachable through
// some branch, where the same item number can occupy the node more than once over time or at once (TasksPerItem >
// 1 on a Lane or a Pool); a synthetic nil-path entry per raw inBody item otherwise, where the item number alone
// already identifies the occurrence, since a node outside any branch can never hold the same item number twice.
func stillBlocked(blocked *topology.BlockedEntries, nodeID string, inBody []int64, lanePaths []LanePathEntry, throughBranch bool) []int64 {
	occupants := make([]topology.LanePathEntry, 0, len(inBody))
	if throughBranch {
		for _, e := range lanePaths {
			occupants = append(occupants, topology.LanePathEntry{ItemNo: e.ItemNo, Path: e.Path})
		}
	} else {
		for _, no := range inBody {
			occupants = append(occupants, topology.LanePathEntry{ItemNo: no})
		}
	}
	return blocked.StillBlocked(nodeID, occupants)
}

// SetLimit adjusts a running node's concurrency limit immediately (see Stage.SetLimit / FanOut.SetLimit /
// Lane.SetLimit — safe on a live conveyor by design). It errors if nothing is running or id names no node.
func (m *Manager) SetLimit(id string, value int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, err := m.handleLocked(id)
	if err != nil {
		return err
	}
	switch h := u.(type) {
	case conveyor.Stage:
		h.SetLimit(value)
	case conveyor.FanOut:
		h.SetLimit(value)
	case conveyor.Pool:
		h.SetLimit(value)
	default:
		// A Lane's entrance is fixed at one child at a time (see conveyor.Pool), so it lands here.
		return fmt.Errorf("node %q has no adjustable limit", id)
	}
	return nil
}

// SetQueueSize adjusts a running stage's or fan-out's waiting room immediately. A branch has none of its own (see
// conveyor.Pool) and errors here.
func (m *Manager) SetQueueSize(id string, value int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, err := m.handleLocked(id)
	if err != nil {
		return err
	}
	switch h := u.(type) {
	case conveyor.Stage:
		h.SetQueueSize(value)
	case conveyor.FanOut:
		h.SetQueueSize(value)
	default:
		return fmt.Errorf("node %q has no adjustable queue size", id)
	}
	return nil
}

// FailItem asks one specific in-flight item to fail: its ItemProcessor returns an error instead of finishing, which
// is exactly what puts the run into the library's error-shutdown — the error becomes Run's result, no new items are
// created, later items are canceled and earlier ones are left to finish. Every other item is untouched, which is the
// point: it is how the demo shows a partial failure rather than a wholesale stop. It errors if nothing is running.
func (m *Manager) FailItem(itemNo int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running || m.failures == nil {
		return errors.New("not running")
	}
	m.failures.FailItem(itemNo)
	return nil
}

// FailTask asks the task item itemNo is running on pool poolID to fail. The task's error poisons its item fail-fast,
// so the item fails too and the run reaches the same error-shutdown as FailItem — what differs, and what makes this
// worth having separately, is that the failure originates in a fan-out's background work rather than in the item's
// own straight-line code. It errors if nothing is running or poolID names something other than a pool of this run.
// A lane child's own failure needs no equivalent here — its journey is the item's own work one level down, so
// FailItem already reaches it (see topology.runNodes).
func (m *Manager) FailTask(poolID string, itemNo int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, err := m.handleLocked(poolID)
	if err != nil {
		return err
	}
	if _, ok := u.(conveyor.Pool); !ok {
		return fmt.Errorf("node %q is not a pool", poolID)
	}
	m.failures.FailTask(poolID, itemNo)
	return nil
}

// SetItemsLimit adjusts the active run's items-in-flight cap immediately (see conveyor.Conveyor.SetItemsLimit —
// safe on a live conveyor by design). Unlike SetLimit/SetQueueSize/SetDelay it names no node: the cap is global to
// the conveyor. It errors if nothing is running.
func (m *Manager) SetItemsLimit(value int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.running || m.built == nil {
		return errors.New("not running")
	}
	m.built.Conveyor.SetItemsLimit(value)
	return nil
}

// SetDelay adjusts a running node's simulated processing time immediately: the next item to reach it reads the new
// value (see topology.Delays).
func (m *Manager) SetDelay(id string, ms int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.handleLocked(id); err != nil {
		return err
	}
	m.delays.Set(id, ms)
	return nil
}

// SetTasksPerItem adjusts how many tasks/children (see conveyor.Branch.NewTasks) one item schedules on branchID: the
// next item to reach it reads the new value (see topology.TaskCounts); an item already running keeps whatever count
// it started with. It errors if nothing is running or branchID names something other than a pool or a lane of this
// run.
func (m *Manager) SetTasksPerItem(branchID string, count int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, err := m.handleLocked(branchID)
	if err != nil {
		return err
	}
	if _, ok := u.(conveyor.Branch); !ok {
		return fmt.Errorf("node %q is not a branch", branchID)
	}
	m.taskCounts.Set(branchID, count)
	return nil
}

// handleLocked resolves id against the active run's handles. Caller holds mu.
func (m *Manager) handleLocked(id string) (conveyor.Unit, error) {
	if !m.running || m.built == nil {
		return nil, errors.New("not running")
	}
	u, ok := m.built.Handles[id]
	if !ok {
		return nil, fmt.Errorf("unknown node id %q", id)
	}
	return u, nil
}

// unitLimit reads a handle's Limit() generically; the implicit start stage exposes none (it is always 1 — see
// Conveyor.StartUnit).
func unitLimit(u conveyor.Unit) int {
	switch h := u.(type) {
	case conveyor.Stage:
		return h.Limit()
	case conveyor.FanOut:
		return h.Limit()
	case conveyor.Pool:
		return h.Limit()
	default:
		return 1 // a Lane's entrance, and the implicit start stage
	}
}

// unitQueueSize reads a handle's QueueSize() generically; a branch and the implicit start stage have none.
func unitQueueSize(u conveyor.Unit) int {
	switch h := u.(type) {
	case conveyor.Stage:
		return h.QueueSize()
	case conveyor.FanOut:
		return h.QueueSize()
	default:
		return 0
	}
}
