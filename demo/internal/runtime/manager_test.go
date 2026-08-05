package runtime

import (
	"strings"
	"testing"
	"time"

	"github.com/cardinalby/go-conveyor/demo/internal/topology"
)

// waitFor polls cond every millisecond until it holds, failing the test if it never does. Every wait in this file is
// on the simulation making progress, which takes real (if short) time — there is no hook to synchronize on.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// twoStageSpec is deliberately fast (10ms a step) and narrow (limit 1, no waiting room) so items pass through in a
// predictable trickle and a test finishes in milliseconds.
func twoStageSpec() topology.Spec {
	return topology.Spec{
		StartDelayMs: 10,
		Nodes: []topology.NodeSpec{
			{ID: "a", Kind: topology.KindStage, Limit: 1, DelayMs: 10},
			{ID: "b", Kind: topology.KindStage, Limit: 1, DelayMs: 10},
		},
	}
}

// fanOutSpec's pools are slow on purpose: the tests below have to inject while a task is genuinely mid-flight, and a
// 300ms window makes that reliable without costing anything — an injection cuts the remaining sleep short, so the
// tests finish as soon as it lands rather than waiting the delay out.
func fanOutSpec() topology.Spec {
	return topology.Spec{
		StartDelayMs: 10,
		Nodes: []topology.NodeSpec{
			{ID: "f", Kind: topology.KindFanOut, Limit: 1, Branches: []topology.BranchSpec{
				{ID: "l1", Kind: topology.KindPool, Limit: 1, DelayMs: 300},
				{ID: "l2", Kind: topology.KindPool, Limit: 1, DelayMs: 300},
			}},
			{ID: "after", Kind: topology.KindStage, Limit: 1, DelayMs: 10},
		},
	}
}

// laneSpec puts a Lane (not a Pool) on the fan-out, with one interior stage — the case fanOutSpec doesn't cover: a
// child travelling a branch's own nodes rather than running one leaf callback.
func laneSpec() topology.Spec {
	return topology.Spec{
		StartDelayMs: 10,
		Nodes: []topology.NodeSpec{
			{ID: "f", Kind: topology.KindFanOut, Limit: 1, Branches: []topology.BranchSpec{
				{ID: "l1", Kind: topology.KindLane, Nodes: []topology.NodeSpec{
					{ID: "l1s1", Kind: topology.KindStage, Limit: 1, DelayMs: 300},
				}},
			}},
			{ID: "after", Kind: topology.KindStage, Limit: 1, DelayMs: 10},
		},
	}
}

// nodeByID finds one node in a State snapshot. State reports nodes in map order, so nothing may index it positionally.
func nodeByID(s State, id string) (NodeState, bool) {
	for _, n := range s.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return NodeState{}, false
}

// occupies reports whether itemNo is in node id's body right now.
func occupies(m *Manager, id string, itemNo int64) bool {
	n, ok := nodeByID(m.State(), id)
	if !ok {
		return false
	}
	for _, no := range n.InBody {
		if no == itemNo {
			return true
		}
	}
	return false
}

// runManager starts spec and guarantees the run is torn down even if the test fails midway, so a failing test cannot
// leak a live conveyor into the rest of the package's tests.
func runManager(t *testing.T, spec topology.Spec) *Manager {
	t.Helper()
	m := New()
	if err := m.Run(spec); err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Cleanup(func() {
		m.Stop()
		waitFor(t, "the run to drain after Stop", func() bool { return !m.State().Running })
	})
	return m
}

func TestFailItemEndsTheRunWithTheInjectedError(t *testing.T) {
	m := runManager(t, twoStageSpec())

	// Item 1 is let through untouched and item 2 is the one failed, so the assertions below distinguish "the clicked
	// item failed" from "everything failed": if the injection were not per-item, item 1's own journey through the
	// same two stages would have failed first and the recorded error would name it instead.
	waitFor(t, "item 2 to reach stage a", func() bool { return occupies(m, "a", 2) })
	if err := m.FailItem(2); err != nil {
		t.Fatalf("FailItem: %v", err)
	}

	waitFor(t, "the run to end", func() bool { return !m.State().Running })
	got := m.State().Error
	if !strings.Contains(got, topology.ErrInjectedFailure.Error()) {
		t.Errorf("run error = %q, want it to mention %q", got, topology.ErrInjectedFailure)
	}
	if !strings.Contains(got, "item 2") {
		t.Errorf("run error = %q, want it to name item 2 (the one that was failed)", got)
	}
}

func TestFailItemCutsTheCurrentDelayShort(t *testing.T) {
	// A whole minute of delay at stage a: the run can only end this quickly if the injection interrupted the sleep
	// rather than waiting it out, which is the entire point of Failures' broadcast channel.
	spec := twoStageSpec()
	spec.Nodes[0].DelayMs = 60_000
	m := runManager(t, spec)

	waitFor(t, "item 1 to reach stage a", func() bool { return occupies(m, "a", 1) })
	if err := m.FailItem(1); err != nil {
		t.Fatalf("FailItem: %v", err)
	}
	waitFor(t, "the run to end well before stage a's 60s delay", func() bool { return !m.State().Running })
}

func TestFailTaskFailsTheOwningItem(t *testing.T) {
	m := runManager(t, fanOutSpec())

	waitFor(t, "item 1's task to start on lane l1", func() bool { return occupies(m, "l1", 1) })
	if err := m.FailTask("l1", 1); err != nil {
		t.Fatalf("FailTask: %v", err)
	}

	waitFor(t, "the run to end", func() bool { return !m.State().Running })
	got := m.State().Error
	if !strings.Contains(got, topology.ErrInjectedFailure.Error()) {
		t.Errorf("run error = %q, want it to mention %q", got, topology.ErrInjectedFailure)
	}
	// The failure has to surface as the pool task's, not the item's own: that distinction is the only thing that
	// makes FailTask worth having next to FailItem.
	if !strings.Contains(got, `pool "l1"`) {
		t.Errorf("run error = %q, want it to name the pool the task ran on", got)
	}
}

func TestFailItemAlsoFailsTheItemsOutstandingPoolTasks(t *testing.T) {
	// An item parked in a fan-out's entry slot is blocked inside the library's own wait, so its pool tasks are the
	// only part of it this package can still interrupt — see Failures.FailItem.
	m := runManager(t, fanOutSpec())

	waitFor(t, "item 1 to enter the fan-out", func() bool { return occupies(m, "f", 1) })
	if err := m.FailItem(1); err != nil {
		t.Fatalf("FailItem: %v", err)
	}
	waitFor(t, "the run to end", func() bool { return !m.State().Running })
	if got := m.State().Error; !strings.Contains(got, topology.ErrInjectedFailure.Error()) {
		t.Errorf("run error = %q, want it to mention %q", got, topology.ErrInjectedFailure)
	}
}

func TestLaneChildTravelsItsInteriorStage(t *testing.T) {
	m := runManager(t, laneSpec())

	waitFor(t, "item 1's child to reach the lane's interior stage", func() bool { return occupies(m, "l1s1", 1) })

	// LanePaths is what lets the UI tell concurrent children of the same item apart (see topology.LanePaths) — a
	// child always inherits its parent's item number, so without it "l1s1" would report only a bare "1".
	n, ok := nodeByID(m.State(), "l1s1")
	if !ok {
		t.Fatal("l1s1 missing from State().Nodes")
	}
	if len(n.LanePaths) != 1 || n.LanePaths[0].ItemNo != 1 || len(n.LanePaths[0].Path) == 0 {
		t.Errorf("l1s1's LanePaths = %+v, want exactly one entry for item 1 with a non-empty path", n.LanePaths)
	}

	waitFor(t, "item 1 to finish", func() bool { return occupies(m, "after", 1) })
}

func TestPoolTaskGetsALanePath(t *testing.T) {
	m := runManager(t, fanOutSpec())

	waitFor(t, "item 1's task to start on pool l1", func() bool { return occupies(m, "l1", 1) })

	// A pool task is registered in LanePaths exactly like a lane child (see topology.LanePathEntry) — without it
	// "l1" would report only a bare "1", indistinguishable from any other item's own top-level slot key.
	n, ok := nodeByID(m.State(), "l1")
	if !ok {
		t.Fatal("l1 missing from State().Nodes")
	}
	if len(n.LanePaths) != 1 || n.LanePaths[0].ItemNo != 1 || len(n.LanePaths[0].Path) == 0 {
		t.Errorf("l1's LanePaths = %+v, want exactly one entry for item 1 with a non-empty path", n.LanePaths)
	}
}

func TestConcurrentPoolTasksOfTheSameItemGetDistinctPaths(t *testing.T) {
	// TasksPerItem > 1 on a pool with room for both at once is the entire point of this: two of item 1's own tasks
	// running on "p" simultaneously must be told apart, exactly as two of the same item's children are on a lane —
	// without distinct paths the UI could not label them "1.1"/"1.2" and they would collide on a bare "1".
	spec := topology.Spec{
		StartDelayMs: 10,
		Nodes: []topology.NodeSpec{
			{ID: "f", Kind: topology.KindFanOut, Limit: 1, Branches: []topology.BranchSpec{
				{ID: "p", Kind: topology.KindPool, Limit: 2, DelayMs: 300, TasksPerItem: 2},
			}},
		},
	}
	m := runManager(t, spec)

	waitFor(t, "both of item 1's tasks to be running on p at once", func() bool {
		n, ok := nodeByID(m.State(), "p")
		return ok && len(n.LanePaths) == 2
	})

	n, _ := nodeByID(m.State(), "p")
	if n.LanePaths[0].ItemNo != 1 || n.LanePaths[1].ItemNo != 1 {
		t.Fatalf("p's LanePaths = %+v, want both entries for item 1", n.LanePaths)
	}
	if len(n.LanePaths[0].Path) == 0 || len(n.LanePaths[1].Path) == 0 {
		t.Fatalf("p's LanePaths = %+v, want non-empty paths", n.LanePaths)
	}
	if n.LanePaths[0].Path[0] == n.LanePaths[1].Path[0] {
		t.Errorf("p's LanePaths = %+v, want the two concurrent tasks' paths to differ", n.LanePaths)
	}
}

func TestLaneEntrancesLanePathsDoNotOutliveTheChildsDwellThere(t *testing.T) {
	// The regression this guards: a naive defer-to-end-of-callback Leave on the entrance would keep its LanePaths
	// entry alive for the child's *entire remaining journey* through the lane's interior, not just its brief dwell
	// at the entrance itself. A later sibling then arriving at the (by-then-vacated) entrance would find that stale
	// leftover still there and misreport its own, current occupant under the earlier sibling's path instead of its
	// own — which is exactly what made a fan-out UI intermittently fail to show a lane's later tasks at all (the
	// mislabeled entrance collided with wherever the earlier sibling actually was, clobbering both).
	m := runManager(t, laneSpec())

	waitFor(t, "item 1's child to reach the lane's interior stage", func() bool { return occupies(m, "l1s1", 1) })

	n, ok := nodeByID(m.State(), "l1")
	if !ok {
		t.Fatal("l1 missing from State().Nodes")
	}
	if len(n.LanePaths) != 0 {
		t.Errorf("l1's (the entrance's) LanePaths = %+v once its child moved on to l1s1, want none", n.LanePaths)
	}
}

func TestLaneEntranceLanePathsSurviveABusyNextNode(t *testing.T) {
	// The regression this guards: releasing the entrance's LanePaths entry right after its own delay — instead of
	// once the child is actually admitted to the first interior node — leaves a window, whenever that first node is
	// busy, where DebugUnitOccupants still reports the entrance's real occupant (a child's slot there is held until
	// its *next* MoveTo actually succeeds, exactly like everywhere else in the library) but LanePaths has nothing
	// for it. The UI then falls back to an ambiguous bare item number there, which collides with that same item's
	// own top-level slot key and made its rectangle flicker into the lane's entrance and back out — see runNodes'
	// own doc.
	spec := topology.Spec{
		StartDelayMs: 10,
		Nodes: []topology.NodeSpec{
			{ID: "f", Kind: topology.KindFanOut, Limit: 1, Branches: []topology.BranchSpec{
				{ID: "l1", Kind: topology.KindLane, DelayMs: 10, TasksPerItem: 2, Nodes: []topology.NodeSpec{
					{ID: "l1s1", Kind: topology.KindStage, Limit: 1, DelayMs: 300},
				}},
			}},
		},
	}
	m := runManager(t, spec)

	// Once l1s1 is occupied by the first child, the entrance is free until the second child reaches it — so the
	// second occupies(m, "l1", 1) below can only be that second child, genuinely stuck there for l1s1's whole 300ms.
	waitFor(t, "the first child to occupy l1s1", func() bool { return occupies(m, "l1s1", 1) })
	waitFor(t, "the second child to reach the entrance", func() bool { return occupies(m, "l1", 1) })
	// Not just "reached": before BlockedEntries became path-aware, checking BlockedLeaving here would have been
	// spuriously satisfied by the *first* child's own identical-numbered mark from when it passed through the
	// entrance earlier (see BlockedEntries' own doc and TestSecondLaneChildIsNotWronglyReportedBlocked) — a real
	// timing gap is what told the two children's dwell times apart instead, and still does here (no need to lean on
	// the fix this test predates). A short real sleep, well inside l1s1's 300ms, reliably lands us past the second
	// child's own 10ms entrance delay instead, still with l1s1 busy (the first child isn't done for a while yet).
	time.Sleep(50 * time.Millisecond)

	n, ok := nodeByID(m.State(), "l1")
	if !ok {
		t.Fatal("l1 missing from State().Nodes")
	}
	if len(n.LanePaths) != 1 {
		t.Errorf("l1's (the entrance's) LanePaths = %+v while its second child is genuinely still there, blocked entering the busy l1s1, want exactly one entry", n.LanePaths)
	}
}

func TestFailItemReachesAChildDeepInsideALane(t *testing.T) {
	// A lane child's own journey is the item's own work one level down (see topology.runNodes) — there is no
	// separate per-task failure path the way a pool has FailTask, so FailItem alone must reach it.
	m := runManager(t, laneSpec())

	waitFor(t, "item 1's child to reach the lane's interior stage", func() bool { return occupies(m, "l1s1", 1) })
	if err := m.FailItem(1); err != nil {
		t.Fatalf("FailItem: %v", err)
	}
	waitFor(t, "the run to end", func() bool { return !m.State().Running })
	if got := m.State().Error; !strings.Contains(got, topology.ErrInjectedFailure.Error()) {
		t.Errorf("run error = %q, want it to mention %q", got, topology.ErrInjectedFailure)
	}
}

func TestFailRejectsBadRequests(t *testing.T) {
	t.Run("not running", func(t *testing.T) {
		m := New()
		if err := m.FailItem(1); err == nil {
			t.Error("FailItem on an idle manager = nil, want an error")
		}
		if err := m.FailTask("l1", 1); err == nil {
			t.Error("FailTask on an idle manager = nil, want an error")
		}
	})

	t.Run("id that is not a pool", func(t *testing.T) {
		m := runManager(t, fanOutSpec())
		// "f" exists but is the fan-out itself, and "nope" exists at all: both are UI bugs rather than user input, so
		// they must be reported rather than silently recorded against a task that can never read them.
		if err := m.FailTask("f", 1); err == nil {
			t.Error(`FailTask("f") = nil, want an error: f is a fan-out, not a pool`)
		}
		if err := m.FailTask("nope", 1); err == nil {
			t.Error(`FailTask("nope") = nil, want an error for an unknown id`)
		}
	})
}

func TestStillBlockedNarrowsMarksToCurrentOccupants(t *testing.T) {
	// The regression this guards: BlockedEntries never unmarks, so a node's own marks include every occurrence that
	// has ever finished its work there. Reported raw, a stale mark made the UI render a later occupant outlined
	// ("done, waiting for room ahead") even though it had not finished — for a plain item, at every node downstream
	// of wherever it was actually marked; for a node reachable through a branch, immediately, for a later
	// child/task of the very same item reusing that same node (TasksPerItem > 1 — see the last two cases below,
	// and TestSecondLaneChildIsNotWronglyReportedBlocked for the end-to-end version of this).
	for _, tc := range []struct {
		name          string
		mark          func(b *topology.BlockedEntries) // marks recorded before the check
		inBody        []int64
		lanePaths     []LanePathEntry
		throughBranch bool
		want          []int64
	}{
		{
			name:   "keeps a mark for an item still in the body",
			mark:   func(b *topology.BlockedEntries) { b.Mark("n", 7, nil) },
			inBody: []int64{7},
			want:   []int64{7},
		},
		{
			name:   "drops a mark for an item that moved on",
			mark:   func(b *topology.BlockedEntries) { b.Mark("n", 7, nil) },
			inBody: []int64{9},
			want:   []int64{},
		},
		{
			name:   "follows inBody's arrival order, not the mark set's",
			mark:   func(b *topology.BlockedEntries) { b.Mark("n", 9, nil); b.Mark("n", 7, nil) },
			inBody: []int64{7, 9},
			want:   []int64{7, 9},
		},
		{
			name:   "empty body keeps nothing",
			mark:   func(b *topology.BlockedEntries) { b.Mark("n", 7, nil); b.Mark("n", 9, nil) },
			inBody: nil,
			want:   []int64{},
		},
		{
			name:   "no marks keeps nothing",
			mark:   func(*topology.BlockedEntries) {},
			inBody: []int64{7},
			want:   []int64{},
		},
		{
			name:          "a branch node matches by path: an earlier child's mark does not leak onto a later one reusing the same node",
			mark:          func(b *topology.BlockedEntries) { b.Mark("n", 3, []int{1}) }, // the first child, path [1] — marked, then gone
			inBody:        []int64{3},                                                    // the second child, same item number, now the sole occupant
			lanePaths:     []LanePathEntry{{ItemNo: 3, Path: []int{2}}},                  // ...at its own path [2]
			throughBranch: true,
			want:          []int64{}, // must NOT be reported blocked — it is a different occurrence, not yet marked itself
		},
		{
			name:          "a branch node still reports a genuinely blocked occurrence",
			mark:          func(b *topology.BlockedEntries) { b.Mark("n", 3, []int{2}) },
			inBody:        []int64{3},
			lanePaths:     []LanePathEntry{{ItemNo: 3, Path: []int{2}}},
			throughBranch: true,
			want:          []int64{3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := topology.NewBlockedEntries()
			tc.mark(b)
			got := stillBlocked(b, "n", tc.inBody, tc.lanePaths, tc.throughBranch)
			if got == nil {
				t.Fatal("got nil, want a non-nil slice (it is JSON-marshalled straight to the UI as [])")
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestSecondLaneChildIsNotWronglyReportedBlocked(t *testing.T) {
	// The regression this guards: a Lane whose TasksPerItem > 1 schedules several children for the same item, and
	// they all walk the exact same interior nodes — so a later child inevitably reaches a stage an earlier sibling
	// already fully passed through (BlockedEntries.Mark'd there, and never unmarked). Before BlockedEntries became
	// path-aware, that stale mark applied to any later occupant sharing the same raw item number, so the later
	// child rendered "done, waiting to leave" from the instant it was admitted — even though its own delay had just
	// started. This is not specific to a lane's last stage (see the demo bug report this fixes): it reproduces on
	// any interior stage a later child reuses, one Limit=1 makes exclusive so the two children can never overlap.
	spec := topology.Spec{
		StartDelayMs: 10,
		Nodes: []topology.NodeSpec{
			{ID: "f", Kind: topology.KindFanOut, Limit: 1, Branches: []topology.BranchSpec{
				{ID: "l1", Kind: topology.KindLane, DelayMs: 10, TasksPerItem: 2, Nodes: []topology.NodeSpec{
					{ID: "l1s1", Kind: topology.KindStage, Limit: 1, DelayMs: 300},
				}},
			}},
		},
	}
	m := runManager(t, spec)

	// Poll until the very first snapshot where the second child (path [2]) is l1s1's sole occupant — the instant its
	// own, fresh 300ms delay can have barely started.
	deadline := time.Now().Add(5 * time.Second)
	var found NodeState
	var ok bool
	for time.Now().Before(deadline) {
		n, exists := nodeByID(m.State(), "l1s1")
		if exists {
			has1, has2 := false, false
			for _, e := range n.LanePaths {
				if len(e.Path) > 0 && e.Path[0] == 1 {
					has1 = true
				}
				if len(e.Path) > 0 && e.Path[0] == 2 {
					has2 = true
				}
			}
			if has2 && !has1 && len(n.InBody) > 0 {
				found, ok = n, true
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if !ok {
		t.Fatal("timed out waiting for the second child to become the sole occupant of l1s1")
	}
	if len(found.BlockedLeaving) != 0 {
		t.Errorf("l1s1's BlockedLeaving = %v the instant the second child became the sole occupant (its own fresh "+
			"300ms delay had just started) — a stale mark from the first child's earlier, completed visit is "+
			"bleeding onto it", found.BlockedLeaving)
	}
}
