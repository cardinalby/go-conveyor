// Mirrors demo/internal/runtime.State / NodeState — the run-mode poll response. Keep in sync by hand.

/** One (item number, lane-child ordinal path) pair currently occupying a node reachable through some lane's
 * interior — see Go's topology.LanePathEntry. A child always inherits its parent's item number, so this is the
 * only way to tell concurrent siblings of the same item apart: path [2] is the second child a top-level lane
 * spawned for this item, [2, 1] that child's own first child one lane deeper. */
export interface LanePathEntry {
  itemNo: number;
  path: number[];
}

/** One node's live state: its current dials and exactly who occupies it (null once nothing is running).
 * pendingEntry is only ever non-empty for a fan-out: the InBody items admitted but whose FanOut.MoveTo call has
 * not returned yet (tasks not dispatched). blockedLeaving is only ever non-empty for the start stage or a plain
 * stage: the InBody items that finished this node's own work and are now trying to advance into the next one.
 * See pipeline/itemPositions.ts's ItemFill for how both feed the item circle's fill. */
export interface NodeState {
  id: string;
  limit: number;
  queueSize: number;
  delayMs: number;
  /** How many tasks/children (see Go's Branch.NewTasks) one item schedules on this branch. Always 0 for anything
   * but a pool or a lane. */
  tasksPerItem: number;
  inBody: number[] | null;
  inQueue: number[] | null;
  pendingEntry: number[];
  blockedLeaving: number[];
  /** Always empty for a node outside any lane's interior — see LanePathEntry. */
  lanePaths: LanePathEntry[];
}

/** A full run-mode snapshot, polled from WASM every tick. */
export interface RunState {
  running: boolean;
  /** True once cancelCtx or stop has been called for this run: no new items are being created any more. */
  stopping: boolean;
  /** True once stop has been called for this run: in-flight items are being canceled at once, not left to finish. */
  forced: boolean;
  error?: string;
  workerCount: number;
  nodes: NodeState[] | null;
}
