// Mirrors demo/internal/topology/schema.go — the wire format sent to WASM's "run" method. Keep in sync by hand.

/** Reserved id for the conveyor's implicit start ("Read") stage. A Spec's own ids must never equal it. */
export const START_ID = "__start__";

export type NodeKind = "stage" | "fanout";

/** A fan-out branch: a Pool (leaf, no interior nodes) or a Lane (a pipeline of its own — see Go's conveyor.Lane).
 * NodeSpec and BranchSpec are mutually recursive: a lane's `nodes` may itself contain a fan-out whose own branches
 * may again be lanes, to any depth. */
export type BranchKind = "pool" | "lane";

/** limit applies to a pool only (a lane's entrance has no SetLimit, fixed at one child at a time). delayMs is the
 * branch's own entrance work, always applied: for a pool it's the callback a scheduled task runs; for a lane it's
 * the entrance's own simulated delay, exactly like the conveyor's own implicit start — a child sleeps it out before
 * moving on, whether or not `nodes` is empty. tasksPerItem is shared (see Go's conveyor.Branch.NewTasks): how many
 * tasks/children one item schedules on this branch. */
export interface BranchSpec {
  id: string;
  kind: BranchKind;
  name: string;
  limit: number;
  delayMs: number;
  tasksPerItem: number;
  nodes?: NodeSpec[]; // lane only
}

/** One node of a Spec, or of a lane's own interior series. branches is present (and non-empty) only for a fanout;
 * delayMs applies only to a stage (a fan-out runs no code of its own, only its branches do). */
export interface NodeSpec {
  id: string;
  kind: NodeKind;
  name: string;
  limit: number;
  queueSize: number;
  delayMs: number;
  branches?: BranchSpec[];
}

/** A full pipeline topology: an ordered list of nodes, left to right. The implicit start stage is not part of
 * Nodes, but StartDelayMs configures the simulated time an item spends there before its first move. */
export interface Spec {
  nodes: NodeSpec[];
  startDelayMs: number;
}
