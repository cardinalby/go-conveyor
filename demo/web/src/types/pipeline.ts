// Pipeline is the editable, build-mode graph the UI mutates directly (context menu, inline edits). It never touches
// Go/WASM by itself — only toSpec() (see ../pipeline/toSpec) turns it into the wire Spec sent to "run".

export type BranchKind = "pool" | "lane";

export interface PoolBranch {
  id: string;
  kind: "pool";
  name: string;
  limit: number;
  delayMs: number;
  tasksPerItem: number;
}

/** delayMs describes the lane's own entrance, always applied, exactly like the conveyor's own implicit start — a
 * child sleeps it out before moving on, whether or not `nodes` is empty (a lane with no interior nodes degrades to
 * a limit-1 pool — see Go's conveyor.Lane — so that sleep is its whole leaf work). */
export interface LaneBranch {
  id: string;
  kind: "lane";
  name: string;
  /** The lane's own entrance's name — "" until renamed, then whatever the user typed, exactly like every other name
   * here. Display-only, for the same reason Pipeline.startName is: a lane's entrance unit is go-conveyor's own and
   * takes no OptName, so this never reaches the Spec. */
  entranceName: string;
  tasksPerItem: number;
  delayMs: number;
  nodes: PipelineNode[];
}

export type BranchNode = PoolBranch | LaneBranch;

export interface StageNode {
  id: string;
  kind: "stage";
  name: string;
  limit: number;
  queueSize: number;
  delayMs: number;
}

export interface FanOutNode {
  id: string;
  kind: "fanout";
  name: string;
  limit: number;
  queueSize: number;
  branches: BranchNode[];
}

/** Used both at Pipeline.nodes (the top level) and at a LaneBranch's own `nodes` (its interior series) — the same
 * recursive shape as Go's NodeSpec, which is what lets a lane's interior contain a fan-out whose branches may
 * themselves be lanes, to any depth. */
export type PipelineNode = StageNode | FanOutNode;

/** startDelayMs/startName configure the implicit start ("Read") stage — it is not one of Nodes, since every
 * conveyor has exactly one automatically. startName is "" until the user renames it, exactly like a node's own name,
 * and is display-only: go-conveyor's start unit is always called "start" and takes no OptName, so unlike a node's
 * name this one never reaches the Spec or the generated code. */
/** itemsLimit caps how many items may be in flight across the whole conveyor at once (see Go's
 * conveyor.Conveyor.SetItemsLimit) — global, unlike every other dial here, which belongs to one node. 0 means
 * unlimited, the default. */
export interface Pipeline {
  nodes: PipelineNode[];
  startDelayMs: number;
  startName: string;
  itemsLimit: number;
}
