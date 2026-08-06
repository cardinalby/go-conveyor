import { newId } from "./ids";
import { defaultStageDelayMs, defaultLaneDelayMs } from "./defaults";
import type { BranchKind, BranchNode, FanOutNode, LaneBranch, Pipeline, PoolBranch, StageNode } from "../types/pipeline";

export function newStage(): StageNode {
  return { id: newId("n"), kind: "stage", name: "", limit: 1, queueSize: 0, delayMs: defaultStageDelayMs(1) };
}

/** A pool for a fan-out that will end up with branchCount branches in total. */
export function newPool(branchCount: number): PoolBranch {
  return { id: newId("p"), kind: "pool", name: "", limit: 1, delayMs: defaultLaneDelayMs(branchCount), tasksPerItem: 1 };
}

/** A lane for a fan-out that will end up with branchCount branches in total, with no interior nodes yet — looks
 * and behaves like a pool pinned at limit 1 until stages are added (see LaneBranch). */
export function newLane(branchCount: number): LaneBranch {
  return { id: newId("l"), kind: "lane", name: "", entranceName: "", tasksPerItem: 1, delayMs: defaultLaneDelayMs(branchCount), nodes: [] };
}

export function newBranch(kind: BranchKind, branchCount: number): BranchNode {
  return kind === "pool" ? newPool(branchCount) : newLane(branchCount);
}

export function newFanOut(branches: BranchNode[]): FanOutNode {
  return { id: newId("f"), kind: "fanout", name: "", limit: 1, queueSize: 0, branches };
}

/** The pipeline shown on first load: Read + 2 stages. The start stage's own delay defaults the same way a plain
 * stage's does (defaultStageDelayMs(1)) rather than 0 — MIN_DELAY_MS is a real floor now, not just an unused
 * lower bound. */
export function defaultPipeline(): Pipeline {
  return { nodes: [newStage(), newStage()], startDelayMs: defaultStageDelayMs(1), startName: "", itemsLimit: 0 };
}
