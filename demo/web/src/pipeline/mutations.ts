import { newId } from "./ids";
import { newBranch, newFanOut, newStage } from "./factory";
import { MAX_LANES } from "./defaults";
import type { BranchKind, BranchNode, FanOutNode, LaneBranch, Pipeline, PipelineNode, PoolBranch } from "../types/pipeline";

/** Rewrites every node-list in the tree — pipeline.nodes, and every LaneBranch.nodes at any depth (a lane's interior
 * may itself contain a fan-out whose branches may again be lanes) — by calling visit(list) once per list. visit
 * returns the very same array reference when it found nothing to do there, which is what lets one generic walk
 * implement any id-addressed, depth-agnostic mutation: it visits every list in the tree, but only the one that
 * actually contains the target id ends up different, so everything above it is rebuilt (immutable path-copy) and
 * everything unrelated is returned unchanged (by reference — see resolve.ts's caches, which depend on that). */
export function rewriteNodeLists(nodes: PipelineNode[], visit: (list: PipelineNode[]) => PipelineNode[]): PipelineNode[] {
  const rewritten = visit(nodes);
  let changed = rewritten !== nodes;
  const next = rewritten.map((n): PipelineNode => {
    if (n.kind !== "fanout") return n;
    let branchesChanged = false;
    const branches = n.branches.map((b): BranchNode => {
      if (b.kind !== "lane") return b;
      const laneNodes = rewriteNodeLists(b.nodes, visit);
      if (laneNodes === b.nodes) return b;
      branchesChanged = true;
      return { ...b, nodes: laneNodes };
    });
    if (!branchesChanged) return n;
    changed = true;
    return { ...n, branches };
  });
  return changed ? next : rewritten;
}

/** Same idea, one level down: visits every FanOutNode.branches array anywhere in the tree (a top-level fan-out, or
 * one nested inside some lane's interior), built on top of rewriteNodeLists so it reaches every depth for free. */
export function rewriteBranchLists(
  nodes: PipelineNode[],
  visit: (branches: BranchNode[], owner: FanOutNode) => BranchNode[],
): PipelineNode[] {
  return rewriteNodeLists(nodes, (list) => {
    let changed = false;
    const next = list.map((n): PipelineNode => {
      if (n.kind !== "fanout") return n;
      const branches = visit(n.branches, n);
      if (branches === n.branches) return n;
      changed = true;
      return { ...n, branches };
    });
    return changed ? next : list;
  });
}

/** Whether id names a stage/fan-out reachable directly from pipeline.nodes (true), rather than only through some
 * lane's own interior (false). Read-only — used by ContextMenu to choose between "Add Stage" and "Add lane Stage"
 * labels, never by a mutation itself. */
export function isTopLevelNode(pipeline: Pipeline, id: string): boolean {
  return pipeline.nodes.some((n) => n.id === id);
}

/** Finds the fan-out that owns branchId, anywhere in the tree (top-level, or nested inside some lane's interior).
 * A branch never lives in any node list itself — only its owning fan-out does — so this is what "before/after" and
 * "add parallel" fired from a pool/lane's own context menu resolve against, and what its "Convert" gating checks. */
export function findBranchAndOwner(pipeline: Pipeline, branchId: string): { branch: BranchNode; owner: FanOutNode } | undefined {
  const search = (nodes: PipelineNode[]): { branch: BranchNode; owner: FanOutNode } | undefined => {
    for (const n of nodes) {
      if (n.kind !== "fanout") continue;
      const branch = n.branches.find((b) => b.id === branchId);
      if (branch) return { branch, owner: n };
      for (const b of n.branches) {
        if (b.kind === "lane") {
          const found = search(b.nodes);
          if (found) return found;
        }
      }
    }
    return undefined;
  };
  return search(pipeline.nodes);
}

export function removeNode(pipeline: Pipeline, id: string): Pipeline {
  return {
    ...pipeline,
    nodes: rewriteNodeLists(pipeline.nodes, (list) => {
      if (!list.some((n) => n.id === id)) return list;
      return list.filter((n) => n.id !== id);
    }),
  };
}

export function addStageBefore(pipeline: Pipeline, id: string): Pipeline {
  return {
    ...pipeline,
    nodes: rewriteNodeLists(pipeline.nodes, (list) => {
      const i = list.findIndex((n) => n.id === id);
      if (i < 0) return list;
      const next = [...list];
      next.splice(i, 0, newStage());
      return next;
    }),
  };
}

export function addStageAfter(pipeline: Pipeline, id: string): Pipeline {
  return {
    ...pipeline,
    nodes: rewriteNodeLists(pipeline.nodes, (list) => {
      const i = list.findIndex((n) => n.id === id);
      if (i < 0) return list;
      const next = [...list];
      next.splice(i + 1, 0, newStage());
      return next;
    }),
  };
}

/** "Add stage after" on the implicit start ("Read") stage: it is not part of Pipeline.nodes, so this just prepends. */
export function addStageFirst(pipeline: Pipeline): Pipeline {
  return { ...pipeline, nodes: [newStage(), ...pipeline.nodes] };
}

/** "Add lane Stage after" on an empty lane: there is nothing to be "before" yet, so this seeds its interior with
 * one stage rather than splicing — the lane's own head has no "start" node the way the conveyor does. A no-op if
 * the lane already has interior nodes (the menu only offers this for an empty one). */
export function addStageFirstInLane(pipeline: Pipeline, laneId: string): Pipeline {
  return {
    ...pipeline,
    nodes: rewriteBranchLists(pipeline.nodes, (branches) => {
      const i = branches.findIndex((b) => b.id === laneId && b.kind === "lane");
      if (i < 0) return branches;
      const lane = branches[i] as LaneBranch;
      if (lane.nodes.length > 0) return branches;
      const next = [...branches];
      next[i] = { ...lane, nodes: [newStage()] };
      return next;
    }),
  };
}

/** "Add parallel Lane"/"Add parallel Pool" on a fan-out or one of its branches: add one more branch of the given
 * kind, up to MAX_LANES (a count of branches total, regardless of kind mix). */
export function addBranch(pipeline: Pipeline, fanOutId: string, kind: BranchKind): Pipeline {
  return {
    ...pipeline,
    nodes: rewriteBranchLists(pipeline.nodes, (branches, owner) => {
      if (owner.id !== fanOutId || branches.length >= MAX_LANES) return branches;
      return [...branches, newBranch(kind, branches.length + 1)];
    }),
  };
}

/** Removes one branch. Down to one survivor, the fan-out collapses back into a plain stage carrying the survivor's
 * settings (symmetric with addParallelToStage) only when the survivor has no interior chain of its own — a pool, or
 * a lane with no stages yet (see LaneBranch). A lane with interior nodes has no flat StageNode equivalent, so it is
 * left as a 1-branch fan-out instead of being force-unwrapped. */
export function removeBranch(pipeline: Pipeline, fanOutId: string, branchId: string): Pipeline {
  return {
    ...pipeline,
    nodes: rewriteNodeLists(pipeline.nodes, (list) => {
      const i = list.findIndex((n) => n.id === fanOutId && n.kind === "fanout");
      if (i < 0) return list;
      const fo = list[i] as FanOutNode;
      const branches = fo.branches.filter((b) => b.id !== branchId);
      if (branches.length === fo.branches.length) return list;

      if (branches.length <= 1) {
        const survivor = branches[0] ?? fo.branches[0];
        if (survivor.kind === "pool" || survivor.nodes.length === 0) {
          const next = [...list];
          next[i] = {
            id: fo.id,
            kind: "stage",
            name: survivor.name || fo.name,
            limit: survivor.kind === "pool" ? survivor.limit : 1,
            queueSize: fo.queueSize,
            delayMs: survivor.delayMs,
          };
          return next;
        }
      }
      const next = [...list];
      next[i] = { ...fo, branches };
      return next;
    }),
  };
}

/** "Convert Pool to Lane" (always available) / "Convert Lane to Pool" (only offered by the menu for an empty
 * lane — enforced here too). Carries over name/delayMs/tasksPerItem; a pool's limit has nowhere to go on a lane
 * (fixed at one child at a time), and a converted-to-pool lane starts at limit 1, matching that same entrance. */
export function convertBranchKind(pipeline: Pipeline, branchId: string, toKind: BranchKind): Pipeline {
  return {
    ...pipeline,
    nodes: rewriteBranchLists(pipeline.nodes, (branches) => {
      const i = branches.findIndex((b) => b.id === branchId);
      if (i < 0) return branches;
      const b = branches[i];
      if (b.kind === toKind) return branches;

      const next = [...branches];
      if (toKind === "lane") {
        if (b.kind !== "pool") return branches;
        const lane: LaneBranch = { id: b.id, kind: "lane", name: b.name, entranceName: "", tasksPerItem: b.tasksPerItem, delayMs: b.delayMs, nodes: [] };
        next[i] = lane;
        return next;
      }
      if (b.kind !== "lane" || b.nodes.length > 0) return branches;
      const pool: PoolBranch = { id: b.id, kind: "pool", name: b.name, limit: 1, delayMs: b.delayMs, tasksPerItem: b.tasksPerItem };
      next[i] = pool;
      return next;
    }),
  };
}

/** "Add parallel Lane"/"Add parallel Pool" on a plain stage: replace it with a fan-out of two branches — the
 * original stage's settings always carry over into a Pool (the closest match to what a plain stage already was: a
 * concurrency limit, nothing else), and the second, freshly-created branch is newBranchKind, matching whichever
 * menu item fired this. */
export function addParallelToStage(pipeline: Pipeline, stageId: string, newBranchKind: BranchKind): Pipeline {
  return {
    ...pipeline,
    nodes: rewriteNodeLists(pipeline.nodes, (list) => {
      const i = list.findIndex((n) => n.id === stageId);
      if (i < 0) return list;
      const stage = list[i];
      if (stage.kind !== "stage") return list;

      const originalPool: PoolBranch = {
        id: newId("p"),
        kind: "pool",
        name: stage.name,
        limit: stage.limit,
        delayMs: stage.delayMs,
        tasksPerItem: 1,
      };
      const fanOut = newFanOut([originalPool, newBranch(newBranchKind, 2)]);

      const next = [...list];
      next[i] = fanOut;
      return next;
    }),
  };
}
