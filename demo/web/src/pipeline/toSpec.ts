import type { BranchNode, Pipeline, PipelineNode } from "../types/pipeline";
import type { BranchSpec, NodeSpec, Spec } from "../types/topology";

function branchToSpec(b: BranchNode): BranchSpec {
  if (b.kind === "pool") {
    return { id: b.id, kind: "pool", name: b.name, limit: b.limit, delayMs: b.delayMs, tasksPerItem: b.tasksPerItem };
  }
  return {
    id: b.id,
    kind: "lane",
    name: b.name,
    limit: 0, // unused for a lane — its entrance has no SetLimit (see conveyor.Lane)
    delayMs: b.delayMs,
    tasksPerItem: b.tasksPerItem,
    nodes: nodesToSpec(b.nodes),
  };
}

function nodesToSpec(nodes: PipelineNode[]): NodeSpec[] {
  return nodes.map((n): NodeSpec => {
    if (n.kind === "stage") {
      return { id: n.id, kind: "stage", name: n.name, limit: n.limit, queueSize: n.queueSize, delayMs: n.delayMs };
    }
    return {
      id: n.id,
      kind: "fanout",
      name: n.name,
      limit: n.limit,
      queueSize: n.queueSize,
      delayMs: 0,
      branches: n.branches.map(branchToSpec),
    };
  });
}

/** Converts the editable build-mode graph into the wire Spec sent to WASM's "run" method. Recurses into a lane
 * branch's own nodes exactly like the top level, since NodeSpec/BranchSpec are the same mutually-recursive shape a
 * lane's interior chain uses (see ../types/topology). */
export function toSpec(pipeline: Pipeline): Spec {
  return { nodes: nodesToSpec(pipeline.nodes), startDelayMs: pipeline.startDelayMs, itemsLimit: pipeline.itemsLimit };
}
