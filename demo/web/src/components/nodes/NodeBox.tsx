import { StageBox } from "./StageBox";
import { FanOutBox } from "./FanOutBox";
import type { ResolvedNode } from "../../pipeline/resolve";
import type { TreeCallbacks } from "./types";

interface Props {
  node: ResolvedNode;
  callbacks: TreeCallbacks;
}

/** Dispatches one node of a lane's interior chain to the right box — a stage or, recursively, a nested fan-out (see
 * FanOutBox, which may itself contain lanes with their own interior chains). No xyflow handles/shellRef: nothing
 * here is a separate canvas node, it's all plain nested markup inside the one top-level fan-out that started the
 * recursion — see LaneBox. */
export function NodeBox({ node, callbacks }: Props) {
  if (node.kind === "stage") {
    return <StageBox stage={node} callbacks={callbacks} />;
  }
  return <FanOutBox fanout={node} callbacks={callbacks} />;
}
