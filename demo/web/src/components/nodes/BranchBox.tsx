import { PoolBox } from "./PoolBox";
import { LaneBox } from "./LaneBox";
import type { ResolvedBranch } from "../../pipeline/resolve";
import type { TreeCallbacks } from "./types";

interface Props {
  branch: ResolvedBranch;
  callbacks: TreeCallbacks;
  /** Composite item key -> the slot it holds at the owning fan-out's entry, for a task badge's fly-in origin — see
   * FanOutBox and shared/TaskStrip. */
  entrySlotByItemKey: Map<string, string>;
}

/** Dispatches one branch of a fan-out to its own look — see PoolBox/LaneBox. */
export function BranchBox({ branch, callbacks, entrySlotByItemKey }: Props) {
  if (branch.kind === "pool") {
    return <PoolBox pool={branch} callbacks={callbacks} entrySlotByItemKey={entrySlotByItemKey} />;
  }
  return <LaneBox lane={branch} callbacks={callbacks} entrySlotByItemKey={entrySlotByItemKey} />;
}
