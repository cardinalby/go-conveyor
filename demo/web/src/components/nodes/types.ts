import type { ResolvedFanOut, ResolvedStage } from "../../pipeline/resolve";
import type { MenuTarget } from "../../types/menu";

export type Mode = "build" | "run";
export type EditField = "limit" | "queueSize" | "delayMs";
export type BranchEditField = "limit" | "delayMs" | "tasksPerItem";

/** The callbacks threaded through the whole node tree — top-level canvas nodes and, recursively, every node nested
 * inside some lane's interior chain (see NodeBox/BranchBox). Every callback is id-parameterized rather than
 * pre-bound to one node, so a single stable reference serves the entire tree regardless of depth — see
 * PipelineCanvas's own doc on why a stable reference matters for memoization. */
export interface TreeCallbacks {
  mode: Mode;
  onContextMenu: (target: MenuTarget, evt: React.MouseEvent) => void;
  onEditNode: (id: string, field: EditField, value: number) => void;
  onEditBranch: (branchId: string, field: BranchEditField, value: number) => void;
  onRenameNode: (id: string, name: string) => void;
  onRenameBranch: (branchId: string, name: string) => void;
  /** A lane's own entrance is not a branch or a node of its own, so renaming it needs its own callback — the same
   * reason PipelineCanvas has onRenameStart for the conveyor's implicit start. */
  onRenameEntrance: (laneId: string, name: string) => void;
  /** Ctrl/⌘-click on a pool task's badge injects a failure for that one task — see shared/TaskStrip. A lane's
   * sub-items need no equivalent: they're items in their own right, failed the same way any item rectangle is. */
  onFailTask: (poolId: string, itemNo: number) => void;
  /** Ctrl/⌘-click on an empty lane's own entrance badge (see LaneBox) fails the whole item — an empty lane isn't a
   * conveyor.Pool on the Go side (see runtime.Manager.FailTask), so onFailTask cannot target it. */
  onFailItem: (itemNo: number) => void;
}

interface Common {
  mode: Mode;
  onContextMenu: (target: MenuTarget, evt: React.MouseEvent) => void;
  /** Reports this node's rendered size so PipelineCanvas can lay out columns from measured widths/heights instead
   * of a fixed grid — see ../../hooks/useReportSize. Top-level canvas nodes only; nested content contributes to the
   * same measurement for free, since it all lives inside the one observed element. */
  reportSize: (id: string, w: number, h: number) => void;
}

// @xyflow/react's Node<T> requires T to extend Record<string, unknown>; the index signatures below satisfy that.

export interface StageNodeData extends Common {
  [key: string]: unknown;
  stage: ResolvedStage;
  callbacks: TreeCallbacks;
}

export interface FanOutNodeData extends Common {
  [key: string]: unknown;
  fanout: ResolvedFanOut;
  callbacks: TreeCallbacks;
}

export interface StartNodeData extends Common {
  [key: string]: unknown;
  /** Already resolved to the display name — the custom one, or DEFAULT_START_NAME (see ../../pipeline/resolve). */
  name: string;
  delayMs: number;
  onEditDelay: (value: number) => void;
  onRename: (name: string) => void;
}
