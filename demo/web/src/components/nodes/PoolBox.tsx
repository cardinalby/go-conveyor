import { EditableTitle } from "../shared/EditableTitle";
import { LabeledSlider } from "../shared/LabeledSlider";
import { TaskStrip } from "../shared/TaskStrip";
import {
  MAX_DELAY_MS,
  MAX_LIMIT,
  MAX_TASKS_PER_ITEM,
  MIN_DELAY_MS,
  MIN_LIMIT,
  MIN_TASKS_PER_ITEM,
} from "../../pipeline/defaults";
import type { ResolvedPool } from "../../pipeline/resolve";
import type { TreeCallbacks } from "./types";

const formatDelay = (ms: number) => `${(ms / 1000).toFixed(2)}s`;

interface Props {
  pool: ResolvedPool;
  callbacks: TreeCallbacks;
  /** Where each item sits at the owning fan-out's entry, for a task badge's fly-in origin — see FanOutBox. */
  entrySlotByItemKey: Map<string, string>;
}

/** A pool branch: a concurrency limit, its own leaf-work delay, how many tasks one item schedules on it, and its
 * occupants rendered as fading badges in place (see TaskStrip) — one item can run several of a pool's tasks at
 * once, which a single moving rectangle cannot represent. */
export function PoolBox({ pool, callbacks, entrySlotByItemKey }: Props) {
  return (
    <div
      className="branch-box"
      onContextMenu={(e) => {
        e.preventDefault();
        e.stopPropagation();
        callbacks.onContextMenu({ kind: "pool", id: pool.id }, e);
      }}
    >
      <EditableTitle value={pool.name} onCommit={(name) => callbacks.onRenameBranch(pool.id, name)} className="branch-name" />
      <LabeledSlider
        label="Limit"
        value={pool.limit}
        min={MIN_LIMIT}
        max={MAX_LIMIT}
        deferCommit
        onChange={(v) => callbacks.onEditBranch(pool.id, "limit", v)}
        title="Maximum number of tasks running in this pool at once"
      />
      <LabeledSlider
        label="Delay"
        value={pool.delayMs}
        min={MIN_DELAY_MS}
        max={MAX_DELAY_MS}
        step={50}
        formatValue={formatDelay}
        onChange={(ms) => callbacks.onEditBranch(pool.id, "delayMs", ms)}
        variant="sim"
        title="Simulated processing time for one task"
      />
      <LabeledSlider
        label="Tasks"
        value={pool.tasksPerItem}
        min={MIN_TASKS_PER_ITEM}
        max={MAX_TASKS_PER_ITEM}
        onChange={(v) => callbacks.onEditBranch(pool.id, "tasksPerItem", v)}
        variant="sim"
        title="Number of tasks created by an item for this Lane/Pool"
      />
      <TaskStrip
        nodeId={pool.id}
        limit={pool.limit}
        inBody={pool.inBody}
        lanePaths={pool.lanePaths}
        entrySlotByItemKey={entrySlotByItemKey}
        reserve={MAX_LIMIT}
        onFail={callbacks.onFailTask}
      />
    </div>
  );
}
