import { EditableTitle } from "../shared/EditableTitle";
import { LabeledSlider } from "../shared/LabeledSlider";
import { TaskStrip } from "../shared/TaskStrip";
import { StartBox } from "./StartBox";
import { NodeBox } from "./NodeBox";
import { MAX_DELAY_MS, MAX_LIMIT, MAX_TASKS_PER_ITEM, MIN_DELAY_MS, MIN_TASKS_PER_ITEM } from "../../pipeline/defaults";
import type { ResolvedLane } from "../../pipeline/resolve";
import type { TreeCallbacks } from "./types";

const formatDelay = (ms: number) => `${(ms / 1000).toFixed(2)}s`;
const TASKS_TITLE = "Number of tasks created by an item for this Lane/Pool";
const ENTRANCE_DELAY_TITLE = "Simulated time an item spends here before its first move";

interface Props {
  lane: ResolvedLane;
  callbacks: TreeCallbacks;
  /** Where each item sits at the owning fan-out's entry, for a task badge's fly-in origin — see FanOutBox. Only used
   * by the empty-lane rendering below, which is a pool in all but name. */
  entrySlotByItemKey: Map<string, string>;
}

/** A lane branch. With no interior nodes yet it behaves exactly like a pool pinned at concurrency 1 (see
 * conveyor.Lane) and looks like one — the same delay/tasks-per-item dials and fading task badges, minus the Limit
 * slider a pool has and a lane never can. Once it gains interior nodes (see NodeBox), its Tasks-per-item dial moves
 * above an "Entrance" gate (see StartBox) — the lane's own implicit start, always present and never removable,
 * exactly like the conveyor's own "Read" one level up — connected to a row of its interior nodes: the lane's own
 * "sub-conveyor" the label describes it as. */
export function LaneBox({ lane, callbacks, entrySlotByItemKey }: Props) {
  const onContextMenu = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    callbacks.onContextMenu({ kind: "lane", id: lane.id }, e);
  };

  if (lane.nodes.length === 0) {
    return (
      <div className="branch-box" onContextMenu={onContextMenu}>
        <EditableTitle value={lane.name} onCommit={(name) => callbacks.onRenameBranch(lane.id, name)} className="branch-name" />
        <LabeledSlider
          label="Delay"
          value={lane.delayMs}
          min={MIN_DELAY_MS}
          max={MAX_DELAY_MS}
          step={50}
          formatValue={formatDelay}
          onChange={(ms) => callbacks.onEditBranch(lane.id, "delayMs", ms)}
          variant="sim"
          title={ENTRANCE_DELAY_TITLE}
        />
        <LabeledSlider
          label="Tasks"
          value={lane.tasksPerItem}
          min={MIN_TASKS_PER_ITEM}
          max={MAX_TASKS_PER_ITEM}
          onChange={(v) => callbacks.onEditBranch(lane.id, "tasksPerItem", v)}
          variant="sim"
          title={TASKS_TITLE}
        />
        {/* Not a conveyor.Pool on the Go side, so a click fails the whole item rather than just this task — see
            runtime.Manager.FailTask and TreeCallbacks.onFailItem. */}
        <TaskStrip
          nodeId={lane.id}
          limit={1}
          inBody={lane.inBody}
          lanePaths={lane.lanePaths}
          entrySlotByItemKey={entrySlotByItemKey}
          reserve={MAX_LIMIT}
          onFail={(_id, itemNo) => callbacks.onFailItem(itemNo)}
        />
      </div>
    );
  }

  return (
    <div className="branch-box branch-box-lane-open" onContextMenu={onContextMenu}>
      <div className="lane-head">
        <EditableTitle value={lane.name} onCommit={(name) => callbacks.onRenameBranch(lane.id, name)} className="branch-name" />
        <LabeledSlider
          label="Tasks"
          value={lane.tasksPerItem}
          min={MIN_TASKS_PER_ITEM}
          max={MAX_TASKS_PER_ITEM}
          onChange={(v) => callbacks.onEditBranch(lane.id, "tasksPerItem", v)}
          variant="sim"
          title={TASKS_TITLE}
        />
      </div>
      <div className="lane-interior">
        <div className="lane-interior-item">
          {/* No onContextMenu/handles: the entrance can't be removed and offers no other action — see StartBox. */}
          <StartBox
            nodeId={lane.id}
            label={lane.entranceName}
            onRename={(name) => callbacks.onRenameEntrance(lane.id, name)}
            delayMs={lane.delayMs}
            onEditDelay={(ms) => callbacks.onEditBranch(lane.id, "delayMs", ms)}
          />
        </div>
        {lane.nodes.map((n) => (
          <div key={n.id} className="lane-interior-item">
            <div className="lane-connector" />
            <NodeBox node={n} callbacks={callbacks} />
          </div>
        ))}
      </div>
    </div>
  );
}
