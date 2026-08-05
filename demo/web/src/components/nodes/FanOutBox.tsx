import { EditableTitle } from "../shared/EditableTitle";
import { LabeledSlider } from "../shared/LabeledSlider";
import { SlotStrip } from "../shared/SlotStrip";
import { BranchBox } from "./BranchBox";
import { bodySlotCount, queueSlotCount } from "../../pipeline/slots";
import { assignBodySlots, keyAssigner } from "../../pipeline/itemPositions";
import { MAX_LIMIT, MAX_QUEUE_SIZE, MIN_LIMIT, MIN_QUEUE_SIZE } from "../../pipeline/defaults";
import type { ResolvedFanOut } from "../../pipeline/resolve";
import type { TreeCallbacks } from "./types";

interface Props {
  fanout: ResolvedFanOut;
  callbacks: TreeCallbacks;
  /** xyflow connection handles, present only when this box is rendered as a top-level canvas node (see
   * FanOutNodeView) — a fan-out nested inside a lane's interior chain (see NodeBox) is plain nested markup. */
  handles?: React.ReactNode;
  /** Attaches to the outer element so PipelineCanvas can measure a top-level node — see ../../hooks/useReportSize.
   * Absent for a nested fan-out, which contributes to its ancestor's own measurement instead. */
  shellRef?: React.Ref<HTMLDivElement>;
}

/** The visual content shared by a top-level fan-out node (FanOutNodeView) and a fan-out nested inside some lane's
 * interior chain (see NodeBox) — matching the library's own model directly, a fan-out is one node containing its
 * branches, not sibling nodes. Its own entry slot strip (top) is where an item parks for as long as its branches'
 * work is outstanding, filled to reflect how far it's progressed — dashed outline ("pending": not every branch has
 * started its task yet), solid ("every branch started, at least one still running"), solid outline ("blocked":
 * every branch's task has finished, waiting to advance) — see pipeline/itemPositions.ts's ItemFill. */
export function FanOutBox({ fanout, callbacks, handles, shellRef }: Props) {
  const queueSlots = queueSlotCount(fanout.queueSize, fanout.inQueue);
  // Where each item sitting at this fan-out's entry actually is, by slot — the origin a branch's task badge flies in
  // from. A fresh keyAssigner, and body-only, mirrors what assignNode does for this same node (body before queue), so
  // these keys match the ones the overlay renders its rectangles under. See assignBodySlots on why it is the slot and
  // not the rectangle.
  const entrySlotByItemKey = assignBodySlots(fanout.id, fanout.limit, fanout.inBody, keyAssigner(fanout.lanePaths)).slotByKey;
  return (
    <div className="node-shell" ref={shellRef}>
      {queueSlots > 0 && (
        <div className="node-queue" style={{ "--queue-slots": queueSlots } as React.CSSProperties}>
          {/* No reserve headroom here, gated on the slot count rather than queueSize, and sized from that count so
              the box can be transitioned — see StageBox's own queue SlotStrip, and .node-queue, for all three. */}
          <SlotStrip nodeId={fanout.id} variant="queue" count={queueSlots} reserve={queueSlots} />
        </div>
      )}
      <div
        className="node-box node-fanout nodrag nopan"
        onContextMenu={(e) => {
          e.preventDefault();
          e.stopPropagation(); // this box may now be nested inside a lane's interior — never bubble to its own menu
          callbacks.onContextMenu({ kind: "fanout", id: fanout.id }, e);
        }}
      >
        {handles}
        <EditableTitle value={fanout.name} onCommit={(name) => callbacks.onRenameNode(fanout.id, name)} />
        <LabeledSlider
          label="Limit"
          value={fanout.limit}
          min={MIN_LIMIT}
          max={MAX_LIMIT}
          deferCommit
          onChange={(v) => callbacks.onEditNode(fanout.id, "limit", v)}
          title="Maximum number of items inside this fan-out at once"
        />
        <LabeledSlider
          label="Queue"
          value={fanout.queueSize}
          min={MIN_QUEUE_SIZE}
          max={MAX_QUEUE_SIZE}
          deferCommit
          onChange={(v) => callbacks.onEditNode(fanout.id, "queueSize", v)}
          title="Maximum number of items waiting to enter this fan-out"
        />
        <SlotStrip nodeId={fanout.id} variant="body" count={bodySlotCount(fanout.limit, fanout.inBody)} reserve={MAX_LIMIT} />
        <div className="fanout-branches">
          {fanout.branches.map((branch) => (
            <BranchBox key={branch.id} branch={branch} callbacks={callbacks} entrySlotByItemKey={entrySlotByItemKey} />
          ))}
        </div>
      </div>
    </div>
  );
}
