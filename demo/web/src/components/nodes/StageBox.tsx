import { EditableTitle } from "../shared/EditableTitle";
import { LabeledSlider } from "../shared/LabeledSlider";
import { SlotStrip } from "../shared/SlotStrip";
import { bodySlotCount, queueSlotCount } from "../../pipeline/slots";
import { MAX_DELAY_MS, MAX_LIMIT, MAX_QUEUE_SIZE, MIN_DELAY_MS, MIN_LIMIT, MIN_QUEUE_SIZE } from "../../pipeline/defaults";
import type { ResolvedStage } from "../../pipeline/resolve";
import type { TreeCallbacks } from "./types";

const formatDelay = (ms: number) => `${(ms / 1000).toFixed(2)}s`;

interface Props {
  stage: ResolvedStage;
  callbacks: TreeCallbacks;
  /** xyflow connection handles, present only when this box is rendered as a top-level canvas node (see
   * StageNodeView) — a stage nested inside a lane's interior chain (see NodeBox) is plain nested markup, connected
   * visually with a CSS connector, not an xyflow edge. */
  handles?: React.ReactNode;
  /** Attaches to the outer element so PipelineCanvas can measure a top-level node — see ../../hooks/useReportSize.
   * Absent for a nested stage, which contributes to its ancestor's own measurement instead. */
  shellRef?: React.Ref<HTMLDivElement>;
}

/** The visual content shared by a top-level stage node (StageNodeView) and a stage nested inside some lane's
 * interior chain (see NodeBox/LaneBox) — title, capacity/delay sliders, body+queue slot strips. Pulled out so both
 * call sites render identically instead of drifting apart. */
export function StageBox({ stage, callbacks, handles, shellRef }: Props) {
  const queueSlots = queueSlotCount(stage.queueSize, stage.inQueue);
  return (
    <div className="node-shell" ref={shellRef}>
      {queueSlots > 0 && (
        <div className="node-queue" style={{ "--queue-slots": queueSlots } as React.CSSProperties}>
          {/* No reserve headroom here, unlike the body strip below: queueSize only ever changes via this node's own
              Queue slider (deferCommit already keeps that from resizing mid-drag), never from simulation activity
              on its own — so sizing exactly to the current queue keeps small queues visually compact.
              Gated on the slot count, not on queueSize: SetQueueSize is admission-only, so shrinking the room — to 0
              included — never evicts whoever is already waiting in it (see conveyor.Stage.SetQueueSize), and
              queueSlotCount keeps a slot for each of them. Gating on queueSize instead would unmount the box out
              from under real occupants and leave them rendering loose beside the node, and the room now shrinks the
              way the library does: one slot at a time, as items leave. */}
          <SlotStrip nodeId={stage.id} variant="queue" count={queueSlots} reserve={queueSlots} />
        </div>
      )}
      <div
        className="node-box nodrag nopan"
        onContextMenu={(e) => {
          e.preventDefault();
          e.stopPropagation(); // this box may now be nested inside a lane's interior — never bubble to its own menu
          callbacks.onContextMenu({ kind: "stage", id: stage.id }, e);
        }}
      >
        {handles}
        <EditableTitle value={stage.name} onCommit={(name) => callbacks.onRenameNode(stage.id, name)} />
        <LabeledSlider
          label="Limit"
          value={stage.limit}
          min={MIN_LIMIT}
          max={MAX_LIMIT}
          deferCommit
          onChange={(v) => callbacks.onEditNode(stage.id, "limit", v)}
          title="Maximum number of items processed by this stage at once"
        />
        <LabeledSlider
          label="Queue"
          value={stage.queueSize}
          min={MIN_QUEUE_SIZE}
          max={MAX_QUEUE_SIZE}
          deferCommit
          onChange={(v) => callbacks.onEditNode(stage.id, "queueSize", v)}
          title="Maximum number of items waiting to enter this stage"
        />
        <LabeledSlider
          label="Delay"
          value={stage.delayMs}
          min={MIN_DELAY_MS}
          max={MAX_DELAY_MS}
          step={50}
          formatValue={formatDelay}
          onChange={(ms) => callbacks.onEditNode(stage.id, "delayMs", ms)}
          variant="sim"
          title="Simulated processing time for this stage"
        />
        <SlotStrip
          nodeId={stage.id}
          variant="body"
          count={bodySlotCount(stage.limit, stage.inBody)}
          reserve={MAX_LIMIT}
          title="Items"
        />
      </div>
    </div>
  );
}
