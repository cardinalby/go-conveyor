import { EditableTitle } from "../shared/EditableTitle";
import { LabeledSlider } from "../shared/LabeledSlider";
import { SlotStrip } from "../shared/SlotStrip";
import { MAX_DELAY_MS, MIN_DELAY_MS } from "../../pipeline/defaults";

const formatDelay = (ms: number) => `${(ms / 1000).toFixed(2)}s`;

interface Props {
  nodeId: string;
  /** The conveyor's own implicit start shows its name — "Read" until renamed (see ../../pipeline/names'
   * DEFAULT_START_NAME); a lane's is the fixed "Entrance" (see LaneBox), the same gate one level down. */
  label: string;
  /** Makes `label` click-to-edit. Present only for the conveyor's own implicit start: a lane's entrance is not a
   * node the user configures, so its label stays fixed. */
  onRename?: (name: string) => void;
  delayMs: number;
  onEditDelay: (value: number) => void;
  /** Present only for the conveyor's own implicit start (see StartNodeView) — a lane's entrance can't be removed
   * and offers no other action, so it has no menu at all. */
  onContextMenu?: (e: React.MouseEvent) => void;
  /** xyflow connection handles, present only for the conveyor's own implicit start — a lane's entrance is plain
   * nested markup, connected with a CSS connector like every other node in its interior chain. */
  handles?: React.ReactNode;
  /** Attaches to the outer element so PipelineCanvas can measure a top-level node — see ../../hooks/useReportSize.
   * Absent for a lane's entrance, which contributes to its ancestor's own measurement instead. */
  shellRef?: React.Ref<HTMLDivElement>;
}

/** The gate every item (or lane child) passes before its first move: always exactly one slot, a Delay dial for the
 * simulated time spent here, dashed border to mark it as implicit rather than something the user added. Shared by
 * the conveyor's own "Read" stage (StartNodeView) and a lane's own "Entrance" (see LaneBox) — the exact same
 * concept one level down (see conveyor.Lane: a lane's entrance paces child creation exactly as the conveyor's own
 * start paces items). */
export function StartBox({ nodeId, label, onRename, delayMs, onEditDelay, onContextMenu, handles, shellRef }: Props) {
  return (
    <div className="node-shell" ref={shellRef}>
      <div className="node-box node-start nodrag nopan" onContextMenu={onContextMenu}>
        {handles}
        {onRename ? <EditableTitle value={label} onCommit={onRename} /> : <div className="node-name">{label}</div>}
        <LabeledSlider
          label="Delay"
          value={delayMs}
          min={MIN_DELAY_MS}
          max={MAX_DELAY_MS}
          step={50}
          formatValue={formatDelay}
          onChange={onEditDelay}
          variant="sim"
          title="Simulated time an item spends here before its first move"
        />
        <SlotStrip nodeId={nodeId} variant="body" count={1} reserve={1} />
      </div>
    </div>
  );
}
