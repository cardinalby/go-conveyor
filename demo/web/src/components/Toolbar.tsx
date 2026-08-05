import { ConveyorItems } from "./ConveyorItems";
import type { ItemProgress } from "../pipeline/itemPositions";

interface Props {
  mode: "build" | "run";
  stopping: boolean;
  forced: boolean;
  error?: string;
  workerCount?: number;
  disabled: boolean;
  showCode: boolean;
  onToggleShowCode: () => void;
  showLegend: boolean;
  onToggleShowLegend: () => void;
  onRun: () => void;
  onStop: () => void;
  onCancelCtx: () => void;
  onReset: () => void;
  /** What is inside the conveyor right now, for the live list in the middle of the bar. Empty outside run mode, where
   * that list is just the flexible gap between the two groups of controls — see ConveyorItems. */
  conveyorItems: ItemProgress[];
  totalStages: number;
  onFailItem: (itemNo: number) => void;
}

export function Toolbar({
  mode,
  stopping,
  forced,
  error,
  workerCount,
  disabled,
  showCode,
  onToggleShowCode,
  showLegend,
  onToggleShowLegend,
  onRun,
  onStop,
  onCancelCtx,
  onReset,
  conveyorItems,
  totalStages,
  onFailItem,
}: Props) {
  return (
    <div className="toolbar">
      <button type="button" className="panel-toggle-button" aria-pressed={showCode} onClick={onToggleShowCode}>
        {showCode ? "Hide code" : "Show code"}
      </button>
      <button type="button" className="panel-toggle-button" aria-pressed={showLegend} onClick={onToggleShowLegend}>
        {showLegend ? "Hide legend" : "Legend"}
      </button>
      <ConveyorItems items={conveyorItems} totalStages={totalStages} onFailItem={onFailItem} />
      {mode === "run" && workerCount !== undefined && (
        <span className="toolbar-workers">Workers: {workerCount}</span>
      )}
      {error && <span className="toolbar-error">{error}</span>}
      {mode === "build" ? (
        <>
          <button type="button" className="panel-toggle-button" onClick={onReset}>
            Reset
          </button>
          <button type="button" className="run-button" onClick={onRun} disabled={disabled}>
            Run
          </button>
        </>
      ) : (
        <>
          {/* Graceful: stops new items from being created, but leaves in-flight ones to finish on their own. */}
          <button type="button" className="cancel-ctx-button" onClick={onCancelCtx} disabled={stopping}>
            {stopping && !forced ? "Canceling…" : "Cancel Ctx"}
          </button>
          {/* Forceful: on top of what Cancel Ctx does, cancels every in-flight item at once. Stays enabled after
              Cancel Ctx so a graceful shutdown can still be escalated. */}
          <button type="button" className="stop-button" onClick={onStop} disabled={forced}>
            {forced ? "Stopping…" : "Stop"}
          </button>
        </>
      )}
    </div>
  );
}
