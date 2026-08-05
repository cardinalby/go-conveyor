import { useEffect, useRef, useState } from "react";

interface Props {
  label: string;
  value: number;
  min: number;
  max: number;
  step?: number;
  formatValue?: (v: number) => string;
  onChange: (value: number) => void;
  /** Defers onChange until the drag/keyboard gesture ends (plus a short settle delay) instead of firing on every
   * intermediate move. Use this when onChange can resize this node's own box (Limit, Queue — see bodySlotCount /
   * TaskStrip / the queue block): committing on every intermediate move would toggle reserved slots' visibility (or,
   * for the queue block, its slot count and width) on every tick of the drag, flickering mid-gesture. The thumb
   * still tracks the cursor smoothly during the drag — only the value that reaches onChange, and therefore any
   * resulting resize, is delayed until there's no gesture left for it to disrupt. Delay has no such effect and is
   * left committing live (the default). */
  deferCommit?: boolean;
  /** "setup" (the default) is a topology dial — Limit, Queue — rendered blue. "sim" is a simulated-processing dial
   * — Delay, Tasks per item — rendered orange, so the two kinds of knob are visually distinguishable at a glance. */
  variant?: "setup" | "sim";
  /** Tooltip explaining what this particular dial controls in its context (e.g. a pool's "Tasks" differs from a
   * lane's) — shown on hover over the whole control, since the label text alone is too small a target. */
  title?: string;
}

const COMMIT_DELAY_MS = 100;

/** A labeled dial used for Limit, Queue size, Delay and Tasks per item alike: a range slider, its current value
 * shown next to it as plain (non-interactive) text — dragging the slider is the only way to change it. "nodrag
 * nopan" keeps React Flow's own pan-gesture listener from stealing the slider thumb's mousedown — see index.css's
 * node section. */
export function LabeledSlider({
  label,
  value,
  min,
  max,
  step = 1,
  formatValue,
  onChange,
  deferCommit = false,
  variant = "setup",
  title,
}: Props) {
  // Only meaningful while deferCommit is set: the slider's own display value while a gesture is in progress, ahead
  // of onChange actually landing — see scheduleCommit.
  const [liveValue, setLiveValue] = useState(value);
  const commitTimeoutRef = useRef<number | null>(null);

  useEffect(() => setLiveValue(value), [value]);

  useEffect(() => {
    return () => {
      if (commitTimeoutRef.current !== null) clearTimeout(commitTimeoutRef.current);
    };
  }, []);

  function scheduleCommit(v: number) {
    if (commitTimeoutRef.current !== null) clearTimeout(commitTimeoutRef.current);
    commitTimeoutRef.current = window.setTimeout(() => {
      commitTimeoutRef.current = null;
      onChange(v);
    }, COMMIT_DELAY_MS);
  }

  const displayValue = deferCommit ? liveValue : value;

  return (
    <div
      className={`labeled-slider nodrag nopan${variant === "sim" ? " labeled-slider-sim" : ""}`}
      title={title}
      onClick={(e) => e.stopPropagation()}
    >
      <span className="labeled-slider-label">{label}</span>
      <input
        type="range"
        min={min}
        max={max}
        step={step}
        value={displayValue}
        onChange={(e) => {
          const v = Number(e.target.value);
          if (deferCommit) setLiveValue(v);
          else onChange(v);
        }}
        onPointerUp={deferCommit ? (e) => scheduleCommit(Number(e.currentTarget.value)) : undefined}
        onKeyUp={deferCommit ? (e) => scheduleCommit(Number(e.currentTarget.value)) : undefined}
      />
      <span className="labeled-slider-value">{formatValue ? formatValue(displayValue) : displayValue}</span>
    </div>
  );
}
