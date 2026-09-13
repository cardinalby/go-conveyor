import { toFanOutBackpressure, type FanOutBackpressure } from "../../types/pipeline";

interface Props {
  value: FanOutBackpressure;
  onChange: (value: FanOutBackpressure) => void;
}

const TITLES: Record<FanOutBackpressure, string> = {
  buffered:
    "Buffered: an item entering the fan-out releases the previous node's slot at once, like at every other node. More items wait inside the fan-out",
  balanced:
    "Balanced: the item keeps the previous node's slot until the first task of its initial batch starts, so a saturated pool pushes back upstream",
  strict:
    "Strict: the item keeps the previous node's slot until every branch of its initial batch has started one task",
};

/** The fan-out's Backpressure dial (see conveyor.FanOut.SetBackpressure), laid out like the Limit / Queue sliders it
 * sits under — same row shape and label column (see index.css's .labeled-slider) — but as a <select>, since it is a
 * choice between named modes rather than a number. Live like the sliders: the change reaches the running conveyor at
 * once and applies to entries after it. "nodrag nopan" for the same reason LabeledSlider carries it. */
export function BackpressureSelect({ value, onChange }: Props) {
  return (
    <div className="labeled-slider nodrag nopan" title={TITLES[value]} onClick={(e) => e.stopPropagation()}>
      <span className="labeled-slider-label">Backpressure</span>
      <select className="backpressure-select" value={value} onChange={(e) => onChange(toFanOutBackpressure(e.target.value))}>
        <option value="buffered">buffered</option>
        <option value="balanced">balanced</option>
        <option value="strict">strict</option>
      </select>
    </div>
  );
}
