import { toFanOutAdmission, type FanOutAdmission } from "../../types/pipeline";

interface Props {
  value: FanOutAdmission;
  onChange: (value: FanOutAdmission) => void;
}

const TITLES: Record<FanOutAdmission, string> = {
  limit: "By limit: an item enters when the fan-out has a free item slot, and the previous node is released at once",
  pools:
    "By pools: an item also needs a branch with a free slot and nothing queued to enter; the previous node is released at once. Items keep entering while any pool can take work, and stop when none can",
  poolsStrict:
    "By pools (strict): as by pools, and the item also keeps the previous node's slot until its tasks have started on every branch it scheduled on — a saturated pool pushes back upstream",
};

/** The fan-out's Admission dial (see conveyor.FanOut.SetAdmission), laid out like the Limit / Queue sliders it sits
 * under — same row shape and label column (see index.css's .labeled-slider) — but as a <select>, since it is a choice
 * between named policies rather than a number. Live like the sliders: the change reaches the running conveyor at
 * once and applies to admissions after it. "nodrag nopan" for the same reason LabeledSlider carries it. */
export function AdmissionSelect({ value, onChange }: Props) {
  return (
    <div className="labeled-slider nodrag nopan" title={TITLES[value]} onClick={(e) => e.stopPropagation()}>
      <span className="labeled-slider-label">Admit</span>
      <select className="admission-select" value={value} onChange={(e) => onChange(toFanOutAdmission(e.target.value))}>
        <option value="limit">by limit</option>
        <option value="pools">by pools</option>
        <option value="poolsStrict">by pools (strict)</option>
      </select>
    </div>
  );
}
