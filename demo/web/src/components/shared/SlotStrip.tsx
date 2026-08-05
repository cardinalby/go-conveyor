import { registerSlotElement, slotKey } from "../../state/slotRegistry";

interface Props {
  nodeId: string;
  variant: "body" | "queue";
  count: number;
  /** Total placeholders to always render (typically MAX_LIMIT / MAX_QUEUE_SIZE), so the strip's width stays fixed
   * regardless of live limit/queueSize or occupancy — a lowered limit never evicts items already inside (see
   * Stage.SetLimit), so `count` (which factors in current occupancy) can otherwise grow well past a node's own
   * dial while items already in flight drain out, resizing this node purely from simulation activity with no
   * slider drag involved to defer (see LabeledSlider's deferCommit, which only covers drag-triggered resizes). The
   * slots beyond `count` are real DOM elements (so the strip's box model width is genuinely fixed) but hidden. */
  reserve: number;
  /** Native tooltip for the whole strip (e.g. "Items"). Omitted for variants that don't want one. */
  title?: string;
}

/** A horizontal strip of empty slot placeholders, sized for a node's body (up to its limit) or waiting room (up to
 * its queue size). Renders no items itself: each slot registers its DOM element (see ../../state/slotRegistry) so
 * the app-wide ItemsOverlay can find exactly where to draw the item rectangle that currently occupies it — see
 * ../ItemsOverlay for why item rectangles live in one global layer instead of inside each strip. */
export function SlotStrip({ nodeId, variant, count, reserve, title }: Props) {
  const total = Math.max(count, reserve);
  return (
    <div className={`slot-strip slot-strip-${variant}`} title={title}>
      {Array.from({ length: total }, (_, i) => (
        <div
          key={i}
          className={`slot${i >= count ? " slot-reserved" : ""}`}
          ref={(el) => registerSlotElement(slotKey(nodeId, variant, i), el)}
        />
      ))}
    </div>
  );
}
