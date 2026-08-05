import { useEffect, useState } from "react";
import { useElementWidth } from "../hooks/useElementWidth";
import { colorForItem, textColorForItem } from "../pipeline/colors";
import { isFailModifier } from "./shared/failModifier";
import type { ItemProgress } from "../pipeline/itemPositions";

// Wide enough that a 3-digit number still has clear air either side of it (a digit is 0.6em, so 9px bold gives ~16px
// of text inside a 31px content box) — the in-slot .item-badge cannot afford that, but nothing constrains this one.
// Must match .toolbar-item's own width in index.css.
const ITEM_W = 34;
const GAP = 4; // matches .slot-strip's own gap, so the row reads as part of the same visual language
const MORE_W = 64; // fixed rather than measured, so the row's width — and therefore its centering — is exact math
const EXIT_MS = 200; // must clear .toolbar-item's opacity transition, or an item unmounts mid-fade

interface Shown {
  completed: number;
  /** Position in the live order, 0 being the earliest item (drawn rightmost). An exiting item keeps the last one it
   * had, so it fades where it stood while the items behind it slide into place over it, rather than holding the row
   * frozen until it unmounts. */
  index: number;
  exiting: boolean;
}

/** How many of `count` items fit in `width`, and how many are left for the "…N more" label. Items are dropped from
 * the later end (see the component doc), and dropping any at all costs the label its own fixed slot — which can push
 * one more item out in turn, hence two steps rather than a single division. */
function fit(width: number, count: number): { visible: number; hidden: number } {
  const step = ITEM_W + GAP;
  const fitsWithoutLabel = Math.max(0, Math.floor((width + GAP) / step)); // n items need n*ITEM_W + (n-1)*GAP
  if (count <= fitsWithoutLabel) {
    return { visible: count, hidden: 0 };
  }
  const room = width - MORE_W - GAP;
  const visible = Math.max(0, Math.floor((room + GAP) / step));
  return { visible, hidden: count - visible };
}

interface Props {
  /** Every item currently in the conveyor, earliest first — see pipeline/itemPositions. Empty in build mode, which
   * is what leaves this component acting as the toolbar's plain flexible spacer. */
  items: ItemProgress[];
  totalStages: number;
  /** Ctrl/⌘-click on one of these bars fails that item, exactly as clicking its rectangle in a slot does — see
   * ItemsOverlay. */
  onFailItem: (itemNo: number) => void;
}

/** The toolbar's live census of what is inside the conveyor: one small progress bar per in-flight item, in the accent
 * color that item wears everywhere else, filled to the fraction of the pipeline's stages it has finished.
 *
 * Laid out right to left — earliest item at the right-hand end — and centered in whatever space the toolbar's other
 * controls leave. When they do not all fit, it is the *later* items that go, replaced by a "…N more" label at the
 * left, so the row always shows the items closest to leaving the system.
 *
 * Everything is absolutely positioned rather than a flex row, because the shifting has to animate: an item's slot is
 * a transform (transition-able), and the row's own centering is a transform of the whole box, so adding or removing
 * an item slides the rest into place instead of snapping them. */
export function ConveyorItems({ items, totalStages, onFailItem }: Props) {
  const { ref, width } = useElementWidth();
  // Items outlive the data by one fade: an item that has left the conveyor is kept here, marked exiting, until its
  // opacity transition has finished — the same "leaving the system rather than vanishing" treatment ItemsOverlay
  // gives its rectangles.
  const [shown, setShown] = useState<Map<number, Shown>>(new Map());

  useEffect(() => {
    setShown((prev) => {
      const next = new Map(prev);
      items.forEach(({ no, completed }, index) => next.set(no, { completed, index, exiting: false }));
      const live = new Set(items.map((it) => it.no));
      prev.forEach((entry, no) => {
        if (live.has(no) || entry.exiting) return;
        next.set(no, { ...entry, exiting: true }); // keeps entry.index: its farewell happens where it stood
        window.setTimeout(() => {
          setShown((cur) => {
            const copy = new Map(cur);
            copy.delete(no);
            return copy;
          });
        }, EXIT_MS);
      });
      return next;
    });
  }, [items]);

  // Nothing to draw until the first measurement lands: at width 0 every item would count as not fitting, and the row
  // would flash a "…N more" label with no items beside it.
  if (width === 0) {
    return <div className="toolbar-items" ref={ref} />;
  }

  const { visible, hidden } = fit(width, items.length);
  const itemsWidth = visible * ITEM_W + Math.max(0, visible - 1) * GAP;
  const rowWidth = itemsWidth + (hidden > 0 ? (visible > 0 ? GAP : 0) + MORE_W : 0);

  return (
    <div className="toolbar-items" ref={ref}>
      {/* right:50% puts this box's right edge on the container's center line; translating it back out by half its own
          width centers it, and animating that transform is what makes the whole row glide when its width changes. */}
      <div className="toolbar-items-row" style={{ width: rowWidth, transform: `translate(${rowWidth / 2}px, -50%)` }}>
        {[...shown.entries()].map(([no, item]) => {
          if (item.index >= visible) return null; // beyond the fold, counted in "…N more" instead
          const pct = totalStages > 0 ? Math.min(100, (item.completed / totalStages) * 100) : 0;
          return (
            <div
              key={no}
              className={`toolbar-item${item.exiting ? " exiting" : ""}`}
              title={item.exiting ? undefined : `Item ${no} — ${item.completed}/${totalStages} stages done, Ctrl/⌘-click to fail it`}
              onClick={(e) => {
                if (item.exiting || !isFailModifier(e)) return;
                e.stopPropagation();
                onFailItem(no);
              }}
              style={
                {
                  transform: `translateX(${-item.index * (ITEM_W + GAP)}px)`,
                  "--item-color": colorForItem(no),
                  "--item-text": textColorForItem(no),
                } as React.CSSProperties
              }
            >
              {/* Two copies of the number, the second clipped to the filled part along with the fill itself: that way
                  it reads against the white background and against the accent color, without either copy needing to
                  know where the fill's edge currently is. */}
              <span className="toolbar-item-label">{no}</span>
              <div className="toolbar-item-fill" style={{ "--fill-clip": `${100 - pct}%` } as React.CSSProperties}>
                <span className="toolbar-item-label">{no}</span>
              </div>
            </div>
          );
        })}
        {hidden > 0 && (
          <span className="toolbar-items-more" style={{ right: visible * (ITEM_W + GAP) }}>
            …{hidden} more
          </span>
        )}
      </div>
    </div>
  );
}
