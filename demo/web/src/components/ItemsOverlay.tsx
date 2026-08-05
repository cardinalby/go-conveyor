import { useEffect, useState } from "react";
import { ViewportPortal, useReactFlow } from "@xyflow/react";
import { getSlotElement } from "../state/slotRegistry";
import { isFailModifier } from "./shared/failModifier";
import { ItemBadge } from "./shared/ItemBadge";
import { colorForItem, textColorForItem } from "../pipeline/colors";
import { parentKey, type ItemFill, type ItemPositions } from "../pipeline/itemPositions";

// Half of .item-badge's own size — centers the rectangle on its slot's midpoint. Not square: the extra width is
// what a lane sub-item's ".1"/".2" suffix (see itemPositions.ts) needs room for.
const ITEM_CENTER_OFFSET_X = 13.5; // half of 27px
const ITEM_CENTER_OFFSET_Y = 9; // half of 18px
const EXIT_OFFSET_X = 90; // flow units an item keeps sliding right once it leaves the last stage, before vanishing
const EXIT_MS = 260; // must clear the CSS transition duration below so the slide/fade actually finishes on screen

interface RenderedItem {
  x: number;
  y: number;
  fill: ItemFill;
  exiting: boolean;
}

/** The root item number a composite key ("42" or "42.1", "42.1.2" — see ../pipeline/itemPositions) belongs to: what
 * a lane's sub-item is colored by, and what a click on it fails as a whole (see topology.runNodes — a child's
 * failure is the item's own, escalated, so there is nothing narrower to fail). */
function rootItemNo(key: string): number {
  return Number(key.split(".", 1)[0]);
}

interface Props {
  itemPositions: ItemPositions;
  /** Ctrl/⌘-click on an item's rectangle asks that one item to fail — see services/api's failItem. */
  onFailItem: (itemNo: number) => void;
}

/** Renders every in-flight item — and every lane sub-item, one level of the label deeper per lane crossing — as a
 * rectangle in one flow-coordinate-space layer, keyed by its composite item key rather than by whichever
 * node/queue currently contains it. That is what makes cross-node movement animate, including a sub-item sliding
 * through its lane's own interior chain: the same DOM element persists in React's tree as it moves from one node's
 * slot to another's, so the CSS transition on its transform actually has something to interpolate between — a
 * rectangle rendered inside each node's own slot strip would instead unmount from the old node and mount fresh in
 * the new one, with nothing to animate.
 *
 * Position is measured, not computed: each slot registers its real DOM element (see ../state/slotRegistry), and
 * this component reads its on-screen rect and converts it to flow coordinates on every tick.
 *
 * An item that disappears from itemPositions (finished the last stage, or a sub-item whose lane work is done) is
 * not removed immediately: it is kept mounted, pushed further right and faded out, then dropped after the
 * transition — "leaving the system" rather than vanishing. An item that has finished a node's own work but is
 * still stuck there, waiting for room ahead, renders outlined instead of solid (dashed if it's a fan-out entry
 * still waiting on its own branches' tasks) — see ../pipeline/itemPositions.
 *
 * Ctrl/⌘-clicking a rectangle injects a failure for the item as a whole — see topology.Failures and rootItemNo.
 * Only an item still in the system is clickable: one already sliding out has nothing left to fail.
 */
export function ItemsOverlay({ itemPositions, onFailItem }: Props) {
  const { screenToFlowPosition } = useReactFlow();
  const [items, setItems] = useState<Map<string, RenderedItem>>(new Map());

  useEffect(() => {
    setItems((prev) => {
      const next = new Map(prev);
      itemPositions.slotKeys.forEach((key, itemKey) => {
        const el = getSlotElement(key);
        if (!el) return;
        const fill = itemPositions.fills.get(itemKey) ?? "solid";
        if (!prev.has(itemKey)) {
          // Freshly appearing (a lane's entrance, or one level deeper): park it at wherever its immediate parent —
          // the item itself, for an entrance — currently sits, still tracked under that shallower key. The very
          // next tick then moves it to its real slot in a second, separate update, and it's *that* second write the
          // CSS transition on transform actually animates — which is what makes it visibly fly in from there
          // instead of popping straight into place. No parent tracked (a plain new top-level item, or the origin
          // has already gone) just renders at its real position immediately, unanimated.
          //
          // Read from `next`, not `prev`: computeItemPositions always assigns a node's own slot key before
          // recursing into its branches (see assignNode), so the parent key is already written into `next` by the
          // time this forEach reaches a deeper one — even when the parent itself is *also* moving this same tick
          // (e.g. the item crosses from the previous stage into the fan-out in the same poll that its first lane
          // child appears). Reading `prev` here would find the parent's stale, pre-this-tick position instead —
          // which is exactly what made a task appear to fly in from the stage before the fan-out.
          const origin = next.get(parentKey(itemKey) ?? "") ?? prev.get(parentKey(itemKey) ?? "");
          if (origin) {
            next.set(itemKey, { ...origin, fill, exiting: false });
            return;
          }
        }
        const rect = el.getBoundingClientRect();
        const pos = screenToFlowPosition({ x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 });
        next.set(itemKey, { x: pos.x, y: pos.y, fill, exiting: false });
      });
      prev.forEach((entry, itemKey) => {
        if (itemPositions.slotKeys.has(itemKey) || entry.exiting) return;
        next.set(itemKey, { ...entry, x: entry.x + EXIT_OFFSET_X, exiting: true });
        window.setTimeout(() => {
          setItems((cur) => {
            const copy = new Map(cur);
            copy.delete(itemKey);
            return copy;
          });
        }, EXIT_MS);
      });
      return next;
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [itemPositions]);

  return (
    <ViewportPortal>
      <div className="items-overlay">
        {Array.from(items.entries()).map(([itemKey, item]) => {
          const no = rootItemNo(itemKey);
          return (
            <ItemBadge
              key={itemKey}
              itemKey={itemKey}
              label={itemKey}
              color={colorForItem(no)}
              textColor={textColorForItem(no)}
              fill={item.fill}
              className={`item-rect-overlay${item.exiting ? " exiting" : ""}`}
              title={item.exiting ? undefined : `Item ${itemKey} — Ctrl/⌘-click to fail it`}
              onClick={(e) => {
                if (item.exiting || !isFailModifier(e)) return;
                e.stopPropagation();
                onFailItem(no);
              }}
              style={{ transform: `translate(${item.x - ITEM_CENTER_OFFSET_X}px, ${item.y - ITEM_CENTER_OFFSET_Y}px)` }}
            />
          );
        })}
      </div>
    </ViewportPortal>
  );
}
