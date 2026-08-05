import { ItemBadge } from "./shared/ItemBadge";
import { colorForItem, textColorForItem } from "../pipeline/colors";

interface Props {
  open: boolean;
}

// A fixed, made-up item number so every preview swatch below shares one recognizable color/text-contrast pairing,
// exactly as pipeline/colors.ts would assign it to a real item 42 — not tied to anything actually running.
const PREVIEW_ITEM_NO = 42;

/** One item-badge preview, reusing the exact ItemBadge component ItemsOverlay/TaskStrip render with — see
 * index.css's .item-badge and its .pending/.blocked modifiers. No positioning class: a legend swatch sits in plain
 * flow, unlike the live, moving/fading versions. */
function Preview({ fill }: { fill?: "pending" | "blocked" }) {
  return (
    <ItemBadge
      label={PREVIEW_ITEM_NO}
      color={colorForItem(PREVIEW_ITEM_NO)}
      textColor={textColorForItem(PREVIEW_ITEM_NO)}
      fill={fill}
      className="legend-preview"
    />
  );
}

/** A sliding left-side drawer explaining what each item/task fill means — see pipeline/itemPositions.ts's ItemFill
 * for the states themselves. Always mounted so the width transition can animate it in/out, same as CodePanel. */
export function LegendPanel({ open }: Props) {
  return (
    <div className={`legend-panel${open ? " legend-panel-open" : ""}`} aria-hidden={!open}>
      <div className="legend-panel-inner">
        <div className="legend-panel-header">
          <h2 className="legend-panel-title">Legend</h2>
        </div>
        <div className="legend-panel-body">
          <ul className="legend-list">
            <li className="legend-item">
              <span className="legend-label">Item (working)</span>
              <Preview />
            </li>
            <li className="legend-item">
              <span className="legend-label">Item (waiting at MoveTo)</span>
              <Preview fill="blocked" />
            </li>
            <li className="legend-item-heading">
              <span className="legend-label">Fan-out stage:</span>
              <ul className="legend-sublist">
                <li className="legend-item">
                  <span className="legend-label">Item (tasks haven't started yet)</span>
                  <Preview fill="pending" />
                </li>
                <li className="legend-item">
                  <span className="legend-label">Item (tasks in progress)</span>
                  <Preview />
                </li>
                <li className="legend-item">
                  <span className="legend-label">Item (tasks finished, waiting to enter next stage)</span>
                  <Preview fill="blocked" />
                </li>
                <li className="legend-item">
                  <span className="legend-label">Pool task belonging to an item</span>
                  <Preview />
                </li>
              </ul>
            </li>
            <li className="legend-item-heading">
              <span className="legend-label">Ctrl/⌘-click an item or a pool task:</span>
              <ul className="legend-sublist">
                <li className="legend-item">
                  <span className="legend-label">
                    Fails just that one — its ItemProcessor (or that one pool task) returns an error, cutting its
                    current delay short. The run then shuts down on that error: no new items are created and later
                    items are canceled, while items already ahead of it finish normally.
                  </span>
                </li>
              </ul>
            </li>
          </ul>
        </div>
      </div>
    </div>
  );
}
