interface Props {
  label: React.ReactNode;
  color: string;
  textColor: string;
  /** See ../../pipeline/itemPositions.ts's ItemFill — "pending"/"blocked" swap the solid fill for an outline.
   * Irrelevant to a pool's task badge (see shared/TaskStrip), which is always the solid look. */
  fill?: "solid" | "pending" | "blocked";
  className?: string;
  style?: React.CSSProperties;
  title?: string;
  onClick?: (e: React.MouseEvent) => void;
  /** Root item number this badge belongs to, exposed as a `data-item-key` DOM attribute — the anchor a pool task
   * or a lane's own entrance looks up (see TaskStrip) to find "where the item currently sits in its fan-out" and
   * fly in from there instead of just fading in. Omit for a badge that's never a fly-in origin (e.g. a legend
   * preview, which is never queried). */
  itemKey?: string | number;
  ref?: React.Ref<HTMLDivElement>;
}

/** The colored rectangle every item — and every pool task — renders as, wherever it appears: a moving item
 * rectangle (see ../ItemsOverlay), a lane's own sub-items sliding through its interior chain (same overlay, a
 * composite id instead of a bare item number), a pool's fading task badges (see ./TaskStrip), or a legend swatch.
 * Positioning is deliberately not this component's concern — each caller lays it out its own way (a global
 * flow-coordinate transform, an absolute offset within a strip, or plain static flow for a legend preview) via
 * `className`/`style`; only the visual (size, color, label, fill) is shared. */
export function ItemBadge({ label, color, textColor, fill = "solid", className, style, title, onClick, itemKey, ref }: Props) {
  return (
    <div
      ref={ref}
      data-item-key={itemKey}
      className={`item-badge${fill !== "solid" ? ` ${fill}` : ""}${className ? ` ${className}` : ""}`}
      title={title}
      onClick={onClick}
      style={{ ...style, "--item-color": color, "--item-text": textColor } as React.CSSProperties}
    >
      {label}
    </div>
  );
}
