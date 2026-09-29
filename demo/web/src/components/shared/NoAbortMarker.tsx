/** The "no abort" tag on the node that is the no-abort point (see Pipeline.noAbortPoint), or, with slot, an empty
 * place of the same size on a node that can become it. Render it as the first child of the node's .node-box: it then
 * sits on the box's top edge and adds nothing to the measured size (see index.css's .no-abort-marker). A click calls
 * onClick: a slot sets the point to its node, the tag resets it to the start. isDefault marks the tag on the start
 * when no other node is set; a click on it does nothing. A right-click does nothing: no node menu, no browser menu. */
export function NoAbortMarker({
  slot = false,
  isDefault = false,
  onClick,
}: {
  slot?: boolean;
  isDefault?: boolean;
  onClick?: () => void;
}) {
  let title =
    "No-abort point: on shutdown, items that have not entered this node yet are aborted at once. " +
    "Click to reset to the starting stage";
  if (slot) title = 'Set "No Abort" here';
  else if (isDefault) title = "No-abort point (default): no item is aborted on shutdown";
  return (
    <div
      className={`no-abort-marker nodrag nopan${slot ? " no-abort-slot" : ""}${isDefault ? " no-abort-default" : ""}`}
      title={title}
      onClick={(e) => {
        e.stopPropagation();
        if (!isDefault) onClick?.();
      }}
      onContextMenu={(e) => {
        e.preventDefault();
        e.stopPropagation();
      }}
    >
      no abort
    </div>
  );
}
