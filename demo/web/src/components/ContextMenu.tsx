import { findBranchAndOwner, isTopLevelNode } from "../pipeline/mutations";
import type { MenuTarget } from "../types/menu";
import type { Pipeline } from "../types/pipeline";

interface Props {
  target: MenuTarget;
  pipeline: Pipeline;
  x: number;
  y: number;
  onRemove: () => void;
  onAddBefore: () => void;
  onAddAfter: () => void;
  onAddParallelPool: () => void;
  onAddParallelLane: () => void;
  onConvertToPool: () => void;
  onConvertToLane: () => void;
  onClose: () => void;
}

/** The build-mode right-click menu. Item set depends on target:
 *  - start: only "Add stage after", per the spec.
 *  - an empty lane (no interior nodes yet): just Remove / "Add lane Stage after" (seeds its first interior stage —
 *    there's nothing to be "before" yet) / "Convert Lane to Pool". No base items: "add a sibling branch" is still
 *    reachable from the fan-out's own menu.
 *  - everything else (stage, fan-out, pool, non-empty lane): the base set (Remove / Add Stage before+after / Add
 *    parallel Lane+Pool), plus "Convert Pool to Lane" for a pool. A stage/fan-out whose own containing list is a
 *    lane's interior (not pipeline.nodes) reads "Add lane Stage before/after" instead — same action underneath,
 *    just naming where it splices.
 */
export function ContextMenu({
  target,
  pipeline,
  x,
  y,
  onRemove,
  onAddBefore,
  onAddAfter,
  onAddParallelPool,
  onAddParallelLane,
  onConvertToPool,
  onConvertToLane,
  onClose,
}: Props) {
  function item(label: string, onClick: () => void) {
    return (
      <button
        type="button"
        className="context-menu-item"
        onClick={() => {
          onClick();
          onClose();
        }}
      >
        {label}
      </button>
    );
  }

  function shell(children: React.ReactNode) {
    return (
      <div
        className="context-menu-backdrop"
        onClick={onClose}
        onContextMenu={(e) => {
          e.preventDefault();
          onClose();
        }}
      >
        <div className="context-menu" style={{ left: x, top: y }} onClick={(e) => e.stopPropagation()}>
          {children}
        </div>
      </div>
    );
  }

  if (target.kind === "start") {
    return shell(item("Add stage after", onAddAfter));
  }

  if (target.kind === "lane") {
    const hit = findBranchAndOwner(pipeline, target.id);
    const hasNodes = hit?.branch.kind === "lane" && hit.branch.nodes.length > 0;
    if (!hasNodes) {
      return shell(
        <>
          {item("Remove", onRemove)}
          {item("Add lane Stage after", onAddAfter)}
          {item("Convert Lane to Pool", onConvertToPool)}
        </>,
      );
    }
  }

  const nested = target.kind === "stage" || target.kind === "fanout" ? !isTopLevelNode(pipeline, target.id) : false;
  const beforeLabel = nested ? "Add lane Stage before" : "Add Stage before";
  const afterLabel = nested ? "Add lane Stage after" : "Add Stage after";

  return shell(
    <>
      {item("Remove", onRemove)}
      {item(beforeLabel, onAddBefore)}
      {item(afterLabel, onAddAfter)}
      {item("Add parallel Lane", onAddParallelLane)}
      {item("Add parallel Pool", onAddParallelPool)}
      {target.kind === "pool" && item("Convert Pool to Lane", onConvertToLane)}
    </>,
  );
}
