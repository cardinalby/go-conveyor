import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ReactFlow, Background, useReactFlow, type Node, type Edge } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { StartNodeView } from "./nodes/StartNodeView";
import { StageNodeView } from "./nodes/StageNodeView";
import { FanOutNodeView } from "./nodes/FanOutNodeView";
import { ItemsOverlay } from "./ItemsOverlay";
import { START_ID } from "../types/topology";
import type { ItemPositions } from "../pipeline/itemPositions";
import type { ResolvedNode, ResolvedStart } from "../pipeline/resolve";
import type { MenuTarget } from "../types/menu";
import type { BranchEditField, EditField, FanOutNodeData, Mode, StageNodeData, StartNodeData, TreeCallbacks } from "./nodes/types";

const nodeTypes = {
  start: StartNodeView,
  stage: StageNodeView,
  fanout: FanOutNodeView,
};

const NODE_GAP = 40;
const ROW_CENTER_Y = 300;
const DEFAULT_SIZE = { w: 220, h: 140 }; // guess used until a node's first real measurement arrives

type Size = { w: number; h: number };

interface Props {
  resolved: ResolvedNode[];
  start: ResolvedStart;
  mode: Mode;
  onContextMenu: (target: MenuTarget, evt: React.MouseEvent) => void;
  onEditNode: (id: string, field: EditField, value: number) => void;
  onEditBranch: (branchId: string, field: BranchEditField, value: number) => void;
  onEditStartDelay: (value: number) => void;
  onRenameNode: (id: string, name: string) => void;
  /** The implicit start stage is not one of `resolved`, so it has its own rename callback — same reason
   * onEditStartDelay is separate from onEditNode. */
  onRenameStart: (name: string) => void;
  onRenameBranch: (branchId: string, name: string) => void;
  onRenameEntrance: (laneId: string, name: string) => void;
  /** Computed by App rather than here, because the toolbar's item list needs the same walk — see
   * pipeline/itemPositions. NO_ITEM_POSITIONS outside run mode. */
  itemPositions: ItemPositions;
  /** Ctrl/⌘-click on an item rectangle / a pool task's badge injects a failure for exactly that one item or task —
   * see ItemsOverlay and shared/TaskStrip. */
  onFailItem: (itemNo: number) => void;
  onFailTask: (poolId: string, itemNo: number) => void;
}

function shallowEqual(a: Record<string, unknown>, b: Record<string, unknown>): boolean {
  const aKeys = Object.keys(a);
  if (aKeys.length !== Object.keys(b).length) return false;
  return aKeys.every((k) => Object.is(a[k], b[k]));
}

/** Fits the viewport to the diagram while its real layout is still arriving — and then stops for good.
 *
 * The static fitView prop below only fits once, against whatever positions exist at that instant; those are still
 * DEFAULT_SIZE guesses (every node reports its true size asynchronously, after its first paint, via
 * ../hooks/useReportSize), and a pipeline restored from a URL hash (see ../state/urlState) can already have several
 * nodes — including fan-outs, wider than the guess — so the mismatch between the guessed and the real layout is
 * large enough to visibly leave the diagram off-center/wrongly zoomed until something else happens to trigger a
 * re-fit. Hence the re-fit as measurements land.
 *
 * Re-fitting on *every* later layout change is too much, though: adding or removing a node, dragging the Queue
 * slider and renaming all move some node's measured box, and re-zooming the whole diagram while the user is working
 * on one node yanks the canvas under them. So this latches — it re-fits only until every node has reported a real
 * size, i.e. until the layout the page opened with is fully measured, and never again. After that the diagram
 * reflows in place and the viewport belongs to the user (panOnScroll).
 *
 * One flag is enough because nothing but an edit changes a node's box after that point: the slot strips render in
 * both modes and the body strip is pinned to MAX_LIMIT (see SlotStrip's `reserve`), so entering run mode — or any
 * amount of simulation activity — resizes nothing.
 *
 * `idsKey` is the top-level canvas nodes, which are exactly the ids that report sizes (StartNodeView, StageNodeView,
 * FanOutNodeView); a node nested in a lane's interior contributes to its ancestor's measurement instead, so waiting
 * on these is waiting on all of them.
 *
 * Rendered inside <ReactFlow> (like ItemsOverlay) because useReactFlow needs that context, which the PipelineCanvas
 * component itself, sitting above <ReactFlow>, does not have. */
function FitViewUntilLayoutSettles({ sizes, idsKey }: { sizes: Record<string, Size>; idsKey: string }) {
  const { fitView } = useReactFlow();
  const settled = useRef(false);
  const fittedOnce = useRef(false);

  useEffect(() => {
    if (settled.current) return;
    fitView({ padding: 0.3, duration: fittedOnce.current ? 200 : 0 });
    fittedOnce.current = true;
    if (idsKey.split(",").every((id) => sizes[id])) settled.current = true;
    // sizes changes exactly when a node's measured box actually changes (see reportSize's own bail-out), idsKey when
    // the topology does; fitView's identity is stable per React Flow's own contract.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sizes, idsKey]);

  return null;
}

/** Renders the pipeline left to right: the implicit start stage, then one node per top-level pipeline node. Column
 * x is a running sum of each node's own measured width (see ../hooks/useReportSize) plus a fixed gap, so a queue
 * box or an extra lane growing never overlaps the next node — everything shifts to make room instead. Every node's
 * y is centered on a shared row line using its own measured height, so a taller fan-out lines up with the
 * (shorter) stages around it rather than sitting proud of them. Not draggable — the graph's shape is edited through
 * the context menu, never by hand. */
export function PipelineCanvas({
  resolved,
  start,
  mode,
  onContextMenu,
  onEditNode,
  onEditBranch,
  onEditStartDelay,
  onRenameNode,
  onRenameStart,
  onRenameBranch,
  onRenameEntrance,
  itemPositions,
  onFailItem,
  onFailTask,
}: Props) {
  const [sizes, setSizes] = useState<Record<string, Size>>({});
  // Reused across renders (see resolvePipeline's own caches): while running, this component re-renders on every
  // poll tick, but most nodes' data is unchanged tick to tick. Handing React Flow back the exact same Node object
  // for an unchanged node — not just an equal one — is what lets React.memo on the node view components actually
  // skip re-rendering them, instead of every node re-rendering 50+ times a second regardless of whether anything
  // about it changed.
  const nodeCache = useRef(new Map<string, Node>()).current;

  /** Builds (or reuses) the Node object React Flow gets for one of our nodes.
   *
   * `measured` is not cosmetic: this is a controlled flow with no onNodesChange, so React Flow's own measurements
   * are never written back into these objects (applyNodeChanges is what normally does that). Its adoptUserNodes
   * treats any object it doesn't recognize by identity as a re-initialized node and resets measured/handleBounds
   * from the object alone — and a node with no dimensions renders `visibility: hidden`. Recovery then depends
   * entirely on React Flow re-observing the element on the resulting "initialized → not initialized" edge; miss one
   * (two store updates coalescing into a single React render) and nothing else will ever bring the node back,
   * because every strip here is width-pinned (see SlotStrip/TaskStrip's `reserve`) so a node's box never changes
   * size during a run and its ResizeObserver has nothing left to fire on. Handing back the size we already measured
   * ourselves (see ../hooks/useReportSize) removes that whole failure mode, and keeps handleBounds alive across
   * ticks too — parseHandles only preserves them for a node that still has measured. */
  const memoNode = useCallback(
    (id: string, type: string, position: { x: number; y: number }, size: Size | undefined, data: Record<string, unknown>): Node => {
      // undefined until this node's first real measurement arrives — that's the one adoption where React Flow
      // genuinely does need to measure the DOM itself.
      const measured = size && { width: size.w, height: size.h };
      const prev = nodeCache.get(id);
      if (
        prev &&
        prev.position.x === position.x &&
        prev.position.y === position.y &&
        prev.measured?.width === measured?.width &&
        prev.measured?.height === measured?.height &&
        shallowEqual(prev.data, data)
      ) {
        return prev;
      }
      const node: Node = { id, type, position, measured, draggable: false, selectable: false, data };
      nodeCache.set(id, node);
      return node;
    },
    [nodeCache],
  );

  const reportSize = useCallback((id: string, w: number, h: number) => {
    setSizes((prev) => {
      const cur = prev[id];
      if (cur && cur.w === w && cur.h === h) return prev;
      return { ...prev, [id]: { w, h } };
    });
  }, []);

  const idsInOrder = [START_ID, ...resolved.map((n) => n.id)];
  const positions: Record<string, { x: number; y: number }> = {};
  let x = NODE_GAP;
  for (const id of idsInOrder) {
    const size = sizes[id] ?? DEFAULT_SIZE;
    positions[id] = { x, y: ROW_CENTER_Y - size.h / 2 };
    x += size.w + NODE_GAP;
  }

  // One stable object for the whole node tree (top-level canvas nodes, and every node/branch nested inside some
  // lane's interior) — see TreeCallbacks. Memoized so an unrelated re-render (a poll tick that changed some other
  // node's occupancy) doesn't hand out a new reference here, which would defeat memoNode's shallowEqual check on
  // `data` for every node.
  const callbacks: TreeCallbacks = useMemo(
    () => ({ mode, onContextMenu, onEditNode, onEditBranch, onRenameNode, onRenameBranch, onRenameEntrance, onFailTask, onFailItem }),
    [mode, onContextMenu, onEditNode, onEditBranch, onRenameNode, onRenameBranch, onRenameEntrance, onFailTask, onFailItem],
  );

  const startData: StartNodeData = {
    mode,
    onContextMenu,
    reportSize,
    name: start.name,
    delayMs: start.delayMs,
    onEditDelay: onEditStartDelay,
    onRename: onRenameStart,
  };
  const nodes: Node[] = [
    memoNode(START_ID, "start", positions[START_ID], sizes[START_ID], startData),
    ...resolved.map((n): Node => {
      const position = positions[n.id];
      const size = sizes[n.id];
      if (n.kind === "stage") {
        const data: StageNodeData = { mode, onContextMenu, reportSize, stage: n, callbacks };
        return memoNode(n.id, "stage", position, size, data);
      }
      const data: FanOutNodeData = { mode, onContextMenu, reportSize, fanout: n, callbacks };
      return memoNode(n.id, "fanout", position, size, data);
    }),
  ];

  const idsKey = idsInOrder.join(",");
  const edges: Edge[] = useMemo(
    () =>
      idsInOrder.slice(0, -1).map((source, i) => ({
        id: `${source}->${idsInOrder[i + 1]}`,
        source,
        target: idsInOrder[i + 1],
        type: "smoothstep",
      })),
    // idsKey (not idsInOrder, a fresh array every render) is the real dependency: the topology's node order, which
    // never changes while running — see the module doc comment on why that's what makes this worth memoizing.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [idsKey],
  );

  return (
    <div className="pipeline-canvas">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        zoomOnScroll={false}
        panOnScroll
        fitView
        fitViewOptions={{ padding: 0.3 }}
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <ItemsOverlay itemPositions={itemPositions} onFailItem={onFailItem} />
        <FitViewUntilLayoutSettles sizes={sizes} idsKey={idsKey} />
      </ReactFlow>
    </div>
  );
}
