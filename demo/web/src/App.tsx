import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { PipelineCanvas } from "./components/PipelineCanvas";
import { CodePanel } from "./components/CodePanel";
import { LegendPanel } from "./components/LegendPanel";
import { ContextMenu } from "./components/ContextMenu";
import { Toolbar } from "./components/Toolbar";
import { generateGoCode } from "./codegen/generateGoCode";
import { defaultPipeline } from "./pipeline/factory";
import {
  addBranch,
  addParallelToStage,
  addStageAfter,
  addStageBefore,
  addStageFirst,
  addStageFirstInLane,
  convertBranchKind,
  findBranchAndOwner,
  removeBranch,
  removeNode,
  rewriteBranchLists,
  rewriteNodeLists,
} from "./pipeline/mutations";
import { clamp, MAX_ITEMS_LIMIT, MIN_ITEMS_LIMIT } from "./pipeline/defaults";
import { useFailModifierClass } from "./hooks/useFailModifierClass";
import { computeItemPositions, NO_ITEM_POSITIONS } from "./pipeline/itemPositions";
import { resolvePipeline, resolveStart } from "./pipeline/resolve";
import { toSpec } from "./pipeline/toSpec";
import { api } from "./services/api";
import { resetBodySlotAssignments } from "./state/bodySlotAssignments";
import { readUrlState, writeUrlState } from "./state/urlState";
import type { UrlAppState } from "./state/urlState";
import { START_ID } from "./types/topology";
import type { BranchNode, Pipeline } from "./types/pipeline";
import type { RunState } from "./types/state";
import type { MenuTarget } from "./types/menu";
import type { EditField } from "./components/nodes/types";

const POLL_INTERVAL_MS = 5;

type Mode = "build" | "run";
type MenuAction = "remove" | "before" | "after" | "parallelPool" | "parallelLane" | "convertToPool" | "convertToLane";

interface MenuState {
  target: MenuTarget;
  x: number;
  y: number;
}

// Resolved once per page load (see the lazy useState below): the URL hash's restored state, or a fresh default
// pipeline plus the same build-mode/panels-closed state a first-ever visit has always started from.
function initialAppState(): UrlAppState {
  return readUrlState() ?? { pipeline: defaultPipeline(), mode: "build", showCode: false, showLegend: false };
}

function App() {
  const [initial] = useState(initialAppState);
  const [pipeline, setPipeline] = useState<Pipeline>(initial.pipeline);
  const [mode, setMode] = useState<Mode>(initial.mode);
  const [runState, setRunState] = useState<RunState | null>(null);
  const [menu, setMenu] = useState<MenuState | null>(null);
  const [wasmReady, setWasmReady] = useState(false);
  const [showCode, setShowCode] = useState(initial.showCode);
  const [showLegend, setShowLegend] = useState(initial.showLegend);
  const pollRef = useRef<number | null>(null);
  const autoStartedRef = useRef(false);

  const generatedCode = useMemo(() => generateGoCode(pipeline), [pipeline]);

  // Lights up whatever item/task is under the cursor while Ctrl/⌘ is held, so it is clear what a click would fail
  // before it happens. Renders nothing and holds no state of its own — see the hook.
  useFailModifierClass();

  useEffect(() => {
    api.ready().then(() => setWasmReady(true));
  }, []);

  // The topology/config and panel state a reload or a shared link should reproduce — never runState, which is
  // live simulation output, not configuration, and would rewrite the URL on every poll tick. Also what turns a
  // hash-less first visit into one with a hash: this effect fires on mount with the (default) initial values just
  // as it does on every later edit.
  useEffect(() => {
    writeUrlState({ pipeline, mode, showCode, showLegend });
  }, [pipeline, mode, showCode, showLegend]);

  const stopPolling = useCallback(() => {
    if (pollRef.current !== null) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, []);

  const startPolling = useCallback(() => {
    if (pollRef.current !== null) return;
    pollRef.current = window.setInterval(() => {
      const state = api.state();
      setRunState(state);
      if (!state.running) {
        stopPolling();
        setMode("build");
      }
    }, POLL_INTERVAL_MS);
  }, [stopPolling]);

  useEffect(() => stopPolling, [stopPolling]);

  // Keeps the app in sync with an out-of-band hash edit — the address bar, a bookmark, browser back/forward —
  // the same way a fresh load already does. Never fires for the app's own writes above: those go through
  // history.replaceState, which (unlike an assignment to location.hash) never dispatches 'hashchange'.
  useEffect(() => {
    function handleHashChange() {
      const restored = readUrlState();
      if (!restored) return;
      setPipeline(restored.pipeline);
      setShowCode(restored.showCode);
      setShowLegend(restored.showLegend);
      setMode((prevMode) => {
        if (restored.mode === "run") {
          if (!wasmReady) return prevMode; // ignore until the module is ready — same gate as the mount-time autostart below
          if (prevMode !== "run") {
            resetBodySlotAssignments();
            setRunState(api.run(toSpec(restored.pipeline)));
            startPolling();
          }
          return "run";
        }
        if (prevMode === "run") {
          stopPolling();
          setRunState(api.stop());
        }
        return "build";
      });
    }
    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, [wasmReady, startPolling, stopPolling]);

  function handleRun() {
    resetBodySlotAssignments(); // a fresh run's item numbers restart from 1 — never inherit a stale slot
    const state = api.run(toSpec(pipeline));
    setRunState(state);
    setMode("run");
    startPolling();
  }

  // A URL restored into "run" mode starts its simulation as soon as it can, rather than sitting in run mode with
  // nothing actually running until Stop-then-Run is clicked. Gated on wasmReady (api.run needs the WASM handler,
  // which loads asynchronously — see services/wasmHandler.ts) and a ref, not an empty dep array, since wasmReady
  // itself flips from false to true well after mount.
  useEffect(() => {
    if (!wasmReady || autoStartedRef.current) return;
    autoStartedRef.current = true;
    if (mode === "run") handleRun();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [wasmReady]);

  // Graceful shutdown: no new items are created from this point on, but every item already in flight keeps running
  // to completion on its own schedule (see Manager.CancelCtx's own doc). Stop is still available afterwards to
  // force those in-flight items to cancel at once.
  function handleCancelCtx() {
    setRunState(api.cancelCtx());
  }

  // Force shutdown: on top of everything Cancel Ctx does, every item still in flight is canceled at once instead of
  // being left to finish (see Manager.Stop's own doc).
  function handleStop() {
    setRunState(api.stop());
  }

  // Build-mode only (see Toolbar) — the pipeline is the only thing this restores, not mode/panels, since those
  // aren't "config" and Reset is never even visible outside build mode to begin with.
  function handleReset() {
    setPipeline(defaultPipeline());
  }

  // Stabilized with useCallback so PipelineCanvas gets the same function reference every render: it (and the node
  // views under it) hand these down as part of each node's `data`, and a changing reference there would defeat the
  // memoization that lets an unaffected node skip re-rendering on every poll tick — see PipelineCanvas/resolve.ts.
  const handleContextMenu = useCallback(
    (target: MenuTarget, evt: React.MouseEvent) => {
      if (mode !== "build") return;
      setMenu({ target, x: evt.clientX, y: evt.clientY });
    },
    [mode],
  );

  // Edits always write through to the build-mode pipeline (so they still hold after returning to build mode — see
  // the spec discussion), and additionally reach the live conveyor immediately when running. Tree-aware (see
  // pipeline/mutations' rewriteNodeLists): id may be a top-level stage/fan-out or one nested inside some lane's
  // interior, at any depth.
  const handleEditNode = useCallback(
    (id: string, field: EditField, value: number) => {
      setPipeline((p) => ({
        ...p,
        nodes: rewriteNodeLists(p.nodes, (list) => {
          const i = list.findIndex((n) => n.id === id);
          if (i < 0) return list;
          const n = list[i];
          const next = [...list];
          if (n.kind === "stage") {
            next[i] = { ...n, [field]: value };
          } else if (field === "limit") {
            next[i] = { ...n, limit: value };
          } else if (field === "queueSize") {
            next[i] = { ...n, queueSize: value };
          } else {
            return list; // a fan-out never sends "delayMs" (see FanOutBox, which has no Delay slider) — it runs no code of its own.
          }
          return next;
        }),
      }));
      if (mode !== "run") return;
      if (field === "limit") api.setLimit(id, value);
      else if (field === "queueSize") api.setQueueSize(id, value);
      else api.setDelay(id, value);
      setRunState(api.state());
    },
    [mode],
  );

  // Same idea for a branch (pool or lane), tree-aware via rewriteBranchLists: branchId may belong to a top-level
  // fan-out or one nested inside some lane's interior. field "limit" is a no-op for a lane (it has none of its own).
  const handleEditBranch = useCallback(
    (branchId: string, field: "limit" | "delayMs" | "tasksPerItem", value: number) => {
      setPipeline((p) => ({
        ...p,
        nodes: rewriteBranchLists(p.nodes, (branches) => {
          const i = branches.findIndex((b) => b.id === branchId);
          if (i < 0) return branches;
          const b = branches[i];
          if (field === "limit" && b.kind !== "pool") return branches;
          const next = [...branches];
          next[i] = { ...b, [field]: value } as BranchNode;
          return next;
        }),
      }));
      if (mode !== "run") return;
      if (field === "limit") api.setLimit(branchId, value);
      else if (field === "delayMs") api.setDelay(branchId, value);
      else api.setTasksPerItem(branchId, value);
      setRunState(api.state());
    },
    [mode],
  );

  const handleEditStartDelay = useCallback(
    (value: number) => {
      setPipeline((p) => ({ ...p, startDelayMs: value }));
      if (mode !== "run") return;
      api.setDelay(START_ID, value);
      setRunState(api.state());
    },
    [mode],
  );

  // Global, unlike every other numeric edit above — it caps items in flight across the whole conveyor rather than
  // belonging to one node (see conveyor.Conveyor.SetItemsLimit), which is why it is a standalone corner control
  // rather than a dial on some node's box.
  const handleEditItemsLimit = useCallback(
    (value: number) => {
      setPipeline((p) => ({ ...p, itemsLimit: value }));
      if (mode !== "run") return;
      api.setItemsLimit(value);
      setRunState(api.state());
    },
    [mode],
  );

  // Click-to-rename on a node/branch title (see EditableTitle) — build-mode config only, like a node's name has
  // always been: go-conveyor bakes a unit's name in at Build time (OptName), so a live conveyor has no rename call
  // to make, unlike the numeric dials above. An empty commit clears back to the positional default (see
  // pipeline/resolve's `n.name || positionalName` fallback), never writes it out as a literal "custom" name.
  const handleRenameNode = useCallback((id: string, name: string) => {
    setPipeline((p) => ({
      ...p,
      nodes: rewriteNodeLists(p.nodes, (list) => {
        const i = list.findIndex((n) => n.id === id);
        if (i < 0) return list;
        const next = [...list];
        next[i] = { ...list[i], name };
        return next;
      }),
    }));
  }, []);

  // The implicit start stage is not one of Pipeline.nodes, so renaming it is a plain field write. It is the one name
  // that stays purely cosmetic even at Build time: go-conveyor's start unit is always "start" and takes no OptName
  // (see conveyor.Conveyor.StartUnit), so nothing carries this into the Spec or the generated code.
  const handleRenameStart = useCallback((name: string) => {
    setPipeline((p) => ({ ...p, startName: name }));
  }, []);

  // A lane's entrance, like the conveyor's own start, is not a node of Pipeline.nodes — it belongs to the lane branch
  // that owns it. Also display-only: a lane's entrance unit is go-conveyor's own and takes no OptName.
  const handleRenameEntrance = useCallback((laneId: string, name: string) => {
    setPipeline((p) => ({
      ...p,
      nodes: rewriteBranchLists(p.nodes, (branches) => {
        const i = branches.findIndex((b) => b.id === laneId);
        if (i < 0) return branches;
        const b = branches[i];
        if (b.kind !== "lane") return branches;
        const next = [...branches];
        next[i] = { ...b, entranceName: name };
        return next;
      }),
    }));
  }, []);

  const handleRenameBranch = useCallback((branchId: string, name: string) => {
    setPipeline((p) => ({
      ...p,
      nodes: rewriteBranchLists(p.nodes, (branches) => {
        const i = branches.findIndex((b) => b.id === branchId);
        if (i < 0) return branches;
        const next = [...branches];
        next[i] = { ...branches[i], name };
        return next;
      }),
    }));
  }, []);

  // Ctrl/⌘-clicking one item rectangle or one pool task's badge makes that single item's ItemProcessor (or that
  // single task) return an error, so what the library does with a partial failure — error-shutdown: the error
  // becomes the run's result, no new items are created, later items are canceled, earlier ones are left to finish —
  // can be watched live instead of only read about. Stabilized with useCallback for the same reason as the edit
  // handlers above: onFailTask travels inside each fan-out node's `data`.
  const handleFailItem = useCallback(
    (itemNo: number) => {
      if (mode !== "run") return;
      setRunState(api.failItem(itemNo));
    },
    [mode],
  );

  const handleFailTask = useCallback(
    (poolId: string, itemNo: number) => {
      if (mode !== "run") return;
      setRunState(api.failTask(poolId, itemNo));
    },
    [mode],
  );

  function closeMenu() {
    setMenu(null);
  }

  function menuAction(kind: MenuAction) {
    if (!menu) return;
    const { target } = menu;
    setPipeline((p) => {
      switch (kind) {
        case "remove":
          if (target.kind === "stage" || target.kind === "fanout") return removeNode(p, target.id);
          if (target.kind === "pool" || target.kind === "lane") {
            const hit = findBranchAndOwner(p, target.id);
            return hit ? removeBranch(p, hit.owner.id, target.id) : p;
          }
          return p;
        case "before":
          if (target.kind === "stage" || target.kind === "fanout") return addStageBefore(p, target.id);
          if (target.kind === "pool" || target.kind === "lane") {
            const hit = findBranchAndOwner(p, target.id);
            return hit ? addStageBefore(p, hit.owner.id) : p;
          }
          return p;
        case "after":
          if (target.kind === "start") return addStageFirst(p);
          if (target.kind === "stage" || target.kind === "fanout") return addStageAfter(p, target.id);
          if (target.kind === "lane") {
            const hit = findBranchAndOwner(p, target.id);
            if (hit?.branch.kind === "lane" && hit.branch.nodes.length === 0) return addStageFirstInLane(p, target.id);
            return hit ? addStageAfter(p, hit.owner.id) : p;
          }
          if (target.kind === "pool") {
            const hit = findBranchAndOwner(p, target.id);
            return hit ? addStageAfter(p, hit.owner.id) : p;
          }
          return p;
        case "parallelPool":
        case "parallelLane": {
          const branchKind = kind === "parallelPool" ? "pool" : "lane";
          if (target.kind === "stage") return addParallelToStage(p, target.id, branchKind);
          if (target.kind === "fanout") return addBranch(p, target.id, branchKind);
          if (target.kind === "pool" || target.kind === "lane") {
            const hit = findBranchAndOwner(p, target.id);
            return hit ? addBranch(p, hit.owner.id, branchKind) : p;
          }
          return p;
        }
        case "convertToLane":
          return target.kind === "pool" ? convertBranchKind(p, target.id, "lane") : p;
        case "convertToPool":
          return target.kind === "lane" ? convertBranchKind(p, target.id, "pool") : p;
        default:
          return p;
      }
    });
  }

  const activeRunState = mode === "run" ? runState : null;
  const resolved = resolvePipeline(pipeline, activeRunState);
  const start = resolveStart(pipeline, activeRunState);
  // Computed once here, not inside PipelineCanvas, because the toolbar's live item list is derived from the same walk
  // — see pipeline/itemPositions.
  const itemPositions = mode === "run" ? computeItemPositions(resolved, start) : NO_ITEM_POSITIONS;

  return (
    <div className="app">
      <Toolbar
        mode={mode}
        stopping={runState?.stopping ?? false}
        forced={runState?.forced ?? false}
        error={runState?.error}
        workerCount={activeRunState?.workerCount}
        disabled={!wasmReady}
        showCode={showCode}
        onToggleShowCode={() => setShowCode((v) => !v)}
        showLegend={showLegend}
        onToggleShowLegend={() => setShowLegend((v) => !v)}
        onRun={handleRun}
        onStop={handleStop}
        onCancelCtx={handleCancelCtx}
        onReset={handleReset}
        conveyorItems={itemPositions.progress}
        totalStages={itemPositions.totalStages}
        onFailItem={handleFailItem}
      />
      <div className="main-content">
        <LegendPanel open={showLegend} />
        <CodePanel open={showCode} code={generatedCode} />
        <PipelineCanvas
          resolved={resolved}
          start={start}
          mode={mode}
          onContextMenu={handleContextMenu}
          onEditNode={handleEditNode}
          onEditBranch={handleEditBranch}
          onEditStartDelay={handleEditStartDelay}
          onRenameNode={handleRenameNode}
          onRenameStart={handleRenameStart}
          onRenameBranch={handleRenameBranch}
          onRenameEntrance={handleRenameEntrance}
          itemPositions={itemPositions}
          onFailItem={handleFailItem}
          onFailTask={handleFailTask}
        />
      </div>
      {menu && (
        <ContextMenu
          target={menu.target}
          pipeline={pipeline}
          x={menu.x}
          y={menu.y}
          onRemove={() => menuAction("remove")}
          onAddBefore={() => menuAction("before")}
          onAddAfter={() => menuAction("after")}
          onAddParallelPool={() => menuAction("parallelPool")}
          onAddParallelLane={() => menuAction("parallelLane")}
          onConvertToPool={() => menuAction("convertToPool")}
          onConvertToLane={() => menuAction("convertToLane")}
          onClose={closeMenu}
        />
      )}
      <div className="items-limit-control" title="Caps how many items may be in flight across the whole conveyor at once (0 = unlimited)">
        <label htmlFor="items-limit-input">ItemsLimit</label>
        <input
          id="items-limit-input"
          type="number"
          min={MIN_ITEMS_LIMIT}
          max={MAX_ITEMS_LIMIT}
          value={pipeline.itemsLimit}
          onChange={(e) => handleEditItemsLimit(clamp(Number(e.target.value), MIN_ITEMS_LIMIT, MAX_ITEMS_LIMIT))}
        />
      </div>
      <a className="repo-link" href="https://github.com/cardinalby/go-conveyor" target="_blank" rel="noopener noreferrer">
        go-conveyor
      </a>
    </div>
  );
}

export default App;
