import { START_ID } from "../types/topology";
import { DEFAULT_ENTRANCE_NAME, DEFAULT_START_NAME, positionalBranchName, positionalNodeName } from "./names";
import type { Pipeline, PipelineNode } from "../types/pipeline";
import type { LanePathEntry, NodeState, RunState } from "../types/state";

export interface ResolvedPool {
  id: string;
  kind: "pool";
  name: string;
  limit: number;
  delayMs: number;
  tasksPerItem: number;
  inBody: number[];
  /** Items whose task on this pool has been accepted but not yet pulled/started (see Wave.Started — used to
   * classify a fan-out entry's fill; a branch never renders its own queue box). */
  inQueue: number[];
  /** What tells two of the same item's concurrent tasks on this pool apart (TasksPerItem > 1) — see TaskStrip,
   * which is what actually renders them "42.1", "42.2" instead of an ambiguous repeated "42". Mirrors
   * ResolvedLane's own field; a pool never has interior nodes of its own, so it only ever contributes the last
   * segment of a path — the rest, if any, comes from however many lanes this pool's own fan-out is nested inside. */
  lanePaths: LanePathEntry[];
}

/** delayMs/inBody/inQueue/blockedLeaving/lanePaths describe the lane's own entrance — always applied, exactly like
 * the conveyor's own implicit start: a child sleeps out delayMs here before moving on, whether or not `nodes` is
 * empty (see ../types/pipeline's LaneBranch). blockedLeaving/lanePaths mirror ResolvedStage's own fields — the
 * entrance is rendered the same way the implicit start is (see nodes/StartBox), just scoped to this lane; lanePaths
 * is what tells concurrent children of the same item apart at the entrance, since they all inherit its number. */
export interface ResolvedLane {
  id: string;
  kind: "lane";
  name: string;
  /** Already resolved to the display name — the custom one, or DEFAULT_ENTRANCE_NAME. */
  entranceName: string;
  tasksPerItem: number;
  delayMs: number;
  inBody: number[];
  inQueue: number[];
  blockedLeaving: number[];
  lanePaths: LanePathEntry[];
  nodes: ResolvedNode[];
}

export type ResolvedBranch = ResolvedPool | ResolvedLane;

export interface ResolvedStage {
  id: string;
  kind: "stage";
  name: string;
  limit: number;
  queueSize: number;
  delayMs: number;
  inBody: number[];
  inQueue: number[];
  /** InBody items that finished this stage's own delay and are now trying to advance into the next node — see
   * ../types/state.ts's NodeState.blockedLeaving and ./itemPositions.ts's ItemFill. */
  blockedLeaving: number[];
  /** Non-empty only when this stage is reachable through some lane's interior — see ../types/state's
   * LanePathEntry. */
  lanePaths: LanePathEntry[];
}

export interface ResolvedFanOut {
  id: string;
  kind: "fanout";
  name: string;
  limit: number;
  queueSize: number;
  inBody: number[];
  inQueue: number[];
  pendingEntry: number[];
  lanePaths: LanePathEntry[];
  branches: ResolvedBranch[];
}

export type ResolvedNode = ResolvedStage | ResolvedFanOut;

export interface ResolvedStart {
  name: string;
  delayMs: number;
  inBody: number[];
  blockedLeaving: number[];
}

function indexById(runState: RunState | null): Map<string, NodeState> {
  const m = new Map<string, NodeState>();
  for (const n of runState?.nodes ?? []) m.set(n.id, n);
  return m;
}

function sameNums(a: number[], b: number[]): boolean {
  if (a === b) return true;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

function sameLanePaths(a: LanePathEntry[], b: LanePathEntry[]): boolean {
  if (a === b) return true;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i].itemNo !== b[i].itemNo || !sameNums(a[i].path, b[i].path)) return false;
  }
  return true;
}

// One cache per resolved shape, keyed by node/branch id, each holding the last object handed out for that id. While
// running, resolvePipeline/resolveStart are called on every poll tick (see App.tsx), so most nodes are unaffected
// by any given tick's update; reusing the previous object when nothing about a node actually changed lets
// React.memo on the node view components (see components/nodes) skip re-rendering the ones that didn't, instead of
// every node re-rendering on every tick regardless. Never pruned, like ../state/bodySlotAssignments — bounded by
// the number of distinct ids a session ever creates, and a stale entry is simply never looked up again. Ids stay
// globally unique regardless of nesting depth, so one flat cache per shape works for a lane's interior nodes too.
const stageCache = new Map<string, ResolvedStage>();
const fanOutCache = new Map<string, ResolvedFanOut>();
const poolCache = new Map<string, ResolvedPool>();
const laneCache = new Map<string, ResolvedLane>();

function memoized<T>(cache: Map<string, T>, id: string, next: T, same: (a: T, b: T) => boolean): T {
  const prev = cache.get(id);
  if (prev && same(prev, next)) return prev;
  cache.set(id, next);
  return next;
}

function sameStage(a: ResolvedStage, b: ResolvedStage): boolean {
  return (
    a.name === b.name &&
    a.limit === b.limit &&
    a.queueSize === b.queueSize &&
    a.delayMs === b.delayMs &&
    sameNums(a.inBody, b.inBody) &&
    sameNums(a.inQueue, b.inQueue) &&
    sameNums(a.blockedLeaving, b.blockedLeaving) &&
    sameLanePaths(a.lanePaths, b.lanePaths)
  );
}

function samePool(a: ResolvedPool, b: ResolvedPool): boolean {
  return (
    a.name === b.name &&
    a.limit === b.limit &&
    a.delayMs === b.delayMs &&
    a.tasksPerItem === b.tasksPerItem &&
    sameNums(a.inBody, b.inBody) &&
    sameNums(a.inQueue, b.inQueue) &&
    sameLanePaths(a.lanePaths, b.lanePaths)
  );
}

function sameLane(a: ResolvedLane, b: ResolvedLane): boolean {
  return (
    a.name === b.name &&
    a.entranceName === b.entranceName &&
    a.tasksPerItem === b.tasksPerItem &&
    a.delayMs === b.delayMs &&
    sameNums(a.inBody, b.inBody) &&
    sameNums(a.inQueue, b.inQueue) &&
    sameNums(a.blockedLeaving, b.blockedLeaving) &&
    sameLanePaths(a.lanePaths, b.lanePaths) &&
    a.nodes.length === b.nodes.length &&
    a.nodes.every((n, i) => n === b.nodes[i])
  );
}

function sameFanOut(a: ResolvedFanOut, b: ResolvedFanOut): boolean {
  return (
    a.name === b.name &&
    a.limit === b.limit &&
    a.queueSize === b.queueSize &&
    sameNums(a.inBody, b.inBody) &&
    sameNums(a.inQueue, b.inQueue) &&
    sameNums(a.pendingEntry, b.pendingEntry) &&
    sameLanePaths(a.lanePaths, b.lanePaths) &&
    a.branches.length === b.branches.length &&
    a.branches.every((branch, i) => branch === b.branches[i])
  );
}

/** Resolves one node list — pipeline.nodes, or a lane's own interior nodes — against live, keeping every id's own
 * cache entry regardless of which list it's found in (see the caches' own doc). Shared by resolvePipeline and, for
 * a lane branch, called again on its interior — the recursion is what lets a lane's interior contain a fan-out
 * whose own branches may again be lanes, to any depth. */
function resolveNodes(nodes: PipelineNode[], live: Map<string, NodeState>): ResolvedNode[] {
  return nodes.map((n, i): ResolvedNode => {
    if (n.kind === "stage") {
      const s = live.get(n.id);
      const next: ResolvedStage = {
        id: n.id,
        kind: "stage",
        name: n.name || positionalNodeName("stage", i),
        limit: s?.limit ?? n.limit,
        queueSize: s?.queueSize ?? n.queueSize,
        delayMs: s?.delayMs ?? n.delayMs,
        inBody: s?.inBody ?? [],
        inQueue: s?.inQueue ?? [],
        blockedLeaving: s?.blockedLeaving ?? [],
        lanePaths: s?.lanePaths ?? [],
      };
      return memoized(stageCache, n.id, next, sameStage);
    }
    const fo = live.get(n.id);
    const fanoutName = n.name || positionalNodeName("fanout", i);
    const kindOrdinal = new Map<string, number>();
    const branches = n.branches.map((b): ResolvedBranch => {
      const idx = kindOrdinal.get(b.kind) ?? 0;
      kindOrdinal.set(b.kind, idx + 1);
      const bs = live.get(b.id);
      if (b.kind === "pool") {
        const next: ResolvedPool = {
          id: b.id,
          kind: "pool",
          name: b.name || positionalBranchName(fanoutName, "pool", idx),
          limit: bs?.limit ?? b.limit,
          delayMs: bs?.delayMs ?? b.delayMs,
          tasksPerItem: bs?.tasksPerItem ?? b.tasksPerItem,
          inBody: bs?.inBody ?? [],
          inQueue: bs?.inQueue ?? [],
          lanePaths: bs?.lanePaths ?? [],
        };
        return memoized(poolCache, b.id, next, samePool);
      }
      const next: ResolvedLane = {
        id: b.id,
        kind: "lane",
        name: b.name || positionalBranchName(fanoutName, "lane", idx),
        entranceName: b.entranceName || DEFAULT_ENTRANCE_NAME,
        tasksPerItem: bs?.tasksPerItem ?? b.tasksPerItem,
        delayMs: bs?.delayMs ?? b.delayMs,
        inBody: bs?.inBody ?? [],
        inQueue: bs?.inQueue ?? [],
        blockedLeaving: bs?.blockedLeaving ?? [],
        lanePaths: bs?.lanePaths ?? [],
        nodes: resolveNodes(b.nodes, live),
      };
      return memoized(laneCache, b.id, next, sameLane);
    });
    const next: ResolvedFanOut = {
      id: n.id,
      kind: "fanout",
      name: fanoutName,
      limit: fo?.limit ?? n.limit,
      queueSize: fo?.queueSize ?? n.queueSize,
      inBody: fo?.inBody ?? [],
      inQueue: fo?.inQueue ?? [],
      pendingEntry: fo?.pendingEntry ?? [],
      lanePaths: fo?.lanePaths ?? [],
      branches,
    };
    return memoized(fanOutCache, n.id, next, sameFanOut);
  });
}

/** Merges the editable build-mode Pipeline with a live RunState (null in build mode) into the values every node
 * view renders: live limit/queueSize/delay/occupants when running, the build-mode config otherwise. Pipeline stays
 * the single source of truth for structure (ids, names, node order, branch membership) in both modes. A blank name
 * resolves to the same positional name go-conveyor itself would assign (see ./names). Each returned node/branch is
 * the same object reference as last time when nothing about it changed — see the memoized() caches above. */
export function resolvePipeline(pipeline: Pipeline, runState: RunState | null): ResolvedNode[] {
  return resolveNodes(pipeline.nodes, indexById(runState));
}

/** The implicit start ("Read") stage is not part of Pipeline — every conveyor has exactly one automatically — so
 * it is resolved separately: its own configured delay (or the live one while running) plus who currently occupies
 * it. */
export function resolveStart(pipeline: Pipeline, runState: RunState | null): ResolvedStart {
  const s = indexById(runState).get(START_ID);
  return {
    name: pipeline.startName || DEFAULT_START_NAME,
    delayMs: s?.delayMs ?? pipeline.startDelayMs,
    inBody: s?.inBody ?? [],
    blockedLeaving: s?.blockedLeaving ?? [],
  };
}
