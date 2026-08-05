// Persists everything a shared/reloaded URL should reproduce — the editable topology/config and which panels are
// open — in location.hash as one compact JSON blob. Deliberately excludes a run's live item occupancy (see
// App.tsx): that's ephemeral simulation state, not configuration, and polls too fast to belong in the URL anyway.

import {
  clamp,
  MAX_DELAY_MS,
  MAX_LIMIT,
  MAX_QUEUE_SIZE,
  MAX_TASKS_PER_ITEM,
  MIN_DELAY_MS,
  MIN_LIMIT,
  MIN_QUEUE_SIZE,
  MIN_TASKS_PER_ITEM,
} from "../pipeline/defaults";
import { newId } from "../pipeline/ids";
import type { BranchNode, FanOutNode, LaneBranch, Pipeline, PipelineNode, PoolBranch, StageNode } from "../types/pipeline";

// Bumped from 1: the pool/lane split changed the wire shape (branches instead of lanes, recursive nodes inside a
// lane). decodeUrlState already treats a version mismatch as "no hash" (see its own doc) and falls back to a fresh
// default pipeline, so a v1 hash from before this change is simply dropped rather than migrated.
const HASH_VERSION = 2;

type UrlMode = "build" | "run";

interface UrlPoolBranch {
  kind: "pool";
  name?: string; // absent means no custom name — see the module doc on nodeToUrl
  limit: number;
  delayMs: number;
  tasksPerItem: number;
}

interface UrlLaneBranch {
  kind: "lane";
  name?: string;
  /** Present only when the user renamed this lane's entrance — same convention as `name`. */
  entranceName?: string;
  tasksPerItem: number;
  delayMs: number;
  nodes: UrlNode[];
}

type UrlBranch = UrlPoolBranch | UrlLaneBranch;

interface UrlStageNode {
  kind: "stage";
  name?: string;
  limit: number;
  queueSize: number;
  delayMs: number;
}

interface UrlFanOutNode {
  kind: "fanout";
  name?: string;
  limit: number;
  queueSize: number;
  branches: UrlBranch[];
}

type UrlNode = UrlStageNode | UrlFanOutNode;

/** The wire shape written to/read from the hash. Ids are deliberately absent — only meaningful within one page
 * session (see ../pipeline/ids), so persisting them would only bloat the URL with values that would just be
 * regenerated on load anyway. name is present only for a node/branch the user actually renamed (see nodeToUrl) — a
 * still-positional one (the vast majority) costs nothing extra in the hash. */
interface UrlPayload {
  v: number;
  startDelayMs: number;
  /** Present only when the user renamed the implicit start stage — same convention as a node's own name (see
   * nodeToUrl). */
  startName?: string;
  nodes: UrlNode[];
  mode: UrlMode;
  showCode: boolean;
  showLegend: boolean;
}

export interface UrlAppState {
  pipeline: Pipeline;
  mode: UrlMode;
  showCode: boolean;
  showLegend: boolean;
}

function branchToUrl(b: BranchNode): UrlBranch {
  const name = b.name ? { name: b.name } : {};
  if (b.kind === "pool") {
    return { kind: "pool", ...name, limit: b.limit, delayMs: b.delayMs, tasksPerItem: b.tasksPerItem };
  }
  const entranceName = b.entranceName ? { entranceName: b.entranceName } : {};
  return { kind: "lane", ...name, ...entranceName, tasksPerItem: b.tasksPerItem, delayMs: b.delayMs, nodes: b.nodes.map(nodeToUrl) };
}

/** name is included only when it's an actual custom name (see EditableTitle/App.handleRenameNode, which never
 * write out an empty one) — "" is the default, indistinguishable from having never set the field at all, so
 * omitting it here keeps an unrenamed pipeline's URL exactly as short as before this field existed. */
function nodeToUrl(n: PipelineNode): UrlNode {
  const name = n.name ? { name: n.name } : {};
  if (n.kind === "stage") {
    return { kind: "stage", ...name, limit: n.limit, queueSize: n.queueSize, delayMs: n.delayMs };
  }
  return { kind: "fanout", ...name, limit: n.limit, queueSize: n.queueSize, branches: n.branches.map(branchToUrl) };
}

// A loosely-typed read of untrusted, already-parsed JSON: Partial<UrlNode> looks like the right parameter type
// here, but Partial of a *discriminated union* drops the discriminant's narrowing power (kind becomes optional on
// both variants, so `n.kind === "fanout"` can no longer prove n.branches exists) — Record<string, unknown> sidesteps
// that entirely, at the cost of needing Number()/Array.isArray() to do the actual validating.
type Loose = Record<string, unknown>;

// Every numeric field is clamped on the way back in — the hash is user-editable (a hand-crafted link, a typo from
// copy-pasting), and clamp() already folds NaN (missing/non-numeric) down to `min`, so a garbled or partial node
// still comes back as a valid, in-range one rather than breaking the sliders that read it.
function urlToBranch(raw: unknown): BranchNode {
  const b = (raw ?? {}) as Loose;
  const name = typeof b.name === "string" ? b.name : "";
  if (b.kind === "lane") {
    const rawNodes = Array.isArray(b.nodes) ? b.nodes : [];
    const lane: LaneBranch = {
      id: newId("l"),
      kind: "lane",
      name,
      entranceName: typeof b.entranceName === "string" ? b.entranceName : "",
      tasksPerItem: clamp(Number(b.tasksPerItem), MIN_TASKS_PER_ITEM, MAX_TASKS_PER_ITEM),
      delayMs: clamp(Number(b.delayMs), MIN_DELAY_MS, MAX_DELAY_MS),
      nodes: rawNodes.map(urlToNode),
    };
    return lane;
  }
  const pool: PoolBranch = {
    id: newId("p"),
    kind: "pool",
    name,
    limit: clamp(Number(b.limit), MIN_LIMIT, MAX_LIMIT),
    delayMs: clamp(Number(b.delayMs), MIN_DELAY_MS, MAX_DELAY_MS),
    // clamp() folds a missing field (NaN) down to MIN_TASKS_PER_ITEM, so a link saved before this field existed
    // still comes back as one task per item — the behavior it actually had.
    tasksPerItem: clamp(Number(b.tasksPerItem), MIN_TASKS_PER_ITEM, MAX_TASKS_PER_ITEM),
  };
  return pool;
}

function urlToNode(raw: unknown): PipelineNode {
  const n = (raw ?? {}) as Loose;
  if (n.kind === "fanout") {
    const rawBranches = Array.isArray(n.branches) ? n.branches : [];
    const fanOut: FanOutNode = {
      id: newId("f"),
      kind: "fanout",
      name: typeof n.name === "string" ? n.name : "",
      limit: clamp(Number(n.limit), MIN_LIMIT, MAX_LIMIT),
      queueSize: clamp(Number(n.queueSize), MIN_QUEUE_SIZE, MAX_QUEUE_SIZE),
      branches: rawBranches.map(urlToBranch),
    };
    return fanOut;
  }
  const stage: StageNode = {
    id: newId("n"),
    kind: "stage",
    name: typeof n.name === "string" ? n.name : "",
    limit: clamp(Number(n.limit), MIN_LIMIT, MAX_LIMIT),
    queueSize: clamp(Number(n.queueSize), MIN_QUEUE_SIZE, MAX_QUEUE_SIZE),
    delayMs: clamp(Number(n.delayMs), MIN_DELAY_MS, MAX_DELAY_MS),
  };
  return stage;
}

export function encodeUrlState(state: UrlAppState): string {
  const payload: UrlPayload = {
    v: HASH_VERSION,
    startDelayMs: state.pipeline.startDelayMs,
    ...(state.pipeline.startName ? { startName: state.pipeline.startName } : {}),
    nodes: state.pipeline.nodes.map(nodeToUrl),
    mode: state.mode,
    showCode: state.showCode,
    showLegend: state.showLegend,
  };
  return encodeURIComponent(JSON.stringify(payload));
}

/** Parses a location.hash value (with or without its leading "#"). Returns null for an empty, malformed, or
 * version-mismatched hash — the caller falls back to a fresh default pipeline in that case, the same as if there
 * were no hash at all. */
export function decodeUrlState(hash: string): UrlAppState | null {
  const raw = hash.startsWith("#") ? hash.slice(1) : hash;
  if (!raw) return null;
  try {
    const payload = JSON.parse(decodeURIComponent(raw)) as Partial<UrlPayload>;
    if (payload.v !== HASH_VERSION || !Array.isArray(payload.nodes)) return null;
    return {
      pipeline: {
        nodes: payload.nodes.map(urlToNode),
        startDelayMs: clamp(Number(payload.startDelayMs), MIN_DELAY_MS, MAX_DELAY_MS),
        startName: typeof payload.startName === "string" ? payload.startName : "",
      },
      mode: payload.mode === "run" ? "run" : "build",
      showCode: payload.showCode === true,
      showLegend: payload.showLegend === true,
    };
  } catch {
    return null;
  }
}

export function readUrlState(): UrlAppState | null {
  return decodeUrlState(window.location.hash);
}

/** Replaces (never pushes) the current history entry's hash — every topology/config edit calls this, including
 * live slider drags, so pushState would flood browser history with one entry per tick. */
export function writeUrlState(state: UrlAppState): void {
  const encoded = encodeUrlState(state);
  const url = `${window.location.pathname}${window.location.search}#${encoded}`;
  window.history.replaceState(null, "", url);
}
