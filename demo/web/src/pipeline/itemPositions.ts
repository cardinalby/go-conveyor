import { START_ID } from "../types/topology";
import { slotKey } from "../state/slotRegistry";
import { stableBodySlotIndex } from "../state/bodySlotAssignments";
import { queueSlots } from "./slots";
import type { LanePathEntry } from "../types/state";
import type { ResolvedFanOut, ResolvedNode, ResolvedStart } from "./resolve";

/** How an item's rectangle should render, everywhere an item is drawn (a start/stage body, a fan-out's own entry
 * slot, or a lane's own interior chain — see FanOutBox/LaneBox):
 *  - "solid"    — the default: in a queue anywhere, or actually doing this node's own work right now — sleeping
 *    out a stage's delay, or (in a fan-out's entry slot) with at least one branch's task still running (Wave.Started
 *    closed, Finished not).
 *  - "pending"  — (fan-out entry slot only) took the slot but not all of its branches' tasks have started yet
 *    (Wave.Started not closed): either FanOut.MoveTo itself hasn't returned (dispatch not confirmed —
 *    runtime.NodeState.PendingEntry), or it has but at least one target branch hasn't pulled the task yet (still in
 *    that branch's InQueue, or — for a lane — still somewhere in its own interior chain).
 *  - "blocked"  — finished this node's own work and is now trying to advance into the next node: a start/stage
 *    item past its own delay (runtime.NodeState.BlockedLeaving), or a fan-out entry whose branch work has entirely
 *    finished (Wave.Finished closed on every branch).
 */
export type ItemFill = "solid" | "pending" | "blocked";

/** One in-flight item's progress through the pipeline, for the toolbar's live item list (see ../components/
 * ConveyorItems). Always a root item — a lane's own sub-items are "inside" whichever top-level node scheduled
 * them, not stages of their own (see computeItemPositions). */
export interface ItemProgress {
  no: number;
  /** How many stages this item has finished, 0..totalStages. */
  completed: number;
}

export interface ItemPositions {
  /** Composite item key -> the slot key it currently occupies, for the single moving-rectangle overlay (see
   * ../components/shared/ItemBadge). "42" for a root item; "42.1", "42.1.2" for a lane's own sub-items, one path
   * segment per lane crossing (see ../types/state's LanePathEntry) — a child always inherits its parent's item
   * number, so the path is what tells concurrent siblings apart. Only start/stage bodies, stage/fan-out queues, a
   * fan-out's own entry slot, and a lane's own interior chain participate — never a pool, or an empty lane (which
   * behaves like one): their occupant is rendered as a fading task badge in place (see TaskStrip), not a rectangle
   * that moves, since one item can occupy several of a pool's slots at once and a single slot key per key cannot
   * represent that. */
  slotKeys: Map<string, string>;
  /** Composite item key -> fill variant; absent (defaults to "solid") for an item actually doing work or sitting in
   * a queue. */
  fills: Map<string, ItemFill>;
  /** Every item currently in the conveyor, earliest (lowest number) first. */
  progress: ItemProgress[];
  /** The denominator for ItemProgress.completed: the implicit Read stage plus every top-level node, a fan-out
   * counting as one. Read is included because an item genuinely spends its start delay there, and the diagram draws
   * it as a stage like any other — so a bar that ignored it would sit empty through work the canvas visibly shows
   * happening. */
  totalStages: number;
}

/** The build-mode value: nothing is running, so there is nothing to place, classify or count. One frozen instance
 * rather than a fresh object per render, so a build-mode render never hands ItemsOverlay a new prop and set it
 * diffing for nothing. Its maps are never written to — every writer builds its own. */
export const NO_ITEM_POSITIONS: ItemPositions = {
  slotKeys: new Map(),
  fills: new Map(),
  progress: [],
  totalStages: 0,
};

/** Groups lanePaths by item number, in the order LanePaths.Snapshot returned them (roughly admission order — see
 * its own Go-side doc), for keyAssigner to consume one at a time per occurrence. */
function pathsByItem(lanePaths: LanePathEntry[]): Map<number, number[][]> {
  const m = new Map<number, number[][]>();
  for (const e of lanePaths) {
    const arr = m.get(e.itemNo);
    if (arr) arr.push(e.path);
    else m.set(e.itemNo, [e.path]);
  }
  return m;
}

/** Builds a "raw item number -> composite key" assigner for one node, consumed once per occurrence (body slots
 * first, then queue — see assignNode) so concurrent same-item occupants at that node each get their own distinct
 * key instead of colliding on a bare item number. Trivially the identity (stringified) for a node outside any
 * branch, which never has lanePaths — see runtime.Manager.State's Depth-gated population. Also reused directly by
 * TaskStrip for a pool's (or an empty lane's) own tasks — its callers zip it against inBody in the exact same
 * slot order as assignBody below, so a pool task's "42.1"/"42.2" comes out consistent with a lane child's. */
export function keyAssigner(lanePaths: LanePathEntry[]): (no: number) => string {
  if (lanePaths.length === 0) return (no) => String(no);
  const paths = pathsByItem(lanePaths);
  const seen = new Map<number, number>();
  return (no) => {
    const idx = seen.get(no) ?? 0;
    seen.set(no, idx + 1);
    const path = paths.get(no)?.[idx];
    return path ? [no, ...path].join(".") : String(no);
  };
}

/** The composite key one level shallower — "42.1" for "42.1.2", "42" for "42.1", null for a bare root key. Shared
 * by ItemsOverlay (a lane sub-item's own fly-in origin) and TaskStrip (a pool/lane task's fly-in origin — see its
 * own TaskBadge): both look up the DOM element carrying this shallower key via `data-item-key` to fly in from
 * wherever the item/task currently sits one branch crossing back. */
export function parentKey(key: string): string | null {
  const i = key.lastIndexOf(".");
  return i === -1 ? null : key.slice(0, i);
}

/** Consumes precomputed composite keys one occurrence at a time per raw item number — see assignNode's own
 * bodyKeyByItem. A second, independent counter from the one inside keyAssigner: markBlocked/classifyFanOutEntries
 * must map the *same* occurrences keyAssigner already resolved during body-slot assignment back to their keys,
 * not resolve them again — keyAssigner's own counter has already moved past them by this point, so calling it a
 * second time for the same occurrences would silently hand out the next, unrelated path (or none at all). */
function keyConsumer(bodyKeyByItem: Map<number, string[]>): (no: number) => string | undefined {
  const seen = new Map<number, number>();
  return (no) => {
    const idx = seen.get(no) ?? 0;
    seen.set(no, idx + 1);
    return bodyKeyByItem.get(no)?.[idx];
  };
}

/** Marks blockedLeaving items (a start/stage's own "finished, trying to advance" signal) as "blocked" — everyone
 * else in that body defaults to "solid". */
function markBlocked(blockedLeaving: number[], fills: Map<string, ItemFill>, bodyKeyByItem: Map<number, string[]>) {
  const consume = keyConsumer(bodyKeyByItem);
  for (const no of blockedLeaving) {
    const key = consume(no);
    if (key) fills.set(key, "blocked");
  }
}

/** Collects every item number appearing in nodes' own inBody/inQueue, or that of anything nested inside them (a
 * fan-out's branches, recursively into a lane's own interior chain) — see classifyFanOutEntries on why a lane
 * branch needs its whole subtree checked, not just its own entrance. */
function collectItemNos(nodes: ResolvedNode[], into: Set<number>): void {
  for (const n of nodes) {
    for (const no of n.inBody) into.add(no);
    for (const no of n.inQueue) into.add(no);
    if (n.kind === "fanout") {
      for (const b of n.branches) {
        for (const no of b.inBody) into.add(no);
        for (const no of b.inQueue) into.add(no);
        if (b.kind === "lane") collectItemNos(b.nodes, into);
      }
    }
  }
}

/** Classifies a fan-out's entry-slot occupants into "pending" / "solid" / "blocked" per the Wave.Started/Finished
 * derivation above, using only data already polled (a branch's InQueue/InBody) plus the existing PendingEntry
 * (dispatch-confirmed) marker — no extra Go-side state needed, unlike a plain stage's "blocked" (see
 * runtime.BlockedEntries): a fan-out's own work finishing is already visible per-branch. A lane's entrance releases
 * on its child's first move (see conveyor.Lane), so once it has interior nodes its own InBody alone goes stale the
 * moment a child travels past it — collectItemNos checks its whole subtree instead. */
function classifyFanOutEntries(fanout: ResolvedFanOut, fills: Map<string, ItemFill>, bodyKeyByItem: Map<number, string[]>) {
  const pending = new Set(fanout.pendingEntry);
  const queuedSomewhere = new Set<number>();
  const runningSomewhere = new Set<number>();
  for (const branch of fanout.branches) {
    for (const no of branch.inQueue) queuedSomewhere.add(no);
    for (const no of branch.inBody) runningSomewhere.add(no);
    if (branch.kind === "lane") collectItemNos(branch.nodes, runningSomewhere);
  }

  const consume = keyConsumer(bodyKeyByItem);
  for (const no of fanout.inBody) {
    const key = consume(no);
    if (!key) continue;
    if (pending.has(no) || queuedSomewhere.has(no)) {
      fills.set(key, "pending"); // dispatch not confirmed yet, or a target branch hasn't pulled it yet
    } else if (!runningSomewhere.has(no)) {
      fills.set(key, "blocked"); // every branch's task finished — trying to advance into the next node
    }
    // else: at least one branch still running — falls back to the "solid" default.
  }
}

/** Assigns body slots for one node/entrance, using the given (shared — see assignNode) keyAssigner, and returns
 * the same "which composite key(s) landed on each raw item number" mapping the assignment just consumed, in slot
 * order — for markBlocked/classifyFanOutEntries to reuse afterwards (see keyConsumer) instead of calling keyFor a
 * second time for the same occurrences, which would silently hand out the *next*, unrelated path (or none). */
function assignBody(
  nodeId: string,
  limit: number,
  inBody: number[],
  keyFor: (no: number) => string,
  slotKeys: Map<string, string>,
): Map<number, string[]> {
  const { slotByKey, keysByItem } = assignBodySlots(nodeId, limit, inBody, keyFor);
  slotByKey.forEach((slot, key) => slotKeys.set(key, slot));
  return keysByItem;
}

/** One node's body-slot assignment, in both the directions its callers need it. */
export interface BodySlotAssignment {
  /** Composite item key -> the body slot key that occurrence currently holds. */
  slotByKey: Map<string, string>;
  /** Raw item number -> its composite key(s), in slot order — see keyConsumer. */
  keysByItem: Map<number, string[]>;
}

/** Lays one node's body occupants over its slots (see ../state/bodySlotAssignments for the stays-put rule) and
 * reports which composite key landed in which slot.
 *
 * Exported because a fan-out's own branches need this for the fan-out itself, not just computeItemPositions' whole-tree
 * walk: a pool task flies in from the *slot* its item occupies at the fan-out entry, and it has to be the slot rather
 * than the item's own rectangle because that rectangle is mid-glide at exactly that moment — it is transitioning in
 * from the previous node (see index.css's .item-rect-overlay), so measuring it yields the position it is leaving
 * rather than the one it is arriving at. Slots never move, so their box is always the honest answer. See
 * ../components/nodes/FanOutBox, which hands this down, and shared/TaskStrip, which flies from it.
 *
 * Safe to call again for a node computeItemPositions has already walked this tick: stableBodySlotIndex reconciles
 * against `inBody` rather than advancing any counter, and `keyFor` must be a fresh keyAssigner per call for the same
 * reason assignNode shares one across a node's body and queue. */
export function assignBodySlots(
  nodeId: string,
  limit: number,
  inBody: number[],
  keyFor: (no: number) => string,
): BodySlotAssignment {
  const slotByKey = new Map<string, string>();
  const keysByItem = new Map<number, string[]>();
  stableBodySlotIndex(nodeId, limit, inBody).forEach((no, i) => {
    if (no === null) return;
    const key = keyFor(no);
    slotByKey.set(key, slotKey(nodeId, "body", i));
    const arr = keysByItem.get(no);
    if (arr) arr.push(key);
    else keysByItem.set(no, [key]);
  });
  return { slotByKey, keysByItem };
}

/** Assigns slotKeys/fills for one node and, recursively, everything inside it (a lane branch's own interior chain,
 * to any depth) — but never touches `completed`, which only computeItemPositions' own top-level walk tracks: a
 * lane's interior is "inside" whichever top-level node scheduled it, not a stage of its own. */
function assignNode(node: ResolvedNode, slotKeys: Map<string, string>, fills: Map<string, ItemFill>): void {
  // One keyFor shared across body *and* queue: a lane child queued here and another, concurrent child of the same
  // item already in body both draw from the same path pool, and must not collide on the same composite key by
  // each independently starting their own count from zero.
  const keyFor = keyAssigner(node.lanePaths);
  const bodyKeyByItem = assignBody(node.id, node.limit, node.inBody, keyFor, slotKeys);
  queueSlots(node.queueSize, node.inQueue).forEach((no, i) => {
    if (no !== null) slotKeys.set(keyFor(no), slotKey(node.id, "queue", i));
  });

  if (node.kind === "stage") {
    markBlocked(node.blockedLeaving, fills, bodyKeyByItem);
    return;
  }
  classifyFanOutEntries(node, fills, bodyKeyByItem);
  for (const branch of node.branches) {
    if (branch.kind !== "lane" || branch.nodes.length === 0) continue;
    // The lane's own entrance participates in the sliding system too, exactly like the conveyor's own implicit
    // start (see nodes/StartBox) — but only once it has interior nodes to be an entrance *to*; an empty lane
    // behaves like a pool and stays on TaskStrip's fading-badge rendering instead (see nodes/LaneBox). No queue of
    // its own (see conveyor.Lane), so its keyFor only ever needs to cover this one, body-only assignment.
    const entranceKeyByItem = assignBody(branch.id, 1, branch.inBody, keyAssigner(branch.lanePaths), slotKeys);
    markBlocked(branch.blockedLeaving, fills, entranceKeyByItem);
    for (const n of branch.nodes) assignNode(n, slotKeys, fills);
  }
}

/** Computes where every currently in-flight item's rectangle belongs, using the exact same layout (stableBodySlotIndex
 * for a body, queueSlots for a waiting room) the node views render — so the overlay always targets the slot the
 * item is actually drawn in — recursing into a lane branch's own interior nodes the same way (see assignNode). It
 * also counts how far through the pipeline each item has got, in the same top-level walk: that count is read
 * straight off the fill classification below rather than re-deriving "is this item done here?", which for a fan-out
 * is the subtle part (see classifyFanOutEntries) and is not worth having two answers to. Only a root item's own
 * progress is counted — a lane's sub-items have no stage count of their own. */
export function computeItemPositions(resolved: ResolvedNode[], start: ResolvedStart): ItemPositions {
  const slotKeys = new Map<string, string>();
  const fills = new Map<string, ItemFill>();
  const completed = new Map<number, number>();

  const startKeyByItem = assignBody(START_ID, 1, start.inBody, keyAssigner([]), slotKeys);
  markBlocked(start.blockedLeaving, fills, startKeyByItem);
  // Read is the first of totalStages, so an item still being read has finished nothing, and one whose start delay is
  // done — the "blocked" fill, now waiting to enter the first node — has finished exactly that one.
  for (const no of start.inBody) completed.set(no, fills.get(String(no)) === "blocked" ? 1 : 0);

  resolved.forEach((n, i) => {
    assignNode(n, slotKeys, fills);
    // Every count below is one more than the node's index, because Read sits in front of node 0.
    // Waiting in front of node i means Read and every node before i are done, and none of i is.
    for (const no of n.inQueue) completed.set(no, i + 1);
    // Inside node i: i + 1 done while it is working here, i + 2 once it is only waiting for room ahead — which is
    // exactly what the "blocked" fill means, for a stage and a fan-out entry alike. Safe to read fills for this
    // item now: an item occupies one body at a time (takeUnit swaps atomically under the run mutex) and
    // BlockedLeaving is reported already narrowed to that body, so any "blocked" mark here is this node's own.
    for (const no of n.inBody) completed.set(no, fills.get(String(no)) === "blocked" ? i + 2 : i + 1);
  });

  const progress = [...completed.entries()]
    .map(([no, done]): ItemProgress => ({ no, completed: done }))
    .sort((a, b) => a.no - b.no);
  return { slotKeys, fills, progress, totalStages: resolved.length + 1 }; // + 1: the implicit Read stage
}
