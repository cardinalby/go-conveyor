import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { useReactFlow } from "@xyflow/react";
import { stableBodySlotIndex } from "../../state/bodySlotAssignments";
import { getSlotElement } from "../../state/slotRegistry";
import { colorForItem, textColorForItem } from "../../pipeline/colors";
import { keyAssigner, parentKey } from "../../pipeline/itemPositions";
import { isFailModifier } from "./failModifier";
import { ItemBadge } from "./ItemBadge";
import type { LanePathEntry } from "../../types/state";

const SLOT_SIZE = 34; // must match .task-slot's width + gap in index.css
const EXIT_MS = 220;

interface TaskEntry {
  itemNo: number;
  /** The composite label this occurrence renders as — "42.1" for item 42's first (or only) task on this branch,
   * "42.1.2" if this branch's own fan-out is itself nested inside a lane — see ../../pipeline/itemPositions'
   * keyAssigner, recomputed fresh every poll rather than carried over from the previous entry: it must always
   * reflect lanePaths' current say on which of this itemNo's concurrent tasks this occurrence actually is, which
   * can change even when the occurrence's own slot (and so its TaskEntry identity) does not. */
  label: string;
  slot: number;
  phase: "enter" | "active" | "exit";
}

interface Props {
  nodeId: string;
  limit: number;
  inBody: number[];
  /** What tells two of the same item's concurrent tasks on this branch apart (TasksPerItem > 1) — see
   * ../../pipeline/resolve's ResolvedPool/ResolvedLane and itemPositions' keyAssigner, which this renders through
   * to label them "42.1", "42.2" instead of an ambiguous repeated "42". */
  lanePaths: LanePathEntry[];
  /** Composite item key -> the slot it holds at the owning fan-out's entry — the origin a freshly-occupied badge flies
   * in from. See ../nodes/FanOutBox, and pipeline/itemPositions' assignBodySlots on why the slot rather than the
   * item's own rectangle. */
  entrySlotByItemKey: Map<string, string>;
  /** Total slot placeholders to always render (MAX_LIMIT) — see SlotStrip's own `reserve` for why: without it, a
   * lowered limit draining already-in-flight tasks (never evicted, per Pool.SetLimit) grows/shrinks this strip's
   * width purely from simulation activity, no slider involved. */
  reserve: number;
  /** Ctrl/⌘-click on a task's badge asks that one task to fail — see services/api's failTask. Takes nodeId (this
   * branch's id) back rather than being pre-bound to it, so a single stable function reference serves every branch;
   * see ../nodes/types.ts on why that matters for the node memoization. An empty lane (see nodes/LaneBox) passes a
   * closure that fails the whole item instead, since it isn't a conveyor.Pool on the Go side. */
  onFail: (nodeId: string, itemNo: number) => void;
}

/** A freshly-occupied task badge flies in from the slot its item holds at this branch's own fan-out entry, instead of
 * just fading in in place — the same entrance a lane's own child gets, which the overlay gives it by parking it at its
 * parent's position (see ../ItemsOverlay). It is a one-off "FLIP" measured directly against the DOM, imperatively, so
 * it costs nothing on renders after the first: on the occupant changing, before the browser paints, offset the badge
 * back to the origin's on-screen position (converted from screen pixels to this node's own, pre-zoom ones — see
 * useReactFlow's getZoom) and make it opaque, force a layout flush so that state actually gets painted once, then clear
 * the offset on the next frame; the CSS transition on `.task-badge`'s own transform is what turns "clear the offset"
 * into a visible glide back to the badge's real slot. React never learns about either inline value (neither is part of
 * the `style` prop below), so its own re-renders never fight with them.
 *
 * Which entry slot is `entrySlotByItemKey`'s say, looked up under `parentKey(label)` — one path segment shallower than
 * this task's own label — rather than the bare root item number: for a pool/lane nested inside another lane, the
 * fan-out entry it actually flew out of is keyed by that shallower composite key (see ../../pipeline/itemPositions'
 * parentKey), not by the bare item number, which nothing carries any more once the item has crossed into an outer lane
 * at all. The deps stay `[itemNo]` and not the slot key: the flight belongs to an occupant arriving, so a later
 * re-assignment of the same occupant's entry slot must not replay it.
 *
 * Keyed by TaskStrip on the *slot*, not the item (see ../../state/bodySlotAssignments — a slot is reused as soon as
 * it frees up), so this component's own instance persists across occupants: the effect depends on itemNo, not `[]`,
 * specifically so a *different* item taking over the same slot re-triggers the fly-in instead of it firing only
 * once, ever, for whichever item happened to be first through that slot. */
function TaskBadge({
  itemNo,
  label,
  originSlotKey,
  active,
  title,
  onClick,
  style,
}: {
  itemNo: number;
  label: string;
  originSlotKey: string | undefined;
  active: boolean;
  title?: string;
  onClick: (e: React.MouseEvent) => void;
  style: React.CSSProperties;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const { getZoom } = useReactFlow();

  useLayoutEffect(() => {
    const el = ref.current;
    // The entry *slot* the item occupies, not the item's own rectangle: that rectangle is transitioning in from the
    // previous node at exactly this moment (see index.css's .item-rect-overlay), so it would report the position it is
    // leaving. The rectangle stays as the fallback for an origin no slot map covers.
    const originKey = parentKey(label) ?? String(itemNo);
    const origin =
      (originSlotKey && getSlotElement(originSlotKey)) ??
      document.querySelector<HTMLElement>(`[data-item-key="${originKey}"]`);
    if (!el || !origin) return; // nothing to fly from — falls back to the plain fade
    // Clear any offset a previous run of this same effect left behind BEFORE measuring, so the "to" position is
    // always this badge's real slot. The effect has to be idempotent: React StrictMode deliberately runs it twice on
    // mount in development, and measuring while still displaced yields a ~zero delta which then overwrites the real
    // offset with translate(0,0) — under transition:none, so the badge snaps into place and the flight is lost. (In a
    // production build StrictMode doesn't double-invoke, so this failed in dev only, which is exactly the trap.)
    el.style.transition = "none";
    el.style.transform = "";
    const originRect = origin.getBoundingClientRect();
    const selfRect = el.getBoundingClientRect();
    const zoom = getZoom() || 1;
    const dx = (originRect.left + originRect.width / 2 - (selfRect.left + selfRect.width / 2)) / zoom;
    const dy = (originRect.top + originRect.height / 2 - (selfRect.top + selfRect.height / 2)) / zoom;
    el.style.transform = `translate(${dx}px, ${dy}px)`;
    // Opaque for the whole flight, overriding .task-badge's opacity:0 — the movement IS this badge's entrance, so
    // there is nothing left for a fade to announce. Without this the badge spends most of the glide still
    // transitioning up from transparent, and since a pool's strip sits inside the very same fan-out box as the
    // entry slot it flew from, the trip is short enough that it reads as a plain fade-in in place.
    el.style.opacity = "1";
    el.getBoundingClientRect(); // force layout so the "from" position above is actually committed before...
    const raf = requestAnimationFrame(() => {
      el.style.transition = ""; // ...this restores the CSS transition and clears the offset, animating it away.
      el.style.transform = "";
    });
    // Hands the element back in its resting state — the exact inverse of what this effect set — so a re-run
    // (StrictMode, or a new occupant taking over this slot) starts from the stylesheet's own values rather than from
    // a half-finished flight. Opacity is included so a next occupant that finds no origin still gets its plain fade
    // instead of inheriting this one's opaque override.
    return () => {
      cancelAnimationFrame(raf);
      el.style.transition = "";
      el.style.transform = "";
      el.style.opacity = "";
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [itemNo]);

  // Hands opacity back to the stylesheet as soon as React has applied .task-badge-visible, which supplies the same
  // opacity:1 — so this is invisible at the time, and it is what lets the *exit* fade still work: an inline 1 left
  // behind would outrank the class being removed and the badge would vanish instantly instead of fading.
  useEffect(() => {
    if (active && ref.current) ref.current.style.opacity = "";
  }, [active]);

  return (
    <ItemBadge
      ref={ref}
      label={label}
      color={colorForItem(itemNo)}
      textColor={textColorForItem(itemNo)}
      className={`task-badge${active ? " task-badge-visible" : ""}`}
      title={title}
      onClick={onClick}
      style={style}
    />
  );
}

/** A pool's occupants (or an empty lane's, which behaves the same way — see nodes/LaneBox), rendered as colored
 * badges that fly in from the fan-out (see TaskBadge) and fade out in place rather than move — one item can run
 * several of a pool's tasks at once, so unlike a stage's body this can never be part of the single moving-rectangle
 * overlay (see ../../pipeline/itemPositions). Slot assignment is the same leftmost-free-slot, stays-put rule as a
 * stage body (see ../../state/bodySlotAssignments), so a task never jumps sideways just because an earlier one
 * finished — including when an item runs several tasks on this branch at once (Tasks per item > 1 and its own
 * Limit > 1): each concurrent task keeps its own slot and its own badge, matched below by slot rather than by item
 * number, since two badges can legitimately share an item number at once.
 *
 * Ctrl/⌘-clicking a badge injects a failure (see topology.Failures) — the fan-out counterpart to doing the same to
 * an item's rectangle. Only a live badge is clickable: one already fading out has already run. */
export function TaskStrip({ nodeId, limit, inBody, lanePaths, entrySlotByItemKey, reserve, onFail }: Props) {
  const [tasks, setTasks] = useState<TaskEntry[]>([]);
  const inBodyKey = inBody.join(",");
  const lanePathsKey = lanePaths.map((e) => `${e.itemNo}:${e.path.join(".")}`).join(",");

  useEffect(() => {
    const bySlot = stableBodySlotIndex(nodeId, limit, inBody);
    const keyFor = keyAssigner(lanePaths);

    setTasks((prev) => {
      const prevBySlot = new Map(prev.map((t) => [t.slot, t]));
      const next: TaskEntry[] = [];
      const filledSlots = new Set<number>();

      bySlot.forEach((no, slot) => {
        if (no === null) return;
        filledSlots.add(slot);
        // Recomputed every poll, even for an occurrence carried forward below — see TaskEntry's own doc on why a
        // stable slot doesn't imply a stable label.
        const label = keyFor(no);
        const p = prevBySlot.get(slot);
        next.push(
          p && p.itemNo === no
            ? { ...p, label, phase: p.phase === "exit" ? "active" : p.phase }
            : { itemNo: no, label, slot, phase: "enter" },
        );
      });

      for (const t of prev) {
        if (filledSlots.has(t.slot)) continue; // already carried forward (or replaced) above
        if (t.phase === "exit") {
          next.push(t); // already fading out, removal timer already scheduled (against this same reference)
          continue;
        }
        const exiting: TaskEntry = { ...t, phase: "exit" };
        next.push(exiting);
        window.setTimeout(() => {
          setTasks((cur) => cur.filter((x) => x !== exiting));
        }, EXIT_MS);
      }
      return next;
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodeId, limit, inBodyKey, lanePathsKey]);

  // Promote freshly-mounted tasks to "active" a frame after mount, so the opacity transition actually has an
  // initial state to animate away from instead of appearing instantly at full opacity.
  useEffect(() => {
    if (!tasks.some((t) => t.phase === "enter")) return;
    const id = requestAnimationFrame(() => {
      setTasks((prev) => prev.map((t) => (t.phase === "enter" ? { ...t, phase: "active" } : t)));
    });
    return () => cancelAnimationFrame(id);
  }, [tasks]);

  const slotCount = Math.max(limit, inBody.length);
  const totalSlots = Math.max(slotCount, reserve);

  return (
    <div className="task-strip" title="Tasks" style={{ width: totalSlots * SLOT_SIZE }}>
      {Array.from({ length: totalSlots }, (_, i) => (
        <div
          key={`slot-${i}`}
          className={`slot task-slot${i >= slotCount ? " slot-reserved" : ""}`}
          style={{ left: i * SLOT_SIZE }}
        />
      ))}
      {tasks.map((t) => (
        <TaskBadge
          key={t.slot}
          itemNo={t.itemNo}
          label={t.label}
          originSlotKey={entrySlotByItemKey.get(parentKey(t.label) ?? String(t.itemNo))}
          active={t.phase === "active"}
          title={t.phase === "exit" ? undefined : `Item ${t.label}'s task — Ctrl/⌘-click to fail it`}
          onClick={(e) => {
            if (t.phase === "exit" || !isFailModifier(e)) return;
            e.stopPropagation();
            onFail(nodeId, t.itemNo);
          }}
          style={{ left: t.slot * SLOT_SIZE }}
        />
      ))}
    </div>
  );
}
