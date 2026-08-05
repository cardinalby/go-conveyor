// A module-level "which body slot(s) each item holds" registry, per node id: leftmost-free-slot on arrival, held
// until the item leaves (never reshuffled just because an earlier slot freed up) — see the spec discussion. An item
// holds more than one slot of the same node only for a fan-out lane running several of that item's tasks at once
// (see Lane.NewTasks / conveyor.UnitOccupants.InBody) — a stage or fan-out body never admits an item more than
// once. Plain module state, not React state: it is read fresh every poll tick from itemPositions/TaskStrip, both
// plain functions/effects, not something a component needs to re-render on its own.
const assignments = new Map<string, Map<number, number[]>>();

/** Returns nodeId's body occupants laid out over `max(limit, inBody.length)` slots, keeping each item in whatever
 * slot(s) it was first assigned and giving a newly-arrived occurrence the smallest free slot index. inBody may
 * list the same item number more than once (concurrent tasks of that item on the same lane — see the module doc);
 * each occurrence gets, and keeps, its own slot. */
export function stableBodySlotIndex(nodeId: string, limit: number, inBody: number[]): (number | null)[] {
  let assignment = assignments.get(nodeId);
  if (!assignment) {
    assignment = new Map();
    assignments.set(nodeId, assignment);
  }

  const countByItem = new Map<number, number>();
  for (const itemNo of inBody) countByItem.set(itemNo, (countByItem.get(itemNo) ?? 0) + 1);

  const used = new Set<number>();
  for (const [itemNo, slots] of Array.from(assignment.entries())) {
    const count = countByItem.get(itemNo) ?? 0;
    if (count === 0) {
      assignment.delete(itemNo);
      continue;
    }
    if (slots.length > count) slots.length = count; // an occurrence left; drop its slot, keep the rest
    slots.forEach((slot) => used.add(slot));
  }

  countByItem.forEach((count, itemNo) => {
    const slots = assignment!.get(itemNo) ?? [];
    if (!assignment!.has(itemNo)) assignment!.set(itemNo, slots);
    while (slots.length < count) {
      let slot = 0;
      while (used.has(slot)) slot++;
      slots.push(slot);
      used.add(slot);
    }
  });

  const size = Math.max(limit, inBody.length);
  const out: (number | null)[] = new Array(size).fill(null);
  assignment.forEach((slots, itemNo) => {
    for (const slot of slots) if (slot < size) out[slot] = itemNo;
  });
  return out;
}

/** Clears all tracked assignments — call once when a new Run starts, so a fresh run's item numbers (which restart
 * from 1) never inherit a stale slot left over from the previous run. */
export function resetBodySlotAssignments(): void {
  assignments.clear();
}
