/** How many body slots a node needs: `limit`, or more if occupancy transiently exceeds it — lowering a limit never
 * evicts items already inside, and a real occupant should never be clipped from view because of that. Actual
 * occupant-to-slot assignment is stateful (see ../state/bodySlotAssignments), not a pure function of this count. */
export function bodySlotCount(limit: number, inBody: number[]): number {
  return Math.max(limit, inBody.length);
}

/** Lays out a node's waiting-room occupants as a strip of `queueSize` slots, FIFO head nearest the node it fronts
 * — i.e. the rightmost slot, since the queue box sits to the node's left and items move left to right. */
export function queueSlots(queueSize: number, inQueue: number[]): (number | null)[] {
  const size = Math.max(queueSize, inQueue.length);
  const slots: (number | null)[] = new Array(size).fill(null);
  for (let i = 0; i < inQueue.length; i++) {
    slots[size - 1 - i] = inQueue[i];
  }
  return slots;
}

export function queueSlotCount(queueSize: number, inQueue: number[]): number {
  return Math.max(queueSize, inQueue.length);
}
