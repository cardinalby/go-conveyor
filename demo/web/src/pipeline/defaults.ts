// Default processing-time formulas, chosen to fully saturate a freshly-built pipeline (every stage keeps up with
// the one before it, so a run visibly fills up rather than idling): a plain exclusive stage takes about as long as
// one item's worth of downstream capacity, a shared stage scales with its limit, and a fan-out's lanes divide the
// work among themselves.
export const BASE_DELAY_MS = 1000; // 1s: a normal linear exclusive stage (limit 1)
export const FANOUT_BASE_DELAY_MS = 500; // 500ms total, divided across a fan-out's lanes

export const MIN_DELAY_MS = 50;
export const MAX_DELAY_MS = 5000;

export const MIN_LIMIT = 1;
export const MAX_LIMIT = 6;
export const MIN_QUEUE_SIZE = 0;
export const MAX_QUEUE_SIZE = 6;
export const MIN_LANES = 2;
export const MAX_LANES = 6;
export const MIN_TASKS_PER_ITEM = 1;
export const MAX_TASKS_PER_ITEM = 10;

export function clamp(value: number, min: number, max: number): number {
  if (Number.isNaN(value)) return min;
  return Math.min(max, Math.max(min, Math.round(value)));
}

export function defaultStageDelayMs(limit: number): number {
  return limit > 1 ? BASE_DELAY_MS * limit : BASE_DELAY_MS;
}

export function defaultLaneDelayMs(laneCount: number): number {
  return Math.max(1, Math.round(FANOUT_BASE_DELAY_MS / Math.max(1, laneCount)));
}
