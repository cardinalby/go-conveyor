// A module-level registry of every currently-rendered slot's DOM element, keyed by "<nodeId>:<body|queue>:<index>".
// SlotStrip registers/unregisters as it mounts, moves and unmounts slots; ItemsOverlay reads from it once per poll
// tick to find out where on screen an item's current slot actually is, so the item can be positioned there.
//
// A plain module-level Map (not React state/context) is deliberate: slot elements themselves never need to trigger
// a render when they change — only ItemsOverlay's own poll-driven re-render needs to read the latest positions, and
// it does so imperatively (getBoundingClientRect) at that moment, not reactively.
const elements = new Map<string, HTMLDivElement>();

export function slotKey(nodeId: string, variant: "body" | "queue", index: number): string {
  return `${nodeId}:${variant}:${index}`;
}

export function registerSlotElement(key: string, el: HTMLDivElement | null): void {
  if (el) elements.set(key, el);
  else elements.delete(key);
}

export function getSlotElement(key: string): HTMLDivElement | undefined {
  return elements.get(key);
}
