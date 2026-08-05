import { useEffect } from "react";

/** The class mirrored onto <body> while the fail modifier is held. index.css keys the hover affordance for items and
 * lane tasks off it — see its .fail-modifier rules. */
const HELD_CLASS = "fail-modifier";

/** Reflects "is the fail modifier (Ctrl/⌘) held right now?" onto a class on <body>, so CSS can highlight the item and
 * task circles that modifier turns into click targets (see ../components/shared/failModifier) before the click lands.
 *
 * Toggles the class imperatively rather than holding it in React state: this flips on every Ctrl/⌘ press and release,
 * nothing but styling depends on it, and routing it through a re-render would re-render the whole canvas — 50 times a
 * second's worth of work already goes through there while running.
 *
 * Three listeners rather than just keydown/keyup, because key events alone do not describe the modifier's state
 * faithfully: a modifier released while this window is not focused (⌘-Tab away and let go) never produces a keyup
 * here, so the class would stick, and one pressed *before* the window regained focus never produces a keydown, so it
 * would be missing. blur handles the first; mousemove — which carries the live modifier state on every event, and
 * which anyone about to hover an item is generating anyway — handles the second. */
export function useFailModifierClass(): void {
  useEffect(() => {
    // Mirrors the class's current state so the common case (mousemove with nothing changed) touches no DOM at all.
    let held = false;
    const sync = (next: boolean) => {
      if (next === held) return;
      held = next;
      document.body.classList.toggle(HELD_CLASS, next);
    };
    const fromEvent = (e: KeyboardEvent | MouseEvent) => sync(e.ctrlKey || e.metaKey);
    const clear = () => sync(false);

    window.addEventListener("keydown", fromEvent);
    window.addEventListener("keyup", fromEvent);
    window.addEventListener("mousemove", fromEvent, { passive: true });
    window.addEventListener("blur", clear);
    return () => {
      window.removeEventListener("keydown", fromEvent);
      window.removeEventListener("keyup", fromEvent);
      window.removeEventListener("mousemove", fromEvent);
      window.removeEventListener("blur", clear);
      clear(); // never leave the class behind on unmount
    };
  }, []);
}
