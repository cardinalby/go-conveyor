/** True for the modifier that turns a click on an item rectangle or a lane task circle into "inject a failure here"
 * rather than a plain click: Ctrl on Windows/Linux, ⌘ on macOS. Both are accepted everywhere rather than being
 * platform-sniffed — ⌘ is the one Mac users have, since Ctrl-click there is the OS's own right-click, and accepting
 * either keeps a shared link's instructions true on whatever machine opens it.
 *
 * Ctrl-click on macOS therefore also raises a contextmenu event, which lands on nothing: App's handleContextMenu
 * ignores context menus outside build mode, and items/tasks only exist in run mode.
 *
 * Used by ../ItemsOverlay (items) and ./TaskStrip (lane tasks); the failure itself is injected Go-side — see
 * topology.Failures. */
export function isFailModifier(evt: React.MouseEvent): boolean {
  return evt.ctrlKey || evt.metaKey;
}
