// Positional naming for a node/branch left unnamed: "stage N" / "fan-out N" for the Nth top-level node (stages and
// fan-outs share one ordinal sequence), "<fan-out> pool K" / "<fan-out> lane K" for a branch — a more readable
// display-only variant of the library's own "<fan-out>.K" (see branch.go's String()); the UI never surfaces
// go-conveyor's own name string, so nothing downstream depends on the two matching. K counts only same-kind
// siblings in that branch list, so a fan-out's first pool and first lane are both "... 1" regardless of which was
// added first. Computed live from the pipeline's current order rather than a creation-time ordinal, since the UI
// freely reorders/removes nodes before a Spec is ever built.

export function positionalNodeName(kind: "stage" | "fanout", index: number): string {
  return kind === "stage" ? `stage ${index + 1}` : `fan-out ${index + 1}`;
}

export function positionalBranchName(fanoutDisplayName: string, kind: "pool" | "lane", indexWithinKind: number): string {
  return `${fanoutDisplayName} ${kind} ${indexWithinKind + 1}`;
}

/** The implicit start stage's display name when the user has not set one. It has no positional variant — a conveyor
 * has exactly one start — but it follows the same "" means default convention as every other name (see
 * resolveStart, and EditableTitle's onCommit("") clearing a custom name). */
export const DEFAULT_START_NAME = "Read";

/** A lane's entrance's display name when the user has not set one. Like DEFAULT_START_NAME it has no positional
 * variant — a lane has exactly one entrance — and follows the same "" means default convention. */
export const DEFAULT_ENTRANCE_NAME = "Entrance";
