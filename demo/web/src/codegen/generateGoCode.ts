import { DEFAULT_ENTRANCE_NAME, DEFAULT_START_NAME, positionalBranchName, positionalNodeName } from "../pipeline/names";
import type { BranchNode, Pipeline, PipelineNode, PoolBranch } from "../types/pipeline";

const INDENT_UNIT = "    ";

function indent(level: number, text: string): string {
  return `${INDENT_UNIT.repeat(level)}${text}`;
}

/** Converts a user-entered node/branch name (see ../components/shared/EditableTitle) into a valid, idiomatic Go
 * identifier: a camelCase/space/punctuation boundary folds to a single underscore, the whole thing lowercases
 * (snake_case), and a leading digit — illegal in a Go identifier — gets a leading underscore of its own. Returns ""
 * for a name with no identifier-safe characters at all (all emoji/punctuation), which the caller falls back from
 * to the positional name. */
function sanitizeGoIdent(name: string): string {
  const snake = name
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  if (!snake) return "";
  return /^[0-9]/.test(snake) ? `_${snake}` : snake;
}

/** Registers name as one of this snippet's variables, appending a numeric suffix on collision. Necessary now that
 * a custom name feeds into an identifier (see sanitizeGoIdent): two differently-named nodes/branches can still
 * sanitize to the same Go identifier, or collide with another node's positional default (e.g. a fan-out named
 * "Stage 2" landing on the same "stage2" a plain stage elsewhere would get by position). */
function uniqueIdent(used: Set<string>, name: string): string {
  let candidate = name;
  for (let n = 2; used.has(candidate); n++) candidate = `${name}_${n}`;
  used.add(candidate);
  return candidate;
}

function declareUnit(hostVar: string, ctor: "AddStage" | "AddFanOut", node: PipelineNode, varName: string): string {
  let line = `${varName} := ${hostVar}.${ctor}()`;
  if (node.limit !== 1) line += `.SetLimit(${node.limit})`;
  if (node.queueSize !== 0) line += `.SetQueueSize(${node.queueSize})`;
  return line;
}

function declarePool(fanOutVar: string, pool: PoolBranch, varName: string): string {
  let line = `${varName} := ${fanOutVar}.AddPool()`;
  if (pool.limit !== 1) line += `.SetLimit(${pool.limit})`;
  return line;
}

/** Declares nodes onto hostVar (the conveyor "c", or one lane's own variable) recursively: a fan-out's pool
 * branches declare inline, and a lane branch declares itself (AddLane, no SetLimit — its entrance always admits one
 * child at a time) then recurses into its own interior nodes exactly like the top level, one level of naming
 * deeper. Populates varById so emitBody can look up any node/branch's variable by id regardless of nesting depth. */
function declareNodes(
  nodes: PipelineNode[],
  hostVar: string,
  nameFor: (kind: "stage" | "fanout", index: number) => string,
  declLines: string[],
  usedIdents: Set<string>,
  varById: Map<string, string>,
  labelById: Map<string, string>,
): void {
  nodes.forEach((node, i) => {
    // The display name is the one the user sees on the node (see ../pipeline/resolve, which resolves it the same
    // way) — it goes in the work comments, while the sanitized identifier goes in the code. The two differ: "Write
    // to DB" is a fine label but has to become write_to_db to be a Go identifier.
    labelById.set(node.id, node.name || positionalNodeName(node.kind, i));
    const idx = i + 1;
    if (node.kind === "stage") {
      const varName = uniqueIdent(usedIdents, sanitizeGoIdent(node.name) || nameFor("stage", idx));
      varById.set(node.id, varName);
      declLines.push(declareUnit(hostVar, "AddStage", node, varName));
      return;
    }
    const varName = uniqueIdent(usedIdents, sanitizeGoIdent(node.name) || nameFor("fanout", idx));
    varById.set(node.id, varName);
    declLines.push(declareUnit(hostVar, "AddFanOut", node, varName));
    // Branch display names count only same-kind siblings (see positionalBranchName), unlike the identifiers below,
    // which are positional across the whole branch list.
    const kindOrdinal = new Map<BranchNode["kind"], number>();
    node.branches.forEach((br, bi) => {
      const kindIdx = kindOrdinal.get(br.kind) ?? 0;
      kindOrdinal.set(br.kind, kindIdx + 1);
      labelById.set(br.id, br.name || positionalBranchName(labelById.get(node.id)!, br.kind, kindIdx));
      const bidx = bi + 1;
      if (br.kind === "pool") {
        const poolVar = uniqueIdent(usedIdents, sanitizeGoIdent(br.name) || `${varName}Pool${bidx}`);
        varById.set(br.id, poolVar);
        declLines.push(declarePool(varName, br, poolVar));
        return;
      }
      const laneVar = uniqueIdent(usedIdents, sanitizeGoIdent(br.name) || `${varName}Lane${bidx}`);
      varById.set(br.id, laneVar);
      declLines.push(`${laneVar} := ${varName}.AddLane()`);
      declareNodes(
        br.nodes,
        laneVar,
        (kind, n) => (kind === "stage" ? `${laneVar}Stage${n}` : `${laneVar}FanOut${n}`),
        declLines,
        usedIdents,
        varById,
        labelById,
      );
    });
  });
}

/** Emits the MoveTo sequence for nodes at indentLevel, looking up each node/branch's variable in varById. Called
 * once for the top-level Run body, and again — one level of indentation deeper — inside a lane branch's own task
 * closure, for its interior nodes; that recursion is what lets a lane's interior contain a fan-out whose own
 * branches may again be lanes, to any depth. The caller is responsible for the trailing "return nil": once for the
 * Run body as a whole, and once per pool/lane task closure this function opens. */
function emitBody(
  nodes: PipelineNode[],
  varById: Map<string, string>,
  labelById: Map<string, string>,
  indentLevel: number,
  bodyLines: string[],
): void {
  nodes.forEach((node) => {
    const varName = varById.get(node.id) ?? node.id;
    const label = labelById.get(node.id) ?? varName;
    if (node.kind === "stage") {
      bodyLines.push(indent(indentLevel, `if err := ${varName}.MoveTo(ctx); err != nil {`));
      bodyLines.push(indent(indentLevel + 1, "return err"));
      bodyLines.push(indent(indentLevel, "}"));
      bodyLines.push("");
      bodyLines.push(indent(indentLevel, `// <${label} work>`));
      bodyLines.push("");
      return;
    }
    bodyLines.push(indent(indentLevel, `if err := ${varName}.MoveTo(ctx, Tasks{`));
    node.branches.forEach((br) => {
      const brVar = varById.get(br.id) ?? br.id;
      const brLabel = labelById.get(br.id) ?? brVar;
      // A single task per item reads exactly like before this dial existed (NewTask, no index) — only a count
      // above 1 switches the illustration over to NewTasks, matching what topology.ItemProcessor actually calls.
      const multi = br.tasksPerItem > 1;
      if (multi) {
        bodyLines.push(indent(indentLevel + 1, `${brVar}.NewTasks(${br.tasksPerItem}, func(ctx context.Context, i int) error {`));
      } else {
        bodyLines.push(indent(indentLevel + 1, `${brVar}.NewTask(func(ctx context.Context) error {`));
      }
      if (br.kind === "lane") {
        // The entrance always does its own work first — see topology.runNodes — whether or not the lane has interior
        // nodes to move on to, exactly like the start stage's own placeholder at the top of the Run body. Named by the
        // entrance's own display name (see nodes/StartBox, where it is click-to-rename): unqualified is unambiguous
        // because this comment sits inside this one lane's task closure.
        bodyLines.push(indent(indentLevel + 2, `// <${br.entranceName || DEFAULT_ENTRANCE_NAME} work>`));
        bodyLines.push("");
        if (br.nodes.length > 0) {
          emitBody(br.nodes, varById, labelById, indentLevel + 2, bodyLines);
        }
      } else {
        bodyLines.push(indent(indentLevel + 2, `// <${brLabel} task${multi ? " i" : ""} work>`));
      }
      bodyLines.push(indent(indentLevel + 2, "return nil"));
      bodyLines.push(indent(indentLevel + 1, "}),"));
    });
    bodyLines.push(indent(indentLevel, "}); err != nil {"));
    bodyLines.push(indent(indentLevel + 1, "return err"));
    bodyLines.push(indent(indentLevel, "}"));
    bodyLines.push("");
  });
}

/** Generates a minimal, illustrative Go snippet showing how to build a conveyor matching spec: one
 * AddStage/AddFanOut/AddPool/AddLane call per node/branch (mirroring demo/internal/topology/build.go's own
 * construction order, recursing into a lane's own interior nodes the same way build.go does), followed by a Run
 * skeleton with one MoveTo per node and a placeholder comment for each unit's own work.
 *
 * Two namings run in parallel here, and they are deliberately not the same string. A *variable* name is the
 * node/branch's custom name sanitized into a Go identifier and de-duplicated (see sanitizeGoIdent/uniqueIdent), or a
 * positional fallback ("stageN", "fanOutStageN", "foNPoolK"/"foNLaneK", and one level deeper inside a lane,
 * "laneVarStageN"). A *work comment* uses the display name instead — exactly what the diagram shows, resolved the same
 * way ../pipeline/resolve does it, including for the two units that have no variable of their own because go-conveyor
 * creates them implicitly: the conveyor's start stage and a lane's entrance. So a stage the user called "Write to DB"
 * reads `write_to_db := c.AddStage()` but `// <Write to DB work>`.
 *
 * Takes the editable Pipeline rather than the wire Spec because those two implicit names are display-only and so are
 * deliberately absent from the Spec, which mirrors Go's schema.go (see ../types/topology). */
export function generateGoCode(pipeline: Pipeline): string {
  const declLines: string[] = ["c := conveyor.NewConveyor()"];
  const usedIdents = new Set<string>();
  const varById = new Map<string, string>();
  const labelById = new Map<string, string>();

  declareNodes(
    pipeline.nodes,
    "c",
    (kind, n) => (kind === "stage" ? `stage${n}` : `fanOutStage${n}`),
    declLines,
    usedIdents,
    varById,
    labelById,
  );

  const bodyLines: string[] = [indent(1, `// <${pipeline.startName || DEFAULT_START_NAME} work>`), ""];
  emitBody(pipeline.nodes, varById, labelById, 1, bodyLines);
  bodyLines.push(indent(1, "return nil"));

  return [...declLines, "", "c.Run(ctx, func(ctx context.Context) error {", ...bodyLines, "})"].join("\n");
}
