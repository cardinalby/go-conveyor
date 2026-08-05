/** What a context-menu click targets. Ids are looked up against the pipeline tree on demand (see
 * ../pipeline/mutations' tree-walkers) rather than carrying a parent reference — a stage/fan-out/pool/lane can live
 * anywhere in the tree (top-level, or nested inside some lane's interior, to any depth) and every mutation resolves
 * "which list contains this id" itself. */
export type MenuTarget =
  | { kind: "start" }
  | { kind: "stage"; id: string }
  | { kind: "fanout"; id: string }
  | { kind: "pool"; id: string }
  | { kind: "lane"; id: string };
