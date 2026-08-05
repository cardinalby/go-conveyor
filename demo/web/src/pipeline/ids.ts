// A module-level counter, not crypto.randomUUID(): ids only need to be unique within one page session, and short
// counter-based ids (n3, n3.2) are far more readable while debugging a JSON payload or a WASM error message.
let counter = 0;

export function newId(prefix: string): string {
  counter += 1;
  return `${prefix}${counter}`;
}
