// Low-level bridge to the WASM module: one global function, a JSON {method, body} request in, a JSON
// {error?, body?} response out, synchronously. Mirrors the depo/demo reference's wasmHandler.ts.

declare global {
  interface Window {
    wasmHandler: (req: string) => string;
    isWasmHandlerReady: Promise<void>;
  }
}

interface MethodResponse<T> {
  error?: string;
  body?: T;
}

/** Resolves once the WASM module has finished loading and registered wasmHandler (see index.html). */
export function ready(): Promise<void> {
  return window.isWasmHandlerReady;
}

export function callWasmMethod<T>(method: string, body: unknown = null): T {
  const raw = window.wasmHandler(JSON.stringify({ method, body }));
  const parsed = JSON.parse(raw) as MethodResponse<T>;
  if (parsed.error) {
    throw new Error(parsed.error);
  }
  return parsed.body as T;
}
