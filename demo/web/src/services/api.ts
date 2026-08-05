import { callWasmMethod, ready } from "./wasmHandler";
import type { Spec } from "../types/topology";
import type { RunState } from "../types/state";

interface NodeValueRequest {
  id: string;
  value: number;
}

// Shared body shape of failItem / failTask (see wasmapi.failureRequest): laneId is read by failTask only, since
// failing an item targets the item wherever it happens to be, not a particular node.
interface FailureRequest {
  itemNo: number;
  laneId?: string;
}

export const api = {
  ready,
  run(spec: Spec): RunState {
    return callWasmMethod<RunState>("run", spec);
  },
  cancelCtx(): RunState {
    return callWasmMethod<RunState>("cancelCtx");
  },
  stop(): RunState {
    return callWasmMethod<RunState>("stop");
  },
  state(): RunState {
    return callWasmMethod<RunState>("state");
  },
  setLimit(id: string, value: number): RunState {
    return callWasmMethod<RunState>("setLimit", { id, value } satisfies NodeValueRequest);
  },
  setQueueSize(id: string, value: number): RunState {
    return callWasmMethod<RunState>("setQueueSize", { id, value } satisfies NodeValueRequest);
  },
  setDelay(id: string, value: number): RunState {
    return callWasmMethod<RunState>("setDelay", { id, value } satisfies NodeValueRequest);
  },
  setTasksPerItem(id: string, value: number): RunState {
    return callWasmMethod<RunState>("setTasksPerItem", { id, value } satisfies NodeValueRequest);
  },
  /** Makes this one item's ItemProcessor return an error, so the library's error-shutdown can be watched without
   * every other item failing too — see runtime.Manager.FailItem. */
  failItem(itemNo: number): RunState {
    return callWasmMethod<RunState>("failItem", { itemNo } satisfies FailureRequest);
  },
  /** Same, but the error originates in one of a fan-out's pool tasks rather than in the item's own code — see
   * runtime.Manager.FailTask. A lane child's failure has no equivalent here — it is the item's own work one level
   * down, so failItem already reaches it. */
  failTask(poolId: string, itemNo: number): RunState {
    return callWasmMethod<RunState>("failTask", { laneId: poolId, itemNo } satisfies FailureRequest);
  },
};
