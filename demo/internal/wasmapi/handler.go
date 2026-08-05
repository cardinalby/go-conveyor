// Constrained for the same reason as main.go's — see there: it is what keeps a host-side `go test ./...` skipping this
// package instead of failing on it.
//go:build js && wasm

// Package wasmapi wires a runtime.Manager to a single JSON dispatcher exposed to JS, mirroring the depo/demo
// reference: one js.FuncOf global takes a {method, body} envelope and returns a {error, body} envelope, both JSON,
// synchronously — there is no Go-initiated push, the UI polls "state".
package wasmapi

import (
	"encoding/json"
	"fmt"
	"syscall/js"

	"github.com/cardinalby/go-conveyor/demo/internal/runtime"
	"github.com/cardinalby/go-conveyor/demo/internal/topology"
)

type methodRequest struct {
	Method string          `json:"method"`
	Body   json.RawMessage `json:"body"`
}

type methodResponse struct {
	Error string `json:"error,omitempty"`
	Body  any    `json:"body,omitempty"`
}

// nodeValueRequest is the body shape shared by setLimit / setQueueSize / setDelay.
type nodeValueRequest struct {
	ID    string `json:"id"`
	Value int    `json:"value"`
}

// failureRequest is the body shape shared by failItem / failTask. LaneID is read by failTask only — failItem targets
// the item wherever it currently is, not at a particular node.
type failureRequest struct {
	ItemNo int64  `json:"itemNo"`
	LaneID string `json:"laneId,omitempty"`
}

type handler struct {
	manager *runtime.Manager
}

// NewHandler builds the JS-callable dispatcher for manager. Register its result under one global name
// (js.Global().Set) from main; see main.go.
func NewHandler(manager *runtime.Manager) js.Func {
	h := &handler{manager: manager}
	return js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) == 0 {
			return encode(methodResponse{Error: "missing request argument"})
		}
		var req methodRequest
		if err := json.Unmarshal([]byte(args[0].String()), &req); err != nil {
			return encode(methodResponse{Error: "invalid request: " + err.Error()})
		}
		body, err := h.dispatch(req)
		if err != nil {
			return encode(methodResponse{Error: err.Error()})
		}
		return encode(methodResponse{Body: body})
	})
}

// encode marshals resp to JSON. It cannot fail on the response shapes this package produces (no channels, funcs or
// cycles in them), so a failure here would be a programmer error — surfaced plainly rather than dropped silently.
func encode(resp methodResponse) string {
	b, err := json.Marshal(resp)
	if err != nil {
		return `{"error":"internal: failed to encode response: ` + err.Error() + `"}`
	}
	return string(b)
}

func (h *handler) dispatch(req methodRequest) (any, error) {
	switch req.Method {
	case "run":
		var spec topology.Spec
		if err := json.Unmarshal(req.Body, &spec); err != nil {
			return nil, err
		}
		if err := h.manager.Run(spec); err != nil {
			return nil, err
		}
		return h.manager.State(), nil
	case "cancelCtx":
		h.manager.CancelCtx()
		return h.manager.State(), nil
	case "stop":
		h.manager.Stop()
		return h.manager.State(), nil
	case "state":
		return h.manager.State(), nil
	case "setLimit":
		return h.applyNodeValue(req.Body, h.manager.SetLimit)
	case "setQueueSize":
		return h.applyNodeValue(req.Body, h.manager.SetQueueSize)
	case "setDelay":
		return h.applyNodeValue(req.Body, h.manager.SetDelay)
	case "setTasksPerItem":
		return h.applyNodeValue(req.Body, h.manager.SetTasksPerItem)
	case "failItem":
		return h.applyFailure(req.Body, func(r failureRequest) error { return h.manager.FailItem(r.ItemNo) })
	case "failTask":
		return h.applyFailure(req.Body, func(r failureRequest) error { return h.manager.FailTask(r.LaneID, r.ItemNo) })
	default:
		return nil, fmt.Errorf("unknown method %q", req.Method)
	}
}

func (h *handler) applyNodeValue(body json.RawMessage, apply func(id string, value int) error) (any, error) {
	var req nodeValueRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if err := apply(req.ID, req.Value); err != nil {
		return nil, err
	}
	return h.manager.State(), nil
}

func (h *handler) applyFailure(body json.RawMessage, apply func(req failureRequest) error) (any, error) {
	var req failureRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if err := apply(req); err != nil {
		return nil, err
	}
	return h.manager.State(), nil
}
