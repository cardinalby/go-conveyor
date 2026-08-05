// Stated explicitly rather than left implicit in the syscall/js import below, because the two are not equivalent to
// the go command: an unconstrained package that cannot compile for the host makes `go build ./...` and `go test ./...`
// *fail* here ("build constraints exclude all Go files in .../syscall/js"), whereas a constrained one is skipped. That
// difference is what lets this module's own tests — internal/runtime, which is pure Go — run under a plain `go test
// ./...` instead of needing every js-only package excluded by hand on the command line.
//go:build js && wasm

// Command demo is a WASM binary that backs the go-conveyor web visualizer: it interprets a topology.Spec the UI
// built, runs it, and reports live state back — see internal/wasmapi for the JS boundary.
package main

import (
	"syscall/js"

	"github.com/cardinalby/go-conveyor/demo/internal/runtime"
	"github.com/cardinalby/go-conveyor/demo/internal/wasmapi"
)

func main() {
	manager := runtime.New()
	js.Global().Set("wasmHandler", wasmapi.NewHandler(manager))
	<-make(chan struct{}) // keep the goroutine (and every worker it spawns) alive; main must not return
}
