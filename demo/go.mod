// Separate module, mirroring bench/: the demo is a WASM binary with its own dependency surface (none beyond the
// standard library today), built independently of the main package. It reaches the conveyor package via the
// replace directive below.
module github.com/cardinalby/go-conveyor/demo

go 1.24

require github.com/cardinalby/go-conveyor v0.0.0

replace github.com/cardinalby/go-conveyor => ../
