// Package topology defines the JSON-serializable description of a conveyor pipeline that the web UI builds, and
// interprets one into a running conveyor.Conveyor plus a matching ItemProcessor. One fixed WASM binary understands
// this schema; building a different pipeline never requires a Go rebuild.
//
// The shape mirrors the library's own recursive model directly: a Spec is the conveyor's top-level series (a linear
// sequence of stages and fan-outs), a fan-out's branches are a Pool or a Lane (see conveyor.Pool / conveyor.Lane),
// and a Lane's interior is a series again — its own ordered list of NodeSpec, which may itself contain fan-outs
// whose branches may themselves be lanes, to any depth. NodeSpec and BranchSpec are mutually recursive for exactly
// that reason.
package topology

// StartID is the id the frontend must use for the conveyor's implicit start ("Read") stage in requests that name a
// node (SetLimitRequest and friends target every other unit by their own id; the start stage's limit is fixed and
// has no such request). It is reserved: a Spec's own node and branch ids must never equal it.
const StartID = "__start__"

// NodeKind classifies one node of a Spec or a Lane's interior series.
type NodeKind string

const (
	KindStage  NodeKind = "stage"
	KindFanOut NodeKind = "fanout"
)

// BranchKind classifies one branch of a fan-out: a Pool (leaf, conveyor.Pool) or a Lane (a pipeline of its own,
// conveyor.Lane).
type BranchKind string

const (
	KindPool BranchKind = "pool"
	KindLane BranchKind = "lane"
)

// BranchSpec is one branch of a FanOutSpec, built as a conveyor.Pool or a conveyor.Lane depending on Kind.
//
// Limit applies to a pool only (conveyor.Pool.SetLimit) — a lane's entrance has no SetLimit, fixed at one child at
// a time (see conveyor.Lane). DelayMs is the branch's own entrance work, always applied: for a pool it is the
// callback a scheduled task runs; for a lane it is the entrance's own simulated delay, exactly like the conveyor's
// own implicit start — a child sleeps it out before moving on, whether or not the lane has any interior nodes to
// move on to (an empty one is the "degrades to a limit-1 pool" case conveyor.Lane documents, so its whole leaf work
// is that one sleep). TasksPerItem is shared (see conveyor.Branch.NewTasks): how many tasks/children one item
// schedules on this branch. Nodes is the lane's own interior series (stage/fan-out, recursively) — always empty for
// a pool, since a pool can never have nodes.
type BranchSpec struct {
	ID           string     `json:"id"`
	Kind         BranchKind `json:"kind"`
	Name         string     `json:"name"`
	Limit        int        `json:"limit"`
	DelayMs      int        `json:"delayMs"`
	TasksPerItem int        `json:"tasksPerItem"`
	Nodes        []NodeSpec `json:"nodes,omitempty"` // lane only
}

// NodeSpec is one node of a Spec, or of a Lane's own interior series: a stage or a fan-out. Fields that do not apply
// to a node's Kind are left zero and ignored (Branches for a stage, DelayMs for a fan-out — a fan-out runs no code
// of its own, only its branches do).
type NodeSpec struct {
	ID        string       `json:"id"`
	Kind      NodeKind     `json:"kind"`
	Name      string       `json:"name"`
	Limit     int          `json:"limit"`
	QueueSize int          `json:"queueSize"`
	DelayMs   int          `json:"delayMs"`            // stage only
	Branches  []BranchSpec `json:"branches,omitempty"` // fanout only, at least 2 branches
}

// Spec is a full pipeline topology built by the UI's build mode: an ordered list of nodes, left to right. The
// implicit start stage is not part of Nodes — every conveyor has exactly one, automatically — but StartDelayMs
// configures the simulated time an item spends there before its first move, same as any other node's delay.
type Spec struct {
	Nodes        []NodeSpec `json:"nodes"`
	StartDelayMs int        `json:"startDelayMs"`
}
