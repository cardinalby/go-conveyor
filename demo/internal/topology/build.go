package topology

import (
	"fmt"

	conveyor "github.com/cardinalby/go-conveyor"
)

// Built is a Spec's interpretation: a ready-to-run Conveyor plus the id -> Unit lookup that the live control
// methods (SetLimit, SetQueueSize) and the ItemProcessor built for the same Spec key off. Handles[StartID] is
// always present. Depth records, for every id, how many branch crossings separate it from the root series (0 for a
// top-level node, 1 for a branch or a node inside it, 2 one lane deeper, and so on) — Manager.State's WorkerCount
// needs it to know which occupants live outside the root scope (see its own doc).
type Built struct {
	Conveyor *conveyor.Conveyor
	Handles  map[string]conveyor.Unit
	Depth    map[string]int
}

// nodeHost is what buildNodes needs to add nodes to a series: the conveyor itself (the root series) or a Lane (a
// branch's own interior series) — both expose the identical AddStage/AddFanOut shape, so one recursive function
// builds either.
type nodeHost interface {
	AddStage(opts ...conveyor.AnyUnitOption) conveyor.Stage
	AddFanOut(opts ...conveyor.AnyUnitOption) conveyor.FanOut
}

// Build interprets spec into a fresh Conveyor: one AddStage/AddFanOut/AddPool/AddLane call per node/branch,
// recursing into a lane's own interior nodes exactly like the top level, with OptName/SetLimit/SetQueueSize applied
// from the spec, plus whatever options the caller passes through — see runtime.Manager.Run, which supplies
// OptShutdownContext so its own force-stop button can cancel in-flight items on demand instead of leaving them to
// finish on their own.
//
// It returns an error for a malformed spec (a blank, reserved or duplicate id, an unknown Kind) rather than letting
// the conveyor package panic on a handle mix-up later, in ItemProcessor.
func Build(spec Spec, options ...conveyor.Option) (*Built, error) {
	c := conveyor.NewConveyor(options...)
	built := &Built{
		Conveyor: c,
		Handles:  map[string]conveyor.Unit{StartID: c.StartUnit()},
		Depth:    map[string]int{StartID: 0},
	}

	claim := func(id string) error {
		switch {
		case id == "":
			return fmt.Errorf("node has a blank id")
		case id == StartID:
			return fmt.Errorf("node id %q is reserved", id)
		}
		if _, exists := built.Handles[id]; exists {
			return fmt.Errorf("duplicate node id %q", id)
		}
		return nil
	}

	if err := buildNodes(spec.Nodes, c, 0, built, claim); err != nil {
		return nil, err
	}
	return built, nil
}

// buildNodes builds nodes onto host (the conveyor root, or one lane's interior series) at the given depth, filling
// built.Handles/Depth as it goes and recursing into every lane branch's own Nodes one depth deeper.
func buildNodes(nodes []NodeSpec, host nodeHost, depth int, built *Built, claim func(string) error) error {
	for _, n := range nodes {
		if err := claim(n.ID); err != nil {
			return err
		}
		switch n.Kind {
		case KindStage:
			built.Handles[n.ID] = host.AddStage(conveyor.OptName(n.Name)).SetLimit(n.Limit).SetQueueSize(n.QueueSize)
			built.Depth[n.ID] = depth
		case KindFanOut:
			f := host.AddFanOut(conveyor.OptName(n.Name)).SetLimit(n.Limit).SetQueueSize(n.QueueSize)
			built.Handles[n.ID] = f
			built.Depth[n.ID] = depth
			for _, br := range n.Branches {
				if err := claim(br.ID); err != nil {
					return err
				}
				switch br.Kind {
				case KindPool:
					built.Handles[br.ID] = f.AddPool(conveyor.OptName(br.Name)).SetLimit(br.Limit)
					built.Depth[br.ID] = depth + 1
				case KindLane:
					lane := f.AddLane(conveyor.OptName(br.Name))
					built.Handles[br.ID] = lane
					built.Depth[br.ID] = depth + 1
					if err := buildNodes(br.Nodes, lane, depth+1, built, claim); err != nil {
						return err
					}
				default:
					return fmt.Errorf("branch %q: unknown kind %q", br.ID, br.Kind)
				}
			}
		default:
			return fmt.Errorf("node %q: unknown kind %q", n.ID, n.Kind)
		}
	}
	return nil
}
