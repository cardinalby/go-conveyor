package pipeline

import (
	"context"
	"errors"

	"github.com/cardinalby/go-conveyor"
)

// conveyorNode is one built node of the conveyor topology, paired with the shared
// stageRuntime(s) that do its work.
type conveyorNode struct {
	kind   Kind
	stage  conveyor.Stage  // Exclusive / Shared
	fanout conveyor.FanOut // FanOut
	lanes  []conveyor.Pool // FanOut
	rt     *stageRuntime   // Exclusive / Shared
	laneRt []*stageRuntime // FanOut, aligned with lanes
}

// conveyorPipeline drives items through a conveyor.Conveyor: one ItemProcessor
// per item moves it through the nodes, running the shared per-node work inline
// (or as fan-out tasks). The conveyor supplies ordering and per-node concurrency
// limits; this type only wires the Spec onto it.
type conveyorPipeline struct {
	c     *conveyor.Conveyor
	obs   *Observer
	nodes []conveyorNode
}

// NewConveyor builds a Pipeline backed by a conveyor.Conveyor from spec.
func NewConveyor(spec Spec, obs *Observer) Pipeline {
	c := conveyor.NewConveyor()
	p := &conveyorPipeline{c: c, obs: obs}
	for _, s := range spec.Stages {
		switch s.Kind {
		case Exclusive:
			p.nodes = append(p.nodes, conveyorNode{
				kind:  Exclusive,
				stage: c.AddStage(conveyor.OptName(s.Name)),
				rt:    obs.runtime(s.Name, s.Work),
			})
		case Shared:
			p.nodes = append(p.nodes, conveyorNode{
				kind:  Shared,
				stage: c.AddStage(conveyor.OptName(s.Name)).SetLimit(s.Limit),
				rt:    obs.runtime(s.Name, s.Work),
			})
		case FanOut:
			// The traditional pipeline's dispatcher hands each item's tasks to the
			// branch pools and immediately takes the next item, so the only thing
			// bounding it is branch capacity. The fan-out's limit is how many items
			// may be inside the node at once, so matching it to the widest lane lets
			// every lane saturate exactly as it does there — the two sides then admit
			// items at the same rate and the benchmark compares only coordination
			// overhead.
			itemsInside := 1
			for _, b := range s.Branches {
				if b.Limit > itemsInside {
					itemsInside = b.Limit
				}
			}
			f := c.AddFanOut(conveyor.OptName(s.Name)).SetLimit(itemsInside)
			node := conveyorNode{kind: FanOut, fanout: f}
			for _, b := range s.Branches {
				node.lanes = append(node.lanes, f.AddPool(conveyor.OptName(b.Name)).SetLimit(b.Limit))
				node.laneRt = append(node.laneRt, obs.runtime(b.Name, b.Work))
			}
			p.nodes = append(p.nodes, node)
		}
	}
	return p
}

// Run feeds items through the conveyor until item n has finished, then cancels to
// stop item creation and drains the rest. That cancellation is the normal stop
// signal, so a context.Canceled result is reported as success; any real item
// error propagates.
func (p *conveyorPipeline) Run(ctx context.Context, n int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err := p.c.Run(ctx, func(ic context.Context) error {
		no64, _ := conveyor.ItemNoFromContext(ic)
		no := int(no64)
		if no > n {
			return nil // overrun item created before cancellation took effect
		}
		// A fan-out's work is joined at the next node, so its wave rides along until
		// there is a MoveTo to name it in.
		var pending []conveyor.Wave
		for i := range p.nodes {
			nd := &p.nodes[i]
			switch nd.kind {
			case Exclusive, Shared:
				if err := nd.stage.MoveTo(ic, pending...); err != nil {
					return err
				}
				pending = pending[:0]
				nd.rt.process(ic, no)
			case FanOut:
				tasks := make(conveyor.Tasks, 0, len(nd.lanes))
				for li := range nd.lanes {
					rt := nd.laneRt[li]
					tasks.Add(nd.lanes[li].NewTask(func(context.Context) error {
						rt.process(ic, no)
						return nil
					}))
				}
				if err := nd.fanout.MoveTo(ic, tasks, pending...); err != nil {
					return err
				}
				// Detached, so the work overlaps with the nodes that follow — the shape this benchmark measures.
				pending = append(pending[:0], nd.fanout.Detach(ic))
			}
		}
		// A trailing fan-out has no later node to be joined at.
		for _, w := range pending {
			<-w.Finished()
			if err := w.Err(); err != nil {
				return err
			}
		}
		p.obs.itemFinished(no)
		if no >= n {
			cancel()
		}
		return nil
	})
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
