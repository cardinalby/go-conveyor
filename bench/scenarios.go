package main

import (
	"time"

	"github.com/cardinalby/go-conveyor/bench/internal/bench"
	"github.com/cardinalby/go-conveyor/bench/internal/pipeline"
)

// scenarios returns the three comparison topologies, all five nodes long, each
// parameterized by the base delay D. The stages around any concurrent node run at
// D (fast feeders/drains); the concurrent node itself runs at limit*D so its
// limit slots exactly absorb the feeder's 1/D arrival rate — the ratio Little's
// law requires to keep all limit slots busy. That is what makes the shared
// stage / fan-out branches genuinely saturate rather than sit half-empty.
func scenarios(limit int) []bench.Scenario {
	shared := limit
	branch := limit

	return []bench.Scenario{
		{
			// 1. Simple linear pipeline: five exclusive stages, each D. Both
			// implementations pipeline items so the steady-state interval is ~D;
			// the gap between the curves is pure coordination overhead.
			Name: "linear",
			Build: func(d time.Duration) pipeline.Spec {
				return pipeline.Spec{Name: "linear", Stages: []pipeline.StageSpec{
					{Name: "s1", Kind: pipeline.Exclusive, Work: d},
					{Name: "s2", Kind: pipeline.Exclusive, Work: d},
					{Name: "s3", Kind: pipeline.Exclusive, Work: d},
					{Name: "s4", Kind: pipeline.Exclusive, Work: d},
					{Name: "s5", Kind: pipeline.Exclusive, Work: d},
				}}
			},
		},
		{
			// 2. A shared stage (limit slots) in the middle, fed by fast exclusive
			// stages. Traditional equivalent: a pool of `limit` goroutines plus a
			// reorder buffer to restore order.
			Name: "shared",
			Build: func(d time.Duration) pipeline.Spec {
				return pipeline.Spec{Name: "shared", Stages: []pipeline.StageSpec{
					{Name: "s1", Kind: pipeline.Exclusive, Work: d},
					{Name: "s2", Kind: pipeline.Exclusive, Work: d},
					{Name: "shared", Kind: pipeline.Shared, Limit: shared, Work: time.Duration(shared) * d},
					{Name: "s4", Kind: pipeline.Exclusive, Work: d},
					{Name: "s5", Kind: pipeline.Exclusive, Work: d},
				}}
			},
			Sat: []bench.SatCheck{{Name: "shared", Limit: shared}},
		},
		{
			// 3. A fan-out stage in the middle: two branches, each `limit` slots,
			// run in parallel per item and joined before the item proceeds.
			// Traditional equivalent: two goroutine pools + a per-item join + a
			// reorder buffer.
			Name: "fanout",
			Build: func(d time.Duration) pipeline.Spec {
				return pipeline.Spec{Name: "fanout", Stages: []pipeline.StageSpec{
					{Name: "s1", Kind: pipeline.Exclusive, Work: d},
					{Name: "s2", Kind: pipeline.Exclusive, Work: d},
					{Name: "fanout", Kind: pipeline.FanOut, Branches: []pipeline.BranchSpec{
						{Name: "branchA", Limit: branch, Work: time.Duration(branch) * d},
						{Name: "branchB", Limit: branch, Work: time.Duration(branch) * d},
					}},
					{Name: "s4", Kind: pipeline.Exclusive, Work: d},
					{Name: "s5", Kind: pipeline.Exclusive, Work: d},
				}}
			},
			Sat: []bench.SatCheck{
				{Name: "branchA", Limit: branch},
				{Name: "branchB", Limit: branch},
			},
		},
	}
}
