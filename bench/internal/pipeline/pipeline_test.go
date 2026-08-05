package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPipelinesEquivalent runs every topology through both implementations and
// asserts they behave identically at the contract level: all items finish, in
// input order, and the concurrent nodes reach their limit. This is what lets the
// benchmark treat the two as interchangeable and attribute any timing difference
// purely to their machinery.
func TestPipelinesEquivalent(t *testing.T) {
	t.Parallel()

	const (
		limit = 4
		d     = 1 * time.Millisecond
		n     = 80
	)

	specs := map[string]Spec{
		"linear": {Name: "linear", Stages: []StageSpec{
			{Name: "s1", Kind: Exclusive, Work: d},
			{Name: "s2", Kind: Exclusive, Work: d},
			{Name: "s3", Kind: Exclusive, Work: d},
			{Name: "s4", Kind: Exclusive, Work: d},
			{Name: "s5", Kind: Exclusive, Work: d},
		}},
		"shared": {Name: "shared", Stages: []StageSpec{
			{Name: "s1", Kind: Exclusive, Work: d},
			{Name: "s2", Kind: Exclusive, Work: d},
			{Name: "shared", Kind: Shared, Limit: limit, Work: time.Duration(limit) * d},
			{Name: "s4", Kind: Exclusive, Work: d},
			{Name: "s5", Kind: Exclusive, Work: d},
		}},
		"fanout": {Name: "fanout", Stages: []StageSpec{
			{Name: "s1", Kind: Exclusive, Work: d},
			{Name: "s2", Kind: Exclusive, Work: d},
			{Name: "fanout", Kind: FanOut, Branches: []BranchSpec{
				{Name: "branchA", Limit: limit, Work: time.Duration(limit) * d},
				{Name: "branchB", Limit: limit, Work: time.Duration(limit) * d},
			}},
			{Name: "s4", Kind: Exclusive, Work: d},
			{Name: "s5", Kind: Exclusive, Work: d},
		}},
	}

	impls := map[string]func(Spec, *Observer) Pipeline{
		"conveyor":    NewConveyor,
		"traditional": NewTraditional,
	}

	// saturated names the nodes that must reach `limit` for each spec.
	saturated := map[string][]string{
		"shared": {"shared"},
		"fanout": {"branchA", "branchB"},
	}

	for specName, spec := range specs {
		for implName, build := range impls {
			t.Run(specName+"/"+implName, func(t *testing.T) {
				t.Parallel()

				obs := NewObserver(n, spec)
				p := build(spec, obs)

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				require.NoError(t, p.Run(ctx, n))

				assert.True(t, obs.FinishedAll(), "every item should finish")
				assert.True(t, obs.OrderedOutput(), "items should finish in input order")

				iv, ok := obs.SteadyInterval(n / 4)
				assert.True(t, ok, "steady-state interval should be measurable")
				assert.Positive(t, iv)

				for _, name := range saturated[specName] {
					assert.Equalf(t, limit, obs.PeakOccupancy(name),
						"%s should saturate to its limit", name)
				}
			})
		}
	}
}
