package conveyor

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

// BenchmarkFanOutSchedule measures one item's trip through a fan-out: entering the node, one Schedule that groups its
// tasks per lane and enqueues them, and the join at the next stage. The lane count is what the grouping cost scales
// with, so it is the parameter. Watch allocs/op: the per-call bookkeeping in addToBody is what this is here to keep
// honest.
func BenchmarkFanOutSchedule(b *testing.B) {
	noop := func(context.Context) error { return nil }
	for _, lanes := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("lanes%d", lanes), func(b *testing.B) {
			c := NewConveyor()
			fo := c.AddFanOut(OptName("fo")).SetLimit(4)
			ls := make([]Pool, 0, lanes)
			for i := 0; i < lanes; i++ {
				ls = append(ls, fo.AddPool(OptName(fmt.Sprintf("l%d", i))).SetLimit(2))
			}
			commit := c.AddStage(OptName("commit")).SetLimit(4)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var done atomic.Int64

			b.ReportAllocs()
			b.ResetTimer()
			_ = c.Run(ctx, func(ic context.Context) error {
				tasks := make([]Task, 0, len(ls))
				for _, l := range ls {
					tasks = append(tasks, l.NewTask(noop))
				}
				err := fo.MoveTo(ic)
				if err == nil {
					err = fo.Schedule(ic, tasks...)
				}
				if err != nil {
					return err
				}
				if err := commit.MoveTo(ic); err != nil {
					return err
				}
				if done.Add(1) >= int64(b.N) {
					cancel()
				}
				return nil
			})
			b.StopTimer()
		})
	}
}

// BenchmarkFanOutSpawnChain measures a body that grows from its own work: one root task on a pool spawns the next link
// from its task context, depth links deep, and the item leaves once the chain is done. Each link is a Schedule under
// the pool-work path plus a slot hand-over on the same pool, so the per-link cost is what to watch.
func BenchmarkFanOutSpawnChain(b *testing.B) {
	for _, depth := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("depth%d", depth), func(b *testing.B) {
			c := NewConveyor()
			fo := c.AddFanOut(OptName("fo")).SetLimit(4)
			pool := fo.AddPool(OptName("pool")).SetLimit(2)
			commit := c.AddStage(OptName("commit")).SetLimit(4)

			var link func(left int) TaskFunc
			link = func(left int) TaskFunc {
				return func(tctx context.Context) error {
					if left == 0 {
						return nil
					}
					return fo.Schedule(tctx, pool.NewTask(link(left-1)))
				}
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var done atomic.Int64

			b.ReportAllocs()
			b.ResetTimer()
			_ = c.Run(ctx, func(ic context.Context) error {
				if err := fo.MoveTo(ic); err != nil {
					return err
				}
				if err := fo.Schedule(ic, pool.NewTask(link(depth-1))); err != nil {
					return err
				}
				if err := commit.MoveTo(ic); err != nil {
					return err
				}
				if done.Add(1) >= int64(b.N) {
					cancel()
				}
				return nil
			})
			b.StopTimer()
		})
	}
}

// BenchmarkFanOutRounds measures the rounds pattern: Schedule one task then Wait for it, rounds times per item, before
// leaving. Each round is a root Schedule plus a Wait that parks the item until the body is idle, so the per-round cost
// is what to watch.
func BenchmarkFanOutRounds(b *testing.B) {
	noop := func(context.Context) error { return nil }
	for _, rounds := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("rounds%d", rounds), func(b *testing.B) {
			c := NewConveyor()
			fo := c.AddFanOut(OptName("fo")).SetLimit(4)
			pool := fo.AddPool(OptName("pool")).SetLimit(2)
			commit := c.AddStage(OptName("commit")).SetLimit(4)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var done atomic.Int64

			b.ReportAllocs()
			b.ResetTimer()
			_ = c.Run(ctx, func(ic context.Context) error {
				if err := fo.MoveTo(ic); err != nil {
					return err
				}
				for i := 0; i < rounds; i++ {
					if err := fo.Schedule(ic, pool.NewTask(noop)); err != nil {
						return err
					}
					if err := fo.Wait(ic); err != nil {
						return err
					}
				}
				if err := commit.MoveTo(ic); err != nil {
					return err
				}
				if done.Add(1) >= int64(b.N) {
					cancel()
				}
				return nil
			})
			b.StopTimer()
		})
	}
}
