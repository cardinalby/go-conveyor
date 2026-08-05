package conveyor

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

// BenchmarkFanOutSchedule measures one item's trip through a fan-out: entering the node, grouping its tasks per lane,
// enqueueing them and joining the wave at the next stage. The lane count is what the grouping cost scales with, so it
// is the parameter. Watch allocs/op: the per-call bookkeeping in scheduleWave is what this is here to keep honest.
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
				tasks := make(Tasks, 0, len(ls))
				for _, l := range ls {
					tasks = append(tasks, l.NewTask(noop))
				}
				err := fo.MoveTo(ic, tasks)
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
