package conveyor

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestFanOutLimitBoundsOutstandingWork pins the fan-out's capacity contract: at no instant may more items have work
// outstanding on the lanes than the node's limit admits. The lanes are given room to spare on purpose — the number
// under test is the fan-out's, not theirs — and the stage after the fan-out is given a waiting room, because that is
// what lets items stream past a node they have not finished working in.
func TestFanOutLimitBoundsOutstandingWork(t *testing.T) {
	for _, limit := range []int{1, 3} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			const items = 8
			c := NewConveyor()
			dbs := c.AddFanOut(OptName("dbs")).SetLimit(limit)
			slow := dbs.AddPool(OptName("slow")).SetLimit(items)
			fast := dbs.AddPool(OptName("fast")).SetLimit(items)
			commit := c.AddStage(OptName("commit")).SetQueueSize(3)

			var wg workGauge
			runNOK(t, c, items, func(ctx context.Context, no int64) error {
				w := wg.item(2)
				err := dbs.MoveTo(ctx, Tasks{
					slow.NewTask(func(context.Context) error {
						defer w.taskDone()
						time.Sleep(20 * time.Millisecond)
						return nil
					}),
					fast.NewTask(func(context.Context) error {
						defer w.taskDone()
						return nil
					}),
				})
				if err != nil {
					return err
				}
				w.scheduled()
				return commit.MoveTo(ctx)
			})

			if peak := wg.peakValue(); peak > limit {
				t.Fatalf("%d items had work outstanding at once, fan-out limit is %d", peak, limit)
			}
		})
	}
}
