package conveyor

import (
	"context"
	"errors"
	"testing"
	"time"
)

// This file pins the fan-out's door — the ordering gate between the item inside a fan-out and the item behind it,
// which opens only when the item ahead publishes the node's rank — and what a leave that fails after the body was
// joined leaves behind.

// TestDoorOpensOnlyWhenTheItemAheadPublishes: whatever the limit, the item behind cannot enter the fan-out until the
// item ahead has published — its first Schedule (even with zero tasks), a leave without scheduling, or a Detach. Wait
// publishes nothing.
func TestDoorOpensOnlyWhenTheItemAheadPublishes(t *testing.T) {
	noop := func(context.Context) error { return nil }
	for _, tc := range []struct {
		name  string
		opens bool
		act   func(ctx context.Context, fo FanOut, pool Pool, commit Stage) error
	}{
		{"schedule a task", true, func(ctx context.Context, fo FanOut, pool Pool, _ Stage) error {
			return fo.Schedule(ctx, pool.NewTask(noop))
		}},
		{"schedule zero tasks", true, func(ctx context.Context, fo FanOut, _ Pool, _ Stage) error {
			return fo.Schedule(ctx)
		}},
		{"leave without scheduling", true, func(ctx context.Context, _ FanOut, _ Pool, commit Stage) error {
			return commit.MoveTo(ctx)
		}},
		{"detach an empty body", true, func(ctx context.Context, fo FanOut, _ Pool, _ Stage) error {
			fo.Detach(ctx)
			return nil
		}},
		{"wait", false, func(ctx context.Context, fo FanOut, _ Pool, _ Stage) error {
			return fo.Wait(ctx)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewConveyor()
			fo := c.AddFanOut(OptName("fo")).SetLimit(2)
			pool := fo.AddPool(OptName("pool"))
			commit := c.AddStage(OptName("commit")).SetLimit(2)

			firstInside := make(chan struct{})
			act := make(chan struct{})
			secondInside := make(chan struct{})
			release := make(chan struct{})
			checked := make(chan struct{})

			isInside := func() bool {
				select {
				case <-secondInside:
					return true
				default:
					return false
				}
			}
			go func() {
				defer close(checked)
				<-firstInside
				time.Sleep(20 * time.Millisecond)
				if isInside() || occupancyOf(c, fo) != 1 {
					t.Errorf("item 2 got into the fan-out before item 1 published anything")
				}
				close(act)
				if tc.opens {
					<-secondInside
				} else {
					time.Sleep(20 * time.Millisecond)
					if isInside() {
						t.Errorf("item 2 got into the fan-out after item 1's %s, which publishes nothing", tc.name)
					}
				}
				close(release) // item 1 returns; if the door was still closed, its completion opens it
			}()

			runNOK(t, c, 2, func(ctx context.Context, no int64) error {
				if err := fo.MoveTo(ctx); err != nil {
					return err
				}
				if no == 2 {
					close(secondInside)
					return nil
				}
				close(firstInside)
				<-act
				if err := tc.act(ctx, fo, pool, commit); err != nil {
					return err
				}
				<-release
				return nil
			})
			<-checked
		})
	}
}

// TestDoorClosedItemMayUseTheWaitingRoom: admission to a fan-out publishes the waiting room's rank, so the item behind
// may step into the waiting room — freeing the node it came from — while the door itself stays closed.
func TestDoorClosedItemMayUseTheWaitingRoom(t *testing.T) {
	c := NewConveyor()
	prev := c.AddStage(OptName("prev"))
	fo := c.AddFanOut(OptName("fo")).SetLimit(2).SetQueueSize(1)
	pool := fo.AddPool(OptName("pool"))

	firstInside := make(chan struct{})
	thirdAtPrev := make(chan struct{})
	open := make(chan struct{})
	release := make(chan struct{})
	checked := make(chan struct{})

	go func() {
		defer close(checked)
		<-firstInside
		// Item 2 steps aside into the waiting room and gives prev up, which is what lets item 3 reach prev.
		<-thirdAtPrev
		if q := queueOccupancy(c, fo); q != 1 {
			t.Errorf("fan-out waiting room holds %d, want 1", q)
		}
		time.Sleep(20 * time.Millisecond)
		if occ := occupancyOf(c, fo); occ != 1 {
			t.Errorf("fan-out occupancy = %d before item 1 scheduled, want 1 (the door is closed)", occ)
		}
		close(open)
		waitFor(t, "item 2 to enter the fan-out", func() bool { return occupancyOf(c, fo) == 2 })
		if q := queueOccupancy(c, fo); q != 0 {
			t.Errorf("fan-out waiting room holds %d after item 2 entered, want 0", q)
		}
		close(release)
	}()

	runNOK(t, c, 3, func(ctx context.Context, no int64) error {
		if err := prev.MoveTo(ctx); err != nil {
			return err
		}
		if no == 3 {
			close(thirdAtPrev)
			<-release
			return nil
		}
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if no == 1 {
			close(firstInside)
			<-open
			if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil })); err != nil {
				return err
			}
		}
		<-release
		return nil
	})
	<-checked
}

// assertClosedBodyRefusesEverything: after a leave closed the body, the own-body path is over — Schedule and Wait
// panic with errBodyClosed, and Detach with detachErr (errNothingToDetach while the item still occupies the fan-out,
// errStageNotEntered once it stands in the next node's waiting room). It runs on the item's goroutine, so it reports
// with Errorf: a Fatalf there would end the worker instead of the test.
func assertClosedBodyRefusesEverything(t *testing.T, ctx context.Context, fo FanOut, pool Pool, detachErr error) {
	t.Helper()
	if st := bodyStateOf(ctx, fo); st != bodyClosed {
		t.Errorf("body state after the failed leave = %d, want closed (%d)", st, bodyClosed)
	}
	for _, call := range []struct {
		name string
		want error
		fn   func()
	}{
		{"Schedule", errBodyClosed, func() {
			_ = fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil }))
		}},
		{"Wait", errBodyClosed, func() { _ = fo.Wait(ctx) }},
		{"Detach", detachErr, func() { fo.Detach(ctx) }},
	} {
		if err := recoveredErr(call.fn); !errors.Is(err, call.want) {
			t.Errorf("%s after the failed leave panicked with %v, want %v", call.name, err, call.want)
		}
	}
}

// TestDoorFailedLeaveBeforeTheWaitingRoom: a leave whose call context expires while the item waits for the next node
// (no waiting room there) leaves the item inside the fan-out, still holding its slot, with a closed body. A later
// MoveTo to the same node succeeds once it frees.
func TestDoorFailedLeaveBeforeTheWaitingRoom(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(2)
	pool := fo.AddPool(OptName("pool"))
	s := c.AddStage(OptName("s")) // exclusive, no waiting room: item 2 blocks at its door

	firstInS := make(chan struct{})
	releaseFirst := make(chan struct{})
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx); err != nil {
			return err
		}
		if no == 1 {
			if err := s.MoveTo(ctx); err != nil {
				return err
			}
			close(firstInS)
			<-releaseFirst
			return nil
		}
		<-firstInS
		dctx, dcancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer dcancel()
		if err := s.MoveTo(dctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("MoveTo with the expired call context = %v, want DeadlineExceeded", err)
		}
		if occ := occupancyOf(c, fo); occ != 1 {
			t.Errorf("fan-out occupancy = %d after the failed leave, want 1 (the item is still inside)", occ)
		}
		if st, ok := unitStatByName(c.Stats(), "fo"); !ok || st.Occupied.Last != 1 {
			t.Errorf("fo Occupied.Last = %d after the failed leave, want 1", st.Occupied.Last)
		}
		assertClosedBodyRefusesEverything(t, ctx, fo, pool, errNothingToDetach)
		close(releaseFirst)
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		if occ := occupancyOf(c, fo); occ != 0 {
			t.Errorf("fan-out occupancy = %d after the retried leave, want 0", occ)
		}
		return nil
	})
}

// TestDoorFailedLeaveAfterTheWaitingRoom: when the next node has a waiting room, the item steps into it — giving up
// the fan-out slot — and a call context canceled there leaves it standing there with a closed body. The retried MoveTo
// resumes waiting from that spot and never counts the item in the waiting room twice.
func TestDoorFailedLeaveAfterTheWaitingRoom(t *testing.T) {
	c := NewConveyor()
	fo := c.AddFanOut(OptName("fo")).SetLimit(2)
	pool := fo.AddPool(OptName("pool"))
	s := c.AddStage(OptName("s")).SetQueueSize(2)

	firstInS := make(chan struct{})
	releaseFirst := make(chan struct{})
	retrying := make(chan struct{})
	go func() {
		<-retrying
		close(releaseFirst) // let item 1 out only once the retry is under way, so the retry meets a full stage
	}()
	runNOK(t, c, 2, func(ctx context.Context, no int64) error {
		if err := fo.MoveTo(ctx); err != nil {
			return err
		}
		if err := fo.Schedule(ctx, pool.NewTask(func(context.Context) error { return nil })); err != nil {
			return err
		}
		if no == 1 {
			if err := s.MoveTo(ctx); err != nil {
				return err
			}
			close(firstInS)
			<-releaseFirst
			return nil
		}
		<-firstInS
		dctx, dcancel := context.WithCancel(ctx)
		defer dcancel()
		go func() {
			waitFor(t, "item 2 to wait in front of s", func() bool { return queueOccupancy(c, s) == 1 })
			dcancel()
		}()
		if err := s.MoveTo(dctx); !errors.Is(err, context.Canceled) {
			t.Errorf("MoveTo with the canceled call context = %v, want Canceled", err)
		}
		if occ := occupancyOf(c, fo); occ != 0 {
			t.Errorf("fan-out occupancy = %d while the item waits in front of s, want 0", occ)
		}
		if q := queueOccupancy(c, s); q != 1 {
			t.Errorf("s waiting room holds %d after the failed leave, want 1 (the item stays there)", q)
		}
		assertClosedBodyRefusesEverything(t, ctx, fo, pool, errStageNotEntered)
		close(retrying)
		if err := s.MoveTo(ctx); err != nil {
			return err
		}
		if q := queueOccupancy(c, s); q != 0 {
			t.Errorf("s waiting room holds %d after the item entered, want 0", q)
		}
		if st, ok := unitStatByName(c.Stats(), "s"); !ok || st.Queued.Max != 1 {
			t.Errorf("s Queued.Max = %d over the run, want 1: a retried leave must not count the item twice", st.Queued.Max)
		}
		return nil
	})
}
