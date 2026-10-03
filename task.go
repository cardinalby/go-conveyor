package conveyor

import (
	"context"
)

// TaskFunc is one task: a piece of a branch's work, or the background work of RetainFor. It receives a context — pass
// it to everything it calls, and to the lane's own nodes if the task runs on a Lane — and should return promptly when
// canceled.
//
// A returned error does not cancel the item. In a fan-out it cancels the context of the item's other tasks there, as
// errgroup.WithContext does. The join (TaskGroup.Wait, FanOut.Wait, or the MoveTo that leaves the fan-out) returns
// it as a TaskError, and the processor decides: return it to fail the item, or handle it and go on.
//
// On a Pool the context can be used with Schedule of the pool's fan-out, to add follow-up work before the task
// returns, and with nothing else: a pool's work has nowhere to go and must not wait for other work. A RetainFor task
// may call no node method with its context.
type TaskFunc = func(ctx context.Context) error

// Task is a bundle of work for one branch, produced by a branch's constructors (NewTask, NewTasks). Submit tasks with
// FanOut.Schedule. The tasks of one call may belong to different branches of the same fan-out; several tasks for the
// same branch start in the order they are listed.
//
// A Task is single-use: submitting the same Task twice panics.
type Task struct {
	branch *branch
	src    taskSource // nil for a statically-empty task (e.g. NewTasks with count 0)
}

// branchName names a task's branch for panic messages, tolerating a zero Task.
func (t Task) branchName() string {
	if t.branch == nil {
		return "<nil branch>"
	}
	return t.branch.String()
}

// taskSource is a lazy source of a Task's callbacks. pull runs no user code, so the scheduler calls it under run.mu,
// which keeps the atomic slot reuse of the branch workers.
//
// A source is stateful and single-use; claim() detects a Task submitted twice. All methods are called under run.mu.
type taskSource interface {
	// claim marks the source as consumed by a Schedule call; it reports false if it was already claimed.
	claim() bool
	// isClaimed reports whether claim has succeeded, without claiming.
	isClaimed() bool
	// pull returns the next callback, or ok == false once the source is exhausted.
	pull() (fn TaskFunc, ok bool)
	// exhausted reports whether the source has no more callbacks. It is known eagerly: right after the last pull.
	exhausted() bool
}

// sourceState carries the claim flag shared by every source implementation (see taskSource.claim).
type sourceState struct {
	claimed bool
}

func (s *sourceState) claim() bool {
	if s.claimed {
		return false
	}
	s.claimed = true
	return true
}

func (s *sourceState) isClaimed() bool { return s.claimed }

// singleSource emits exactly one callback (NewTask). It is released on pull so a consumed source pins
// nothing.
type singleSource struct {
	sourceState
	fn TaskFunc
}

func (s *singleSource) exhausted() bool { return s.fn == nil }

func (s *singleSource) pull() (TaskFunc, bool) {
	fn := s.fn
	if fn == nil {
		return nil, false
	}
	s.fn = nil
	return fn, true
}

// countSource emits count callbacks built on demand from the index (NewTasks). It is O(1) memory regardless
// of count.
type countSource struct {
	sourceState
	fn    func(ctx context.Context, index int) error
	count int
	next  int
}

func (s *countSource) exhausted() bool { return s.next >= s.count }

func (s *countSource) pull() (TaskFunc, bool) {
	if s.next >= s.count {
		return nil, false
	}
	fn, i := s.fn, s.next
	s.next++
	return func(ctx context.Context) error { return fn(ctx, i) }, true
}
