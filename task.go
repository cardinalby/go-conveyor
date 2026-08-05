package conveyor

import (
	"context"
	"iter"
)

// TaskFunc is one piece of a branch's work. It receives a context — pass it to everything it calls, and to the
// lane's own nodes if the branch is a Lane — and should return promptly when canceled. A returned error cancels
// the item.
//
// On a Pool the context cannot be used with MoveTo: a pool's work has nowhere to go.
type TaskFunc = func(ctx context.Context) error

// Task is a bundle of work for one branch, produced by a branch's constructors (NewTask, NewTasks, NewTasksGen,
// NewTasksChan). Collect tasks into a Tasks value and submit them with FanOut.MoveTo.
//
// A Task is single-use: submitting the same Task twice panics.
type Task struct {
	branch *branch
	src    taskSource // nil for a statically-empty task (e.g. NewTasks with count <= 0)
}

// Tasks is the collection of work passed to FanOut.MoveTo. Build it as a literal —
// conveyor.Tasks{s3.NewTasks(n, up), db.NewTask(idx)} — or accumulate it with Add. Its tasks may belong to
// different branches of the same fan-out; several tasks for the same branch run in the order they appear here.
type Tasks []Task

// Add appends tasks in place and returns the collection, for optional chaining.
func (ts *Tasks) Add(tasks ...Task) *Tasks {
	*ts = append(*ts, tasks...)
	return ts
}

// branchName names a task's branch for panic messages, tolerating a zero Task.
func (t Task) branchName() string {
	if t.branch == nil {
		return "<nil branch>"
	}
	return t.branch.String()
}

// taskSource is a lazy source of a Task's callbacks. Sources come in two kinds:
//   - synchronous (isSync() == true): pull runs no user code, so the scheduler calls it under run.mu (the fast
//     path, preserving the atomic slot-reuse of the branch workers);
//   - asynchronous: pull runs user code (a generator body, a channel receive) and may block, so the scheduler
//     calls it WITHOUT run.mu, single-flight per source (see taskCollection.pulling), with the slot reserved for
//     the duration.
//
// A source is stateful and single-use; claim() detects a Task submitted twice. All methods except an async pull
// are called under run.mu.
type taskSource interface {
	// claim marks the source as consumed by a MoveTo call; it reports false if it was already claimed.
	claim() bool
	// isSync reports whether pull is free of user code and safe to call under run.mu.
	isSync() bool
	// pull returns the next callback, or ok == false once the source is exhausted. Async sources treat ctx
	// cancellation as exhaustion (they stop consuming user input). Sync sources ignore ctx.
	pull(ctx context.Context) (fn TaskFunc, ok bool)
	// exhausted reports whether the source is known to have no more callbacks without pulling. An async source
	// cannot know ahead and reports false until a pull has returned ok == false.
	exhausted() bool
	// release gives up whatever the source holds, for work that will never run because its item was canceled. It is
	// what stops a suspended generator (see genSource), and must be idempotent and safe on a source never pulled from.
	// Unlike the other methods it is called WITHOUT run.mu, since stopping a generator resumes it to unwind and so
	// runs user code.
	release()
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

// release is a no-op for the eager sources, which hold nothing but their own state; the streaming ones override it.
func (s *sourceState) release() {}

// singleSource emits exactly one callback (NewTask). It is released on pull so a consumed source pins
// nothing.
type singleSource struct {
	sourceState
	fn TaskFunc
}

func (s *singleSource) isSync() bool    { return true }
func (s *singleSource) exhausted() bool { return s.fn == nil }

func (s *singleSource) pull(context.Context) (TaskFunc, bool) {
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

func (s *countSource) isSync() bool    { return true }
func (s *countSource) exhausted() bool { return s.next >= s.count }

func (s *countSource) pull(context.Context) (TaskFunc, bool) {
	if s.next >= s.count {
		return nil, false
	}
	fn, i := s.fn, s.next
	s.next++
	return func(ctx context.Context) error { return fn(ctx, i) }, true
}

// genSource emits the callbacks yielded by a user iterator (NewTasksGen), suspended between pulls via
// iter.Pull. The pull coroutine is created lazily on the first pull and stopped as soon as the source is done
// (generator exhausted, nil yield, or ctx canceled), so an early stop releases it promptly.
type genSource struct {
	sourceState
	seq  iter.Seq[TaskFunc]
	next func() (TaskFunc, bool)
	stop func()
	done bool
}

func (s *genSource) isSync() bool    { return false }
func (s *genSource) exhausted() bool { return s.done }

func (s *genSource) pull(ctx context.Context) (TaskFunc, bool) {
	if s.done {
		return nil, false
	}
	if ctx != nil && context.Cause(ctx) != nil {
		s.finish()
		return nil, false
	}
	if s.next == nil {
		s.next, s.stop = iter.Pull(s.seq)
		s.seq = nil
	}
	fn, ok := s.next()
	if !ok {
		s.finish()
		return nil, false
	}
	if fn == nil {
		// Misuse: fail the item (fail-fast) through the returned callback instead of panicking on an internal
		// goroutine; the source stops here.
		s.finish()
		return func(context.Context) error { return errNilTaskFunc }, true
	}
	return fn, true
}

// finish marks the source exhausted and releases the pull coroutine (idempotent).
func (s *genSource) finish() {
	s.done = true
	if s.stop != nil {
		s.stop()
		s.next, s.stop = nil, nil
	}
}

// release stops a generator whose remaining work will never be pulled. Without it the iter.Pull coroutine created by
// the first pull would stay parked for the life of the process, since nothing else ever reaches this source again once
// its collection has been dropped (see run.dropHead).
func (s *genSource) release() { s.finish() }

// chanSource emits the callbacks received from a user channel (NewTasksChan) until it is closed or the item's
// ctx is canceled.
type chanSource struct {
	sourceState
	ch   <-chan TaskFunc
	done bool
}

func (s *chanSource) isSync() bool    { return false }
func (s *chanSource) exhausted() bool { return s.done }

func (s *chanSource) pull(ctx context.Context) (TaskFunc, bool) {
	if s.done {
		return nil, false
	}
	var canceled <-chan struct{} // nil (never ready) when ctx is nil, e.g. under white-box tests
	if ctx != nil {
		// Check cancellation first: with callbacks already buffered on the channel, the select below would pick a
		// ready case at random and could keep starting work for a canceled item.
		if context.Cause(ctx) != nil {
			s.finish()
			return nil, false
		}
		canceled = ctx.Done()
	}
	select {
	case fn, ok := <-s.ch:
		if !ok {
			s.done = true
			return nil, false
		}
		if fn == nil {
			s.finish()
			return func(context.Context) error { return errNilTaskFunc }, true
		}
		return fn, true
	case <-canceled:
		s.finish()
		return nil, false
	}
}

func (s *chanSource) finish() {
	s.done = true
	s.ch = nil
}

// release drops the channel. The producer is the caller's own goroutine and is required to respect the item's ctx, so
// there is nothing here to unblock — this only stops the source from being read again and lets the channel go.
func (s *chanSource) release() { s.finish() }
