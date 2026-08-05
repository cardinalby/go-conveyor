package conveyor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// testTimeout bounds every test run so a deadlock fails loudly instead of hanging the suite. It is generous on
// purpose: no test asserts timing, and under -race with GOMAXPROCS=1 the race detector's per-synchronization cost
// dominates a child-heavy scenario by more than an order of magnitude.
const testTimeout = 60 * time.Second

// optCancelItemsOnShutdown is the OptShutdownContext spelling of "cancel every in-flight item as soon as shutdown
// begins": a shutdown context that is already done. Tests that only need a hard shutdown use this instead of
// spelling out a factory.
func optCancelItemsOnShutdown() Option {
	return OptShutdownContext(func(error) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, cancel
	})
}

// optShutdownGracePeriod is the OptShutdownContext spelling of "let in-flight items drain for d, then cancel them".
func optShutdownGracePeriod(d time.Duration) Option {
	return OptShutdownContext(func(error) (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), d)
	})
}

// runUntil runs c until `want` items have been created, then cancels the run context and returns Run's error.
// proc receives the item's number, so a test can vary behavior per item.
func runUntil(t *testing.T, c *Conveyor, want int64, proc func(ctx context.Context, no int64) error) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	return c.Run(ctx, func(ic context.Context) error {
		no, ok := ItemNoFromContext(ic)
		if !ok {
			return errors.New("item context carries no item number")
		}
		if no > want {
			// An extra item or two may be created before the cancellation lands; they take no part in the test.
			return nil
		}
		err := proc(ic, no)
		if no == want {
			// Stop creating items only after the last interesting one has done its work. Items still in flight
			// are left to finish (no OptShutdownContext, so nothing bounds them), so their assertions still hold.
			cancel()
		}
		return err
	})
}

// runN runs c for exactly `want` items and fails the test unless Run ended with the expected shutdown cause
// (context.Canceled), i.e. no item failed.
func runNOK(t *testing.T, c *Conveyor, want int64, proc func(ctx context.Context, no int64) error) {
	t.Helper()
	err := runUntil(t, c, want, proc)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run failed: %v", err)
	}
}

// runOnce runs a single item through c and returns Run's error. Used for the many tests that only need one
// journey.
func runOnce(t *testing.T, c *Conveyor, proc func(ctx context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	var once sync.Once
	return c.Run(ctx, func(ic context.Context) error {
		var err error
		once.Do(func() {
			err = proc(ic)
			cancel()
		})
		return err // later items (created before the cancellation landed) take no part
	})
}

// gauge tracks concurrent occupancy: peak concurrency and total entries.
type gauge struct {
	mu      sync.Mutex
	cur     int
	peak    int
	entries int
}

func (g *gauge) enter() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.cur++
	g.entries++
	if g.cur > g.peak {
		g.peak = g.cur
	}
}

func (g *gauge) leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.cur--
}

func (g *gauge) snapshot() (peak, entries int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.peak, g.entries
}

// current reports how many are inside right now. Use it to synchronize on the body of a stage having been reached: a
// node's occupancy is taken inside MoveTo, before the item's code runs, so it answers a different question.
func (g *gauge) current() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cur
}

// hold runs body while counted by the gauge.
func (g *gauge) hold(body func() error) error {
	g.enter()
	defer g.leave()
	return body()
}

// workGauge tracks how many items have fan-out work outstanding — scheduled on the branches and not yet finished — and
// the peak that number reached. It is the instrument for the fan-out's capacity bound: what the node's limit must
// cap is exactly this number, whatever the item does after scheduling.
type workGauge struct {
	mu   sync.Mutex
	cur  int
	peak int
}

// item returns a handle for one item's work of `tasks` tasks.
func (g *workGauge) item(tasks int) *itemWork {
	return &itemWork{g: g, remaining: tasks}
}

func (g *workGauge) peakValue() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.peak
}

// itemWork counts one item's outstanding tasks. The window opens at scheduled() — called once MoveTo has returned,
// so the tasks really are on the branches — and closes when the last task reports taskDone(). Work that finished
// before scheduled() is never counted, which can only understate the peak, never inflate it.
type itemWork struct {
	g         *workGauge
	remaining int
	counted   bool
	done      bool
}

func (w *itemWork) scheduled() {
	w.g.mu.Lock()
	defer w.g.mu.Unlock()
	if w.done {
		return
	}
	w.counted = true
	w.g.cur++
	if w.g.cur > w.g.peak {
		w.g.peak = w.g.cur
	}
}

func (w *itemWork) taskDone() {
	w.g.mu.Lock()
	defer w.g.mu.Unlock()
	if w.remaining--; w.remaining > 0 {
		return
	}
	w.done = true
	if w.counted {
		w.counted = false
		w.g.cur--
	}
}

// recorder collects an ordered log of events under a lock.
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, fmt.Sprintf(format, args...))
}

func (r *recorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

// numbers collects an ordered log of int64s (item numbers, indexes).
type numbers struct {
	mu   sync.Mutex
	vals []int64
}

func (n *numbers) add(v int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.vals = append(n.vals, v)
}

func (n *numbers) all() []int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]int64(nil), n.vals...)
}

// assertStrictlyIncreasing fails unless vals is 1,2,3,... — the fingerprint of an order-preserving stage.
func assertStrictlyIncreasing(t *testing.T, vals []int64, msg string) {
	t.Helper()
	for i, v := range vals {
		if v != int64(i+1) {
			t.Fatalf("%s: expected 1..n in order, got %v", msg, vals)
		}
	}
}

// assertNonDecreasing fails unless vals never goes backwards.
func assertNonDecreasing(t *testing.T, vals []int64, msg string) {
	t.Helper()
	for i := 1; i < len(vals); i++ {
		if vals[i] < vals[i-1] {
			t.Fatalf("%s: order went backwards at %d: %v", msg, i, vals)
		}
	}
}

// assertPanics runs fn and fails unless it panics with an error matching want.
func assertPanics(t *testing.T, want error, fn func()) {
	t.Helper()
	defer func() {
		rec := recover()
		if rec == nil {
			t.Fatalf("expected a panic with %v, got none", want)
		}
		err, ok := rec.(error)
		if !ok || !errors.Is(err, want) {
			t.Fatalf("expected a panic matching %v, got %v", want, rec)
		}
	}()
	fn()
}

// recoveredErr runs fn and returns the error it panicked with, or nil if it did not panic. Unlike assertPanics it
// asserts nothing, which is what makes it safe on a worker goroutine: assertPanics reports a mismatch with Fatalf,
// and Fatalf off the test goroutine calls runtime.Goexit — so the task would never return, its wave would never
// settle, and the run would hang instead of failing. Capture the panic here and assert on the test goroutine.
func recoveredErr(fn func()) (err error) {
	defer func() {
		rec := recover()
		if rec == nil {
			return
		}
		if e, ok := rec.(error); ok {
			err = e
			return
		}
		err = fmt.Errorf("%v", rec)
	}()
	fn()
	return nil
}

// waitFor blocks until cond returns true and reports whether it did, failing the test on timeout. Used to
// synchronize on runtime state (occupancy, counters) without sleeping.
//
// It reports the timeout with Errorf rather than Fatalf: most call sites run on a helper goroutine, where Fatalf is
// not allowed (it would end that goroutine instead of the test, leaving the failure half-reported). Callers that must
// stop early can check the result; the rest simply proceed, and their own assertions fail.
func waitFor(t *testing.T, msg string, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(200 * time.Microsecond)
	}
	t.Errorf("timed out waiting for %s", msg)
	return false
}

// occupancyOf reports the live occupancy of a unit, for tests that assert on runtime state.
func occupancyOf(c *Conveyor, u Unit) int {
	r := c.currentRun.Load()
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.occupancy[u.unit().index].val
}

// unitStatByName finds a Stats entry by node name.
func unitStatByName(s Stats, name string) (UnitStat, bool) {
	for _, u := range s.Units {
		if u.Unit.String() == name {
			return u, true
		}
	}
	return UnitStat{}, false
}

// sprintf is a tiny alias so tests can build expected event strings inline.
func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

// queueOccupancy reports how many items are waiting in front of a node right now — the waiting room's own counter,
// which is capacity on the node rather than a unit of its own.
func queueOccupancy(c *Conveyor, u Unit) int {
	r := c.currentRun.Load()
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.queued[u.unit().index].val
}

// branchScope reports a branch's own scope id. Branch is an interface, so this reaches through to the implementation — which
// only an in-package test can do, and which is the point: the scope is internal bookkeeping, not API.
func branchScope(b Branch) int { return b.(*branch).series.id }
