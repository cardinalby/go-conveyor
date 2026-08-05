// Package bench sweeps a base delay D across a range and, for each value,
// measures the per-item throughput cost of a Spec built at that D, once with the
// conveyor and once with the traditional channels/goroutines pipeline. Because
// both are driven through the same pipeline.Pipeline interface and measured from
// the same pipeline.Observer data, the only thing that differs between the two
// series is the coordination machinery under test.
package bench

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cardinalby/go-conveyor/bench/internal/pipeline"
)

// Config controls the sweep and how each point is measured.
type Config struct {
	N       int           // items pushed per run
	Drop    int           // items trimmed at each end for the steady-state window
	Repeats int           // runs per point; the best (lowest) interval is kept
	DMin    time.Duration // smallest nonzero base delay
	DMax    time.Duration // largest base delay
	Steps   int           // number of nonzero D values (log-spaced from DMin to DMax)
}

// DValues returns the sweep's base delays: 0 first (the pure-overhead point),
// then Steps values log-spaced from DMin to DMax. Log spacing matches the
// chart's log D axis — evenly spaced points there — and concentrates samples at
// small D, where coordination overhead is actually visible next to the work;
// the large-D tail, where the curves are flat, gets only a few (expensive)
// points instead of most of the sweep's runtime.
func (cfg Config) DValues() []time.Duration {
	return append([]time.Duration{0}, logspace(cfg.DMin, cfg.DMax, cfg.Steps)...)
}

// SatCheck names a stage/branch expected to reach Limit occupancy, so the sweep
// can flag points where the intended saturation was not achieved.
type SatCheck struct {
	Name  string
	Limit int
}

// Scenario builds a Spec for a given base delay D, plus the saturation it expects.
type Scenario struct {
	Name  string
	Build func(d time.Duration) pipeline.Spec
	Sat   []SatCheck
}

// Point is one measured (or skipped) sample: the per-item interval at delay D,
// and MinSatFrac (the least peak/limit ratio across the scenario's SatCheck
// nodes, 1 when none) so under-saturated points can be flagged.
type Point struct {
	D          time.Duration
	Interval   time.Duration
	MinSatFrac float64
	Skipped    bool
}

// Series is one labeled line of points (e.g. "conveyor", "traditional").
type Series struct {
	Label  string
	Points []Point
}

// Result holds one scenario's measured series, ready to tabulate or chart.
type Result struct {
	ScenarioName string
	Series       []Series
}

// Impl is one measurable pipeline implementation. Key names it on the command
// line and in result file names (e.g. fanout_conv.csv); Label is the chart
// legend text.
type Impl struct {
	Key   string
	Label string
	Build func(pipeline.Spec, *pipeline.Observer) pipeline.Pipeline
}

// Impls lists the implementations under comparison, in the order the parent
// process runs them.
func Impls() []Impl {
	return []Impl{
		{Key: "chans", Label: "chans+goroutines", Build: pipeline.NewTraditional},
		{Key: "conv", Label: "conveyor", Build: pipeline.NewConveyor},
	}
}

// ImplByKey resolves a -impl flag value.
func ImplByKey(key string) (Impl, bool) {
	for _, im := range Impls() {
		if im.Key == key {
			return im, true
		}
	}
	return Impl{}, false
}

// RunImpl sweeps sc under cfg for a single implementation and returns its
// series. Each variant runs in its own process (see the command doc in main.go),
// so the two never share a Go runtime — no inherited heap/GC state, OS threads
// or timer pressure can couple one variant's measurements to the other's.
// Measurements within the sweep are strictly sequential. progress, if non-nil,
// is called after each point.
//
// A D=0 point is prepended to the sweep: with every stage's work zeroed, the
// measured interval is the pure coordination cost of moving an item through the
// pipeline — the machinery overhead with nothing to hide behind. Saturation is
// not expected there (items fly through, so concurrent nodes never fill) and is
// not checked.
func RunImpl(sc Scenario, im Impl, cfg Config, progress func(d time.Duration, pt Point)) Series {
	s := Series{Label: im.Label}
	for _, d := range cfg.DValues() {
		sat := sc.Sat
		if d == 0 {
			sat = nil
		}
		pt := measure(sc.Build(d), im.Build, cfg, sat)
		pt.D = d
		s.Points = append(s.Points, pt)
		if progress != nil {
			progress(d, pt)
		}
	}
	return s
}

// measure runs the spec cfg.Repeats times and keeps the run with the lowest
// steady-state interval (least affected by scheduling/GC noise), recording that
// run's saturation.
func measure(spec pipeline.Spec, build func(pipeline.Spec, *pipeline.Observer) pipeline.Pipeline, cfg Config, sat []SatCheck) Point {
	pt := Point{Skipped: true, MinSatFrac: 1}
	for r := 0; r < cfg.Repeats; r++ {
		obs := pipeline.NewObserver(cfg.N, spec)
		p := build(spec, obs)
		if err := p.Run(context.Background(), cfg.N); err != nil {
			continue
		}
		iv, ok := obs.SteadyInterval(cfg.Drop)
		if !ok {
			continue
		}
		if pt.Skipped || iv < pt.Interval {
			pt.Skipped = false
			pt.Interval = iv
			pt.MinSatFrac = minSatFrac(obs, sat)
		}
	}
	return pt
}

// minSatFrac returns the least peak/limit ratio across sat (1 when sat is empty).
func minSatFrac(obs *pipeline.Observer, sat []SatCheck) float64 {
	m := 1.0
	for _, s := range sat {
		if s.Limit <= 0 {
			continue
		}
		if f := float64(obs.PeakOccupancy(s.Name)) / float64(s.Limit); f < m {
			m = f
		}
	}
	return m
}

// logspace returns n values from lo to hi inclusive in geometric progression
// (each step multiplies by a constant ratio), so they land evenly spaced on a
// log axis. lo must be > 0 for a geometric ladder; n <= 1 returns just lo.
func logspace(lo, hi time.Duration, n int) []time.Duration {
	if n <= 1 || lo <= 0 || hi <= lo {
		return []time.Duration{lo}
	}
	out := make([]time.Duration, n)
	ratio := math.Pow(float64(hi)/float64(lo), 1/float64(n-1))
	v := float64(lo)
	for i := 0; i < n; i++ {
		out[i] = time.Duration(v)
		v *= ratio
	}
	out[n-1] = hi // land exactly on hi despite float accumulation
	return out
}

// seriesCSVHeader is the fixed header of a per-(scenario, impl) result file:
// the base delay, the measured per-item interval (empty cell for a skipped
// point) and the point's minimum saturation fraction.
const seriesCSVHeader = "D_ms,interval_ms,min_sat_frac"

// WriteSeriesCSV renders one implementation's series as a raw-data table, one
// row per D point (see seriesCSVHeader).
func WriteSeriesCSV(w io.Writer, s Series) error {
	if _, err := fmt.Fprintln(w, seriesCSVHeader); err != nil {
		return err
	}
	for _, pt := range s.Points {
		interval := ""
		if !pt.Skipped {
			interval = fmt.Sprintf("%.4f", ms(pt.Interval))
		}
		if _, err := fmt.Fprintf(w, "%.4f,%s,%.2f\n", ms(pt.D), interval, pt.MinSatFrac); err != nil {
			return err
		}
	}
	return nil
}

// ReadSeriesCSV parses a file written by WriteSeriesCSV back into points (the
// series label is not stored in the file; the caller knows it from the file
// name's impl key).
func ReadSeriesCSV(r io.Reader) ([]Point, error) {
	sc := bufio.NewScanner(r)
	if !sc.Scan() {
		return nil, fmt.Errorf("empty file (want %q header)", seriesCSVHeader)
	}
	if got := strings.TrimSpace(sc.Text()); got != seriesCSVHeader {
		return nil, fmt.Errorf("unexpected header %q (want %q)", got, seriesCSVHeader)
	}
	var pts []Point
	for line := 2; sc.Scan(); line++ {
		fields := strings.Split(strings.TrimSpace(sc.Text()), ",")
		if len(fields) != 3 {
			return nil, fmt.Errorf("line %d: %d fields (want 3)", line, len(fields))
		}
		d, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return nil, fmt.Errorf("line %d: D_ms: %w", line, err)
		}
		pt := Point{D: fromMs(d), Skipped: true}
		if fields[1] != "" {
			iv, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				return nil, fmt.Errorf("line %d: interval_ms: %w", line, err)
			}
			pt.Interval = fromMs(iv)
			pt.Skipped = false
		}
		if pt.MinSatFrac, err = strconv.ParseFloat(fields[2], 64); err != nil {
			return nil, fmt.Errorf("line %d: min_sat_frac: %w", line, err)
		}
		pts = append(pts, pt)
	}
	return pts, sc.Err()
}

func ms(d time.Duration) float64     { return float64(d) / float64(time.Millisecond) }
func fromMs(v float64) time.Duration { return time.Duration(v * float64(time.Millisecond)) }
