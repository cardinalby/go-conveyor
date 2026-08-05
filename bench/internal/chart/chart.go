// Package chart renders bench.Result values to PNG line charts.
package chart

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"

	"github.com/cardinalby/go-conveyor/bench/internal/bench"
)

// Save renders one PNG for res. X is the base delay D in milliseconds; Y is the
// steady-state throughput in items/sec (the inverse of the measured per-item
// completion interval, higher = better). Both axes are logarithmic, so the
// small-D region — where coordination overhead is visible next to the work —
// gets as much visual room as the large-D tail. The D=0 point (pure overhead,
// see bench.Run) cannot sit on a log axis, so it is drawn at half the smallest
// nonzero D under a tick labeled "0". The two series — conveyor and
// traditional — are plotted together so their gap (the conveyor's extra
// coordination overhead) is read directly; where they overlap, the conveyor is
// effectively free. Skipped points are omitted.
func Save(res bench.Result, path string) error {
	p := plot.New()
	p.Title.Text = res.ScenarioName
	p.X.Label.Text = "base delay D (ms)"
	p.Y.Label.Text = "throughput (items/sec)"
	p.X.Scale = plot.LogScale{}
	p.Y.Scale = plot.LogScale{}
	zeroX := zeroPos(res)
	p.X.Tick.Marker = logTicksWithZero(zeroX)
	p.Y.Tick.Marker = logTicksDense{}
	p.Add(plotter.NewGrid())

	var args []any
	for _, s := range res.Series {
		pts := make(plotter.XYs, 0, len(s.Points))
		for _, pt := range s.Points {
			if pt.Skipped || pt.Interval <= 0 {
				continue
			}
			x := float64(pt.D) / 1e6 // ns -> ms
			if pt.D == 0 {
				if zeroX == 0 {
					continue // no nonzero D to anchor the pseudo-position to
				}
				x = zeroX
			}
			pts = append(pts, plotter.XY{
				X: x,
				Y: float64(time.Second) / float64(pt.Interval), // interval -> items/sec
			})
		}
		if len(pts) == 0 {
			continue
		}
		args = append(args, s.Label, pts)
	}
	if len(args) == 0 {
		return fmt.Errorf("chart.Save(%s): no data points to plot", res.ScenarioName)
	}
	if err := plotutil.AddLinePoints(p, args...); err != nil {
		return fmt.Errorf("chart.Save(%s): %w", res.ScenarioName, err)
	}
	p.Legend.Top = true
	p.Legend.Left = true

	if err := p.Save(9*vg.Inch, 5*vg.Inch, path); err != nil {
		return fmt.Errorf("chart.Save(%s): %w", res.ScenarioName, err)
	}
	return nil
}

// zeroPos returns the X position (ms) standing in for D=0 on the log axis: half
// the smallest nonzero measured D, or 0 when there is none.
func zeroPos(res bench.Result) float64 {
	minD := time.Duration(0)
	for _, s := range res.Series {
		for _, pt := range s.Points {
			if pt.D > 0 && (minD == 0 || pt.D < minD) {
				minD = pt.D
			}
		}
	}
	return float64(minD) / 1e6 / 2
}

// logTicksWithZero is the standard log ticker plus one extra tick labeled "0" at
// the pseudo-position representing the D=0 point (skipped when zeroX is 0).
func logTicksWithZero(zeroX float64) plot.TickerFunc {
	return func(min, max float64) []plot.Tick {
		ticks := plot.LogTicks{Prec: -1}.Ticks(min, max)
		if zeroX > 0 {
			ticks = append(ticks, plot.Tick{Value: zeroX, Label: "0"})
		}
		return ticks
	}
}

// logTicksDense is the standard log ticker with the 2× and 5× minor ticks of
// each decade labeled as well (…, 100, 200, 500, 1000, 2000, 5000, …), so a
// log axis spanning several decades reads without mantissa guesswork.
type logTicksDense struct{}

func (logTicksDense) Ticks(min, max float64) []plot.Tick {
	ticks := plot.LogTicks{Prec: -1}.Ticks(min, max)
	for i, t := range ticks {
		if t.Label != "" || t.Value <= 0 {
			continue
		}
		exp := math.Floor(math.Log10(t.Value))
		mant := t.Value / math.Pow(10, exp)
		if math.Abs(mant-2) < 1e-9 || math.Abs(mant-5) < 1e-9 {
			ticks[i].Label = strconv.FormatFloat(t.Value, 'f', -1, 64)
		}
	}
	return ticks
}
