// Command bench measures the per-item throughput cost of the conveyor against a
// hand-rolled channels/goroutines pipeline that is functionally equivalent, over
// three topologies (linear, one shared stage, one fan-out stage) and a sweep of
// the base delay D. Both pipelines are driven through the same harness and
// measured from the same observer data (see the pipeline package), so each chart
// isolates the conveyor's coordination overhead relative to hand-written code.
//
// It runs in two modes:
//
//   - measuring child (-impl=chans|conv): sweeps every scenario with that one
//     implementation and writes raw data to <out>/<scenario>_<impl>.csv.
//   - orchestrating parent (no -impl): re-executes itself once per
//     implementation — each variant measures in a FRESH process, so no shared
//     Go runtime state (heap/GC pacing, OS threads, timer pressure) couples one
//     variant's numbers to the other's — then reads the children's CSV files
//     and renders one <out>/<scenario>.png chart per scenario.
//
// Usage:
//
//	cd pkg/basics/conveyor/bench && go run .
//
// Flags tune the sweep and per-point sampling; run `go run . -h` for the list.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/cardinalby/go-conveyor/bench/internal/bench"
	"github.com/cardinalby/go-conveyor/bench/internal/chart"
)

func main() {
	cfg := bench.Config{}
	var limit int
	var resultsDir, implKey string
	flag.IntVar(&cfg.N, "n", 400, "items pushed through the pipeline per run")
	flag.IntVar(&cfg.Drop, "drop", 100, "items trimmed at each end for the steady-state window")
	flag.IntVar(&cfg.Repeats, "repeats", 3, "runs per data point; the best (lowest) interval is kept")
	flag.DurationVar(&cfg.DMin, "dmin", 10*time.Microsecond, "smallest nonzero base delay D")
	flag.DurationVar(&cfg.DMax, "dmax", 100*time.Millisecond, "largest base delay D")
	flag.IntVar(&cfg.Steps, "steps", 50, "number of nonzero D values (log-spaced from dmin to dmax)")
	flag.IntVar(&limit, "limit", 50, "shared-stage / fan-out-branch concurrency limit")
	flag.StringVar(&resultsDir, "out", "results", "output directory for CSV and PNG files")
	flag.StringVar(&implKey, "impl", "", "measure only this variant (chans|conv) and write <scenario>_<impl>.csv; empty = orchestrate both in child processes and render charts")
	flag.Parse()

	if cfg.N-2*cfg.Drop < 2 {
		fatalf("n (%d) must exceed 2*drop (%d) by at least 2", cfg.N, 2*cfg.Drop)
	}
	if err := os.MkdirAll(resultsDir, 0o755); err != nil {
		fatalf("mkdir: %v", err)
	}

	scs := scenarios(limit)
	if implKey != "" {
		im, ok := bench.ImplByKey(implKey)
		if !ok {
			fatalf("unknown -impl %q (want chans or conv)", implKey)
		}
		runChild(scs, im, cfg, resultsDir)
		return
	}
	runParent(scs, cfg, resultsDir)
}

// runChild measures every scenario with one implementation and writes the raw
// per-scenario CSV files.
func runChild(scs []bench.Scenario, im bench.Impl, cfg bench.Config, resultsDir string) {
	fmt.Printf("[%s] sweep: %d scenarios x %d D-values, n=%d, repeats=%d\n",
		im.Key, len(scs), len(cfg.DValues()), cfg.N, cfg.Repeats)
	for _, sc := range scs {
		fmt.Printf("[%s] running %s ...\n", im.Key, sc.Name)
		minSat := 1.0
		series := bench.RunImpl(sc, im, cfg, func(_ time.Duration, pt bench.Point) {
			if !pt.Skipped && pt.MinSatFrac < minSat {
				minSat = pt.MinSatFrac
			}
		})
		if len(sc.Sat) > 0 && minSat < 0.8 {
			fmt.Printf("[%s]   ⚠ saturation only reached %.0f%% of limit — the concurrent node is under-loaded\n",
				im.Key, minSat*100)
		}
		path := seriesPath(resultsDir, sc.Name, im.Key)
		if err := writeSeriesFile(path, series); err != nil {
			fatalf("%v", err)
		}
		fmt.Printf("[%s]   wrote %s\n", im.Key, path)
	}
}

// runParent re-executes this binary once per implementation (sequentially, each
// in a fresh process), then assembles the children's CSV files into charts.
func runParent(scs []bench.Scenario, cfg bench.Config, resultsDir string) {
	fmt.Printf("sweep: %d scenarios x %d impls x %d D-values (0, %v..%v) x %d repeats, n=%d\n",
		len(scs), len(bench.Impls()), len(cfg.DValues()), cfg.DMin, cfg.DMax, cfg.Repeats, cfg.N)
	fmt.Printf("rough runtime estimate: %v\n\n", estimateRuntime(scs, cfg))

	exe, err := os.Executable()
	if err != nil {
		fatalf("cannot locate own binary: %v", err)
	}
	for _, im := range bench.Impls() {
		cmd := exec.Command(exe, append(os.Args[1:], "-impl="+im.Key)...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fatalf("child -impl=%s: %v", im.Key, err)
		}
	}

	for _, sc := range scs {
		res := bench.Result{ScenarioName: sc.Name}
		for _, im := range bench.Impls() {
			pts, err := readSeriesFile(seriesPath(resultsDir, sc.Name, im.Key))
			if err != nil {
				fatalf("%v", err)
			}
			res.Series = append(res.Series, bench.Series{Label: im.Label, Points: pts})
		}
		pngPath := filepath.Join(resultsDir, sc.Name+".png")
		if err := chart.Save(res, pngPath); err != nil {
			fatalf("%v", err)
		}
		fmt.Printf("wrote %s\n", pngPath)
	}
}

// seriesPath names one (scenario, impl) raw-data file, e.g. results/fanout_conv.csv.
func seriesPath(dir, scenario, implKey string) string {
	return filepath.Join(dir, fmt.Sprintf("%s_%s.csv", scenario, implKey))
}

func writeSeriesFile(path string, s bench.Series) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := bench.WriteSeriesCSV(f, s); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return f.Close()
}

func readSeriesFile(path string) ([]bench.Point, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	pts, err := bench.ReadSeriesCSV(f)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return pts, nil
}

// estimateRuntime gives a rough wall-clock estimate: each run is dominated by
// N items at ~D each plus a fill of ~limit*D, summed over the sweep's D values
// (see bench.Config.DValues), both impls, all repeats and scenarios. It is
// deliberately approximate.
func estimateRuntime(scs []bench.Scenario, cfg bench.Config) time.Duration {
	var total time.Duration
	for _, d := range cfg.DValues() {
		perRun := time.Duration(cfg.N)*d + 5*d // items + small fill/drain
		total += perRun * time.Duration(len(scs)*len(bench.Impls())*cfg.Repeats)
	}
	return total.Round(time.Second)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
