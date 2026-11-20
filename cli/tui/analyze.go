// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package tui

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
	"github.com/gdamore/tcell/v2"
	"github.com/navidys/tvxwidgets"
	"github.com/rivo/tview"
)

// progressInterval is how often the progress of an analysis is sampled
// and displayed.
const progressInterval = 500 * time.Millisecond

// historyLength is the number of samples displayed by the sparklines.
const historyLength = 240

// previousTotals describes the database before the analysis started.
type previousTotals struct {
	known    bool
	when     time.Time
	prefixes int64
	files    int64
}

// rate displays a rate of progress as a sparkline of its history and a
// gauge of its current value relative to the highest seen.
type rate struct {
	sparkline *tvxwidgets.Sparkline
	gauge     *tvxwidgets.UtilModeGauge
	layout    *tview.Flex
}

func newRate(title string, color tcell.Color) *rate {
	r := &rate{sparkline: tvxwidgets.NewSparkline(), gauge: tvxwidgets.NewUtilModeGauge()}
	r.sparkline.SetBorder(true).SetTitle(" " + title + " completed per second ")
	r.sparkline.SetLineColor(color)
	r.gauge.SetBorder(true).SetTitle(" " + title + "/s, relative to the peak ")
	// Rates are displayed relative to the peak, which is not a warning.
	r.gauge.SetWarnPercentage(101)
	r.gauge.SetCritPercentage(101)
	r.layout = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(r.sparkline, 0, 1, false).
		AddItem(r.gauge, 3, 0, false)
	return r
}

func (r *rate) update(history []float64, current, peak float64) {
	r.sparkline.SetData(history)
	r.sparkline.SetDataTitle(fmt.Sprintf("%v/s, peak %v/s", count(int64(current)), count(int64(peak))))
	r.gauge.SetLabel(fmt.Sprintf("%v/s ", count(int64(current))))
	pc := 0.0
	if peak > 0 {
		pc = 100 * current / peak
	}
	r.gauge.SetValue(pc)
}

func count(v int64) string { return strings.TrimSpace(cli.FmtCount(v)) }

type analyzeScreen struct {
	u           *ui
	layout      *tview.Flex
	info        *tview.TextView
	progress    *tvxwidgets.PercentageModeGauge
	prefixes    *rate
	files       *rate
	stats       *rate
	outstanding *tvxwidgets.BarChart
	events      *tvxwidgets.BarChart
	output      *tview.TextView

	mu       sync.Mutex
	cancel   context.CancelFunc
	previous previousTotals
}

// The bars displayed by the bar charts.
const (
	barPrefixes  = "prefixes"
	barStats     = "stats"
	barSyncScans = "sync scans"
	barErrors    = "errors"
	barSlow      = "slow"
	barDeleted   = "deleted"
	barUnchanged = "unchanged"
	barRestated  = "restated"
	// minBarScale is the minimum maximum value of the bar charts so that
	// small values are not displayed as full height bars.
	minBarScale = 10
)

func newAnalyzeScreen(u *ui) *analyzeScreen {
	a := &analyzeScreen{u: u}
	a.info = tview.NewTextView().SetDynamicColors(true)
	a.info.SetBorder(true).SetTitle(" Analyze ")
	a.progress = tvxwidgets.NewPercentageModeGauge()
	a.progress.SetBorder(true).SetTitle(" Prefixes scanned, relative to the previous analysis ")
	a.prefixes = newRate("Prefixes", tcell.ColorGreen)
	a.files = newRate("Files", tcell.ColorAqua)
	a.stats = newRate("Stats", tcell.ColorFuchsia)
	a.outstanding = tvxwidgets.NewBarChart()
	a.outstanding.SetBorder(true)
	a.outstanding.SetTitle(" Outstanding operations ")
	a.outstanding.AddBar(barPrefixes, 0, tcell.ColorGreen)
	a.outstanding.AddBar(barStats, 0, tcell.ColorFuchsia)
	a.outstanding.AddBar(barSyncScans, 0, tcell.ColorYellow)
	a.events = tvxwidgets.NewBarChart()
	a.events.SetBorder(true)
	a.events.SetTitle(" Events ")
	a.events.AddBar(barUnchanged, 0, tcell.ColorGreen)
	a.events.AddBar(barDeleted, 0, tcell.ColorYellow)
	a.events.AddBar(barRestated, 0, tcell.ColorAqua)
	a.events.AddBar(barSlow, 0, tcell.ColorOrange)
	a.events.AddBar(barErrors, 0, tcell.ColorRed)
	a.output = tview.NewTextView().SetDynamicColors(false).SetScrollable(true)
	a.output.SetBorder(true).SetTitle(" Output ")
	// The changed function is called by whichever goroutine writes to the
	// view, eg. analyze's, and hence must not update it directly. The
	// update is queued from a new goroutine so that a write from the tui's
	// goroutine cannot block on a full queue.
	a.output.SetChangedFunc(func() {
		go u.app.QueueUpdateDraw(func() { a.output.ScrollToEnd() })
	})
	rates := tview.NewFlex().
		AddItem(a.prefixes.layout, 0, 1, false).
		AddItem(a.files.layout, 0, 1, false).
		AddItem(a.stats.layout, 0, 1, false)
	charts := tview.NewFlex().
		AddItem(a.outstanding, 0, 1, false).
		AddItem(a.events, 0, 2, false)
	a.layout = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(a.info, 9, 0, false).
		AddItem(a.progress, 3, 0, false).
		AddItem(rates, 0, 3, false).
		AddItem(charts, 0, 2, false).
		AddItem(a.output, 6, 0, true)
	return a
}

func (a *analyzeScreen) input(ev *tcell.EventKey) *tcell.EventKey {
	switch ev.Key() {
	case tcell.KeyUp:
		// Up at the top of the output selects the menu bar.
		if row, _ := a.output.GetScrollOffset(); row > 0 {
			return ev
		}
		a.u.menu.Activate()
		return nil
	case tcell.KeyEscape:
		if a.cancelAnalysis() {
			a.u.setStatus("canceling the analysis, waiting for all filesystem operations to finish")
		} else {
			a.u.showView()
		}
		return nil
	case tcell.KeyRune:
		if a.u.isAnalyzing() {
			return ev
		}
		switch ev.Rune() {
		case 'a':
			a.u.startAnalyze(false)
			return nil
		case 'f':
			a.u.startAnalyze(true)
			return nil
		}
	}
	return ev
}

// cancelAnalysis cancels the running analysis, if any, and returns true
// if there was one.
func (a *analyzeScreen) cancelAnalysis() bool {
	a.mu.Lock()
	cancel := a.cancel
	a.mu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

// sample is a snapshot of the progress of an analysis.
type sample struct {
	analyze.Summary
	statsStarted int64
	elapsed      time.Duration
	done         bool
	// The rates, their peaks and histories for prefixes, files and stats.
	rates, peaks [3]float64
	history      [3][]float64
}

// render displays s. It must be called on the tui's goroutine.
func (a *analyzeScreen) render(s sample, kind string) {
	a.mu.Lock()
	prev := a.previous
	a.mu.Unlock()
	a.info.SetText(formatSample(s, prev, kind))
	if prev.known && prev.prefixes > 0 {
		a.progress.SetMaxValue(int(prev.prefixes))
		a.progress.SetValue(int(min(s.PrefixesFinished, prev.prefixes)))
	} else {
		a.progress.SetMaxValue(0)
	}
	for i, r := range []*rate{a.prefixes, a.files, a.stats} {
		r.update(s.history[i], s.rates[i], s.peaks[i])
	}
	setBars(a.outstanding, map[string]int64{
		barPrefixes:  s.PrefixesStarted - s.PrefixesFinished,
		barStats:     s.statsStarted - s.FSStats,
		barSyncScans: s.SynchronousScans,
	})
	setBars(a.events, map[string]int64{
		barUnchanged: s.ParentUnchanged,
		barDeleted:   s.PrefixesDeleted,
		barRestated:  s.FilesRestated,
		barSlow:      s.SlowScans,
		barErrors:    s.Errors,
	})
}

func formatSample(s sample, prev previousTotals, kind string) string {
	var b strings.Builder
	state, hints := "[green::b]running[-::-]", "escape: cancel"
	if s.done {
		state, hints = "[::b]completed[::-]", "escape: return to the view  a: analyze again  f: analyze --force"
	}
	fmt.Fprintf(&b, "%v %v for %v  [::d]%v[::-]\n\n", state, kind, s.elapsed.Truncate(time.Second), hints)
	previous := func(v int64) string {
		if !prev.known {
			return "unknown"
		}
		return count(v)
	}
	fmt.Fprintf(&b, "[::b]%-10s %16s %20s[::-]\n", "", "scanned", "previous analysis")
	fmt.Fprintf(&b, "%-10s %16s %20s\n", "prefixes", count(s.PrefixesFinished), previous(prev.prefixes))
	fmt.Fprintf(&b, "%-10s %16s %20s\n", "files", count(s.Files), previous(prev.files))
	fmt.Fprintf(&b, "%-10s %16s   mean stat latency %v\n", "stat ops", count(s.FSStats), time.Duration(s.FSStatMeanLatency).Truncate(time.Microsecond))
	when := "unknown"
	if prev.known && !prev.when.IsZero() {
		when = prev.when.Format(time.DateTime)
	}
	fmt.Fprintf(&b, "[::d]previous analysis: %v; errors %v, deleted %v, unchanged %v, restated %v (%v changed)[::-]",
		when, count(s.Errors), count(s.PrefixesDeleted), count(s.ParentUnchanged), count(s.FilesRestated), count(s.FilesRestatedChanged))
	return b.String()
}

func setBars(c *tvxwidgets.BarChart, values map[string]int64) {
	scale := int64(minBarScale)
	for name, v := range values {
		v = max(v, 0)
		c.SetBarValue(name, int(v))
		scale = max(scale, v)
	}
	c.SetMaxValue(int(scale))
}

// startAnalyze displays the analyze screen and runs an analysis, with
// --snapshot if the filesystem has a snapshot_dir, closing the database
// first and reopening it, and reloading the view, once it completes. If an
// analysis is already running the analyze screen is just displayed. It
// must be called on the tui's goroutine.
func (u *ui) startAnalyze(force bool) {
	a := u.analyze
	u.body.SwitchToPage(analyzePage)
	u.app.SetFocus(a.output)
	u.mu.Lock()
	if u.analyzing {
		u.mu.Unlock()
		return
	}
	u.analyzing = true
	done := make(chan struct{})
	u.analysisDone = done
	u.mu.Unlock()
	a.output.Clear()
	snapshot := u.cfg.fs.Config().SnapshotDir != ""
	kind := "analyze"
	if force {
		kind += " --force"
	}
	if snapshot {
		kind += " --snapshot"
	}
	ctx, cancel := context.WithCancel(u.ctx)
	a.mu.Lock()
	a.previous = u.previousTotals()
	a.cancel = cancel
	a.mu.Unlock()
	a.render(sample{}, kind)
	u.view.reset()
	u.closeSource()
	flags := &analyze.Flags{
		FSFlag:    cli.FSFlag{FS: u.cfg.fs.Config().Name},
		Progress:  true,
		Force:     force,
		Snapshot:  snapshot,
		SlowScans: idudb.DefaultSlowScanThreshold,
	}
	p := newTUIProgress(u, kind)
	go u.runAnalysis(ctx, cancel, done, p, flags)
}

// runAnalysis runs an analysis started by startAnalyze.
func (u *ui) runAnalysis(ctx context.Context, cancel context.CancelFunc, done chan struct{}, p *tuiProgress, flags *analyze.Flags) {
	defer close(done)
	a := u.analyze
	start := time.Now()
	err := u.cfg.analyze(ctx, p, a.output, flags)
	p.Stop()
	cancel()
	a.mu.Lock()
	a.cancel = nil
	a.mu.Unlock()
	u.mu.Lock()
	u.analyzing = false
	u.mu.Unlock()
	if u.isQuitting() {
		// The database is not reopened since the tui is exiting.
		return
	}
	if err != nil {
		fmt.Fprintf(a.output, "\nanalyze failed: %v\n", err)
		u.setStatus("analyze failed after %v, press escape to return to the view", time.Since(start).Truncate(time.Second))
		u.showError("analyze failed after %v:\n\n%v", time.Since(start).Truncate(time.Second), err)
	} else {
		u.setStatus("analyze completed in %v", time.Since(start).Truncate(time.Second))
	}
	if err := u.openSource(); err != nil {
		u.app.QueueUpdateDraw(func() { u.openFailed(err) })
		return
	}
	u.app.QueueUpdateDraw(func() {
		if err == nil {
			u.showView()
		}
		u.view.load(u.cfg.target)
	})
}

// previousTotals returns the totals for the filesystem recorded by the
// previous analysis, if known. It must be called before the database is
// closed for the analysis.
func (u *ui) previousTotals() previousTotals {
	src := u.source()
	if src == nil {
		return previousTotals{}
	}
	sum, err := src.Summary(u.ctx, src.Root())
	if err != nil || !sum.HasSubtree {
		return previousTotals{}
	}
	return previousTotals{
		known:    true,
		when:     src.LastUpdate(u.ctx),
		prefixes: sum.Subtree.Prefix,
		files:    sum.Subtree.Files,
	}
}

// tuiProgress implements analyze.Progress by displaying the progress of an
// analysis on the analyze screen.
type tuiProgress struct {
	u     *ui
	kind  string
	start time.Time
	done  chan struct{}
	once  sync.Once

	mu           sync.Mutex
	s            analyze.Summary
	statsStarted int64

	// smu guards the following, which are used to compute the rates.
	smu     sync.Mutex
	last    sample
	history [3][]float64
	peaks   [3]float64
}

func newTUIProgress(u *ui, kind string) *tuiProgress {
	p := &tuiProgress{u: u, kind: kind, start: time.Now(), done: make(chan struct{})}
	go p.display()
	return p
}

func (p *tuiProgress) display() {
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			s := p.sample(false)
			p.u.app.QueueUpdateDraw(func() { p.u.analyze.render(s, p.kind) })
		}
	}
}

// sample returns the current progress, updating the rates and their
// histories since the previous sample.
func (p *tuiProgress) sample(done bool) sample {
	p.mu.Lock()
	s := sample{Summary: p.s, statsStarted: p.statsStarted, elapsed: time.Since(p.start), done: done}
	p.mu.Unlock()
	if s.FSStats > 0 {
		s.FSStatMeanLatency = s.FSStatsTotal / s.FSStats
	}
	p.smu.Lock()
	defer p.smu.Unlock()
	if secs := (s.elapsed - p.last.elapsed).Seconds(); secs > 0 {
		current := [3]int64{s.PrefixesFinished, s.Files, s.FSStats}
		previous := [3]int64{p.last.PrefixesFinished, p.last.Files, p.last.FSStats}
		for i := range current {
			r := float64(current[i]-previous[i]) / secs
			s.rates[i] = r
			p.peaks[i] = max(p.peaks[i], r)
			p.history[i] = append(p.history[i], r)
			if n := len(p.history[i]); n > historyLength {
				p.history[i] = p.history[i][n-historyLength:]
			}
		}
	}
	for i := range p.history {
		s.history[i] = append([]float64(nil), p.history[i]...)
	}
	s.peaks = p.peaks
	p.last = s
	return s
}

func (p *tuiProgress) update(fn func(s *analyze.Summary)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fn(&p.s)
}

// Before implements analyze.Progress.
func (p *tuiProgress) Before() time.Time {
	p.mu.Lock()
	p.statsStarted++
	p.mu.Unlock()
	return time.Now()
}

// After implements analyze.Progress.
func (p *tuiProgress) After(start time.Time) {
	p.update(func(s *analyze.Summary) { s.FSStats++; s.FSStatsTotal += int64(time.Since(start)) })
}

// PrefixStarted implements analyze.Progress.
func (p *tuiProgress) PrefixStarted() { p.update(func(s *analyze.Summary) { s.PrefixesStarted++ }) }

// PrefixDone implements analyze.Progress.
func (p *tuiProgress) PrefixDone(deleted int, files int64) {
	p.update(func(s *analyze.Summary) {
		s.PrefixesFinished++
		s.Files += files
		s.PrefixesDeleted += int64(deleted)
	})
}

// Error implements analyze.Progress.
func (p *tuiProgress) Error() { p.update(func(s *analyze.Summary) { s.Errors++ }) }

// ParentUnchanged implements analyze.Progress.
func (p *tuiProgress) ParentUnchanged() { p.update(func(s *analyze.Summary) { s.ParentUnchanged++ }) }

// ChildrenUnchanged implements analyze.Progress.
func (p *tuiProgress) ChildrenUnchanged() {
	p.update(func(s *analyze.Summary) { s.ChildrenUnchanged++ })
}

// SlowScan implements analyze.Progress.
func (p *tuiProgress) SlowScan() { p.update(func(s *analyze.Summary) { s.SlowScans++ }) }

// SyncScans implements analyze.Progress.
func (p *tuiProgress) SyncScans(n int64) {
	p.update(func(s *analyze.Summary) { s.SynchronousScans = max(s.SynchronousScans, n) })
}

// Restated implements analyze.Progress.
func (p *tuiProgress) Restated(files, changed int) {
	p.update(func(s *analyze.Summary) {
		s.FilesRestated += int64(files)
		s.FilesRestatedChanged += int64(changed)
	})
}

// Stop implements analyze.Progress.
func (p *tuiProgress) Stop() {
	p.once.Do(func() { close(p.done) })
}

// Summary implements analyze.Progress.
func (p *tuiProgress) Summary() analyze.Summary {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.s
	if s.FSStats > 0 {
		s.FSStatMeanLatency = s.FSStatsTotal / s.FSStats
	}
	return s
}

// Report implements analyze.Progress, it displays the final progress.
func (p *tuiProgress) Report(context.Context) {
	s := p.sample(true)
	p.u.app.QueueUpdateDraw(func() { p.u.analyze.render(s, p.kind) })
}
