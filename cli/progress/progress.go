// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package progress provides the text based display of the progress of an
// analysis used by the analyze command, see NewAnalyze, and the memory
// statistics, see ReadMemory, also displayed by the tui.
package progress

import (
	"context"
	"os"
	"runtime"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/logging/ctxlog"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type progressStats struct {
	numPrefixesStarted, numPrefixesFinished int64
	numFiles                                int64
	numParentUnchanged                      int64
	numChildrenUnchanged                    int64
	numErrors                               int64
	numSyncScans                            int64
	numDeleted                              int64
	numStatsStarted, numStatsFinished       int64
	numSlowScans                            int64
	numRestated, numRestatedChanged         int64
	statsTotalTime                          int64
	start                                   time.Time
	lastGC                                  time.Time
	memstats                                runtime.MemStats
	sysMemstats                             *sysMemstats
}

var _ analyze.Progress = (*progressTracker)(nil)

type progressTracker struct {
	sync.Mutex
	stop     func()
	interval time.Duration
	progressStats
	displayUnchanged bool
}

func newProgressTracker(ctx context.Context, interval time.Duration, display, displayUnchanged bool, wg *sync.WaitGroup) *progressTracker {
	pt := &progressTracker{
		interval:         interval,
		displayUnchanged: displayUnchanged,
	}
	pt.start = time.Now()
	pt.sysMemstats = &sysMemstats{}
	pt.refreshMemstatsLocked()
	var once sync.Once
	ctx, cancel := context.WithCancel(ctx)
	pt.stop = func() {
		once.Do(func() {
			cancel()
			wg.Wait()
		})
	}
	if display {
		go func() {
			pt.display(ctx)
			wg.Done()
		}()
	} else {
		wg.Done()
	}
	return pt
}

func (pt progressStats) meanStatLatency() int64 {
	sum := pt.statsTotalTime
	count := pt.numStatsFinished
	if count > 0 {
		return sum / count
	}
	return 0
}

// NewAnalyze implements analyze.NewProgress. If display is true the
// progress is displayed on a single line, rewritten once a second, on an
// interactive terminal and on a new line every 30 seconds otherwise. A
// summary is displayed, and logged, by Report.
func NewAnalyze(ctx context.Context, display bool) analyze.Progress {
	var wg sync.WaitGroup
	wg.Add(1)
	return newProgressTracker(ctx, time.Second, display, true, &wg)
}

// Stop implements analyze.Progress.
func (pt *progressTracker) Stop() {
	pt.stop()
}

// Summary implements analyze.Progress.
func (pt *progressTracker) Summary() analyze.Summary {
	pt.Lock()
	cpy := pt.progressStats
	pt.Unlock()
	return analyze.Summary{
		PrefixesStarted:      cpy.numPrefixesStarted,
		PrefixesFinished:     cpy.numPrefixesFinished,
		SynchronousScans:     cpy.numSyncScans,
		SlowScans:            cpy.numSlowScans,
		FSStats:              cpy.numStatsFinished,
		FSStatsTotal:         cpy.statsTotalTime,
		FSStatMeanLatency:    cpy.meanStatLatency(),
		Files:                cpy.numFiles,
		ParentUnchanged:      cpy.numParentUnchanged,
		ChildrenUnchanged:    cpy.numChildrenUnchanged,
		Errors:               cpy.numErrors,
		PrefixesDeleted:      cpy.numDeleted,
		FilesRestated:        cpy.numRestated,
		FilesRestatedChanged: cpy.numRestatedChanged,
	}
}

// Implements idudb.ScanProgress
func (pt *progressTracker) Before() time.Time {
	pt.Lock()
	defer pt.Unlock()
	pt.numStatsStarted++
	return time.Now()
}

// Implements idudb.ScanProgress
func (pt *progressTracker) After(start time.Time) {
	took := time.Since(start)
	pt.Lock()
	defer pt.Unlock()
	pt.numStatsFinished++
	pt.statsTotalTime += int64(took)
}

func (pt *progressTracker) PrefixStarted() {
	pt.Lock()
	defer pt.Unlock()
	pt.numPrefixesStarted++
}

func (pt *progressTracker) PrefixDone(deleted int, files int64) {
	pt.Lock()
	defer pt.Unlock()
	pt.numPrefixesFinished++
	pt.numFiles += files
	pt.numDeleted += int64(deleted)
}

func (pt *progressTracker) Error() {
	pt.Lock()
	defer pt.Unlock()
	pt.numErrors++
}

func (pt *progressTracker) ParentUnchanged() {
	pt.Lock()
	defer pt.Unlock()
	pt.numParentUnchanged++
}

func (pt *progressTracker) ChildrenUnchanged() {
	pt.Lock()
	defer pt.Unlock()
	pt.numChildrenUnchanged++
}

func (pt *progressTracker) SlowScan() {
	pt.Lock()
	defer pt.Unlock()
	pt.numSlowScans++
}

func (pt *progressTracker) SyncScans(numSyncScans int64) {
	pt.Lock()
	defer pt.Unlock()
	if numSyncScans > pt.numSyncScans {
		pt.numSyncScans = numSyncScans
	}
}

func (pt *progressTracker) Restated(files, changed int) {
	pt.Lock()
	defer pt.Unlock()
	pt.numRestated += int64(files)
	pt.numRestatedChanged += int64(changed)
}

func (pt *progressTracker) refreshMemstatsLocked() {
	if time.Since(pt.lastGC) > (5 * time.Minute) {
		runtime.GC()
		runtime.ReadMemStats(&pt.memstats)
		pt.sysMemstats.update()
		pt.lastGC = time.Now()
	}
}

// Report implements analyze.Progress.
func (pt *progressTracker) Report(ctx context.Context) {
	pt.Lock()
	pt.refreshMemstatsLocked()
	cpy := pt.progressStats
	pt.Unlock()

	ifmt := message.NewPrinter(language.English)
	ifmt.Printf("\n")
	ifmt.Printf("          prefixes : % 15v\n", cpy.numPrefixesFinished)
	ifmt.Printf("             files : % 15v\n", cpy.numFiles)
	if pt.displayUnchanged {
		ifmt.Printf("  parent unchanged : % 15v\n", cpy.numParentUnchanged)
		ifmt.Printf("children unchanged : % 15v\n", cpy.numChildrenUnchanged)
	}
	ifmt.Printf("           deleted : % 15v\n", cpy.numDeleted)
	if cpy.numRestated > 0 {
		ifmt.Printf("    files restated : % 15v (%v changed)\n", cpy.numRestated, cpy.numRestatedChanged)
	}
	ifmt.Printf("            errors : % 15v\n", cpy.numErrors)
	ifmt.Printf("        sync scans : % 15v\n", cpy.numSyncScans)
	ifmt.Printf("        slow scans : % 15v\n", cpy.numSlowScans)
	ifmt.Printf("          stat ops : % 15v\n", cpy.numStatsFinished)
	ifmt.Printf("   total stat time : % 15v\n", time.Duration(cpy.statsTotalTime))
	ifmt.Printf(" mean stat latency : % 15v\n", time.Duration(cpy.meanStatLatency()))
	ifmt.Printf("          run time : % 15v\n", time.Since(cpy.start).Truncate(time.Second))
	ifmt.Printf("        heap alloc : % 15.6fGiB\n", float64(cpy.memstats.HeapAlloc)/(1024*1024*1024))
	ifmt.Printf("    max heap alloc : % 15.6fGiB\n", float64(cpy.memstats.HeapSys)/(1024*1024*1024))
	ifmt.Printf("   max process RSS : % 15.6fGiB\n", cpy.sysMemstats.MaxRSSGiB())
	cpy.log(ctx)
}

func (pt progressStats) log(ctx context.Context) {
	ctxlog.Info(ctx, "summary",
		"prefixes started", pt.numPrefixesStarted,
		"prefixes", pt.numPrefixesFinished,
		"deleted", pt.numDeleted,
		"files", pt.numFiles,
		"parent_unchanged", pt.numParentUnchanged,
		"children_unchanged", pt.numChildrenUnchanged,
		"errors", pt.numErrors,
		"sync_scans", pt.numSyncScans,
		"slow_scans", pt.numSlowScans,
		"files_restated", pt.numRestated,
		"files_restated_changed", pt.numRestatedChanged,
		"stat_ops", pt.numStatsFinished,
		"num_goroutines", runtime.NumGoroutine(),
		"run_time", time.Since(pt.start),
		"heap_alloc_GiB", float64(pt.memstats.HeapAlloc)/(1024*1024*1024),
		"max_heap_alloc_GiB", float64(pt.memstats.HeapSys)/(1024*1024*1024),
		"max_process_RSS_GiB", pt.sysMemstats.MaxRSSGiB())
}

func isInteractive() bool {
	info, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func (pt *progressTracker) display(ctx context.Context) {
	ifmt := message.NewPrinter(language.English)
	cr := "\r"
	if !isInteractive() {
		pt.interval = time.Second * 30
		cr = "\n"
	}
	lastReport := time.Now()
	var lastPrefixes, lastStats, lastSyncScans int64

	for {
		select {
		case <-time.After(pt.interval):
		case <-ctx.Done():
			return
		}
		pt.Lock()
		pt.refreshMemstatsLocked()
		cpy := pt.progressStats
		pt.Unlock()

		since := time.Since(lastReport)

		current := cpy.numPrefixesFinished
		prefixRate := (float64(current - lastPrefixes)) / since.Seconds()
		lastPrefixes = current

		current = cpy.numStatsFinished
		statRate := (float64(current - lastStats)) / since.Seconds()
		lastStats = current
		var statLatency time.Duration
		if current > 0 {
			statLatency = time.Duration(cpy.statsTotalTime / current)
		}

		current = cpy.numSyncScans
		syncRate := (float64(current - lastSyncScans)) / since.Seconds()
		lastSyncScans = current

		lastReport = time.Now()

		started, finished := cpy.numPrefixesStarted, cpy.numPrefixesFinished

		runningFor := time.Since(cpy.start).Truncate(time.Second)

		cpy.log(ctx)

		if pt.displayUnchanged {
			ifmt.Printf("% 10v(%3v) prefixes, % 10v files, % 8.0f (prefixes/s), % 8.0f (stats/second), % 8v (latency), % 6v (outstanding), % 8.0f (sync scans/s), % 8v unchanged, % 5v (slow scans), % 5v errors, % 8v, (%s) %s",
				finished,
				started-finished,
				cpy.numFiles,
				prefixRate,
				statRate,
				statLatency,
				cpy.numStatsStarted-cpy.numStatsFinished,
				syncRate,
				cpy.numParentUnchanged+cpy.numChildrenUnchanged,
				cpy.numSlowScans,
				cpy.numErrors,
				runningFor,
				time.Now().Format("15:04:05"),
				cr)
			continue
		}

		ifmt.Printf("% 10v(%3v) prefixes, % 10v files, % 8.0f (prefixes/s), % 8.0f (stats/second), % 8v (latency), % 6v (outstanding), % 8.0f (sync scans/s), % 5v (slow scans), % 5v errors, % 8v, (%s) %s",
			finished,
			started-finished,
			cpy.numFiles,
			prefixRate,
			statRate,
			statLatency,
			cpy.numStatsStarted-cpy.numStatsFinished,
			syncRate,
			cpy.numSlowScans,
			cpy.numErrors,
			runningFor,
			time.Now().Format("15:04:05"),
			cr)
	}
}
