// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"runtime"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type anaylzeSummary struct {
	Operation         string        `json:"operation"`
	Command           string        `json:"command"`
	Duration          time.Duration `json:"duration"`
	PrefixesStarted   int64         `json:"prefixes_started"`
	PrefixesFinished  int64         `json:"prefixes_finished"`
	SynchronousScans  int64         `json:"synchronous_scans"`
	FSStats           int64         `json:"fs_stats"`
	FSStatsTotal      int64         `json:"fs_stats_total"`
	FSStatMeanLatency int64         `json:"fs_stat_mean_latency"`
	Files             int64         `json:"files"`
	ParentUnchanged   int64         `json:"parent_unchanged"`
	ChildrenUnchanged int64         `json:"children_unchanged"`
	Errors            int64         `json:"errors"`
	PrefixesDeleted   int64         `json:"prefixes_deleted"`
}

type progressStats struct {
	numPrefixesStarted, numPrefixesFinished int64
	numFiles                                int64
	numParentUnchanged                      int64
	numChildrenUnchanged                    int64
	numErrors                               int64
	numSyncScans                            int64
	numDeleted                              int64
	numStatsStarted, numStatsFinished       int64
	statsTotalTime                          int64
	start                                   time.Time
	lastGC                                  time.Time
	memstats                                runtime.MemStats
	sysMemstats                             *sysMemstats
}

type progressTracker struct {
	sync.Mutex
	interval time.Duration
	progressStats
}

func newProgressTracker(ctx context.Context, interval time.Duration, display bool) *progressTracker {
	pt := &progressTracker{
		interval: interval,
	}
	pt.start = time.Now()
	pt.sysMemstats = &sysMemstats{}
	pt.refreshMemstatsLocked()
	if display {
		go pt.display(ctx)
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

func (pt *progressTracker) summarize() anaylzeSummary {
	pt.Lock()
	cpy := pt.progressStats
	pt.Unlock()
	return anaylzeSummary{
		PrefixesStarted:   cpy.numPrefixesStarted,
		PrefixesFinished:  cpy.numPrefixesFinished,
		SynchronousScans:  cpy.numSyncScans,
		FSStats:           cpy.numStatsFinished,
		FSStatsTotal:      cpy.statsTotalTime,
		FSStatMeanLatency: cpy.meanStatLatency(),
		Files:             cpy.numFiles,
		ParentUnchanged:   cpy.numParentUnchanged,
		ChildrenUnchanged: cpy.numChildrenUnchanged,
		Errors:            cpy.numErrors,
		PrefixesDeleted:   cpy.numDeleted,
	}
}

func (pt *progressTracker) statStarted() {
	pt.Lock()
	defer pt.Unlock()
	pt.numStatsStarted++
}

func (pt *progressTracker) statFinished(start time.Time) {
	took := time.Since(start)
	pt.Lock()
	defer pt.Unlock()
	pt.numStatsFinished++
	pt.statsTotalTime += int64(took)
}

func (pt *progressTracker) incStartPrefix() {
	pt.Lock()
	defer pt.Unlock()
	pt.numPrefixesStarted++
}

func (pt *progressTracker) incDonePrefix(errors, deleted int, files int64) {
	pt.Lock()
	defer pt.Unlock()
	pt.numPrefixesFinished++
	pt.numErrors += int64(errors)
	pt.numFiles += int64(files)
	pt.numDeleted += int64(deleted)
}

func (pt *progressTracker) incParentUnchanged() {
	pt.Lock()
	defer pt.Unlock()
	pt.numParentUnchanged++
}

func (pt *progressTracker) incChildrenUnchanged() {
	pt.Lock()
	defer pt.Unlock()
	pt.numChildrenUnchanged++
}

func (pt *progressTracker) setSyncScans(numSyncScans int64) {
	pt.Lock()
	defer pt.Unlock()
	if numSyncScans > 0 {
		pt.numSyncScans = numSyncScans
	}
}

func (pt *progressTracker) refreshMemstatsLocked() {
	if time.Since(pt.lastGC) > (5 * time.Minute) {
		runtime.GC()
		runtime.ReadMemStats(&pt.memstats)
		pt.sysMemstats.update()
		pt.lastGC = time.Now()
	}
}

func (pt *progressTracker) summary(ctx context.Context) {
	pt.Lock()
	pt.refreshMemstatsLocked()
	cpy := pt.progressStats
	pt.Unlock()

	ifmt := message.NewPrinter(language.English)
	ifmt.Printf("\n")
	ifmt.Printf("          prefixes : % 15v\n", cpy.numPrefixesFinished)
	ifmt.Printf("             files : % 15v\n", cpy.numFiles)
	ifmt.Printf("  parent unchanged : % 15v\n", cpy.numParentUnchanged)
	ifmt.Printf("children unchanged : % 15v\n", cpy.numChildrenUnchanged)
	ifmt.Printf("           deleted : % 15v\n", cpy.numDeleted)
	ifmt.Printf("            errors : % 15v\n", cpy.numErrors)
	ifmt.Printf("        sync scans : % 15v\n", cpy.numSyncScans)
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
	internal.Log(ctx, internal.LogProgress, "summary",
		"prefixes started", pt.numPrefixesStarted,
		"prefixes", pt.numPrefixesFinished,
		"deleted", pt.numDeleted,
		"files", pt.numFiles,
		"parent_unchanged", pt.numParentUnchanged,
		"children_unchanged", pt.numChildrenUnchanged,
		"errors", pt.numErrors,
		"sync_scans", pt.numSyncScans,
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
		prefixRate := (float64(current - lastPrefixes)) / float64(since.Seconds())
		lastPrefixes = current

		current = cpy.numStatsFinished
		statRate := (float64(current - lastStats)) / float64(since.Seconds())
		lastStats = current
		var statLatency time.Duration
		if current > 0 {
			statLatency = time.Duration(cpy.statsTotalTime / current)
		}

		current = cpy.numSyncScans
		syncRate := (float64(current - lastSyncScans)) / float64(since.Seconds())
		lastSyncScans = current

		lastReport = time.Now()

		started, finished := cpy.numPrefixesStarted, cpy.numPrefixesFinished

		runningFor := time.Since(cpy.start).Truncate(time.Second)

		cpy.log(ctx)

		ifmt.Printf("% 10v(%3v) prefixes, % 10v files, % 8.0f (prefixes/s), % 8.0f (stats/second), % 8v (latency), % 6v (outstanding), % 8.0f (sync scans/s), % 8v unchanged, % 5v errors, % 8v, (%s) %s",
			finished,
			started-finished,
			cpy.numFiles,
			prefixRate,
			statRate,
			statLatency,
			cpy.numStatsStarted-cpy.numStatsFinished,
			syncRate,
			cpy.numParentUnchanged+cpy.numChildrenUnchanged,
			cpy.numErrors,
			runningFor,
			time.Now().Format("15:04:05"),
			cr)
	}
}
