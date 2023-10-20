// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"expvar"
	"os"
	"runtime"
	"sync/atomic"
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

type progressTracker struct {
	numPrefixesStarted, numPrefixesFinished int64
	numFiles                                int64
	numParentUnchanged                      int64
	numChildrenUnchanged                    int64
	numErrors                               int64
	numSyncScans                            int64
	numDeleted                              int64
	numStats                                int64
	statsTotalTime                          int64
	interval                                time.Duration
	start                                   time.Time
	lastGC                                  time.Time
	memstats                                runtime.MemStats
	sysMemstats                             *sysMemstats
}

func newProgressTracker(ctx context.Context, interval time.Duration, display bool) *progressTracker {
	pt := &progressTracker{
		interval:    interval,
		start:       time.Now(),
		sysMemstats: &sysMemstats{},
	}
	pt.refreshMemstats()
	if display {
		go pt.display(ctx)
	}
	return pt
}

func (pt *progressTracker) meanStatLatency() int64 {
	sum := atomic.LoadInt64(&pt.statsTotalTime)
	count := atomic.LoadInt64(&pt.numStats)
	if count > 0 {
		return sum / count
	}
	return 0
}
func (pt *progressTracker) summarize() anaylzeSummary {
	return anaylzeSummary{
		PrefixesStarted:   atomic.LoadInt64(&pt.numPrefixesStarted),
		PrefixesFinished:  atomic.LoadInt64(&pt.numPrefixesFinished),
		SynchronousScans:  atomic.LoadInt64(&pt.numSyncScans),
		FSStats:           atomic.LoadInt64(&pt.numStats),
		FSStatsTotal:      atomic.LoadInt64(&pt.statsTotalTime),
		FSStatMeanLatency: pt.meanStatLatency(),
		Files:             atomic.LoadInt64(&pt.numFiles),
		ParentUnchanged:   atomic.LoadInt64(&pt.numParentUnchanged),
		ChildrenUnchanged: atomic.LoadInt64(&pt.numChildrenUnchanged),
		Errors:            atomic.LoadInt64(&pt.numErrors),
		PrefixesDeleted:   atomic.LoadInt64(&pt.numDeleted),
	}
}

func (pt *progressTracker) updateStatLatency(start time.Time) {
	took := time.Since(start)
	atomic.AddInt64(&pt.numStats, 1)
	atomic.AddInt64(&pt.statsTotalTime, int64(took))
}

func (pt *progressTracker) incStartPrefix() {
	atomic.AddInt64(&pt.numPrefixesStarted, 1)
}

func (pt *progressTracker) incDonePrefix(errors, deleted int, files int64) {
	atomic.AddInt64(&pt.numPrefixesFinished, 1)
	atomic.AddInt64(&pt.numErrors, int64(errors))
	atomic.AddInt64(&pt.numFiles, int64(files))
	atomic.AddInt64(&pt.numDeleted, int64(deleted))
}

func (pt *progressTracker) incParentUnchanged() {
	atomic.AddInt64(&pt.numParentUnchanged, 1)
}

func (pt *progressTracker) incChildrenUnchanged() {
	atomic.AddInt64(&pt.numChildrenUnchanged, 1)
}

func (pt *progressTracker) setSyncScans(numSyncScans int64) {
	if numSyncScans > 0 {
		atomic.StoreInt64(&pt.numSyncScans, numSyncScans)
	}
}

func (pt *progressTracker) refreshMemstats() bool {
	if time.Since(pt.lastGC) > (5 * time.Minute) {
		runtime.GC()
		runtime.ReadMemStats(&pt.memstats)
		pt.sysMemstats.update()
		pt.lastGC = time.Now()
		return true
	}
	return false
}

func (pt *progressTracker) summary(ctx context.Context) {
	pt.refreshMemstats()
	ifmt := message.NewPrinter(language.English)
	ifmt.Printf("\n")
	ifmt.Printf("          prefixes : % 15v\n", atomic.LoadInt64(&pt.numPrefixesFinished))
	ifmt.Printf("             files : % 15v\n", atomic.LoadInt64(&pt.numFiles))
	ifmt.Printf("  parent unchanged : % 15v\n", atomic.LoadInt64(&pt.numParentUnchanged))
	ifmt.Printf("children unchanged : % 15v\n", atomic.LoadInt64(&pt.numChildrenUnchanged))
	ifmt.Printf("           deleted : % 15v\n", atomic.LoadInt64(&pt.numDeleted))
	ifmt.Printf("            errors : % 15v\n", atomic.LoadInt64(&pt.numErrors))
	ifmt.Printf("        sync scans : % 15v\n", atomic.LoadInt64(&pt.numSyncScans))
	ifmt.Printf("          stat ops : % 15v\n", atomic.LoadInt64(&pt.numStats))
	ifmt.Printf("   total stat time : % v\n", time.Duration(atomic.LoadInt64(&pt.statsTotalTime)))
	ifmt.Printf(" mean stat latency : % 15v\n", time.Duration(pt.meanStatLatency()))
	ifmt.Printf("          run time : % 15v\n", time.Since(pt.start).Truncate(time.Second))
	ifmt.Printf("        heap alloc : % 15.6fGiB\n", float64(pt.memstats.HeapAlloc)/(1024*1024*1024))
	ifmt.Printf("    max heap alloc : % 15.6fGiB\n", float64(pt.memstats.HeapSys)/(1024*1024*1024))
	ifmt.Printf("   max process RSS : % 15.6fGiB\n", pt.sysMemstats.MaxRSSGiB())
	pt.log(ctx)
}
func (pt *progressTracker) log(ctx context.Context) {
	internal.Log(ctx, internal.LogProgress, "summary",
		"prefixes started", atomic.LoadInt64(&pt.numPrefixesStarted),
		"prefixes", atomic.LoadInt64(&pt.numPrefixesFinished),
		"deleted", atomic.LoadInt64(&pt.numDeleted),
		"files", atomic.LoadInt64(&pt.numFiles),
		"parent unchanged", atomic.LoadInt64(&pt.numParentUnchanged),
		"children unchanged", atomic.LoadInt64(&pt.numChildrenUnchanged),
		"errors", atomic.LoadInt64(&pt.numErrors),
		"sync scans", atomic.LoadInt64(&pt.numSyncScans),
		"stat ops", atomic.LoadInt64(&pt.numStats),
		"run time", time.Since(pt.start),
		"heap alloc GiB", float64(pt.memstats.HeapAlloc)/(1024*1024*1024),
		"max heap alloc GiB", float64(pt.memstats.HeapSys)/(1024*1024*1024),
		"max process RSS GiB", pt.sysMemstats.MaxRSSGiB())
}

func isInteractive() bool {
	info, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

var progressMap = expvar.NewMap("cloudeng.io/idu.progress")

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
		if pt.refreshMemstats() {
			fl := &expvar.Float{}
			fl.Set(float64(pt.memstats.HeapAlloc) / (1024 * 1024 * 1024))
			progressMap.Set("heap-alloc-GiB", fl)
			fl.Set(float64(pt.memstats.HeapSys) / (1024 * 1024 * 1024))
			progressMap.Set("max-heap-alloc-GiB", fl)
			fl.Set(pt.sysMemstats.MaxRSSGiB())
			progressMap.Set("max-RSS-GiB", fl)
		}

		since := time.Since(lastReport)

		current := atomic.LoadInt64(&pt.numPrefixesFinished)
		prefixRate := (float64(current - lastPrefixes)) / float64(since.Seconds())
		lastPrefixes = current

		current = atomic.LoadInt64(&pt.numStats)
		statRate := (float64(current - lastStats)) / float64(since.Seconds())
		lastStats = current
		var statLatency time.Duration
		if current > 0 {
			statLatency = time.Duration(atomic.LoadInt64(&pt.statsTotalTime) / current)
		}

		current = atomic.LoadInt64(&pt.numSyncScans)
		syncRate := (float64(current - lastSyncScans)) / float64(since.Seconds())
		lastSyncScans = current

		lastReport = time.Now()

		started, finished := atomic.LoadInt64(&pt.numPrefixesStarted), atomic.LoadInt64(&pt.numPrefixesFinished)

		runningFor := time.Since(pt.start).Truncate(time.Second)

		ifmt.Printf("% 8v(%3v) prefixes, % 8v files, % 6.0f (prefixes/s), % 6.0f (stats/second), %v (latency), % 6.0f (sync scans/s), % 8v unchanged, % 5v errors, % 8v, (%s) %s",
			finished,
			started-finished,
			atomic.LoadInt64(&pt.numFiles),
			prefixRate,
			statRate,
			statLatency,
			syncRate,
			atomic.LoadInt64(&pt.numChildrenUnchanged),
			atomic.LoadInt64(&pt.numErrors),
			runningFor,
			time.Now().Format("15:04:05"),
			cr)
		pt.log(ctx)
	}
}
