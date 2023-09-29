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

type progressTracker struct {
	numPrefixesStarted, numPrefixesFinished int64
	numFiles, numUnchanged                  int64
	numErrors                               int64
	syncScans                               int64
	numStats                                int64
	interval                                time.Duration
	start                                   time.Time
	lastGC                                  time.Time
	memstats                                runtime.MemStats
	sysMemstats                             *sysMemstats
}

func newProgressTracker(ctx context.Context, interval time.Duration) *progressTracker {
	pt := &progressTracker{
		interval:    interval,
		start:       time.Now(),
		sysMemstats: &sysMemstats{},
	}
	pt.refreshMemstats()
	go pt.display(ctx)
	return pt
}

func (pt *progressTracker) startPrefix() {
	atomic.AddInt64(&pt.numPrefixesStarted, 1)
}

func (pt *progressTracker) donePrefix(errors, files, stats int) {
	atomic.AddInt64(&pt.numPrefixesFinished, 1)
	atomic.AddInt64(&pt.numErrors, int64(errors))
	atomic.AddInt64(&pt.numFiles, int64(files))
	atomic.AddInt64(&pt.numStats, int64(stats))
}

func (pt *progressTracker) unchanged() {
	atomic.AddInt64(&pt.numUnchanged, 1)
}

func (pt *progressTracker) walkerStats(syncScans int64) {
	if syncScans > 0 {
		atomic.StoreInt64(&pt.syncScans, syncScans)
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
	ifmt.Printf("        prefixes : % 15v\n", atomic.LoadInt64(&pt.numPrefixesFinished))
	ifmt.Printf("           files : % 15v\n", atomic.LoadInt64(&pt.numFiles))
	ifmt.Printf("       unchanged : % 15v\n", atomic.LoadInt64(&pt.numUnchanged))
	ifmt.Printf("          errors : % 15v\n", atomic.LoadInt64(&pt.numErrors))
	ifmt.Printf("      sync scans : % 15v\n", atomic.LoadInt64(&pt.syncScans))
	ifmt.Printf("        stat ops : % 15v\n", atomic.LoadInt64(&pt.numStats))
	ifmt.Printf("        run time : % 15v\n", time.Since(pt.start).Truncate(time.Second))
	ifmt.Printf("      heap alloc : % 15.6fGiB\n", float64(pt.memstats.HeapAlloc)/(1024*1024*1024))
	ifmt.Printf("  max heap alloc : % 15.6fGiB\n", float64(pt.memstats.HeapSys)/(1024*1024*1024))
	ifmt.Printf(" max process RSS : % 15.6fGiB\n", pt.sysMemstats.MaxRSSGiB())
	pt.log(ctx)
}
func (pt *progressTracker) log(ctx context.Context) {
	internal.Log(ctx, globalLogger, internal.LogPrefix, "summary",
		"prefixes started", atomic.LoadInt64(&pt.numPrefixesStarted),
		"prefixes", atomic.LoadInt64(&pt.numPrefixesFinished),
		"files", atomic.LoadInt64(&pt.numFiles),
		"unchanged", atomic.LoadInt64(&pt.numUnchanged),
		"errors", atomic.LoadInt64(&pt.numErrors),
		"sync scans", atomic.LoadInt64(&pt.syncScans),
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
	var lastPrefixes, lastStats int64

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

		lastReport = time.Now()

		started, finished := atomic.LoadInt64(&pt.numPrefixesStarted), atomic.LoadInt64(&pt.numPrefixesFinished)

		runningFor := time.Since(pt.start).Truncate(time.Second)

		ifmt.Printf("% 8v(%3v) prefixes, % 8v files, % 9.0f (prefixes/s), % 9.0f (stats/second) % 8v unchanged, % 5v errors,% 8v, (%s) %s",
			finished,
			started-finished,
			atomic.LoadInt64(&pt.numFiles),
			prefixRate,
			statRate,
			atomic.LoadInt64(&pt.numUnchanged),
			atomic.LoadInt64(&pt.numErrors),
			runningFor,
			time.Now().Format("15:04:05"),
			cr)
		pt.log(ctx)
	}
}
