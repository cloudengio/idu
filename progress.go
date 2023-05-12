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
	"syscall"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type progressUpdate struct {
	prefixStart int
	prefixDone  int
	files       int
	deletions   int
	errors      int
	reused      int
}

type progressTracker struct {
	ch                                      chan progressUpdate
	numPrefixesStarted, numPrefixesFinished int64
	numFiles, numReused                     int64
	numDeletions, numErrors, lastFiles      int64
	interval                                time.Duration
	start                                   time.Time
	lastGC                                  time.Time
	memstats                                runtime.MemStats
	rusage                                  syscall.Rusage
}

func newProgressTracker(ctx context.Context, interval time.Duration) *progressTracker {
	pt := &progressTracker{
		ch:       make(chan progressUpdate, 10),
		interval: interval,
		start:    time.Now(),
	}
	pt.refreshMemstats()
	go pt.display(ctx)
	return pt
}

func (pt *progressTracker) send(ctx context.Context, u progressUpdate) {
	if pt == nil {
		return
	}
	select {
	case <-ctx.Done():
		return
	case pt.ch <- u:
	}
}

func (pt *progressTracker) refreshMemstats() {
	if time.Since(pt.lastGC) > (5 * time.Minute) {
		runtime.GC()
		runtime.ReadMemStats(&pt.memstats)
		syscall.Getrusage(0, &pt.rusage)
		pt.lastGC = time.Now()
	}
}

func (pt *progressTracker) summary() {
	pt.refreshMemstats()
	ifmt := message.NewPrinter(language.English)
	ifmt.Printf("\n")
	ifmt.Printf("        prefixes : % 15v\n", atomic.LoadInt64(&pt.numPrefixesFinished))
	ifmt.Printf("           files : % 15v\n", atomic.LoadInt64(&pt.numFiles))
	ifmt.Printf("prefix deletions : % 15v\n", atomic.LoadInt64(&pt.numDeletions))
	ifmt.Printf("          reused : % 15v\n", atomic.LoadInt64(&pt.numReused))
	ifmt.Printf("          errors : % 15v\n", atomic.LoadInt64(&pt.numErrors))
	ifmt.Printf("        run time : % 15v\n", time.Since(pt.start))
	ifmt.Printf("      heap alloc : % 15.6fGiB\n", float64(pt.memstats.HeapAlloc)/(1024*1024*1024))
	ifmt.Printf("  max heap alloc : % 15.6fGiB\n", float64(pt.memstats.HeapSys)/(1024*1024*1024))
	ifmt.Printf(" max process RSS : %15.6fGiB\n", float64(pt.rusage.Maxrss)/(1024*1024))
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
	for {
		select {
		case update := <-pt.ch:
			atomic.AddInt64(&pt.numPrefixesStarted, int64(update.prefixStart))
			atomic.AddInt64(&pt.numPrefixesFinished, int64(update.prefixDone))
			atomic.AddInt64(&pt.numFiles, int64(update.files))
			atomic.AddInt64(&pt.numDeletions, int64(update.deletions))
			atomic.AddInt64(&pt.numReused, int64(update.reused))
			atomic.AddInt64(&pt.numErrors, int64(update.errors))

			progressMap.Add("started", int64(update.prefixStart))
			progressMap.Add("finished", int64(update.prefixDone))
			progressMap.Add("files", int64(update.files))
			progressMap.Add("deletions", int64(update.deletions))
			progressMap.Add("reused", int64(update.reused))
			progressMap.Add("errors", int64(update.errors))
			fl := &expvar.Float{}
			fl.Set(float64(pt.memstats.HeapAlloc) / (1024 * 1024 * 1024))
			progressMap.Set("heap-alloc-GiB", fl)
			fl.Set(float64(pt.memstats.HeapSys) / (1024 * 1024 * 1024))
			progressMap.Set("max-heap-alloc-GiB", fl)
			fl.Set(float64(pt.rusage.Maxrss) / (1024 * 1024))
			progressMap.Set("max-RSS-GiB", fl)

		case <-ctx.Done():
			return
		}
		if since := time.Since(lastReport); since > pt.interval {
			last := atomic.SwapInt64(&pt.lastFiles, atomic.LoadInt64(&pt.numFiles))
			rate := float64(pt.numFiles-last) / since.Seconds()
			started, finished := atomic.LoadInt64(&pt.numPrefixesStarted), atomic.LoadInt64(&pt.numPrefixesFinished)
			ifmt.Printf("% 8v(%3v) prefixes, % 8v files, % 8v reused, % 6v errors, % 9.2f stats/second  % 8v, (%s)  (%3.6f/%3.6f/%3.6f GiB) %s",
				finished,
				started-finished,
				atomic.LoadInt64(&pt.numFiles),
				atomic.LoadInt64(&pt.numReused),
				atomic.LoadInt64(&pt.numErrors),
				rate,
				time.Since(pt.start).Truncate(time.Second),
				time.Now().Format("15:04:05"),
				float64(pt.memstats.HeapAlloc)/(1024*1024*1024),
				float64(pt.memstats.HeapSys)/(1024*1024*1024),
				float64(pt.rusage.Maxrss)/(1024*1024),
				cr)
			lastReport = time.Now()
		}
	}
}
