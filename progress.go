// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type progressUpdate struct {
	prefix int
	files  int
	errors int
	reused int
}

type progressTracker struct {
	ch                                                     chan progressUpdate
	numPrefixes, numFiles, lastFiles, numReused, numErrors int64
	interval                                               time.Duration
	start                                                  time.Time
}

func newProgressTracker(ctx context.Context, interval time.Duration) *progressTracker {
	pt := &progressTracker{
		ch:       make(chan progressUpdate, 10),
		interval: interval,
		start:    time.Now(),
	}
	go pt.display(ctx)
	return pt
}

func (pt *progressTracker) send(ctx context.Context, u progressUpdate) {
	select {
	case <-ctx.Done():
		return
	case pt.ch <- u:
	default:
		// drain stail updates if the channel is full.
		for {
			select {
			case <-pt.ch:
			default:
				return
			}
		}
	}
}

func (pt *progressTracker) summary() {
	ifmt := message.NewPrinter(language.English)
	ifmt.Printf("\n")
	ifmt.Printf("prefixes : % 15v\n", atomic.LoadInt64(&pt.numPrefixes))
	ifmt.Printf("   files : % 15v\n", atomic.LoadInt64(&pt.numFiles))
	ifmt.Printf("  reused : % 15v\n", atomic.LoadInt64(&pt.numReused))
	ifmt.Printf("  errors : % 15v\n", atomic.LoadInt64(&pt.numErrors))
	ifmt.Printf("run time : % 15v\n", time.Since(pt.start))
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
	for {
		select {
		case update := <-pt.ch:
			atomic.AddInt64(&pt.numPrefixes, int64(update.prefix))
			atomic.AddInt64(&pt.numFiles, int64(update.files))
			atomic.AddInt64(&pt.numReused, int64(update.reused))
			atomic.AddInt64(&pt.numErrors, int64(update.errors))
		case <-ctx.Done():
			return
		}
		if since := time.Since(lastReport); since > pt.interval {
			last := atomic.SwapInt64(&pt.lastFiles, atomic.LoadInt64(&pt.numFiles))
			rate := float64(pt.numFiles-last) / since.Seconds()
			ifmt.Printf("% 8v prefixes, % 8v files, % 8v reused, % 6v errors, % 8.2f stats/second  % 8v  %s",
				atomic.LoadInt64(&pt.numPrefixes),
				atomic.LoadInt64(&pt.numFiles),
				atomic.LoadInt64(&pt.numReused),
				atomic.LoadInt64(&pt.numErrors),
				rate,
				time.Since(pt.start).Truncate(time.Second),
				cr)
			lastReport = time.Now()
		}
	}
}
