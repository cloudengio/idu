// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/sync/errgroup"
	"cloudeng.io/sync/syncsort"
)

type lstatIssuer struct {
	cfg config.Prefix
	fs  filewalk.FS
	w   *walker
	pt  *progressTracker

	statsMu sync.Mutex
	min     time.Duration
	max     time.Duration
}

type lstatResult struct {
	info file.Info
	err  error
}

func newLStatIssuer(w *walker, cfg config.Prefix, fs filewalk.FS) *lstatIssuer {
	return &lstatIssuer{
		cfg: cfg,
		fs:  fs,
		w:   w,
		pt:  w.pt,
	}
}

func (lsi *lstatIssuer) updateLatencyStats(start time.Time) {
	lsi.pt.updateStatLatency(start)
	lsi.statsMu.Lock()
	defer lsi.statsMu.Unlock()
	took := time.Since(start)
	lsi.min = min(lsi.min, took)
	lsi.max = max(lsi.max, took)

}

func (lsi *lstatIssuer) lstat(ctx context.Context, state *prefixState, prefix, filename string) (file.Info, error) {
	start := time.Now()
	info, err := lsi.fs.LStat(ctx, filename)
	lsi.updateLatencyStats(start)
	if err != nil {
		internal.Log(ctx, internal.LogError, "stat error", "prefix", lsi.cfg.Prefix, "file", filename, "error", err)
		lsi.w.dbLog(ctx, filename, []byte(err.Error()))
	}
	return info, err
}

func (lsi *lstatIssuer) lstatContents(ctx context.Context, state *prefixState, prefix string, contents []filewalk.Entry) (file.InfoList, error) {
	if len(contents) < lsi.cfg.ConcurrentStatsThreshold {
		return lsi.syncIssue(ctx, state, prefix, contents)
	}
	return lsi.asyncIssue(ctx, state, prefix, contents, lsi.w.cfg.ConcurrentStats)
}

func (lsi *lstatIssuer) syncIssue(ctx context.Context, state *prefixState, prefix string, contents []filewalk.Entry) (file.InfoList, error) {
	var children file.InfoList
	for _, entry := range contents {
		filename := lsi.fs.Join(prefix, entry.Name)
		info, err := lsi.lstat(ctx, state, prefix, filename)
		if err != nil {
			continue
		}
		if entry.IsDir() {
			atomic.AddInt64(&state.nchildren, 1)
			children = append(children, info)
		} else {
			atomic.AddInt64(&state.nfiles, 1)
		}
		state.current.AppendInfo(info)
	}
	return children, nil
}

func (lsi *lstatIssuer) asyncIssue(ctx context.Context, state *prefixState, prefix string, contents []filewalk.Entry, concurrency int) (file.InfoList, error) {
	g := &errgroup.T{}
	g = errgroup.WithConcurrency(g, min(concurrency, len(contents)))
	// The channel must be deep enough to hold all of the items that
	// can be returned.
	ch := make(chan syncsort.Item[lstatResult], len(contents))
	seq := syncsort.NewSequencer(ctx, ch)
	for _, entry := range contents {
		name := entry.Name
		item := seq.NextItem(lstatResult{})
		filename := lsi.fs.Join(prefix, name)
		g.Go(func() error {
			info, err := lsi.lstat(ctx, state, prefix, filename)
			item.V = lstatResult{info, err}
			ch <- item
			return nil
		})
	}
	g.Wait()
	close(ch)
	var children file.InfoList
	for seq.Scan() {
		res := seq.Item()
		if res.err != nil {
			continue
		}
		if res.info.IsDir() {
			atomic.AddInt64(&state.nchildren, 1)
			children = append(children, res.info)
		} else {
			atomic.AddInt64(&state.nfiles, 1)
		}
		state.current.AppendInfo(res.info)
	}
	return children, seq.Err()
}
