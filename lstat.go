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

	limitCh chan struct{}
}

type lstatResult struct {
	info file.Info
	err  error
}

func initLimiter(cfg config.Prefix) chan struct{} {
	ch := make(chan struct{}, cfg.ConcurrentStats)
	for i := 0; i < cap(ch); i++ {
		ch <- struct{}{}
	}
	return ch
}

func newLStatIssuer(w *walker, cfg config.Prefix, fs filewalk.FS) *lstatIssuer {
	lsi := &lstatIssuer{
		cfg:     cfg,
		fs:      fs,
		w:       w,
		pt:      w.pt,
		limitCh: initLimiter(cfg),
	}
	return lsi
}

func (lsi *lstatIssuer) lstat(ctx context.Context, state *prefixState, prefix, filename string) (file.Info, error) {
	start := time.Now()
	lsi.pt.statStarted()
	info, err := lsi.fs.LStat(ctx, filename)
	lsi.pt.statFinished(start)
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
	return lsi.asyncIssue(ctx, state, prefix, contents)
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

func (lsi *lstatIssuer) waitOnLimiter(ctx context.Context) error {
	select {
	case <-lsi.limitCh:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (lsi *lstatIssuer) releaseLimiter() {
	lsi.limitCh <- struct{}{}
}

func (lsi *lstatIssuer) asyncIssue(ctx context.Context, state *prefixState, prefix string, contents []filewalk.Entry) (file.InfoList, error) {
	concurrency := min(lsi.w.cfg.ConcurrentStats, len(contents))

	g := &errgroup.T{}
	g = errgroup.WithConcurrency(g, concurrency)
	// The channel must be large enough to hold all of the items that
	// can be returned.
	ch := make(chan syncsort.Item[lstatResult], len(contents))
	seq := syncsort.NewSequencer(ctx, ch)
	for _, entry := range contents {
		name := entry.Name
		item := seq.NextItem(lstatResult{})
		filename := lsi.fs.Join(prefix, name)
		if err := lsi.waitOnLimiter(ctx); err != nil {
			return file.InfoList{}, err
		}
		g.Go(func() error {
			info, err := lsi.lstat(ctx, state, prefix, filename)
			item.V = lstatResult{info, err}
			ch <- item
			lsi.releaseLimiter()
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
