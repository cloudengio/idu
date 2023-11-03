// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

/*
import (
	"context"
	"time"

	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/sync/errgroup"
	"cloudeng.io/sync/syncsort"
)

type lstatIssuer struct {
	fs             filewalk.FS
	pt             *progressTracker
	errLogger      lstatErrorLogger
	concurrency    int
	asyncThreshold int
	limitCh        chan struct{}
}

type lstatResult struct {
	info file.Info
	err  error
}

func initLimiter(concurrency int) chan struct{} {
	ch := make(chan struct{}, concurrency)
	for i := 0; i < cap(ch); i++ {
		ch <- struct{}{}
	}
	return ch
}

type lstatErrorLogger func(ctx context.Context, filename string, err error)

func newLStatIssuer(pt *progressTracker, errLogger lstatErrorLogger, concurrency, asyncThreshold int, fs filewalk.FS) *lstatIssuer {
	lsi := &lstatIssuer{
		errLogger:      errLogger,
		fs:             fs,
		pt:             pt,
		concurrency:    concurrency,
		asyncThreshold: asyncThreshold,
		limitCh:        initLimiter(concurrency),
	}
	return lsi
}

func (lsi *lstatIssuer) lstat(ctx context.Context, filename string) (file.Info, error) {
	start := time.Now()
	lsi.pt.statStarted()
	info, err := lsi.fs.LStat(ctx, filename)
	lsi.pt.statFinished(start)
	if err != nil {
		lsi.errLogger(ctx, filename, err)
	}
	return info, err
}

func (lsi *lstatIssuer) lstatContents(ctx context.Context, prefix string, contents []filewalk.Entry) (children, all file.InfoList, nFiles, nErrors int64, errs error) {
	if len(contents) < lsi.asyncThreshold {
		return lsi.syncIssue(ctx, prefix, contents)
	}
	return lsi.asyncIssue(ctx, prefix, contents)
}

func (lsi *lstatIssuer) syncIssue(ctx context.Context, prefix string, contents []filewalk.Entry) (children, all file.InfoList, nFiles, nErrors int64, err error) {
	for _, entry := range contents {
		filename := lsi.fs.Join(prefix, entry.Name)
		info, err := lsi.lstat(ctx, filename)
		if err != nil {
			nErrors++
			continue
		}
		if entry.IsDir() {
			children = append(children, info)
		} else {
			nFiles++
		}
		all = all.AppendInfo(info)
	}
	return children, all, nFiles, nErrors, nil
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

func (lsi *lstatIssuer) asyncIssue(ctx context.Context, prefix string, contents []filewalk.Entry) (children, all file.InfoList, nFiles, nErrors int64, err error) {
	concurrency := min(lsi.concurrency, len(contents))

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
		if err = lsi.waitOnLimiter(ctx); err != nil {
			return
		}
		g.Go(func() error {
			info, err := lsi.lstat(ctx, filename)
			item.V = lstatResult{info, err}
			ch <- item
			lsi.releaseLimiter()
			return nil
		})
	}
	g.Wait()
	close(ch)
	for seq.Scan() {
		res := seq.Item()
		if res.err != nil {
			nErrors++
			continue
		}
		if res.info.IsDir() {
			children = append(children, res.info)
		} else {
			nFiles++
		}
		all = all.AppendInfo(res.info)
	}
	err = seq.Err()
	return
}
*/
