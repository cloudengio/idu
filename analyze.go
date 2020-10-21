// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"cloudeng.io/cmd/idu/internal/exclusions"
	"cloudeng.io/cmdutil"
	"cloudeng.io/errors"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/path/cloudpath"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type analyzeFlags struct {
	Concurrency int  `subcmd:"concurrency,-1,number of threads to use for scanning"`
	Incremental bool `subcmd:"incremental,true,incremental mode uses the existing database to avoid as much unnecssary work as possible"`
}

// TODO(cnicolaou): determine a means of adding S3, GCP scanners etc without
// pulling in all of their dependencies into this package and module.
// For example, consider running them as external commands accessing the
// same database (eg. go run cloudeng.io/aws/filewalk ...).

type scanState struct {
	fs          filewalk.Filesystem
	exclusions  *exclusions.T
	progressCh  chan progressUpdate
	incremental bool
}

type progressUpdate struct {
	prefix int
	files  int
	reused int
}

func (sc *scanState) fileFn(ctx context.Context, prefix string, info *filewalk.Info, ch <-chan filewalk.Contents) ([]filewalk.Info, error) {
	pi := filewalk.PrefixInfo{
		ModTime: info.ModTime,
		UserID:  info.UserID,
		Mode:    info.Mode,
		Size:    info.Size,
	}
	calculator := globalConfig.CalculatorFor(prefix)
	debug(ctx, 2, "prefix: %v\n", prefix)
	for results := range ch {
		debug(ctx, 2, "result: %v %v\n", prefix, results.Err)
		if err := results.Err; err != nil {
			if sc.fs.IsPermissionError(err) {
				debug(ctx, 1, "permission denied: %v\n", prefix)
			} else {
				debug(ctx, 1, "error: %v: %v\n", prefix, err)
			}
			pi.Err = err.Error()
			break
		}
		for _, file := range results.Files {
			pi.DiskUsage += calculator.Calculate(file.Size)
			pi.Files = append(pi.Files, file)
		}
		pi.Children = append(pi.Children, results.Children...)
	}
	if err := globalDatabaseManager.Set(ctx, prefix, &pi); err != nil {
		return nil, err
	}
	if sc.progressCh != nil {
		sc.progressCh <- progressUpdate{prefix: 1, files: len(pi.Files)}
	}
	return pi.Children, nil
}

func (sc *scanState) prefixFn(ctx context.Context, prefix string, info *filewalk.Info, err error) (bool, []filewalk.Info, error) {
	if err != nil {
		if sc.fs.IsPermissionError(err) {
			debug(ctx, 1, "permission denied: %v\n", prefix)
			return true, nil, nil
		}
		debug(ctx, 1, "error: %v\n", prefix)
		return true, nil, err
	}
	if sc.exclusions.Exclude(prefix) {
		debug(ctx, 1, "exclude: %v\n", prefix)
		return true, nil, nil
	}
	if !sc.incremental {
		return false, nil, nil
	}

	var existing filewalk.PrefixInfo
	var unchanged bool
	ok, err := globalDatabaseManager.Get(ctx, prefix, &existing)
	if err == nil && ok {
		if existing.ModTime == info.ModTime &&
			filewalk.FileMode(existing.Mode) == info.Mode {
			unchanged = true
		}
	}
	if unchanged {
		if sc.progressCh != nil {
			sc.progressCh <- progressUpdate{reused: len(existing.Children)}
		}
		debug(ctx, 2, "unchanged: %v: #children: %v\n", prefix, len(existing.Children))
		// safe to skip unchanged leaf directories.
		return len(existing.Children) == 0, existing.Children, nil
	}
	return false, nil, nil
}

func isInteractive() bool {
	info, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func analyze(ctx context.Context, values interface{}, args []string) error {
	flagValues := values.(*analyzeFlags)
	start := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	exclusions := exclusions.New(globalConfig.Exclusions)
	for _, arg := range args {
		if !cloudpath.IsLocal(arg) {
			return fmt.Errorf("currently only local filesystems are supported: %v", arg)
		}
	}
	fs := filewalk.LocalFilesystem(1000)

	progressCh := make(chan progressUpdate, 100)
	var numPrefixes, numFiles, lastFiles, numReused int64
	sc := scanState{
		exclusions:  exclusions,
		fs:          fs,
		progressCh:  progressCh,
		incremental: flagValues.Incremental,
	}

	ifmt := message.NewPrinter(language.English)
	defer func() {
		ifmt.Printf("\n")
		ifmt.Printf("prefixes : % 15v\n", atomic.LoadInt64(&numPrefixes))
		ifmt.Printf("   files : % 15v\n", atomic.LoadInt64(&numFiles))
		ifmt.Printf("  reused : % 15v\n", atomic.LoadInt64(&numReused))
		ifmt.Printf("run time : % 15v\n", time.Since(start))
	}()

	cmdutil.HandleSignals(cancel, os.Interrupt, os.Kill)

	updateDuration := time.Second
	cr := "\r"
	if !isInteractive() {
		updateDuration = time.Second * 30
		cr = "\n"
	}

	go func() {
		lastReport := time.Now()
		for {
			select {
			case update := <-progressCh:
				atomic.AddInt64(&numPrefixes, int64(update.prefix))
				atomic.AddInt64(&numFiles, int64(update.files))
				atomic.AddInt64(&numReused, int64(update.reused))
			case <-ctx.Done():
				return
			}
			if since := time.Since(lastReport); since > updateDuration {
				last := atomic.SwapInt64(&lastFiles, numFiles)
				rate := float64(numFiles-last) / float64(since.Seconds())
				ifmt.Printf("% 15v prefixes, % 15v files, % 15v reused, % 8.2f stats/second  % 8v  %s",
					atomic.LoadInt64(&numPrefixes),
					atomic.LoadInt64(&numFiles),
					atomic.LoadInt64(&numReused),
					rate,
					time.Since(start).Truncate(time.Millisecond*500),
					cr)
				lastReport = time.Now()
			}
		}
	}()

	walker := filewalk.New(sc.fs, filewalk.Concurrency(flagValues.Concurrency))
	errs := errors.M{}
	errs.Append(walker.Walk(ctx, sc.prefixFn, sc.fileFn, args...))
	errs.Append(globalDatabaseManager.Close(ctx))
	cancel()
	return errs.Err()
}
