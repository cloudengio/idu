// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/errors"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/asyncstat"
	"cloudeng.io/file/localfs"
	"cloudeng.io/logging/ctxlog"
)

func showDefaults() {
	fmt.Printf("scan_size: %v\n", filewalk.DefaultScanSize)
	fmt.Printf("concurrent_scans: %v\n", filewalk.DefaultConcurrentScans)
	fmt.Printf("concurrent_stats: %v\n", asyncstat.DefaultAsyncStats)
	fmt.Printf("concurrent_stats_threshold: %v\n", asyncstat.DefaultAsyncThreshold)
}

type analyzeFlags struct {
	Progress    bool          `subcmd:"progress,true,show progress"`
	Force       bool          `subcmd:"force,false,reanalyze even if the database is up to date"`
	SlowScans   time.Duration `subcmd:"slow-scan-duration,10s,duration at which scans are reported as slow"`
	Defaults    bool          `subcmd:"show-defaults,false,display default scanning options and exit"`
	RefreshTopN bool          `subcmd:"refresh-topn,false,recompute the top-N rankings from scratch once the analysis is complete; they are otherwise maintained incrementally and refreshed automatically when needed"`
}

type analyzeCmd struct {
	// openDB may be overridden by tests.
	openDB func(context.Context, *iduconfig.Settings) (*idudb.DB, error)
	// beforeClose, if set, is called by tests immediately before the
	// database is closed.
	beforeClose func()
}

func (alz *analyzeCmd) analyze(ctx context.Context, values interface{}, args []string) error {
	// TODO(cnicolaou): generalize this to other filesystems.
	fs := localfs.New()
	return alz.analyzeFS(ctx, fs, values.(*analyzeFlags), args)
}

func (alz *analyzeCmd) analyzeFS(ctx context.Context, fwfs filewalk.FS, af *analyzeFlags, args []string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := useMaxProcs(ctx); err != nil {
		ctxlog.Error(ctx, "failed to set max procs", "error", err)
	}
	if af.Defaults {
		showDefaults()
		return nil
	}
	start := time.Now()
	ctx, f, target, err := lookupPrefix(ctx, globalConfig, prefixArg(args))
	if err != nil {
		return err
	}
	root, ok := f.(*iduconfig.Root)
	if !ok {
		return fmt.Errorf("filesystem %v is a key range, analyzing key ranges is not yet supported", iduconfig.Label(f))
	}
	cfg := &root.Settings
	if cfg.SetMaxThreads > 0 {
		debug.SetMaxThreads(cfg.SetMaxThreads)
		ctxlog.Info(ctx, "set max threads", "max-threads", cfg.SetMaxThreads)
	}
	openDB := alz.openDB
	if openDB == nil {
		openDB = openWritableDB
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open/create database: %v: %w", cfg.Database, err)
	}
	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = db.Close(context.WithoutCancel(ctx))
		}
	}()

	// Cancel the walk on interrupt/termination so active filesystem and
	// database operations finish cleanly before the database is closed.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	sigDone := make(chan struct{})
	defer func() {
		signal.Stop(sigCh)
		close(sigDone)
	}()
	go func() {
		select {
		case <-sigCh:
			cancel()
			fmt.Printf("interrupted, waiting for all filesystem operations to finish\n")
		case <-sigDone:
		}
	}()

	pctx, pcancel := context.WithCancel(ctx)
	defer pcancel() // cancel progress tracker
	var wg sync.WaitGroup
	wg.Add(1)
	pt := newProgressTracker(pctx, time.Second, af.Progress, true, &wg)

	opts := append(idudb.AnalyzeOptionsFromConfig(cfg),
		idudb.WithFS(fwfs),
		idudb.WithProgress(pt),
		idudb.WithForce(af.Force),
		idudb.WithSlowScanThreshold(af.SlowScans),
	)
	analyzer := idudb.NewAnalyzer(db, root.Root, opts...)

	c := analyzer.Configuration()
	fmt.Printf("configuration: scan size %v, concurrent scans %v, concurrent stats %v, concurrent stats threshold %v\n", c.ScanSize, c.ConcurrentScans, c.ConcurrentStats, c.ConcurrentStatsThreshold)

	errs := errors.M{}
	errs.Append(analyzer.Analyze(ctx, target))
	if af.RefreshTopN && errs.Err() == nil {
		errs.Append(db.RefreshTopN(ctx))
	}
	pcancel() // cancel progress tracker.
	wg.Wait()

	if alz.beforeClose != nil {
		alz.beforeClose()
	}
	errs.Append(summarizeAndLog(ctx, db, pt, start))
	dbClosed = true
	if errs.Err() == nil {
		return nil
	}
	return errs.Squash(context.Canceled)
}

func cl() string {
	out := strings.Builder{}
	for _, arg := range os.Args {
		if strings.ContainsAny(arg, " \t") {
			fmt.Fprintf(&out, "%q", arg)
		} else {
			out.WriteString(arg)
		}
		out.WriteString(" ")
	}
	return strings.TrimSpace(out.String())
}

func summarizeAndLog(ctx context.Context, db *idudb.DB, pt *progressTracker, start time.Time) error {
	defer pt.summary(ctx)
	s := pt.summarize()
	s.Operation = "analyze"
	s.Command = cl()
	s.Duration = time.Since(start)
	buf, err := json.Marshal(s)
	if err != nil {
		errs := errors.M{}
		errs.Append(err)
		errs.Append(db.Close(ctx))
		return errs.Err()
	}
	return db.LogAndClose(ctx, start, time.Now(), buf)
}
