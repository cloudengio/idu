// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package analyze implements idu's commands that create and maintain the
// database: analyze, logs, errors (both of which can erase entries) and
// database locate. Progress reporting is supplied by the caller via
// NewProgress, so that main controls how, and whether, it is displayed.
package analyze

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmdutil/cmdtypes"
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

// Flags are the flags for the analyze command.
type Flags struct {
	cli.FSFlag
	Progress    bool          `subcmd:"progress,true,show progress"`
	Force       bool          `subcmd:"force,false,reanalyze even if the database is up to date"`
	SlowScans   time.Duration `subcmd:"slow-scan-duration,10s,duration at which scans are reported as slow"`
	Defaults    bool          `subcmd:"show-defaults,false,display default scanning options and exit"`
	RefreshTopN bool          `subcmd:"refresh-topn,false,recompute the recursive totals and top-N rankings from scratch once the analysis is complete; they are otherwise maintained incrementally and rebuilt automatically when needed"`
}

// Commands implements the commands that create and maintain the
// database, ie. analyze, logs, errors and database locate.
type Commands struct {
	env         *cli.Env
	newProgress NewProgress
	// openDB may be overridden by tests.
	openDB func(context.Context, *iduconfig.Settings) (*idudb.DB, error)
	// beforeClose, if set, is called by tests immediately before the
	// database is closed.
	beforeClose func()
}

// New returns the Commands that use env. newProgress creates the Progress
// for each analysis, if it is nil progress is neither displayed nor
// reported.
func New(env *cli.Env, newProgress NewProgress) *Commands {
	return &Commands{env: env, newProgress: newProgress}
}

func (alz *Commands) progress(ctx context.Context, display bool) Progress {
	if alz.newProgress == nil {
		return nullProgress{}
	}
	return alz.newProgress(ctx, display)
}

// Analyze implements the analyze command, values must be a *Flags.
func (alz *Commands) Analyze(ctx context.Context, values interface{}, args []string) error {
	// TODO(cnicolaou): generalize this to other filesystems.
	fs := localfs.New()
	return alz.analyzeFS(ctx, fs, values.(*Flags), args)
}

func (alz *Commands) analyzeFS(ctx context.Context, fwfs filewalk.FS, af *Flags, args []string) error {
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
	env := alz.env.ForFS(af.FS)
	ctx, f, target, err := env.LookupPrefix(ctx, cli.PrefixArg(args))
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
		openDB = env.OpenWritableDB
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

	pt := alz.progress(ctx, af.Progress)
	defer pt.Stop()

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
	pt.Stop()

	if alz.beforeClose != nil {
		alz.beforeClose()
	}
	errs.Append(summarizeAndLog(ctx, db, pt, start))
	dbClosed = true
	reportDatabaseSize(ctx, cfg.Database)
	return cli.SquashCanceled(errs.Err())
}

// reportDatabaseSize reports the size of the database, which is excluded from
// the analysis itself, once it has been closed.
func reportDatabaseSize(ctx context.Context, dir string) {
	size, err := idudb.DatabaseSize(dir)
	if err != nil {
		ctxlog.Error(ctx, "failed to determine the database size", "database", dir, "error", err)
		return
	}
	fmt.Printf("database: %v: %v\n", dir, cmdtypes.ByteSize(size))
	ctxlog.Info(ctx, "database size", "database", dir, "bytes", size)
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

func summarizeAndLog(ctx context.Context, db *idudb.DB, pt Progress, start time.Time) error {
	defer pt.Report(ctx)
	s := pt.Summary()
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
