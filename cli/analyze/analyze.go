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
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
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
	Snapshot    bool          `subcmd:"snapshot,false,'write a snapshot of the statistics for the entire filesystem to its snapshot_dir once the analysis has completed successfully, it is an error if snapshot_dir is not set'"`
	RefreshTopN bool          `subcmd:"refresh-topn,false,recompute the recursive totals and top-N rankings from scratch once the analysis is complete; they are otherwise maintained incrementally and rebuilt automatically when needed"`
}

// Commands implements the commands that create and maintain the
// database, ie. analyze, logs, errors and database locate.
type Commands struct {
	env         *cli.Env
	newProgress NewProgress
	snapshot    SnapshotFunc
	out         io.Writer
	// openDB may be overridden by tests.
	openDB func(context.Context, *iduconfig.Settings) (*idudb.DB, error)
	// beforeClose, if set, is called by tests immediately before the
	// database is closed.
	beforeClose func()
}

// SnapshotFunc writes a snapshot of the stats for the entire filesystem f,
// computed from its database, to its snapshot_dir and returns its path.
type SnapshotFunc func(ctx context.Context, f iduconfig.Filesystem) (string, error)

// Option configures the Commands created by New.
type Option func(*Commands)

// WithProgress sets the function used to create the Progress for each
// analysis. Without it progress is neither displayed nor reported.
func WithProgress(newProgress NewProgress) Option {
	return func(c *Commands) {
		c.newProgress = newProgress
	}
}

// WithSnapshot sets the function used to write a snapshot once an
// analysis has completed, as requested by --snapshot, which is an error
// without it.
func WithSnapshot(fn SnapshotFunc) Option {
	return func(c *Commands) {
		c.snapshot = fn
	}
}

// WithOutput sets the writer for the messages, other than progress, that
// are displayed during an analysis, eg. its configuration, it defaults to
// os.Stdout.
func WithOutput(w io.Writer) Option {
	return func(c *Commands) {
		c.out = w
	}
}

func (alz *Commands) output() io.Writer {
	if alz.out == nil {
		return os.Stdout
	}
	return alz.out
}

// New returns the Commands that use env.
func New(env *cli.Env, opts ...Option) *Commands {
	c := &Commands{env: env}
	for _, fn := range opts {
		fn(c)
	}
	return c
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
	ctx, root, target, err := alz.lookupRoot(ctx, env, af, args)
	if err != nil {
		return err
	}
	var f iduconfig.Filesystem = root
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
			fmt.Fprintf(alz.output(), "interrupted, waiting for all filesystem operations to finish\n")
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
	fmt.Fprintf(alz.output(), "configuration: scan size %v, concurrent scans %v, concurrent stats %v, concurrent stats threshold %v\n", c.ScanSize, c.ConcurrentScans, c.ConcurrentStats, c.ConcurrentStatsThreshold)

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
	reportDatabaseSize(ctx, alz.output(), env, cfg.Database)
	if af.Snapshot && errs.Err() == nil {
		errs.Append(alz.takeSnapshot(ctx, f))
	}
	return cli.SquashCanceled(errs.Err())
}

// lookupRoot returns the root filesystem, and the target within it, to be
// analyzed and checks that any requested snapshot can be written so that
// the analysis is not started if it cannot.
func (alz *Commands) lookupRoot(ctx context.Context, env *cli.Env, af *Flags, args []string) (context.Context, *iduconfig.Root, string, error) {
	ctx, f, target, err := env.LookupPrefix(ctx, cli.PrefixArg(args))
	if err != nil {
		return ctx, nil, "", err
	}
	root, ok := f.(*iduconfig.Root)
	if !ok {
		return ctx, nil, "", fmt.Errorf("filesystem %v is a key range, analyzing key ranges is not yet supported", iduconfig.Label(f))
	}
	return ctx, root, target, alz.checkSnapshot(af, f)
}

// checkSnapshot returns an error if a snapshot is requested but cannot be
// written, so that the analysis is not started.
func (alz *Commands) checkSnapshot(af *Flags, f iduconfig.Filesystem) error {
	switch {
	case !af.Snapshot:
		return nil
	case f.Config().SnapshotDir == "":
		return fmt.Errorf("--snapshot requires snapshot_dir to be set for %v", iduconfig.Label(f))
	case alz.snapshot == nil:
		return fmt.Errorf("--snapshot is not supported")
	}
	return nil
}

// takeSnapshot writes a snapshot for f. It must be called once the
// database has been closed, and hence the analysis logged, so that the
// snapshot is more recent than the last update to the database.
func (alz *Commands) takeSnapshot(ctx context.Context, f iduconfig.Filesystem) error {
	if ctx.Err() != nil {
		// The analysis was interrupted.
		return nil
	}
	path, err := alz.snapshot(ctx, f)
	if err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}
	fmt.Fprintf(alz.output(), "snapshot: %v\n", path)
	return nil
}

// reportDatabaseSize reports the size of the database, which is excluded from
// the analysis itself, once it has been closed.
func reportDatabaseSize(ctx context.Context, out io.Writer, env *cli.Env, dir string) {
	size, err := idudb.DatabaseSize(dir)
	if err != nil {
		ctxlog.Error(ctx, "failed to determine the database size", "database", dir, "error", err)
		return
	}
	fmt.Fprintf(out, "database: %v: %v\n", dir, strings.TrimSpace(env.FmtSize(size)))
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
