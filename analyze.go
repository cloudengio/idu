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

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmdutil"
	"cloudeng.io/errors"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/asyncstat"
	"cloudeng.io/file/filewalk/localfs"
)

func showDefaults() {
	fmt.Printf("scan_size: %v\n", filewalk.DefaultScanSize)
	fmt.Printf("concurrent_scans: %v\n", filewalk.DefaultConcurrentScans)
	fmt.Printf("concurrent_stats: %v", asyncstat.DefaultAsyncStats)
	fmt.Printf("concurrent_stats_threadhold: %v\n", asyncstat.DefaultAsyncThreshold)
}

type analyzeFlags struct {
	Progress  bool          `subcmd:"progress,true,show progress"`
	SlowScans time.Duration `subcmd:"slow-scan-duration,10s,duration at which scans are reported as slow"`
	Defaults  bool          `subcmd:"show-defaults,false,display default scanning options and exit"`
}

type analyzeCmd struct{}

func (alz *analyzeCmd) analyze(ctx context.Context, values interface{}, args []string) error {
	// TODO(cnicolaou): generalize this to other filesystems.
	fs := localfs.New()
	return alz.analyzeFS(ctx, fs, values.(*analyzeFlags), args)
}

func (alz *analyzeCmd) analyzeFS(ctx context.Context, fwfs filewalk.FS, af *analyzeFlags, args []string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := useMaxProcs(ctx); err != nil {
		internal.Log(ctx, internal.LogError, "failed to set max procs", "error", err)
	}
	if af.Defaults {
		showDefaults()
		return nil
	}
	start := time.Now()
	ctx, cfg, err := internal.LookupPrefix(ctx, globalConfig, args[0])
	if err != nil {
		return err
	}
	if cfg.SetMaxThreads > 0 {
		debug.SetMaxThreads(cfg.SetMaxThreads)
		internal.Log(ctx, internal.LogProgress, "set max threads", "max-threads", cfg.SetMaxThreads)
	}
	var sdb internal.ScanDB
	sdb, err = internal.NewScanDB(ctx, cfg)
	if err != nil {
		return fmt.Errorf("open/create database: %v: %v", cfg.Database, err)
	}
	if err := sdb.DeleteErrors(ctx, args[0]); err != nil {
		return fmt.Errorf("DeleteErrors: %v", err)
	}
	defer sdb.Close(ctx)

	// Close the database as quickly as possible.
	signal.Reset(os.Interrupt, syscall.SIGTERM)
	cmdutil.HandleSignals(func() {
		cancel()
		sdb.Close(ctx)
		fmt.Printf("database closed, waiting for all filesystem operations to finish\n")
	}, os.Interrupt, os.Kill)

	pctx, pcancel := context.WithCancel(ctx)
	defer pcancel() // cancel progress tracker
	var wg sync.WaitGroup
	wg.Add(1)
	pt := newProgressTracker(pctx, time.Second, af.Progress, true, &wg)

	w := &walker{
		cfg:      cfg,
		db:       sdb,
		fs:       fwfs,
		pt:       pt,
		slowScan: af.SlowScans,
	}

	w.lsi = asyncstat.New(fwfs,
		asyncstat.WithAsyncStats(cfg.ConcurrentStats),
		asyncstat.WithAsyncThreshold(cfg.ConcurrentStatsThreshold),
		asyncstat.WithErrorLogger(w.logLStatError),
		asyncstat.WithLatencyTracker(pt))

	walker := filewalk.New(
		w.fs,
		w,
		filewalk.WithConcurrentScans(cfg.ConcurrentScans),
		filewalk.WithScanSize(cfg.ScanSize),
	)
	w.fw = walker

	wc := walker.Configuration()
	ic := w.lsi.Configuration()
	fmt.Printf("configuration: scan size %v, concurrent scans %v, concurrent stats %v, concurrent stats threshold %v\n", wc.ScanSize, wc.ConcurrentScans, ic.AsyncStats, ic.AsyncThreshold)

	errs := errors.M{}
	errs.Append(walker.Walk(ctx, args[0]))
	pcancel() // cancel progress tracker.
	wg.Wait()

	errs.Append(alz.summarizeAndLog(ctx, sdb, pt, start))
	return errs.Squash(context.Canceled)
}

func cl() string {
	out := strings.Builder{}
	for _, arg := range os.Args {
		if strings.Contains(arg, " \t") {
			out.WriteString(fmt.Sprintf("%q", arg))
		} else {
			out.WriteString(arg)
		}
		out.WriteString(" ")
	}
	return strings.TrimSpace(out.String())
}

func (alz *analyzeCmd) summarizeAndLog(ctx context.Context, sdb internal.ScanDB, pt *progressTracker, start time.Time) error {
	defer pt.summary(ctx)
	if sdb == nil {
		return nil
	}
	s := pt.summarize()
	s.Operation = "analyze"
	s.Command = cl()
	s.Duration = time.Since(start)
	buf, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if sdb == nil {
		return nil
	}
	return sdb.LogAndClose(ctx, start, time.Now(), buf)
}

type walker struct {
	cfg      config.Prefix
	db       internal.ScanDB
	fs       filewalk.FS
	fw       *filewalk.Walker[prefixState]
	pt       *progressTracker
	slowScan time.Duration
	lsi      *asyncstat.T
}

type prefixState struct {
	parentUnchanged bool
	info            file.Info
	existing        prefixinfo.T
	current         prefixinfo.T
	nfiles          int64
	nchildren       int64
	ndeleted        int
	start           time.Time
	contentsStart   time.Time
}

func (w *walker) dbLogErr(ctx context.Context, prefix string, val []byte) {
	w.db.LogError(ctx, prefix, time.Now(), val)
	w.pt.incErrors()
}

func (w *walker) logLStatError(ctx context.Context, filename string, err error) {
	internal.Log(ctx,
		internal.LogError, "stat error",
		"file", filename,
		"error", err)
	w.dbLogErr(ctx, filename, []byte(err.Error()))
}

func (w *walker) handlePrefix(ctx context.Context, state *prefixState, prefix string, info file.Info) (stop, unchanged bool, _ error) {

	if info.Mode()&os.ModeSymlink == os.ModeSymlink {
		// Ignore symlinks.
		symlink, _ := w.fs.Readlink(ctx, prefix)
		internal.Log(ctx, internal.LogPrefix, "symlink prefix ignored",
			"prefix", w.cfg.Prefix,
			"path", prefix,
			"symlink", prefix,
			"target", symlink)
		return true, false, nil
	}

	// info was obtained via lstat/stat and hence will have system level information
	// such as uid, gid, dev, ino etc.
	current, err := prefixinfo.New(prefix, info)
	if err != nil {
		w.dbLogErr(ctx, prefix, []byte(err.Error()))
		// system level info (uid, gid, dev, ino) is not available.
	}

	state.current = current

	ok, err := w.db.GetPrefixInfo(ctx, prefix, &state.existing)
	if !ok {
		// a new entry
		return false, false, nil
	}
	if err != nil {
		// Some sort of database read error.
		return true, false, err
	}

	if state.existing.Unchanged(state.current) {
		// Cam reuse all file entries, but will need to restat all
		// prefixes/directories in any case.
		state.current.SetInfoList(state.existing.FilesOnly())
		w.pt.incParentUnchanged()
		return false, true, nil
	}
	return false, false, nil
}

func (w *walker) Prefix(ctx context.Context, state *prefixState, prefix string, info file.Info, err error) (stop bool, _ file.InfoList, retErr error) {
	if err != nil {
		internal.Log(ctx, internal.LogError, "prefix error",
			"prefix", w.cfg.Prefix,
			"path", prefix,
			"error", err)
		w.dbLogErr(ctx, prefix, []byte(err.Error()))
		if w.fs.IsPermissionError(err) || w.fs.IsNotExist(err) {
			// Don't return these errors via the walker.
			return true, nil, nil
		}
		return true, nil, err
	}

	if w.cfg.Exclude(prefix) {
		internal.Log(ctx, internal.LogPrefix, "prefix exclusion",
			"prefix", w.cfg.Prefix,
			"path", prefix)
		return true, nil, nil
	}

	state.start = time.Now()
	state.contentsStart = state.start
	internal.Log(ctx, internal.LogPrefix, "prefix start",
		"start", state.start,
		"prefix", w.cfg.Prefix,
		"path", prefix)

	stop, state.parentUnchanged, retErr = w.handlePrefix(ctx, state, prefix, info)
	if retErr != nil {
		w.dbLogErr(ctx, prefix, []byte(retErr.Error()))
		return true, nil, retErr
	}
	state.info = info
	if !stop {
		w.pt.incStartPrefix()
	}
	return stop, nil, err
}

func (w *walker) Contents(ctx context.Context, state *prefixState, prefix string, contents []filewalk.Entry) (file.InfoList, error) {
	sinceLast := time.Since(state.contentsStart)
	state.contentsStart = time.Now()
	if sinceLast > w.slowScan {
		internal.Log(ctx, internal.LogProgress, "slow scan",
			"prefix", w.cfg.Prefix,
			"path", prefix,
			"duration", sinceLast.String())
		w.pt.incSlowScans()
	}
	if state.parentUnchanged {
		// Need to traverse sub-directories even if the parent is unchanged.
		toStat := []filewalk.Entry{}
		for _, entry := range contents {
			if !entry.IsDir() {
				state.nfiles++
				continue
			}
			toStat = append(toStat, entry)
		}
		children, all, err := w.lsi.Process(ctx, prefix, toStat)
		if err != nil {
			w.dbLogErr(ctx, prefix, []byte(err.Error()))
		}
		state.current.AppendInfoList(all)
		state.nfiles += int64(len(all) - len(children))
		state.nchildren += int64(len(children))
		return children, nil
	}

	children, all, err := w.lsi.Process(ctx, prefix, contents)
	if err != nil {
		w.dbLogErr(ctx, prefix, []byte(err.Error()))
	}
	state.nfiles += int64(len(all) - len(children))
	state.nchildren += int64(len(children))
	state.current.AppendInfoList(all)
	return children, nil
}

func (w *walker) Done(ctx context.Context, state *prefixState, prefix string, err error) error {
	if err != nil {
		internal.Log(ctx, internal.LogPrefix, "prefix done with error",
			"prefix", w.cfg.Prefix,
			"path", prefix,
			"error", err)
		w.dbLogErr(ctx, prefix, []byte(err.Error()))
	}

	defer func(state *prefixState) {
		internal.Log(ctx, internal.LogPrefix, "prefix done",
			"prefix", w.cfg.Prefix,
			"path", prefix,
			"parent-unchanged", state.parentUnchanged,
			"children-unchanged", state.parentUnchanged,
			"duration", time.Since(state.start).String(),
			"nfiles", state.nfiles,
			"ndeleted", state.ndeleted)
		stats := w.fw.Stats()
		w.pt.setSyncScans(stats.SynchronousScans)
		w.pt.incDonePrefix(state.ndeleted, state.nfiles)
	}(state)

	n, unchanged, err := w.handleDeletedOrChangedPrefixes(ctx, prefix, state.parentUnchanged, state.current, state.existing)
	state.ndeleted += n
	if err != nil {
		return err
	}

	if err := w.db.SetPrefixInfo(ctx, prefix, unchanged, &state.current); err != nil {
		internal.Log(ctx, internal.LogPrefix, "prefix done",
			"prefix", w.cfg.Prefix,
			"path", prefix,
			"error", err)
		return err
	}

	return nil
}

func (w *walker) handleDeletedOrChangedPrefixes(ctx context.Context, prefix string, parentUnchanged bool, current, previous prefixinfo.T) (int, bool, error) {
	var deleted []string
	cm := map[string]file.Info{}
	for _, cur := range current.InfoList() {
		if cur.IsDir() {
			cm[cur.Name()] = cur
		}
	}
	childrenUnchanged := parentUnchanged
	for _, prev := range previous.InfoList() {
		if prev.IsDir() {
			pi, ok := cm[prev.Name()]
			if !ok {
				childrenUnchanged = false
				deleted = append(deleted, prev.Name())
				continue
			}
			if !pi.ModTime().Equal(prev.ModTime()) || pi.Mode() != prev.Mode() {
				childrenUnchanged = false
			}
		}
	}

	if childrenUnchanged {
		w.pt.incChildrenUnchanged()
		return 0, true, nil
	}

	var errs errors.M
	ndeleted := 0

	for _, d := range deleted {
		p := w.fs.Join(prefix, d)
		if err := w.db.DeletePrefix(ctx, p); err != nil {
			errs.Append(err)
			w.dbLogErr(ctx, p, []byte(err.Error()))
		}
		ndeleted++
	}

	return ndeleted, false, errs.Err()
}
