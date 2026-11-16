// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/errors"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/asyncstat"
	"cloudeng.io/file/localfs"
)

// ScanProgress is notified of the progress of an analysis. Its methods may
// be called concurrently.
type ScanProgress interface {
	// asyncstat.LatencyTracker is used to track the latency of
	// filesystem stat operations.
	asyncstat.LatencyTracker
	// PrefixStarted is called when the scan of a prefix starts.
	PrefixStarted()
	// PrefixDone is called when the scan of a prefix completes with the
	// number of child prefixes deleted and files found.
	PrefixDone(deleted int, files int64)
	// Error is called whenever an error is logged to the database.
	Error()
	// ParentUnchanged is called when a prefix is found to be unchanged.
	ParentUnchanged()
	// ChildrenUnchanged is called when all of the immediate child
	// prefixes of a prefix are found to be unchanged.
	ChildrenUnchanged()
	// SlowScan is called when a scan exceeds the slow scan threshold.
	SlowScan()
	// SyncScans is called with the running total of synchronous scans.
	SyncScans(n int64)
}

type nullProgress struct{}

func (nullProgress) Before() time.Time     { return time.Time{} }
func (nullProgress) After(time.Time)       {}
func (nullProgress) PrefixStarted()        {}
func (nullProgress) PrefixDone(int, int64) {}
func (nullProgress) Error()                {}
func (nullProgress) ParentUnchanged()      {}
func (nullProgress) ChildrenUnchanged()    {}
func (nullProgress) SlowScan()             {}
func (nullProgress) SyncScans(int64)       {}

// Analyzer scans a filesystem and records the metadata for every prefix
// (directory) and file in a DB. Subsequent analyses are incremental, in that
// only prefixes that have changed since the previous analysis are rescanned.
type Analyzer struct {
	db   *DB
	root string
	opts analyzeOptions
}

// NewAnalyzer returns a new Analyzer for the filesystem rooted at root that
// records its results in db. AnalyzeOptionsFromConfig can be used to
// create options from an iduconfig.Prefix.
func NewAnalyzer(db *DB, root string, opts ...AnalyzeOption) *Analyzer {
	a := &Analyzer{
		db:   db,
		root: root,
		opts: analyzeOptions{
			slowScan: DefaultSlowScanThreshold,
			progress: nullProgress{},
		},
	}
	for _, fn := range opts {
		fn(&a.opts)
	}
	if a.opts.fs == nil {
		a.opts.fs = localfs.New()
	}
	if a.opts.exclude == nil {
		a.opts.exclude = func(string) bool { return false }
	}
	return a
}

// Configuration describes the effective configuration of an Analyzer.
type Configuration struct {
	ScanSize                 int
	ConcurrentScans          int
	ConcurrentStats          int
	ConcurrentStatsThreshold int
}

// Configuration returns the effective configuration used by the Analyzer,
// that is, including any default values.
func (a *Analyzer) Configuration() Configuration {
	wc := a.newFilewalk(&walker{}).Configuration()
	ic := a.newAsyncStat(&walker{}).Configuration()
	return Configuration{
		ScanSize:                 wc.ScanSize,
		ConcurrentScans:          wc.ConcurrentScans,
		ConcurrentStats:          ic.AsyncStats,
		ConcurrentStatsThreshold: ic.AsyncThreshold,
	}
}

func (a *Analyzer) newFilewalk(w *walker) *filewalk.Walker[prefixState] {
	return filewalk.New(a.opts.fs, w,
		filewalk.WithConcurrentScans(a.opts.concurrentScans),
		filewalk.WithScanSize(a.opts.scanSize),
	)
}

func (a *Analyzer) newAsyncStat(w *walker) *asyncstat.T {
	return asyncstat.New(a.opts.fs,
		asyncstat.WithAsyncStats(a.opts.concurrentStats),
		asyncstat.WithAsyncThreshold(a.opts.concurrentStatsThreshold),
		asyncstat.WithErrorLogger(w.logLStatError),
		asyncstat.WithLatencyTracker(a.opts.progress))
}

func sameRoot(goos, a, b string) bool {
	ca, cb := filepath.Clean(a), filepath.Clean(b)
	if goos == "windows" {
		return strings.EqualFold(ca, cb)
	}
	return ca == cb
}

// Analyze scans target, which must be the root or a prefix beneath it,
// and updates the database accordingly. Any errors previously recorded for
// target and its descendants are deleted before the scan starts. The
// precomputed summaries are only marked as consistent with the current
// configuration once the entire root has been scanned successfully.
// Analyze returns when all filesystem and database operations have
// completed, including when ctx is canceled.
func (a *Analyzer) Analyze(ctx context.Context, target string) error {
	if err := a.db.deleteErrors(ctx, target); err != nil {
		return fmt.Errorf("DeleteErrors: %v", err)
	}
	w := &walker{
		root:      a.root,
		db:        a.db,
		fs:        a.opts.fs,
		pt:        a.opts.progress,
		exclude:   a.opts.exclude,
		slowScan:  a.opts.slowScan,
		reAnalyze: a.opts.force,
	}
	w.lsi = a.newAsyncStat(w)
	w.fw = a.newFilewalk(w)

	errs := errors.M{}
	errs.Append(w.fw.Walk(ctx, target))
	// Summaries are only known to be consistent with the current configuration
	// once the entire prefix has been scanned without interruption.
	if errs.Err() == nil && ctx.Err() == nil && sameRoot(runtime.GOOS, target, a.root) {
		errs.Append(a.db.markSummariesComplete(ctx))
	}
	return errs.Err()
}

// scanStore represents the database operations used by the walker.
type scanStore interface {
	getPrefixInfo(ctx context.Context, key string, pi *prefixinfo.T) (bool, error)
	setPrefixInfo(ctx context.Context, key string, unchanged bool, pi *prefixinfo.T) error
	logError(ctx context.Context, key string, when time.Time, detail []byte) error
	deletePrefix(ctx context.Context, prefix string) error
}

type walker struct {
	root      string
	db        scanStore
	fs        filewalk.FS
	fw        *filewalk.Walker[prefixState]
	pt        ScanProgress
	exclude   func(string) bool
	slowScan  time.Duration
	lsi       *asyncstat.T
	reAnalyze bool
}

type prefixState struct {
	parentUnchanged   bool
	childrenUnchanged bool
	info              file.Info
	existing          prefixinfo.T
	current           prefixinfo.T
	nfiles            int64
	nchildren         int64
	ndeleted          int
	start             time.Time
	contentsStart     time.Time
	contentsErr       error
}

func (w *walker) dbLogErr(ctx context.Context, prefix string, val []byte) {
	_ = w.db.logError(ctx, prefix, time.Now(), val)
	w.pt.Error()
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
			"prefix", w.root,
			"path", prefix,
			"symlink", prefix,
			"target", symlink)
		return true, false, nil
	}

	// info was obtained via lstat/stat and hence will have system
	// level information such as uid, gid, dev, ino etc.
	xattr, err := w.fs.XAttr(ctx, prefix, info)
	if err != nil {
		w.dbLogErr(ctx, prefix, []byte(err.Error()))
		internal.Log(ctx, internal.LogPrefix, "prefix xattr error",
			"prefix", w.root,
			"path", prefix,
			"error", err)
	}
	info.SetSys(xattr)
	current := prefixinfo.New(prefix, info)
	state.current = current

	ok, err := w.db.getPrefixInfo(ctx, prefix, &state.existing)
	if !ok {
		// a new entry
		return false, false, nil
	}
	if err != nil {
		// Some sort of database read error.
		return true, false, err
	}

	if !w.reAnalyze && state.existing.Unchanged(state.current) {
		// Can reuse all file entries, but will need to restat all
		// prefixes/directories in any case.
		state.current.SetInfoList(state.existing.FilesOnly())
		w.pt.ParentUnchanged()
		return false, true, nil
	}
	return false, false, nil
}

func (w *walker) Prefix(ctx context.Context, state *prefixState, prefix string, info file.Info, err error) (stop bool, _ file.InfoList, retErr error) {
	if err != nil {
		internal.Log(ctx, internal.LogError, "prefix error",
			"prefix", w.root,
			"path", prefix,
			"error", err)
		w.dbLogErr(ctx, prefix, []byte(err.Error()))
		if w.fs.IsPermissionError(err) || w.fs.IsNotExist(err) {
			// Don't return these errors via the walker.
			return true, nil, nil
		}
		return true, nil, err
	}

	if w.exclude(prefix) {
		internal.Log(ctx, internal.LogPrefix, "prefix exclusion",
			"prefix", w.root,
			"path", prefix)
		return true, nil, nil
	}

	state.start = time.Now()
	state.contentsStart = state.start
	internal.Log(ctx, internal.LogPrefix, "prefix start",
		"start", state.start,
		"prefix", w.root,
		"path", prefix)

	stop, state.parentUnchanged, retErr = w.handlePrefix(ctx, state, prefix, info)
	if retErr != nil {
		w.dbLogErr(ctx, prefix, []byte(retErr.Error()))
		return true, nil, retErr
	}
	state.info = info
	if !stop {
		w.pt.PrefixStarted()
	}
	return stop, nil, err
}

func (w *walker) processStats(ctx context.Context, prefix string, toStat []filewalk.Entry) (children, all file.InfoList, err error) {
	children, all, err = w.lsi.Process(ctx, prefix, toStat)
	if err != nil {
		w.dbLogErr(ctx, prefix, []byte(err.Error()))
		return nil, nil, err
	}
	for i, fi := range all {
		filename := w.fs.Join(prefix, fi.Name())
		xattr, _ := w.fs.XAttr(ctx, filename, fi)
		// Regardless of any error, set the system information
		// to be of type filewalk.XAttr since all other code
		// assumes it so.
		all[i].SetSys(xattr)
	}
	return children, all, nil
}

func (w *walker) Contents(ctx context.Context, state *prefixState, prefix string, contents []filewalk.Entry) (file.InfoList, error) {
	sinceLast := time.Since(state.contentsStart)
	state.contentsStart = time.Now()
	if sinceLast > w.slowScan {
		internal.Log(ctx, internal.LogProgress, "slow scan",
			"prefix", w.root,
			"path", prefix,
			"duration", sinceLast.String())
		w.pt.SlowScan()
	}
	toStat := make([]filewalk.Entry, 0, len(contents))
	for _, entry := range contents {
		path := w.fs.Join(prefix, entry.Name)
		if w.exclude(path) {
			internal.Log(ctx, internal.LogPrefix, "entry exclusion",
				"prefix", w.root,
				"path", path)
			continue
		}
		if state.parentUnchanged && !entry.IsDir() {
			state.nfiles++
			continue
		}
		toStat = append(toStat, entry)
	}
	children, all, err := w.processStats(ctx, prefix, toStat)
	if err != nil {
		state.contentsErr = err
		return nil, err
	}
	state.current.AppendInfoList(all)
	state.nfiles += int64(len(all) - len(children))
	state.nchildren += int64(len(children))
	return children, nil
}

func (w *walker) Done(ctx context.Context, state *prefixState, prefix string, err error) error {
	if err != nil && !errors.Is(err, state.contentsErr) {
		internal.Log(ctx, internal.LogPrefix, "prefix done with error",
			"prefix", w.root,
			"path", prefix,
			"error", err)
		w.dbLogErr(ctx, prefix, []byte(err.Error()))
	}

	defer func(state *prefixState) {
		internal.Log(ctx, internal.LogPrefix, "prefix done",
			"prefix", w.root,
			"path", prefix,
			"parent-unchanged", state.parentUnchanged,
			"children-unchanged", state.childrenUnchanged,
			"duration", time.Since(state.start).String(),
			"nfiles", state.nfiles,
			"ndeleted", state.ndeleted)
		stats := w.fw.Stats()
		w.pt.SyncScans(stats.SynchronousScans)
		w.pt.PrefixDone(state.ndeleted, state.nfiles)
	}(state)

	if err != nil || state.contentsErr != nil {
		// Preserve any previously stored subtree metadata if scanning this
		// directory failed; only record/confirm empty entries when no prior
		// entries exist (e.g. an inaccessible directory).
		if len(state.existing.InfoList()) == 0 {
			if state.existing.ModTime().IsZero() {
				if serr := w.db.setPrefixInfo(ctx, prefix, false, &state.current); serr != nil {
					return serr
				}
			} else if state.parentUnchanged && state.contentsErr == nil {
				state.childrenUnchanged = true
				w.pt.ChildrenUnchanged()
			}
		}
		return state.contentsErr
	}

	n, unchanged, err := w.handleDeletedOrChangedPrefixes(ctx, prefix, state.parentUnchanged, state.current, state.existing)
	state.ndeleted += n
	state.childrenUnchanged = unchanged
	if err != nil {
		return err
	}

	if err := w.db.setPrefixInfo(ctx, prefix, unchanged, &state.current); err != nil {
		internal.Log(ctx, internal.LogPrefix, "prefix done",
			"prefix", w.root,
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
	prevDirs := 0
	for _, prev := range previous.InfoList() {
		if prev.IsDir() {
			prevDirs++
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
	if len(cm) != prevDirs {
		childrenUnchanged = false
	}

	if childrenUnchanged {
		w.pt.ChildrenUnchanged()
		return 0, true, nil
	}

	var errs errors.M
	ndeleted := 0

	for _, d := range deleted {
		p := w.fs.Join(prefix, d)
		if err := w.db.deletePrefix(ctx, p); err != nil {
			errs.Append(err)
			w.dbLogErr(ctx, p, []byte(err.Error()))
			continue
		}
		ndeleted++
	}

	return ndeleted, false, errs.Err()
}
