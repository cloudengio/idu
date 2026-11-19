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

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/errors"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/asyncstat"
	"cloudeng.io/file/localfs"
	"cloudeng.io/logging/ctxlog"
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
	// Restated is called when files in an unchanged prefix are re-stat'ed,
	// see WithRestat, with the number of files re-stat'ed and the number
	// of those that had changed.
	Restated(files, changed int)
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
func (nullProgress) Restated(int, int)     {}

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
// create options from an iduconfig.Settings.
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
		restat:    a.opts.restat,
		slowScan:  a.opts.slowScan,
		reAnalyze: a.opts.force,
	}
	w.lsi = a.newAsyncStat(w)
	w.fw = a.newFilewalk(w)

	if !a.db.topn.enabled() {
		// The rankings and recursive totals, if any, will not reflect the
		// changes made by this analysis.
		if _, err := a.db.invalidateTopN(ctx); err != nil {
			return err
		}
	}
	obs := a.observer()
	if obs != nil {
		if err := obs.ScanStarted(ctx, ScanInfo{
			Root:    a.root,
			Target:  target,
			Rewrite: a.db.rewriteSummaries,
		}); err != nil {
			return fmt.Errorf("Observer.ScanStarted: %w", err)
		}
		a.db.observer = obs
		a.db.previousSummaryFailed.Store(false)
		if a.db.recordsLinks() {
			a.db.links = newLinkCounts()
		}
		defer func() { a.db.observer, a.db.links = nil, nil }()
	}

	errs := errors.M{}
	errs.Append(w.fw.Walk(ctx, target))
	// Summaries are only known to be consistent with the current configuration
	// once the entire prefix has been scanned without interruption.
	if errs.Err() == nil && ctx.Err() == nil && sameRoot(runtime.GOOS, target, a.root) {
		errs.Append(a.db.markSummariesComplete(ctx))
	}
	if obs == nil {
		return errs.Err()
	}
	scanErr := errs.Err()
	if scanErr == nil {
		scanErr = ctx.Err()
	}
	// Persist all of the changes before reporting that they are done,
	// regardless of any errors or cancelation.
	if err := a.db.db.Flush(context.WithoutCancel(ctx)); err != nil {
		errs.Append(err)
		if scanErr == nil {
			scanErr = err
		}
	}
	errs.Append(obs.ScanDone(ctx, scanErr))
	return errs.Err()
}

// observer returns the Observer, if any, to be notified of all changes,
// ie. the one that maintains the top-N rankings and/or the one
// supplied via WithObserver.
func (a *Analyzer) observer() Observer {
	var obs multiObserver
	if a.db.topn.enabled() {
		obs = append(obs, newTopNObserver(a.db))
	}
	if a.opts.observer != nil {
		obs = append(obs, a.opts.observer)
	}
	switch len(obs) {
	case 0:
		return nil
	case 1:
		return obs[0]
	}
	return obs
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
	restat    func(string) bool
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
	// restatIndex maps the names of the files in an unchanged prefix to
	// their position in current, it is created when the first file is
	// re-stat'ed.
	restatIndex map[string]int
	// restatChanged is true if any of the re-stat'ed files had changed.
	restatChanged bool
}

func (w *walker) dbLogErr(ctx context.Context, prefix string, val []byte) {
	_ = w.db.logError(ctx, prefix, time.Now(), val)
	w.pt.Error()
}

func (w *walker) logLStatError(ctx context.Context, filename string, err error) {
	ctxlog.Error(ctx, "stat error",
		"file", filename,
		"error", err)
	w.dbLogErr(ctx, filename, []byte(err.Error()))
}

func (w *walker) handlePrefix(ctx context.Context, state *prefixState, prefix string, info file.Info) (stop, unchanged bool, _ error) {

	if info.Mode()&os.ModeSymlink == os.ModeSymlink {
		// Ignore symlinks.
		symlink, _ := w.fs.Readlink(ctx, prefix)
		ctxlog.Debug(ctx, "symlink prefix ignored",
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
		ctxlog.Debug(ctx, "prefix xattr error",
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
		ctxlog.Error(ctx, "prefix error",
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
		ctxlog.Debug(ctx, "prefix exclusion",
			"prefix", w.root,
			"path", prefix)
		return true, nil, nil
	}

	state.start = time.Now()
	state.contentsStart = state.start
	ctxlog.Debug(ctx, "prefix start",
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
		ctxlog.Info(ctx, "slow scan",
			"prefix", w.root,
			"path", prefix,
			"duration", sinceLast.String())
		w.pt.SlowScan()
	}
	toStat := make([]filewalk.Entry, 0, len(contents))
	var restat []filewalk.Entry
	for _, entry := range contents {
		path := w.fs.Join(prefix, entry.Name)
		if w.exclude(path) {
			ctxlog.Debug(ctx, "entry exclusion",
				"prefix", w.root,
				"path", path)
			continue
		}
		if state.parentUnchanged && !entry.IsDir() {
			state.nfiles++
			if w.restat != nil && w.restat(path) {
				restat = append(restat, entry)
			}
			continue
		}
		toStat = append(toStat, entry)
	}
	if len(restat) > 0 {
		if err := w.restatFiles(ctx, state, prefix, restat); err != nil {
			state.contentsErr = err
			return nil, err
		}
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

// restatFiles stats the files in entries, which are in the unchanged
// prefix, and replaces the stored metadata for any that have changed.
func (w *walker) restatFiles(ctx context.Context, state *prefixState, prefix string, entries []filewalk.Entry) error {
	_, all, err := w.processStats(ctx, prefix, entries)
	if err != nil {
		return err
	}
	if state.restatIndex == nil {
		state.restatIndex = map[string]int{}
		for i, fi := range state.current.InfoList() {
			if !fi.IsDir() {
				state.restatIndex[fi.Name()] = i
			}
		}
	}
	current := state.current.InfoList()
	changed := 0
	for _, fi := range all {
		i, ok := state.restatIndex[fi.Name()]
		if !ok || fi.IsDir() {
			// The prefix has changed since it was found to be unchanged,
			// the next analysis will rescan it.
			continue
		}
		if fileUnchanged(state.existing, current[i], fi) {
			continue
		}
		current[i] = fi
		changed++
	}
	if changed > 0 {
		state.current.SetInfoList(current)
		state.restatChanged = true
	}
	w.pt.Restated(len(entries), changed)
	return nil
}

// fileUnchanged returns true if the file described by cur, as returned by
// stat, is unchanged from prev, as stored for prefix pi.
func fileUnchanged(pi prefixinfo.T, prev, cur file.Info) bool {
	if prev.Size() != cur.Size() || !prev.ModTime().Equal(cur.ModTime()) || prev.Mode() != cur.Mode() {
		return false
	}
	px, cx := pi.XAttrInfo(prev), pi.XAttrInfo(cur)
	return px.UID == cx.UID && px.GID == cx.GID && px.FileID == cx.FileID &&
		px.Blocks == cx.Blocks && px.Hardlinks == cx.Hardlinks
}

func (w *walker) Done(ctx context.Context, state *prefixState, prefix string, err error) error {
	if err != nil && !errors.Is(err, state.contentsErr) {
		ctxlog.Debug(ctx, "prefix done with error",
			"prefix", w.root,
			"path", prefix,
			"error", err)
		w.dbLogErr(ctx, prefix, []byte(err.Error()))
	}

	defer func(state *prefixState) {
		ctxlog.Debug(ctx, "prefix done",
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

	if err := w.db.setPrefixInfo(ctx, prefix, unchanged && !state.restatChanged, &state.current); err != nil {
		ctxlog.Debug(ctx, "prefix done",
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
