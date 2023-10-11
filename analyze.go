// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/errors"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/localfs"
)

// TODO: errors, using structred logs, write to database?

type analyzeFlags struct {
	UseDB bool          `subcmd:"use-db,false,database backed scan that avoids re-scanning unchanged directories"`
	Newer time.Duration `subcmd:"newer,24h,only scans directories and files that have changed since the specified duration"`
}

type analyzeCmd struct{}

func (alz *analyzeCmd) analyze(ctx context.Context, values interface{}, args []string) error {
	af := values.(*analyzeFlags)
	start := time.Now()
	ctx, prefix, err := internal.LookupPrefix(ctx, globalConfig, args[0])
	if err != nil {
		return err
	}
	var db database.DB
	if af.UseDB {
		db, err = internal.OpenDatabase(ctx, prefix)
		if err != nil {
			return err
		}
		if err := db.DeleteErrors(ctx, args[0]); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // cancel progress tracker, status reporting etc.

	// TODO(cnicolaou): generalize this to other filesystems.
	fs := localfs.New()
	pt := newProgressTracker(ctx, time.Second)

	w := walker{
		prefix: prefix,
		db:     db,
		fs:     fs,
		pt:     pt,
		usedb:  af.UseDB,
		since:  time.Now().Add(-af.Newer),
	}
	walkerStatus := make(chan filewalk.Status, 10)
	walker := filewalk.New(w.fs,
		&w,
		filewalk.WithConcurrency(prefix.Concurrency),
		filewalk.WithScanSize(prefix.ScanSize),
		filewalk.WithReporting(walkerStatus, time.Second, time.Second*10),
	)

	go w.status(ctx, walkerStatus)

	errs := errors.M{}
	errs.Append(walker.Walk(ctx, args[0]))
	errs.Append(alz.summarizeAndLog(ctx, db, pt, start))
	return errs.Err()
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

func (alz *analyzeCmd) summarizeAndLog(ctx context.Context, db database.DB, pt *progressTracker, start time.Time) error {
	defer pt.summary(ctx)
	if db == nil {
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
	if db == nil {
		return nil
	}
	return db.LogAndClose(ctx, start, time.Now(), buf)
}

type walker struct {
	prefix config.Prefix
	db     database.DB
	fs     filewalk.FS
	pt     *progressTracker
	since  time.Time
	usedb  bool
	nstats int64

	errorMap map[string]struct{}
}

type prefixState struct {
	prefix    string
	unchanged bool
	info      file.Info
	existing  prefixinfo.T
	pi        prefixinfo.T
	start     time.Time
	nerrors   int
	nfiles    int
	nchildren int
	nstats    int
	ndeleted  int
}

func (w *walker) status(ctx context.Context, ch <-chan filewalk.Status) {
	for {
		select {
		case <-ctx.Done():
			return
		case s := <-ch:
			w.pt.setSyncScans(s.SynchronousScans)
			if len(s.SlowPrefix) > 0 {
				internal.Log(ctx, internal.LogPrefix, "slow scan", "prefix", w.prefix.Prefix, "path", s.SlowPrefix, "duration", s.ScanDuration)
			}
		}
	}
}

func (w *walker) dbLog(ctx context.Context, prefix string, val []byte) {
	if w.db == nil {
		return
	}
	w.db.LogError(ctx, prefix, time.Now(), []byte(val))
}

func (w *walker) handlePrefix(ctx context.Context, state *prefixState, prefix string, info file.Info, err error) (stop, unchanged bool, fi []filewalk.Entry, _ error) {
	if err != nil {
		internal.Log(ctx, internal.LogError, "prefix error", "prefix", w.prefix.Prefix, "path", prefix, "error", err)
		w.dbLog(ctx, prefix, []byte(err.Error()))
		if w.fs.IsPermissionError(err) || w.fs.IsNotExist(err) {
			// Don't track these errors in the walker.
			return true, false, nil, nil
		}
		return true, false, nil, err
	}

	state.pi, err = prefixinfo.New(info)
	if err != nil {
		w.dbLog(ctx, prefix, []byte(err.Error()))
		return true, false, nil, err
	}

	if info.Mode()&os.ModeSymlink == os.ModeSymlink {
		// Ignore symlinks.
		symlink, _ := w.fs.Readlink(ctx, prefix)
		internal.Log(ctx, internal.LogPrefix, "symlink prefix ignored", "prefix", w.prefix.Prefix, "path", prefix, "symlink", prefix, "target", symlink)
		return true, false, nil, nil
	}

	if w.prefix.Exclude(prefix) {
		internal.Log(ctx, internal.LogPrefix, "prefix exclusion", "prefix", w.prefix.Prefix, "path", prefix)
		return true, false, nil, nil
	}

	if w.usedb {
		ok, err := getPrefixInfo(ctx, w.db, prefix, &state.existing)
		if !ok || err != nil {
			return false, false, nil, err
		}
		if state.existing.Unchanged(state.pi) {
			w.pt.incUnchanged()
			return false, true, nil, nil
		}
	}
	return false, false, nil, nil
}

func (w *walker) Prefix(ctx context.Context, state *prefixState, prefix string, info file.Info, err error) (stop bool, _ []filewalk.Entry, _ error) {
	start := time.Now()

	internal.Log(ctx, internal.LogPrefix, "prefix start", "start", start, "prefix", w.prefix.Prefix, "path", prefix)

	state.start = time.Now()
	stop, unchanged, fl, err := w.handlePrefix(ctx, state, prefix, info, err)

	state.unchanged = unchanged
	state.info = info
	if !stop {
		w.pt.incStartPrefix()
	}

	if err != nil {
		w.dbLog(ctx, prefix, []byte(err.Error()))
	}
	return stop, fl, err
}

func (w *walker) Contents(ctx context.Context, state *prefixState, prefix string, contents []filewalk.Entry, err error) ([]filewalk.Entry, error) {

	if err != nil {
		internal.Log(ctx, internal.LogError, "listing error", "prefix", w.prefix.Prefix, "path", prefix, "error", err)
		w.dbLog(ctx, prefix, []byte(err.Error()))
		state.nerrors++
		if w.fs.IsPermissionError(err) || w.fs.IsNotExist(err) {
			// Don't return an error to the walker, just log it.
			return nil, nil
		}
		return nil, err
	}

	var children []filewalk.Entry
	if state.unchanged {
		// Only need to traverse sub-directories if this directory is unchanged.
		for _, entry := range contents {
			if entry.IsDir() {
				children = append(children, entry)
				state.nchildren++
			} else {
				state.nfiles++
			}
		}
		return children, nil
	}

	for _, entry := range contents {
		if entry.IsDir() {
			children = append(children, entry)
			state.nchildren++
			state.pi.AppendEntry(entry)
		} else {
			state.nfiles++
		}
		file := w.fs.Join(prefix, entry.Name)
		info, err := w.fs.LStat(ctx, file)
		if err != nil {
			internal.Log(ctx, internal.LogError, "stat error", "prefix", w.prefix.Prefix, "path", prefix, "file", file, "error", err)
			w.dbLog(ctx, w.fs.Join(prefix, file), []byte(err.Error()))
			continue
		}
		state.nstats++
		if info.Mode()&os.ModeSymlink == os.ModeSymlink {
			// ignore symbolic links
			symlink, _ := w.fs.Readlink(ctx, file)
			internal.Log(ctx, internal.LogPrefix, "symlink entry ignored", "prefix", w.prefix.Prefix, "path", prefix, "symlink", file, "target", symlink)
			continue
		}
		state.pi.AppendInfo(info)
	}
	return children, nil
}

func (w *walker) Done(ctx context.Context, state *prefixState, prefix string) error {
	if state.unchanged {
		return nil
	}
	if err := state.pi.Finalize(); err != nil {
		internal.Log(ctx, internal.LogPrefix, "prefix done", "prefix", w.prefix.Prefix, "path", prefix, "error", err)
		w.dbLog(ctx, prefix, []byte(err.Error()))
		return err
	}

	defer func(state *prefixState) {
		internal.Log(ctx, internal.LogPrefix, "prefix done", "prefix", w.prefix.Prefix, "path", prefix, "unchanged", state.unchanged, "duration", time.Since(state.start), "nerrors", state.nerrors, "nfiles", state.nfiles, "nstats", state.nstats, "ndeleted", state.ndeleted)
		w.pt.incDonePrefix(state.nerrors, state.nfiles, state.nstats)
	}(state)

	if !w.usedb {
		return nil
	}

	if err := setPrefixInfo(ctx, w.db, prefix, &state.pi); err != nil {
		internal.Log(ctx, internal.LogPrefix, "prefix done", "prefix", w.prefix.Prefix, "path", prefix, "error", err)
		return err
	}

	n, err := w.handleDeletedPrefixes(ctx, prefix, state.pi, state.existing)
	state.ndeleted += n
	if err != nil {
		return err
	}
	return nil
}

func (w *walker) handleDeletedPrefixes(ctx context.Context, prefix string, current, previous prefixinfo.T) (int, error) {
	var deleted []string
	cm := make(map[string]struct{}, len(current.Entries()))
	for _, cur := range current.Entries() {
		cm[cur.Name] = struct{}{}
	}
	for _, prev := range previous.Entries() {
		if _, ok := cm[prev.Name]; !ok {
			deleted = append(deleted, prev.Name)
		}
	}
	var errs errors.M
	ndeleted := 0
	for _, d := range deleted {
		p := w.fs.Join(prefix, d)
		if err := w.db.DeletePrefix(ctx, p); err != nil {
			errs.Append(err)
			w.dbLog(ctx, p, []byte(err.Error()))
		}
		ndeleted++
	}
	return ndeleted, errs.Err()
}

func getPrefixInfo(ctx context.Context, db database.DB, key string, pi *prefixinfo.T) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	buf, err := db.Get(ctx, key)
	if err != nil {
		return false, err
	}
	if buf == nil {
		return false, nil
	}

	return true, pi.UnmarshalBinary(buf)
}

func setPrefixInfo(ctx context.Context, db database.DB, key string, pi *prefixinfo.T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	buf, err := pi.MarshalBinary()
	if err != nil {
		return err
	}
	return db.SetBatch(ctx, key, buf)
}
