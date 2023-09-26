// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/errors"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/localfs"
)

// TODO: errors, using structred logs, write to database?

type analyzeFlags struct {
	UsingDB bool          `subcmd:"using-db,false,database backed scan that avoids re-scanning unchanged directories"`
	Newer   time.Duration `subcmd:"newer,24h,scan that only scans files that have changed since the specified duration"`
}

type analyzeCmd struct{}

func (alz *analyzeCmd) analyze(ctx context.Context, values interface{}, args []string) error {
	fv := values.(*analyzeFlags)
	start := time.Now()
	prefix, path, err := internal.LookupPrefix(globalConfig, args[0])
	if err != nil {
		return err
	}
	var db database.DB
	if fv.UsingDB {
		db, err = internal.OpenDatabase(ctx, prefix)
		if err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // cancel progress tracker, status reporting etc.

	fs := localfs.New()
	pt := newProgressTracker(ctx, time.Second)
	defer pt.summary(ctx)

	errs := errors.M{}
	//	errorMap, err := deleteErrors(ctx, prefix)
	//	if err != nil {
	//		return fmt.Errorf("deleting errors: %v", err)
	//	}

	w := walker{
		prefix: prefix,
		path:   path,
		db:     db,
		fs:     fs,
		pt:     pt,
		usedb:  fv.UsingDB,
		since:  time.Now().Add(-fv.Newer),
		//		errorMap:    errorMap,
	}
	walkerStatus := make(chan filewalk.Status, 10)
	walker := filewalk.New(w.fs,
		filewalk.WithConcurrency(prefix.Concurrency),
		filewalk.WithScanSize(prefix.ScanSize),
		filewalk.WithReporting(walkerStatus, time.Second, time.Second*10),
	)

	go w.status(ctx, walkerStatus)

	errs.Append(walker.Walk(ctx, w.prefixFn, w.contentsFn, args[0]))

	op := fmt.Sprintf("analyze: %v", os.Args)
	errs.Append(db.LogAndClose(ctx, start, time.Now(), []byte(op)))

	return errs.Err()
}

func (w *walker) status(ctx context.Context, ch <-chan filewalk.Status) {
	for {
		select {
		case <-ctx.Done():
			return
		case s := <-ch:
			w.pt.walkerStats(s.SynchronousScans, s.FSStats.NumList, s.FSStats.NumStat)
			if len(s.SlowPrefix) > 0 {
				internal.Log(ctx, globalLogger, internal.LogPrefix, "slow scan", "prefix", w.prefix.Prefix, "path", s.SlowPrefix, "duration", s.ScanDuration)
			}
		}
	}
}

/*
func deleteErrors(ctx context.Context, prefix string) (map[string]struct{}, error) {
	db, err := globalDatabaseManager.DatabaseFor(ctx, prefix)
	if err != nil {
		return nil, err
	}
	em := map[string]struct{}{}
	deletions := []string{}
	sc := db.NewScanner("", 0, internal.ScanErrors())
	for sc.Scan(ctx) {
		p, pi := sc.PrefixInfo()
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		em[p] = struct{}{}
		deletions = append(deletions, p)
		debug(ctx, 2, "error for %v will be deleted: %v", prefix, pi.Err)
	}
	errs := &errors.M{}
	if len(deletions) > 0 {
		n, err := db.DeleteErrors(ctx, deletions)
		if err != nil {
			debug(ctx, 2, "delete error for %v: %v", deletions[n+1], err)
		}
		debug(ctx, 2, "deleted %v errors", n)
		errs.Append(err)
	}
	errs.Append(sc.Err())
	if err := errs.Err(); err != nil {
		debug(ctx, 2, "deletion errors: %v", err)
	}
	return em, errs.Err()
}
*/

type walker struct {
	prefix   config.Prefix
	path     string
	db       database.DB
	fs       filewalk.Filesystem
	pt       *progressTracker
	since    time.Time
	usedb    bool
	errorMap map[string]struct{}
}

func (w *walker) contentsFn(ctx context.Context, prefix string, unchanged bool, info file.Info, contents filewalk.Contents, _ <-chan filewalk.Contents) (file.InfoList, error) {
	w.pt.startPrefix()

	uid, _ := strconv.ParseInt(info.User(), 10, 64)
	gid, _ := strconv.ParseInt(info.Group(), 10, 64)
	pi := internal.PrefixInfo{
		ModTime: info.ModTime(),
		UserID:  uid,
		GroupID: gid,
		Mode:    info.Mode(),
		Size:    info.Size(),
	}

	// need to get lstat, stat handling correct. Try stat first, if it fails,
	// use lstat, look at old logic in List.
	nerrors := 0
	for results := range contents.Entries {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if err := results.Err; err != nil {
			internal.Log(ctx, globalLogger, internal.LogError, "listing error", "prefix", w.prefix.Prefix, "path", prefix, "error", err)
			w.db.LogError(ctx, time.Now(), prefix, []byte(err.Error()))
			pi.Err = err.Error()
			nerrors++
			break
		}
		for _, file := range results.Files {
			pi.Files = append(pi.Files, file)
		}
		pi.Children = append(pi.Children, results.Children...)
	}

	/*	_, deleted, err := handleDeletedChildren(ctx, layout, prefix, pi.Children)
		if err != nil {
			debug(ctx, 1, "deletion error: %v: %v\n", prefix, err)
			pi.Err = timestampedError(fmt.Sprintf("deletion: %v", err))
			// Take care to keep any undeleted children in the database so that
			// they can be deleted in a subsequent invocation.
			if deleted > 0 && deleted+1 < len(pi.Children) {
				pi.Children = pi.Children[deleted+1:]
			}
		}*/

	// only update the database
	deleted := 0

	w.pt.donePrefix(deleted, nerrors, len(pi.Files))

	if w.usedb && !unchanged {
		if err := setPrefixInfo(ctx, w.db, prefix, &pi); err != nil {
			return nil, err
		}
	}

	return pi.Children, nil
}

/*
	func findMissing(prefix string, previous, current file.InfoList) (remaining file.InfoList, deleted []string) {
		cm := make(map[string]struct{}, len(previous))
		for _, cur := range current {
			cm[cur.Name()] = struct{}{}
		}
		for _, prev := range previous {
			if _, ok := cm[prev.Name()]; !ok {
				deleted = append(deleted, prefix+prev.Name())
			} else {
				remaining = append(remaining, prev)
			}
		}
		return
	}

	func handleDeletedChildren(ctx context.Context, layout config.Layout, prefix string, children file.InfoList) (file.InfoList, int, error) {
		var existing internal.PrefixInfo
		ok, err := globalDatabaseManager.Get(ctx, prefix, &existing)
		if err != nil {
			return nil, 0, fmt.Errorf("database: %v: %v", prefix, err)
		}
		if !ok {
			return nil, 0, nil
		}
		if !strings.HasSuffix(prefix, layout.Separator) {
			prefix += layout.Separator
		}
		remaining, deletedChildren := findMissing(prefix, existing.Children, children)
		var deleted int
		if len(deletedChildren) > 0 {
			debug(ctx, 1, "deleting (recursively): %v: %v\n", prefix, len(deletedChildren))
			debug(ctx, 1, "deleting (recursively): %v\n", strings.Join(deletedChildren, ", "))
			deleted, err = globalDatabaseManager.Delete(ctx, layout.Separator, prefix, deletedChildren)
			debug(ctx, 1, "deleted (recursively): %v: remaining %v\n", deleted, len(remaining))
			if err != nil {
				fmt.Printf("delete error: %v %v\n", prefix, err)
			}
		}
		return remaining, deleted, err
	}
*/

func (w *walker) prefixFn(ctx context.Context, prefix string, info file.Info, err error) (stop, unchanged bool, _ file.InfoList, _ error) {
	start := time.Now()

	internal.Log(ctx, globalLogger, internal.LogPrefix, "prefix start", "start", "prefix", w.prefix.Prefix, "path", prefix, "start", start)

	stop, unchanged, fl, err := w.handlePrefix(ctx, prefix, info, err)
	done := time.Now()

	internal.Log(ctx, globalLogger, internal.LogPrefix, "prefix done", "done", "prefix", w.prefix.Prefix, "path", prefix, "unchanged", unchanged, "done", done, "duration", done.Sub(start), "error", err)

	if err != nil {
		w.db.LogError(ctx, time.Now(), prefix, []byte(err.Error()))
	}
	return stop, unchanged, fl, err
}

func (w *walker) handlePrefix(ctx context.Context, prefix string, info file.Info, err error) (stop, unchanged bool, fi file.InfoList, _ error) {
	if err != nil {
		internal.Log(ctx, globalLogger, internal.LogError, "prefix error", "prefix", w.prefix.Prefix, "path", prefix, "error", err)
		w.db.LogError(ctx, time.Now(), prefix, []byte(err.Error()))
		if w.fs.IsPermissionError(err) || w.fs.IsNotExist(err) {
			// Don't track these errors in the walker.
			return true, false, nil, nil
		}
		return true, false, nil, err
	}

	if w.prefix.Exclude(prefix) {
		internal.Log(ctx, globalLogger, internal.LogPrefix, "prefix exclusion", "prefix", w.prefix.Prefix, "path", prefix)
		return true, false, nil, nil
	}

	if w.usedb {
		var existing internal.PrefixInfo
		ok, err := getPrefixInfo(ctx, w.db, prefix, &existing)
		if !ok || err != nil {
			return false, false, nil, err
		}
		if existing.ModTime == info.ModTime() &&
			existing.Mode == info.Mode() {
			w.pt.unchanged()
			return false, true, nil, nil
		}
	}
	// TODO: scan since a certain time.

	return false, false, nil, nil

	/*
		var existing internal.PrefixInfo
		var unchanged bool

		ok, err := globalDatabaseManager.Get(ctx, prefix, &existing)
		if err == nil && ok {
			if existing.ModTime == info.ModTime() &&
				existing.Mode == info.Mode() {
				unchanged = true
			}
		}
		_, hasError := sc.errorMap[prefix]
		if hasError {
			debug(ctx, 2, "previous error existed for %v", prefix)
		}
		if unchanged && !hasError {
			sc.pt.send(ctx, progressUpdate{reused: len(existing.Children)})
			debug(ctx, 2, "unchanged: %v: #children: %v\n", prefix, len(existing.Children))
			// safe to skip unchanged leaf directories.
			return len(existing.Children) == 0, existing.Children, nil
		}
		return false, nil, nil
	*/
}
