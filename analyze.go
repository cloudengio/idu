// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"expvar"
	"fmt"
	"os"
	"strings"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/exclusions"
	"cloudeng.io/cmdutil"
	"cloudeng.io/errors"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/path/cloudpath"
)

type analyzeFlags struct {
	Concurrency int  `subcmd:"concurrency,-1,number of threads to use for scanning"`
	Incremental bool `subcmd:"incremental,true,incremental mode uses the existing database to avoid as much unnecssary work as possible"`
	ScanSize    int  `subcmd:"scan-size,10000,control the number of items to fetch from the filesystem in a single operation"`
}

// TODO(cnicolaou): determine a means of adding S3, GCP scanners etc without
// pulling in all of their dependencies into this package and module.
// For example, consider running them as external commands accessing the
// same database (eg. go run cloudeng.io/aws/filewalk ...).

type scanState struct {
	fs          filewalk.Filesystem
	exclusions  *exclusions.T
	pt          *progressTracker
	incremental bool
	errorMap    map[string]struct{}
}

var activeMap = expvar.NewMap("cloudeng.io/idu.analyze-contents")
var prefixMap = expvar.NewMap("cloudeng.io/idu.analyze-prefix")

type stringer string

func (s stringer) String() string {
	return `"` + string(s) + `"`
}

func timestampedError(err string) string {
	ts := time.Now().Format(time.StampMilli)
	return fmt.Sprintf("%v: %v", ts, err)
}

func formatVarUpdate(status string, nFiles, nChildren int) stringer {
	return stringer(fmt.Sprintf("%v: %v: %v/%v", time.Now().Format(time.Stamp), status, nFiles, nChildren))
}

func (sc *scanState) fileFn(ctx context.Context, prefix string, info file.Info, ch <-chan filewalk.Contents) (file.InfoList, error) {
	activeMap.Set(prefix, formatVarUpdate("start", 0, 0))
	defer activeMap.Delete(prefix)
	sc.pt.send(ctx, progressUpdate{prefixStart: 1})
	pi := internal.PrefixInfo{
		ModTime: info.ModTime(),
		UserID:  info.User(),
		GroupID: info.Group(),
		Mode:    info.Mode(),
		Size:    info.Size(),
	}
	layout := globalConfig.LayoutFor(prefix)
	debug(ctx, 1, "prefix: %v\n", prefix)
	nerrors := 0
	for results := range ch {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		activeMap.Set(prefix, formatVarUpdate("results", len(results.Files), len(results.Children)))
		if err := results.Err; err != nil {
			if sc.fs.IsPermissionError(err) {
				debug(ctx, 1, "permission denied: %v\n", prefix)
			} else {
				debug(ctx, 1, "error: %v: %v\n", prefix, err)
			}
			pi.Err = timestampedError(err.Error())
			nerrors++
			break
		}
		debug(ctx, 2, "prefix: %v # files: %v # children: %v\n", prefix, len(results.Files), len(results.Children))
		for _, file := range results.Files {
			debug(ctx, 3, "prefix/file: %v/%v\n", prefix, file.Name())
			pi.DiskUsage += layout.Calculator.Calculate(file.Size())
			pi.Files = append(pi.Files, file)
		}
		pi.Children = append(pi.Children, results.Children...)
		activeMap.Set(prefix, formatVarUpdate("listing", len(pi.Files), len(pi.Children)))
	}
	activeMap.Set(prefix, formatVarUpdate("processing", len(pi.Files), len(pi.Children)))
	_, deleted, err := handleDeletedChildren(ctx, layout, prefix, pi.Children)
	if err != nil {
		debug(ctx, 1, "deletion error: %v: %v\n", prefix, err)
		pi.Err = timestampedError(fmt.Sprintf("deletion: %v", err))
		// Take care to keep any undeleted children in the database so that
		// they can be deleted in a subsequent invocation.
		if deleted > 0 && deleted+1 < len(pi.Children) {
			pi.Children = pi.Children[deleted+1:]
		}
	}
	// only update the database
	if err := globalDatabaseManager.Set(ctx, prefix, &pi); err != nil {
		return nil, err
	}
	sc.pt.send(ctx, progressUpdate{prefixDone: 1, deletions: deleted, errors: nerrors, files: len(pi.Files)})
	return pi.Children, nil
}

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

func (sc *scanState) prefixFn(ctx context.Context, prefix string, info file.Info, err error) (bool, file.InfoList, error) {
	prefixMap.Set(prefix, stringer(time.Now().Format(time.StampMilli)))
	defer prefixMap.Delete(prefix)
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
}

func analyze(ctx context.Context, values interface{}, args []string) error {
	flagValues := values.(*analyzeFlags)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	exclusions := exclusions.New(globalConfig.Exclusions)
	prefix := args[0]
	if !cloudpath.IsLocal(prefix) {
		return fmt.Errorf("currently only local filesystems are supported: %v", prefix)
	}
	cmdutil.HandleSignals(cancel, os.Interrupt, os.Kill)
	fs := filewalk.LocalFilesystem(flagValues.ScanSize)
	pt := newProgressTracker(ctx, time.Second)
	defer pt.summary()

	errs := errors.M{}
	errorMap, err := deleteErrors(ctx, prefix)
	if err != nil {
		return fmt.Errorf("deleting errors: %v", err)
	}
	sc := scanState{
		exclusions:  exclusions,
		fs:          fs,
		pt:          pt,
		incremental: flagValues.Incremental,
		errorMap:    errorMap,
	}
	walker := filewalk.New(sc.fs, filewalk.WithConcurrency(flagValues.Concurrency))
	errs.Append(walker.Walk(ctx, sc.prefixFn, sc.fileFn, prefix))
	errs.Append(globalDatabaseManager.CloseAll(ctx))
	cancel()
	return errs.Err()
}

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
