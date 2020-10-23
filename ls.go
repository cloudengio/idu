// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmdutil"
	"cloudeng.io/errors"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/sync/errgroup"
)

type lsFlags struct {
	Limit      int    `subcmd:"limit,-1,'limit the number of items to list'"`
	TopN       int    `subcmd:"top,10,'show the top prefixes by file/prefix counts and disk usage, set to zero to disable'"`
	Summary    bool   `subcmd:"summary,true,show summary statistics"`
	ShowDirs   bool   `subcmd:"prefixes,false,show information on each prefix"`
	ShowFiles  bool   `subcmd:"files,false,show information on individual files"`
	ShowErrors bool   `subcmd:"errors,false,show information on individual errors"`
	User       string `subcmd:"user,,show information for this user only"`
}

func lsTree(ctx context.Context, pt *progressTracker, db filewalk.Database, root, user string, flags *lsFlags) (files, children, disk *heap.KeyedInt64, nerrors int64, err error) {
	files, children, disk = heap.NewKeyedInt64(heap.Descending), heap.NewKeyedInt64(heap.Descending), heap.NewKeyedInt64(heap.Descending)
	if flags.ShowDirs {
		fmt.Printf("     disk usage :  # files : # dirs : directory/prefix\n")
	}
	sc := db.NewScanner(root, flags.Limit, filewalk.ScanLimit(1000))
	for sc.Scan(ctx) {
		prefix, pi := sc.PrefixInfo()
		if len(user) > 0 && pi.UserID != user {
			continue
		}
		if err := pi.Err; len(err) > 0 {
			nerrors++
			if flags.ShowErrors {
				fmt.Printf("%s: %s\n", prefix, pi.Err)
			}
			pt.send(ctx, progressUpdate{prefix: 1, errors: 1})
			continue
		}
		files.Update(prefix, int64(len(pi.Files)))
		children.Update(prefix, int64(len(pi.Children)))
		disk.Update(prefix, pi.DiskUsage)
		if flags.ShowDirs || flags.ShowFiles {
			fmt.Printf("% 15v : % 8v : % 6v : %s\n", fsize(pi.DiskUsage), len(pi.Files), len(pi.Children), prefix)
			if flags.ShowFiles {
				for _, fi := range pi.Files {
					fmt.Printf("    % 15v : % 40v: % 10v : %v\n", fsize(fi.Size), fi.ModTime, globalUserManager.nameForUID(fi.UserID), fi.Name)
				}
			}
		} else {
			pt.send(ctx, progressUpdate{prefix: 1, files: len(pi.Files)})
		}
	}
	err = sc.Err()
	return
}

func topNMetrics(top []struct {
	K string
	V int64
}) []filewalk.Metric {
	m := make([]filewalk.Metric, len(top))
	for i, kv := range top {
		m[i] = filewalk.Metric{Prefix: kv.K, Value: kv.V}
	}
	return m
}

func lsr(ctx context.Context, values interface{}, args []string) error {
	flagValues := values.(*lsFlags)

	if len(args) > 1 {
		flagValues.ShowFiles = false
		flagValues.ShowDirs = false
		flagValues.Summary = true
	}

	type results struct {
		root                  string
		files, children, disk *heap.KeyedInt64
		errors                int64
		db                    filewalk.Database
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmdutil.HandleSignals(cancel, os.Interrupt, os.Kill)

	key := ""
	if usr := flagValues.User; len(usr) > 0 {
		key = globalUserManager.uidForName(usr)
	}

	pt := newProgressTracker(ctx, time.Second)
	listers := &errgroup.T{}
	listers = errgroup.WithConcurrency(listers, len(args))
	resultsCh := make(chan results)
	for _, root := range args {
		root := root
		db, err := globalDatabaseManager.DatabaseFor(ctx, root, filewalk.ReadOnly())
		if err != nil {
			return err
		}
		listers.Go(func() error {
			files, children, disk, errors, err := lsTree(
				ctx,
				pt,
				db,
				root,
				key,
				flagValues,
			)
			resultsCh <- results{
				root:     root,
				files:    files,
				children: children,
				errors:   errors,
				disk:     disk,
				db:       db,
			}
			return err
		})
	}

	errs := errors.M{}
	go func() {
		errs.Append(listers.Wait())
		close(resultsCh)
	}()

	for result := range resultsCh {
		if flagValues.TopN == 0 {
			continue
		}
		files, children, disk, nErrors := result.files, result.children, result.disk, result.errors
		db := result.db
		heading := fmt.Sprintf("\n\nResults for %v", result.root)
		fmt.Println(heading)
		fmt.Println(strings.Repeat("=", len(heading)))

		nFiles, nChildren, nBytes := files.Sum(), children.Sum(), disk.Sum()
		topFiles, topChildren, topBytes := topNMetrics(files.TopN(flagValues.TopN)),
			topNMetrics(children.TopN(flagValues.TopN)),
			topNMetrics(disk.TopN(flagValues.TopN))

		printSummaryStats(ctx, os.Stdout, db, nFiles, nChildren, nBytes, nErrors, flagValues.TopN, topFiles, topChildren, topBytes)
	}
	errs.Append(globalDatabaseManager.Close(ctx))
	return errs.Err()
}

func listErrors(ctx context.Context, values interface{}, args []string) error {
	db, err := globalDatabaseManager.DatabaseFor(ctx, args[0], filewalk.ReadOnly())
	if err != nil {
		return err
	}
	sc := db.NewScanner("", 0, filewalk.ScanErrors())
	for sc.Scan(ctx) {
		prefix, info := sc.PrefixInfo()
		fmt.Printf("%v: %v\n", prefix, info.Err)
	}
	errs := errors.M{}
	errs.Append(sc.Err())
	errs.Append(globalDatabaseManager.Close(ctx))
	return errs.Err()
}
