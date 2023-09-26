// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database/boltdb"
)

type lsFlags struct {
	Limit      int    `subcmd:"limit,-1,'limit the number of items to list'"`
	TopN       int    `subcmd:"top,10,'show the top prefixes by file/prefix counts and disk usage, set to zero to disable'"`
	Recurse    bool   `subcmd:"recurse,false,list prefixes recursively"`
	Summary    bool   `subcmd:"summary,true,show summary statistics"`
	ShowDirs   bool   `subcmd:"prefixes,false,show information on each prefix"`
	ShowFiles  bool   `subcmd:"files,false,show information on individual files"`
	ShowErrors bool   `subcmd:"errors,false,show information on individual errors"`
	User       string `subcmd:"user,,show information for this user only"`
}

type logFlags struct {
	internal.TimeRangeFlags
}

type errorFlags struct {
	Prefix bool   `subcmd:"prefix,false,list errors by prefix"`
	From   string `subcmd:"from,,the time to start listing errors"`
	To     string `subcmd:"to,,the time to stop listing errors"`
}

type lister struct {
	prefix config.Prefix
}

/*
func (l *lister) lsTree(ctx context.Context, pt *progressTracker, db internal.Database, root, user string, flags *lsFlags) (files, children, disk *heap.KeyedInt64, nerrors int64, err error) {
	files, children, disk = heap.NewKeyedInt64(heap.Descending), heap.NewKeyedInt64(heap.Descending), heap.NewKeyedInt64(heap.Descending)
	if flags.ShowDirs {
		fmt.Printf("     disk usage :  # files : # dirs : directory/prefix\n")
	}
	sc := db.NewScanner(root, flags.Limit, internal.ScanLimit(10000))
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
			if !flags.ShowDirs && !flags.ShowFiles {
				pt.send(ctx, progressUpdate{prefixStart: 1, prefixDone: 1, errors: 1})
			}
			continue
		}
		files.Update(prefix, int64(len(pi.Files)))
		children.Update(prefix, int64(len(pi.Children)))
		storageBytes := l.prefix.StorageBytes(pi.Size)
		disk.Update(prefix, storageBytes)
		if flags.ShowDirs || flags.ShowFiles {
			fmt.Printf("% 15v : % 8v : % 6v : %s\n", fmtSize(storageBytes), len(pi.Files), len(pi.Children), prefix)
			if flags.ShowDirs {
				for _, fi := range pi.Children {
					fmt.Printf("    % 15v : % 40v: % 10v : %v\n", fmtSize(fi.Size()), fi.ModTime(), globalUserManager.nameForUID(fi.User()), fi.Name())
				}
			}
			if flags.ShowFiles {
				for _, fi := range pi.Files {
					fmt.Printf("    % 15v : % 40v: % 10v : %v\n", fmtSize(fi.Size()), fi.ModTime(), globalUserManager.nameForUID(fi.User()), fi.Name())
				}
			}
			continue
		}
		pt.send(ctx, progressUpdate{prefixStart: 1, prefixDone: 1, files: len(pi.Files)})
	}
	err = sc.Err()
	return
}
*/
/*
func topNMetrics(top []struct {
	K string
	V int64
}) []internal.Metric {
	m := make([]internal.Metric, len(top))
	for i, kv := range top {
		m[i] = internal.Metric{Prefix: kv.K, Value: kv.V}
	}
	return m
}*/

func (l *lister) prefixes(ctx context.Context, values interface{}, args []string) error {
	flagValues := values.(*lsFlags)
	if len(args) > 1 {
		flagValues.ShowFiles = false
		flagValues.ShowDirs = false
		flagValues.Summary = true
	}
	_, path, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], boltdb.ReadOnly())
	if err != nil {
		return err
	}
	return db.Scan(ctx, path, func(_ context.Context, k string, v []byte) bool {
		var pi internal.PrefixInfo
		if err := pi.UnmarshalBinary(v); err != nil {
			return false
		}
		fmt.Printf("%v %#v\n", k, pi)
		return strings.HasPrefix(k, path)
	})
}

/*
	type results struct {
		root                  string
		files, children, disk *heap.KeyedInt64
		errors                int64
		db                    internal.Database
	}


	key := ""
	if usr := flagValues.User; len(usr) > 0 {
		key = globalUserManager.uidForName(usr)
	}

	var pt *progressTracker
	if !flagValues.ShowFiles && !flagValues.ShowDirs {
		pt = newProgressTracker(ctx, time.Second)
	}
	listers := &errgroup.T{}
	listers = errgroup.WithConcurrency(listers, len(args))
	resultsCh := make(chan results)
	for _, root := range args {
		root := root
		db, err := globalDatabaseManager.DatabaseFor(ctx, root, internal.ReadOnly())
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
		heading := fmt.Sprintf("\n\nResults for %v", result.root)
		fmt.Println(heading)
		fmt.Println(strings.Repeat("=", len(heading)))

		nFiles, nChildren, nBytes := files.Sum(), children.Sum(), disk.Sum()
		topFiles, topChildren, topBytes := topNMetrics(files.TopN(flagValues.TopN)),
			topNMetrics(children.TopN(flagValues.TopN)),
			topNMetrics(disk.TopN(flagValues.TopN))

		printSummaryStats(ctx, os.Stdout, nFiles, nChildren, nBytes, nErrors, flagValues.TopN, topFiles, topChildren, topBytes)
	}
	errs.Append(db.Close())
	return errs.Err()
}*/

func (l *lister) errors(ctx context.Context, values interface{}, args []string) error {
	ef := values.(*errorFlags)
	_, path, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], boltdb.ReadOnly())
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	if ef.Prefix {
		return db.VisitErrorsKey(ctx, path,
			func(_ context.Context, when time.Time, key string, detail []byte) bool {
				fmt.Printf("%s: %s\n", key, detail)
				return true
			})
	}
	from, to := time.Time{}, time.Now()
	if len(ef.From) > 0 {
		from, err = time.Parse(time.RFC3339, ef.From)
		if err != nil {
			return err
		}
	}
	if len(ef.To) > 0 {
		to, err = time.Parse(time.RFC3339, ef.To)
		if err != nil {
			return err
		}
	}
	return db.VisitErrorsWhen(ctx, from, to, func(_ context.Context, when time.Time, key string, detail []byte) bool {
		fmt.Printf("%s: %s\n", key, detail)
		return true
	})
}

func (l *lister) logs(ctx context.Context, values interface{}, args []string) error {
	fv := values.(*logFlags)
	_, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], boltdb.ReadOnly())
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	from, to, err := fv.TimeRangeFlags.FromTo()
	if err != nil {
		return err
	}
	return db.VisitLogs(ctx, from, to,
		func(_ context.Context, begin, end time.Time, detail []byte) bool {
			fmt.Printf("%v...%v: %v: %s\n", begin, end, end.Sub(begin), detail)
			return true
		})
}
