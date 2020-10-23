// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cloudeng.io/errors"
	"cloudeng.io/file/filewalk"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type summaryFlags struct {
	TopN int `subcmd:"top,20,show the top prefixes by file count and disk usage"`
}

type userFlags struct {
	TopN       int    `subcmd:"top,10,show the top prefixes by file count and disk usage"`
	ListUsers  bool   `subcmd:"list-users,false,list available users"`
	AllUsers   bool   `subcmd:"all-users,false,summarize usage for all users"`
	WriteFiles string `subcmd:"reports-dir,,write per-user statistics to the specified directory"`
}

type groupFlags struct {
	TopN       int    `subcmd:"top,10,show the top prefixes by file count and disk usage"`
	ListGroups bool   `subcmd:"list-groups,false,list available groups"`
	AllGroups  bool   `subcmd:"all-groups,false,summarize usage for all groups"`
	WriteFiles string `subcmd:"reports-dir,,write per-group statistics to the specified directory"`
}

func printSummaryStats(ctx context.Context, out io.Writer, db filewalk.Database, nFiles, nChildren, nBytes, nErrors int64, topN int, topFiles, topChildren, topBytes []filewalk.Metric) {
	ifmt := message.NewPrinter(language.English)

	printMetric := func(metric []filewalk.Metric, bytes bool) {
		for _, m := range metric {
			name := globalUserManager.nameForPrefix(ctx, db, m.Prefix)
			if bytes {
				ifmt.Fprintf(out, "%20v: %v (%v)\n", fsize(m.Value), m.Prefix, name)
			} else {
				ifmt.Fprintf(out, "%20v: %v (%v)\n", m.Value, m.Prefix, name)
			}
		}
	}
	ifmt.Fprintf(out, "% 20v : total disk usage\n", fsize(nBytes))
	ifmt.Fprintf(out, "% 20v : total files\n", nFiles)
	ifmt.Fprintf(out, "% 20v : total children\n", nChildren)
	ifmt.Fprintf(out, "% 20v : total errors\n", nErrors)

	fmt.Fprintf(out, "Top %v prefixes by disk usage\n", topN)
	printMetric(topBytes, true)

	fmt.Fprintf(out, "Top %v prefixes by file count\n", topN)
	printMetric(topFiles, false)

	fmt.Fprintf(out, "Top %v prefixes by child count\n", topN)
	printMetric(topChildren, false)
}

func getAllStats(ctx context.Context, db filewalk.Database, n int, opts ...filewalk.MetricOption) (
	nFiles, nChildren, nBytes, nErrors int64,
	topFiles, topChildren, topBytes []filewalk.Metric,
	err error) {
	errs := errors.M{}
	nFiles, err = db.Total(ctx, filewalk.TotalFileCount, opts...)
	errs.Append(err)
	nChildren, err = db.Total(ctx, filewalk.TotalPrefixCount, opts...)
	errs.Append(err)
	nBytes, err = db.Total(ctx, filewalk.TotalDiskUsage, opts...)
	errs.Append(err)
	nErrors, err = db.Total(ctx, filewalk.TotalErrorCount, opts...)
	errs.Append(err)
	topFiles, err = db.TopN(ctx, filewalk.TotalFileCount, n, opts...)
	errs.Append(err)
	topChildren, err = db.TopN(ctx, filewalk.TotalPrefixCount, n, opts...)
	errs.Append(err)
	topBytes, err = db.TopN(ctx, filewalk.TotalDiskUsage, n, opts...)
	errs.Append(err)
	err = errs.Err()
	return
}

func summary(ctx context.Context, values interface{}, args []string) error {
	flagValues := values.(*summaryFlags)
	db, err := globalDatabaseManager.DatabaseFor(ctx, args[0], filewalk.ReadOnly())
	if err != nil {
		return err
	}
	defer globalDatabaseManager.Close(ctx)
	nFiles, nChildren, nBytes, nErrors,
		topFiles, topChildren, topBytes, err :=
		getAllStats(ctx, db, flagValues.TopN, filewalk.Global())
	if err != nil {
		return err
	}
	printSummaryStats(ctx, os.Stdout, db, nFiles, nChildren, nBytes, nErrors, flagValues.TopN, topFiles, topChildren, topBytes)
	return globalDatabaseManager.Close(ctx)
}

func printUsers(ctx context.Context, db filewalk.Database) error {
	users, err := db.UserIDs(ctx)
	if err != nil {
		return err
	}
	for i, u := range users {
		name := globalUserManager.nameForUID(u)
		fmt.Printf("%s(%s)", u, name)
		if i < (len(users) - 1) {
			fmt.Printf(", ")
		} else {
			fmt.Println()
		}
	}
	return nil
}

func printGroups(ctx context.Context, db filewalk.Database) error {
	groups, err := db.GroupIDs(ctx)
	if err != nil {
		return err
	}
	for i, u := range groups {
		name := globalUserManager.nameForGID(u)
		fmt.Printf("%s(%s)", u, name)
		if i < (len(groups) - 1) {
			fmt.Printf(", ")
		} else {
			fmt.Println()
		}
	}
	return nil
}

func createReportsDirIfNeeded(dir string) error {
	if len(dir) > 0 {
		if err := os.MkdirAll(dir, 0777); err != nil {
			fmt.Printf("failed to create directory %v for statistics: %v", dir, err)
			return err
		}
	}
	return nil
}

func reportForUserOrGroup(dir, name string) (io.Writer, func() error, error) {
	if len(dir) == 0 {
		return os.Stdout, func() error { return nil }, nil
	}
	name = filepath.Join(dir, name+".txt")
	f, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		return os.Stdout, func() error { return nil }, err
	}
	return f, func() error { return f.Close() }, nil
}

func userSummary(ctx context.Context, values interface{}, args []string) error {
	flagValues := values.(*userFlags)
	db, err := globalDatabaseManager.DatabaseFor(ctx, args[0], filewalk.ReadOnly())
	if err != nil {
		return err
	}
	args = args[1:]
	if flagValues.ListUsers {
		return printUsers(ctx, db)
	}
	if len(args) == 0 {
		args = []string{os.Getenv("USER")}
	}
	if flagValues.AllUsers {
		args, err = db.UserIDs(ctx)
		if err != nil {
			return err
		}
	}
	errs := errors.M{}
	errs.Append(createReportsDirIfNeeded(flagValues.WriteFiles))
	for _, usr := range args {
		name := globalUserManager.nameForUID(usr)
		key := globalUserManager.uidForName(name)
		out, close, err := reportForUserOrGroup(flagValues.WriteFiles, name)
		errs.Append(err)
		nFiles, nChildren, nBytes, nErrors, topFiles, topChildren, topBytes, err := getAllStats(ctx, db, flagValues.TopN, filewalk.UserID(key))
		errs.Append(err)
		fmt.Fprintf(out, "\nSummary for %v (%v)\n", name, usr)
		printSummaryStats(ctx, out, db, nFiles, nChildren, nBytes, nErrors, flagValues.TopN, topFiles, topChildren, topBytes)
		errs.Append(close())
	}
	errs.Append(globalDatabaseManager.Close(ctx))
	return errs.Err()
}

func groupSummary(ctx context.Context, values interface{}, args []string) error {
	flagValues := values.(*groupFlags)
	db, err := globalDatabaseManager.DatabaseFor(ctx, args[0], filewalk.ReadOnly())
	if err != nil {
		return err
	}
	args = args[1:]
	if flagValues.ListGroups {
		return printGroups(ctx, db)
	}
	if len(args) == 0 && !flagValues.AllGroups {
		return fmt.Errorf("no group specified")
	}
	if flagValues.AllGroups {
		args, err = db.GroupIDs(ctx)
		if err != nil {
			return err
		}
	}

	errs := errors.M{}
	errs.Append(createReportsDirIfNeeded(flagValues.WriteFiles))
	for _, grp := range args {
		name := globalUserManager.nameForGID(grp)
		key := globalUserManager.gidForName(grp)
		out, close, err := reportForUserOrGroup(flagValues.WriteFiles, name)
		errs.Append(err)
		nFiles, nChildren, nBytes, nErrors,
			topFiles, topChildren, topBytes, err := getAllStats(ctx, db, flagValues.TopN, filewalk.GroupID(key))
		errs.Append(err)
		fmt.Fprintf(out, "\nSummary for %v (%v)\n", name, grp)
		printSummaryStats(ctx, out, db, nFiles, nChildren, nBytes, nErrors, flagValues.TopN, topFiles, topChildren, topBytes)
		errs.Append(close())
	}
	errs.Append(globalDatabaseManager.Close(ctx))

	return errs.Err()
}
