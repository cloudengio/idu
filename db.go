// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"strings"

	"cloudeng.io/errors"
	"cloudeng.io/file/filewalk"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type eraseFlags struct {
	ReallyDelete bool `subcmd:"really,false,must be set to erase the database"`
}

func dbErase(ctx context.Context, values interface{}, args []string) error {
	flagValues := values.(*eraseFlags)
	if !flagValues.ReallyDelete {
		fmt.Printf("use --really to erase/delete the database\n")
		return nil
	}
	dbCfg, ok := globalConfig.DatabaseFor(args[0])
	if !ok {
		return fmt.Errorf("no database found for %v", args[0])
	}
	fmt.Printf("deleting: %s\n", dbCfg.Description)
	return dbCfg.Delete(ctx)
}

func dbRefreshStats(ctx context.Context, values interface{}, args []string) error {
	db, err := globalDatabaseManager.DatabaseFor(ctx, args[0], filewalk.ResetStats())
	if err != nil {
		return err
	}
	sc := db.NewScanner(args[0], 0, filewalk.ScanLimit(500))
	i := 0
	printer := message.NewPrinter(language.English)
	for sc.Scan(ctx) {
		prefix, info := sc.PrefixInfo()
		layout := globalConfig.LayoutFor(prefix)
		info.DiskUsage = 0
		for _, file := range info.Files {
			info.DiskUsage += layout.Calculator.Calculate(file.Size)
		}
		if err := db.Set(ctx, prefix, info); err != nil {
			return fmt.Errorf("failed to set: %v: %v", prefix, err)
		}
		if i%1000 == 0 && i != 0 {
			printer.Printf("processed: % 15v\r", i)
		}
		i++
	}
	if sc.Err() != nil {
		return fmt.Errorf("scanner error: %v", sc.Err())
	}
	return globalDatabaseManager.CloseAll(ctx)
}

func printDBStats(prefix string, stats []filewalk.DatabaseStats) {
	ifmt := message.NewPrinter(language.English)
	ifmt.Printf("%v\n", prefix)
	ifmt.Printf("%s\n\n", strings.Repeat("=", len(prefix)))
	for i, s := range stats {
		ifmt.Printf("         Name: %v\n", s.Name)
		ifmt.Printf("  Description: %v\n", s.Description)
		ifmt.Printf("    # Entries: % 10v\n", s.NumEntries)
		ifmt.Printf("         Size: % 10v\n", fsize(s.Size))
		if i < len(stats)-1 {
			ifmt.Printf("\n")
		}
	}
}

func getStats(ctx context.Context, prefix string) ([]filewalk.DatabaseStats, error) {
	db, err := globalDatabaseManager.DatabaseFor(ctx, prefix, filewalk.ReadOnly())
	if err != nil {
		return nil, err
	}
	defer globalDatabaseManager.Close(ctx, prefix)
	return db.Stats()
}

func dbStats(ctx context.Context, values interface{}, args []string) error {
	for i, prefix := range args {
		stats, err := getStats(ctx, prefix)
		if err != nil {
			return err
		}
		printDBStats(prefix, stats)
		if i < (len(args) - 1) {
			fmt.Println()
		}
	}
	return nil
}

func dbTotalSizeAndKeys(ctx context.Context, prefix string) (size, entries int64, err error) {
	stats, err := getStats(ctx, prefix)
	if err != nil {
		return
	}
	for _, stat := range stats {
		entries += stat.NumEntries
		size += stat.Size
	}
	return
}

func dbCompact(ctx context.Context, values interface{}, args []string) error {
	var errs errors.M
	ifmt := message.NewPrinter(language.English)
	for _, prefix := range args {
		beforeSize, beforeEntries, _ := dbTotalSizeAndKeys(ctx, prefix)
		if err := globalDatabaseManager.Compact(ctx, prefix); err != nil {
			errs.Append(err)
			continue
		}
		afterSize, afterEntries, err := dbTotalSizeAndKeys(ctx, prefix)
		errs.Append(err)
		ifmt.Printf("compacted database for: %v: entries %v -> %v, size %v -> %v\n",
			prefix,
			beforeEntries, afterEntries,
			fsize(beforeSize), fsize(afterSize))

	}
	return errs.Err()
}

func dbRmPrefixes(ctx context.Context, values interface{}, args []string) error {
	var errs errors.M
	for _, prefix := range args {
		db, err := globalDatabaseManager.DatabaseFor(ctx, prefix)
		if err != nil {
			errs.Append(err)
			continue
		}
		layout := globalConfig.LayoutFor(prefix)
		_, err = db.Delete(ctx, layout.Separator, []string{prefix}, true)
		errs.Append(err)
	}
	errs.Append(globalDatabaseManager.CloseAll(ctx))
	return errs.Err()
}
