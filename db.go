// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"

	"cloudeng.io/cmd/idu/internal"
)

type dbCmd struct{}

type locateFlags struct {
	Verbose bool `subcmd:"verbose,false,enable verbose output"`
}

func (db *dbCmd) locate(ctx context.Context, values interface{}, args []string) error {
	lf := values.(*locateFlags)
	ctx, prefix, err := internal.LookupPrefix(ctx, globalConfig, args[0])
	if err != nil {
		return err
	}
	if !lf.Verbose {
		fmt.Printf("%v\n", prefix.Database)
		return nil
	}
	info, err := os.Stat(prefix.Database)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("atabase for %v is at: %v\n", args[0], prefix.Database)
			return nil
		}
		return err
	}
	fmt.Printf("database for %v is at: %v (%v)\n", args[0], prefix.Database, fmtSize(info.Size()))
	return nil
}

/*

func (db *database) printDBStats(prefix string, stats []internal.DatabaseStats) {
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

func (db *database) getStats(ctx context.Context, prefix string) ([]internal.DatabaseStats, error) {
	ldb, err := globalDatabaseManager.DatabaseFor(ctx, prefix, internal.ReadOnly())
	if err != nil {
		return nil, err
	}
	defer globalDatabaseManager.Close(ctx, prefix)
	return ldb.Stats()
}

func (db *database) stats(ctx context.Context, values interface{}, args []string) error {
	for i, prefix := range args {
		stats, err := db.getStats(ctx, prefix)
		if err != nil {
			return err
		}
		db.printDBStats(prefix, stats)
		if i < (len(args) - 1) {
			fmt.Println()
		}
	}
	return nil
}

func (db *database) dbTotalSizeAndKeys(ctx context.Context, prefix string) (size, entries int64, err error) {
	stats, err := db.getStats(ctx, prefix)
	if err != nil {
		return
	}
	for _, stat := range stats {
		entries += stat.NumEntries
		size += stat.Size
	}
	return
}

func (db *database) dbCompact(ctx context.Context, values interface{}, args []string) error {
	var errs errors.M
	ifmt := message.NewPrinter(language.English)
	for _, prefix := range args {
		beforeSize, beforeEntries, _ := db.dbTotalSizeAndKeys(ctx, prefix)
		if err := globalDatabaseManager.Compact(ctx, prefix); err != nil {
			errs.Append(err)
			continue
		}
		afterSize, afterEntries, err := db.dbTotalSizeAndKeys(ctx, prefix)
		errs.Append(err)
		ifmt.Printf("compacted database for: %v: entries %v -> %v, size %v -> %v\n",
			prefix,
			beforeEntries, afterEntries,
			fsize(beforeSize), fsize(afterSize))

	}
	return errs.Err()
}

func (db *database) dbRmPrefixes(ctx context.Context, values interface{}, args []string) error {
	var errs errors.M
	for _, prefix := range args {
		db, err := globalDatabaseManager.DatabaseFor(ctx, prefix)
		if err != nil {
			errs.Append(err)
			continue
		}
		layout := globalConfig.LayoutFor(prefix)
		_, err = db.Delete(ctx, layout.Separator, []string{prefix}, true)
		if err != nil {
			errs.Append(fmt.Errorf("prefix deletion: %v", err))
		}
		_, err = db.DeleteErrors(ctx, []string{prefix})
		if err != nil {
			errs.Append(fmt.Errorf("error deletion: %v", err))
		}
	}
	errs.Append(globalDatabaseManager.CloseAll(ctx))
	return errs.Err()
}
*/
