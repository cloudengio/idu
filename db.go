// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"

	"cloudeng.io/file/filewalk"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type eraseFlags struct {
	ReallyDelete bool `subcmd:"really,false,must be set to erase the database"`
}

func erase(ctx context.Context, values interface{}, args []string) error {
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

func refreshStats(ctx context.Context, values interface{}, args []string) error {
	db, err := globalDatabaseManager.DatabaseFor(ctx, args[0], filewalk.ResetStats())
	if err != nil {
		return err
	}
	sc := db.NewScanner("", 0, filewalk.ScanLimit(500))
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
		return sc.Err()
	}
	return globalDatabaseManager.Close(ctx)
}
