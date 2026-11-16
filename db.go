// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"cloudeng.io/cmd/idu/internal"
)

type dbCmd struct{}

type locateFlags struct {
	Verbose bool `subcmd:"verbose,false,enable verbose output"`
}

func dirSize(path string) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func (db *dbCmd) locate(ctx context.Context, values interface{}, args []string) error {
	lf := values.(*locateFlags)
	_, prefix, _, err := internal.LookupPrefix(ctx, globalConfig, args[0])
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
			fmt.Printf("database for %v is at: %v\n", args[0], prefix.Database)
			return nil
		}
		return err
	}
	size := info.Size()
	if info.IsDir() {
		if total, err := dirSize(prefix.Database); err == nil {
			size = total
		}
	}
	fmt.Printf("database for %v is at: %v (%v)\n", args[0], prefix.Database, fmtSize(size))
	return nil
}
