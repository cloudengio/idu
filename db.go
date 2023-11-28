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
	_, prefix, err := internal.LookupPrefix(ctx, globalConfig, args[0])
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
