// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package analyze

import (
	"context"
	"fmt"
	"os"

	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
)

// LocateFlags are the flags for the database locate command.
type LocateFlags struct {
	cli.FSFlag
	Verbose bool `subcmd:"verbose,false,enable verbose output"`
}

// Locate implements the database locate command, values must be a
// *LocateFlags.
func (c *Commands) Locate(_ context.Context, values interface{}, args []string) error {
	lf := values.(*LocateFlags)
	f, _, err := c.env.ForFS(lf.FS).LookupFS(cli.PrefixArg(args))
	if err != nil {
		return err
	}
	prefix := f.Config()
	if !lf.Verbose {
		fmt.Printf("%v\n", prefix.Database)
		return nil
	}
	info, err := os.Stat(prefix.Database)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("database for %v is at: %v\n", f.Location(), prefix.Database)
			return nil
		}
		return err
	}
	size := info.Size()
	if info.IsDir() {
		total, err := idudb.DatabaseSize(prefix.Database)
		if err != nil {
			return err
		}
		size = total
	}
	fmt.Printf("database for %v is at: %v (%v)\n", f.Location(), prefix.Database, c.env.FmtSize(size))
	return nil
}
