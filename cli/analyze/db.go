// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package analyze

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"cloudeng.io/cmd/idu/internal/cli"
)

// LocateFlags are the flags for the database locate command.
type LocateFlags struct {
	cli.FSFlag
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
		if total, err := dirSize(prefix.Database); err == nil {
			size = total
		}
	}
	fmt.Printf("database for %v is at: %v (%v)\n", f.Location(), prefix.Database, c.env.FmtSize(size))
	return nil
}
