// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/database/boltdb"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmdutil/flags"
)

type TimeRangeFlags struct {
	Since time.Duration `subcmd:"since,0s,'display entries since the specified duration, it takes precedence over from/to'"`
	From  flags.Time    `subcmd:"from,,display entries starting at this time/date"`
	To    flags.Time    `subcmd:"to,,display entries ending at this time/date"`
}

func (tr *TimeRangeFlags) FromTo() (from, to time.Time, set bool, err error) {
	set = tr.Since != 0 || !from.IsZero() || !to.IsZero()
	if !set {
		return
	}
	to = time.Now()
	if tr.Since > 0 {
		from = to.Add(-tr.Since)
		return
	}
	if !tr.From.IsDefault() {
		from = tr.From.Get().(time.Time)
	}
	if !tr.To.IsDefault() {
		to = tr.To.Get().(time.Time)
	}
	return
}

func LookupPrefix(ctx context.Context, all config.T, prefix string) (context.Context, config.Prefix, error) {
	if filepath.IsLocal(prefix) || len(prefix) == 0 {
		if dir, err := os.Getwd(); err == nil {
			prefix = filepath.Join(dir, prefix)
		}
	}
	cfg, _, ok := all.ForPrefix(prefix)
	if !ok {
		return ctx, cfg, fmt.Errorf("no configuration for %v", prefix)
	}
	return ctx, cfg, nil
}

func OpenDatabase(ctx context.Context, cfg config.Prefix, opts ...boltdb.Option) (database.DB, error) {
	doneCh := make(chan struct {
		db  database.DB
		err error
	}, 1)

	go func() {
		withTimeout := []boltdb.Option{boltdb.WithTimeout(10 * time.Second)}
		db, err := boltdb.Open(cfg.Database, cfg.Prefix, append(withTimeout, opts...)...)
		doneCh <- struct {
			db  database.DB
			err error
		}{db, err}

	}()
	start := time.Now()
	delayed := false
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-doneCh:
			if delayed {
				fmt.Println()
			}
			return res.db, res.err
		case <-time.After(time.Second):
			fmt.Printf("waiting for database to open: %v: %s\t\t\r", cfg.Database, time.Since(start).Truncate(time.Second))
			delayed = true
		}
	}
}

func OpenPrefixAndDatabase(ctx context.Context, all config.T, prefix string, opts ...boltdb.Option) (context.Context, config.Prefix, database.DB, error) {
	ctx, cfg, err := LookupPrefix(ctx, all, prefix)
	if err != nil {
		return ctx, config.Prefix{}, nil, err
	}
	db, err := OpenDatabase(ctx, cfg, opts...)
	if err != nil {
		return ctx, config.Prefix{}, nil, fmt.Errorf("failed to open database for %v in %v: %v\n", cfg.Prefix, cfg.Database, err)
	}
	return ctx, cfg, db, nil
}

type prefixInfo struct {
	name string
	prefixinfo.T
}

// PrefixInfoAsFSInfo returns a fs.FileInfo for the supplied PrefixInfo.
func PrefixInfoAsFSInfo(pi prefixinfo.T, name string) fs.FileInfo {
	return &prefixInfo{T: pi, name: name}
}

func (pi *prefixInfo) Name() string {
	return pi.name
}

func (pi *prefixInfo) IsDir() bool {
	return pi.Mode().IsDir()
}

func (pi *prefixInfo) Sys() any {
	return nil
}
