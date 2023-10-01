// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/database/boltdb"
	"cloudeng.io/cmdutil/flags"
	"golang.org/x/exp/slog"
)

type TimeRangeFlags struct {
	Since time.Duration `subcmd:"since,0s,'display entries since the specified duration, it takes precedence over from/to'"`
	From  flags.Time    `subcmd:"from,,display entries starting at this time/date"`
	To    flags.Time    `subcmd:"to,,display entries ending at this time/date"`
}

func (tr *TimeRangeFlags) FromTo() (from, to time.Time, err error) {
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

type LogLevel int

const (
	LogProgress LogLevel = iota
	LogError
	LogPrefix
	LogInfo
)

var Verbosity LogLevel = LogError

func Log(ctx context.Context, logger *slog.Logger, level LogLevel, msg string, args ...interface{}) {
	if level > Verbosity {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:]) // skip [Callers, infof]
	r := slog.NewRecord(time.Now(), slog.LevelInfo, msg, 0)
	r.Add(args...)
	_ = logger.Handler().Handle(ctx, r)
}

func LookupPrefix(all config.T, prefix string) (config.Prefix, string, error) {
	if filepath.IsLocal(prefix) || len(prefix) == 0 {
		if dir, err := os.Getwd(); err == nil {
			prefix = filepath.Join(dir, prefix)
		}
	}
	cfg, path, ok := all.ForPrefix(prefix)
	if !ok {
		return cfg, "", fmt.Errorf("no configuration for %v", prefix)
	}
	return cfg, path, nil
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
			fmt.Printf("waiting for database to open: %v: %s\t\t\r", cfg.Database, time.Now().Sub(start).Truncate(time.Second))
			delayed = true
		}
	}
}

func OpenPrefixAndDatabase(ctx context.Context, all config.T, prefix string, opts ...boltdb.Option) (config.Prefix, string, database.DB, error) {
	cfg, path, err := LookupPrefix(all, prefix)
	if err != nil {
		return config.Prefix{}, "", nil, err
	}
	db, err := OpenDatabase(ctx, cfg, opts...)
	if err != nil {
		return config.Prefix{}, "", nil, fmt.Errorf("failed to open database for %v in %v: %v\n", cfg.Prefix, cfg.Database, err)
	}
	return cfg, path, db, nil
}
