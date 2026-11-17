// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/idudb/backends/badgerdb"
	"cloudeng.io/logging/ctxlog"
	"github.com/dgraph-io/badger/v4"
	badgeroptions "github.com/dgraph-io/badger/v4/options"
)

// openBadgerDB opens the badger database backend at path.
func openBadgerDB(ctx context.Context, path string, readonly bool) (idudb.Database, error) {
	var opts []badgerdb.Option
	if readonly {
		opts = append(opts, badgerdb.ReadOnly())
	}
	if deadline, ok := ctx.Deadline(); ok {
		if timeout := time.Until(deadline); timeout > 0 {
			opts = append(opts, badgerdb.WithTimeout(timeout))
		}
	}
	bopts := badger.DefaultOptions(path).
		WithLogger(badgerdb.NewLogger(ctxlog.Logger(ctx))).
		WithDetectConflicts(false).
		WithMetricsEnabled(false).
		WithIndexCacheSize(64 << 20)
	if !readonly {
		bopts = bopts.
			WithCompactL0OnClose(true).
			WithCompression(badgeroptions.ZSTD).
			WithBlockCacheSize(32 << 20)
	}
	opts = append(opts, badgerdb.WithBadgerOptions(bopts))
	return badgerdb.Open(path, opts...)
}

// openProgress is where messages are written while waiting for a database
// to open, it may be overridden by tests.
var openProgress io.Writer = os.Stdout

// databaseFactory creates the database backend and may be overridden by tests.
var databaseFactory = openBadgerDB

// openDatabase opens the database backend at path, waiting until it is
// opened or ctx is canceled and displaying a message while waiting.
func openDatabase(ctx context.Context, path string, readonly bool) (idudb.Database, error) {
	type result struct {
		db  idudb.Database
		err error
	}
	doneCh := make(chan result, 1)
	go func() {
		db, err := databaseFactory(ctx, path, readonly)
		doneCh <- result{db, err}
	}()
	start := time.Now()
	delayed := false
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			closeCtx := context.WithoutCancel(ctx)
			go func() {
				res := <-doneCh
				if res.err == nil && res.db != nil {
					_ = res.db.Close(closeCtx)
				}
			}()
			return nil, ctx.Err()
		case res := <-doneCh:
			if delayed {
				fmt.Fprintln(openProgress)
			}
			return res.db, res.err
		case <-ticker.C:
			fmt.Fprintf(openProgress, "waiting for database to open: %v: %s\t\t\r", path, time.Since(start).Truncate(time.Second))
			delayed = true
		}
	}
}

// openReadOnlyDB opens the database at path for reading only.
func openReadOnlyDB(ctx context.Context, path string) (idudb.Reader, error) {
	db, err := openDatabase(ctx, path, true)
	if err != nil {
		return nil, err
	}
	if err := idudb.CheckFormat(ctx, db); err != nil {
		_ = db.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("%v: %w", path, err)
	}
	return db, nil
}

// openWritableDB opens the database specified by cfg for use by analyze.
func openWritableDB(ctx context.Context, cfg iduconfig.Prefix) (*idudb.DB, error) {
	raw, err := openDatabase(ctx, cfg.Database, false)
	if err != nil {
		return nil, err
	}
	db, err := idudb.New(ctx, raw, idudb.OptionsFromConfig(cfg)...)
	if err != nil {
		_ = raw.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("%v: %w", cfg.Database, err)
	}
	return db, nil
}

// openPrefixReadOnly resolves prefix to its configuration and the target
// path to operate on and opens the corresponding database for reading.
func openPrefixReadOnly(ctx context.Context, all iduconfig.T, prefix string) (context.Context, iduconfig.Prefix, string, idudb.Reader, error) {
	ctx, cfg, target, err := lookupPrefix(ctx, all, prefix)
	if err != nil {
		return ctx, iduconfig.Prefix{}, "", nil, err
	}
	db, err := openReadOnlyDB(ctx, cfg.Database)
	if err != nil {
		return ctx, iduconfig.Prefix{}, "", nil, fmt.Errorf("failed to open database for %v in %v: %w", cfg.Prefix, cfg.Database, err)
	}
	return ctx, cfg, target, db, nil
}

// eraseEntries opens the database for prefix and calls erase on it.
func eraseEntries(ctx context.Context, prefix string, erase func(context.Context, idudb.Database) error) error {
	ctx, cfg, _, err := lookupPrefix(ctx, globalConfig, prefix)
	if err != nil {
		return err
	}
	db, err := openDatabase(ctx, cfg.Database, false)
	if err != nil {
		return fmt.Errorf("failed to open database for %v in %v: %w", cfg.Prefix, cfg.Database, err)
	}
	if err := erase(ctx, db); err != nil {
		_ = db.Close(context.WithoutCancel(ctx))
		return fmt.Errorf("failed to erase entries in the database for %v in %v: %w", cfg.Prefix, cfg.Database, err)
	}
	return db.Close(ctx)
}
