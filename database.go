// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
)

// openProgress is where messages are written while waiting for a database
// to open, it may be overridden by tests.
var openProgress io.Writer = os.Stdout

// databaseFactory creates the database backend and may be overridden by tests.
var databaseFactory idudb.BackendFactory = badgerBackend

// dbOpenOptions returns the options used to open all databases.
func dbOpenOptions() []idudb.OpenOption {
	return []idudb.OpenOption{
		idudb.WithBackendFactory(databaseFactory),
		idudb.WithOpenProgress(openProgress),
	}
}

// openDatabase opens the database backend at path, waiting until it is
// opened or ctx is canceled and displaying a message while waiting.
func openDatabase(ctx context.Context, path string, readonly bool) (idudb.Database, error) {
	return idudb.OpenBackend(ctx, path, readonly, dbOpenOptions()...)
}

// openReadOnlyDB opens the database specified by cfg for reading only.
func openReadOnlyDB(ctx context.Context, cfg *iduconfig.Settings) (idudb.Reader, error) {
	return idudb.Open(ctx, cfg, dbOpenOptions()...)
}

// openWritableDB opens the database specified by cfg for use by analyze.
func openWritableDB(ctx context.Context, cfg *iduconfig.Settings) (*idudb.DB, error) {
	return idudb.OpenWritable(ctx, cfg, dbOpenOptions())
}

// openPrefixReadOnly resolves prefix to its configuration and the target
// path to operate on and opens the corresponding database for reading.
func openPrefixReadOnly(ctx context.Context, all iduconfig.T, prefix string) (context.Context, *iduconfig.Settings, string, idudb.Reader, error) {
	ctx, f, target, err := lookupPrefix(ctx, all, prefix)
	if err != nil {
		return ctx, nil, "", nil, err
	}
	cfg := f.Config()
	db, err := openReadOnlyDB(ctx, cfg)
	if err != nil {
		return ctx, nil, "", nil, fmt.Errorf("failed to open database for %v in %v: %w", iduconfig.Label(f), cfg.Database, err)
	}
	return ctx, cfg, target, db, nil
}

// eraseEntries opens the database for prefix and calls erase on it.
func eraseEntries(ctx context.Context, prefix string, erase func(context.Context, idudb.Database) error) error {
	ctx, f, _, err := lookupPrefix(ctx, globalConfig, prefix)
	if err != nil {
		return err
	}
	cfg := f.Config()
	db, err := openDatabase(ctx, cfg.Database, false)
	if err != nil {
		return fmt.Errorf("failed to open database for %v in %v: %w", iduconfig.Label(f), cfg.Database, err)
	}
	if err := erase(ctx, db); err != nil {
		_ = db.Close(context.WithoutCancel(ctx))
		return fmt.Errorf("failed to erase entries in the database for %v in %v: %w", iduconfig.Label(f), cfg.Database, err)
	}
	return db.Close(ctx)
}
