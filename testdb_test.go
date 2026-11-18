// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"testing"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/idudb/backends/badgerdb"
)

// openRawDB opens the database at path for reading and writing without
// going through idudb. It must only be used by tests that need to inject
// fixtures or corrupt entries that cannot be created via idudb.
func openRawDB(_ context.Context, path string) (idudb.Database, error) {
	return badgerdb.Open(path)
}

// openPrefixRaw is like openPrefixReadOnly but returns a raw, writable,
// database handle, see openRawDB.
func openPrefixRaw(ctx context.Context, all iduconfig.T, prefix string) (context.Context, *iduconfig.Settings, string, idudb.Database, error) {
	f, target, err := lookupFS(all, prefix)
	if err != nil {
		return ctx, nil, "", nil, err
	}
	cfg := f.Config()
	db, err := openRawDB(ctx, cfg.Database)
	return ctx, cfg, target, db, err
}

// withFS sets the --fs flag value for the duration of the test.
func withFS(t *testing.T, name string) {
	t.Helper()
	orig := globalFlags.FS
	globalFlags.FS = name
	t.Cleanup(func() { globalFlags.FS = orig })
}
