// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/idudb/backends/badgerdb"
	"cloudeng.io/cmd/idu/internal"
)

// openRawDB opens the database at path for reading and writing without
// going through idudb. It must only be used by tests that need to inject
// fixtures or corrupt entries that cannot be created via idudb.
func openRawDB(_ context.Context, path string) (idudb.Database, error) {
	return badgerdb.Open(path)
}

// openPrefixRaw is like openPrefixReadOnly but returns a raw, writable,
// database handle, see openRawDB.
func openPrefixRaw(ctx context.Context, all iduconfig.T, prefix string) (context.Context, iduconfig.Prefix, string, idudb.Database, error) {
	ctx, cfg, target, err := internal.LookupPrefix(ctx, all, prefix)
	if err != nil {
		return ctx, iduconfig.Prefix{}, "", nil, err
	}
	db, err := openRawDB(ctx, cfg.Database)
	return ctx, cfg, target, db, err
}
