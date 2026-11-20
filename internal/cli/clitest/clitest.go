// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package clitest provides support for testing idu's command groups.
package clitest

import (
	"context"
	"testing"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/idudb/backends/badgerdb"
	"cloudeng.io/cmd/idu/internal/cli"
)

// OpenRawDB opens the database at path for reading and writing without
// going through idudb. It must only be used by tests that need to inject
// fixtures or corrupt entries that cannot be created via idudb.
func OpenRawDB(_ context.Context, path string) (idudb.Database, error) {
	return badgerdb.Open(path)
}

// OpenPrefixRaw is like cli.Env.OpenPrefixReadOnly but returns a raw,
// writable, database handle, see OpenRawDB.
func OpenPrefixRaw(ctx context.Context, env *cli.Env, prefix string) (context.Context, *iduconfig.Settings, string, idudb.Database, error) {
	f, target, err := env.LookupFS(prefix)
	if err != nil {
		return ctx, nil, "", nil, err
	}
	cfg := f.Config()
	db, err := OpenRawDB(ctx, cfg.Database)
	return ctx, cfg, target, db, err
}

// WithFS sets env.FS, ie. the --fs flag, for the duration of the test.
func WithFS(t *testing.T, env *cli.Env, name string) {
	t.Helper()
	orig := env.FS
	env.FS = name
	t.Cleanup(func() { env.FS = orig })
}

// WithConfig sets env.Config for the duration of the test.
func WithConfig(t *testing.T, env *cli.Env, cfg iduconfig.T) {
	t.Helper()
	orig := env.Config
	env.Config = cfg
	t.Cleanup(func() { env.Config = orig })
}
