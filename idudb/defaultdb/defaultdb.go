// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package defaultdb opens idu databases using the backend used by the idu
// command, ie. badgerdb. It allows other commands to read the databases
// that idu creates without having to choose and configure a backend.
//
// For example, to read the summaries for a configured filesystem f:
//
//	sr, err := summary.Open(ctx, f.Config(), defaultdb.OpenOptions()...)
//
// and to read its current statistics, see snapshot.Computer.Current:
//
//	sf, path, err := snapshot.Computer{}.Current(ctx, f, dir, n, defaultdb.OpenOptions()...)
package defaultdb

import (
	"context"
	"fmt"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/idudb/backends/badgerdb"
)

// BackendFactory returns the factory for the default backend.
func BackendFactory() idudb.BackendFactory {
	return idudb.NewBackendFactory(badgerdb.NewBackend)
}

// OpenOptions returns the options for opening a database with the default
// backend, followed by opts, eg. idudb.WithOpenProgress to display a
// message while waiting for a database that is locked by another process,
// eg. an analysis, to be released.
func OpenOptions(opts ...idudb.OpenOption) []idudb.OpenOption {
	return append([]idudb.OpenOption{idudb.WithBackendFactory(BackendFactory())}, opts...)
}

// OpenReadOnly opens the database specified by cfg for reading only, see
// OpenOptions for opts.
func OpenReadOnly(ctx context.Context, cfg *iduconfig.Settings, opts ...idudb.OpenOption) (idudb.Reader, error) {
	db, err := idudb.Open(ctx, cfg, OpenOptions(opts...)...)
	if err != nil {
		return nil, fmt.Errorf("failed to open database %v: %w", cfg.Database, err)
	}
	return db, nil
}
