// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package badgerdb

import (
	"context"
	"time"

	"cloudeng.io/logging/ctxlog"
	"github.com/dgraph-io/badger/v4"
	badgeroptions "github.com/dgraph-io/badger/v4/options"
)

// NewBackend opens the badger database at path configured for use by idu,
// opening it for reading only if readonly is true. Log messages are written
// to the logger in ctx, see cloudeng.io/logging/ctxlog, and any deadline on
// ctx is used as the timeout for acquiring the database's lock.
func NewBackend(ctx context.Context, path string, readonly bool) (*Database, error) {
	opts := make([]Option, 0, 3)
	if readonly {
		opts = append(opts, ReadOnly())
	}
	if deadline, ok := ctx.Deadline(); ok {
		if timeout := time.Until(deadline); timeout > 0 {
			opts = append(opts, WithTimeout(timeout))
		}
	}
	bopts := badger.DefaultOptions(path).
		WithLogger(NewLogger(ctxlog.Logger(ctx))).
		WithDetectConflicts(false).
		WithMetricsEnabled(false).
		WithIndexCacheSize(64 << 20)
	if !readonly {
		bopts = bopts.
			WithCompactL0OnClose(true).
			WithCompression(badgeroptions.ZSTD).
			WithBlockCacheSize(32 << 20)
	}
	opts = append(opts, WithBadgerOptions(bopts))
	return Open(path, opts...)
}
