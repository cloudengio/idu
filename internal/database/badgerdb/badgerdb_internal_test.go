// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package badgerdb

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestFlushReplacesFailedWriteBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	bopts := badger.DefaultOptions(dir).WithLogger(nil)
	raw, err := Open(dir, WithBadgerOptions(bopts))
	if err != nil {
		t.Fatal(err)
	}
	db := raw.(*Database)
	defer db.Close(ctx)

	if err := db.Set(ctx, "/k1", []byte("v1"), true); err != nil {
		t.Fatal(err)
	}
	// Cancel the underlying WriteBatch so Flush fails.
	db.batch.batch.Cancel()
	if err := db.Flush(ctx); err == nil {
		t.Fatal("expected Flush to fail on canceled WriteBatch")
	}

	// Subsequent batched writes and Flush must succeed using a fresh WriteBatch.
	if err := db.Set(ctx, "/k2", []byte("v2"), true); err != nil {
		t.Fatalf("Set after failed Flush: %v", err)
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush after failed Flush: %v", err)
	}
	var buf bytes.Buffer
	if err := db.Get(ctx, "/k2", &buf); err != nil || buf.String() != "v2" {
		t.Errorf("Get(/k2) = (%q, %v), want (\"v2\", nil)", buf.String(), err)
	}
}

func TestBatchSetAndFlushAfterClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	bopts := badger.DefaultOptions(dir).WithLogger(nil)
	raw, err := Open(dir, WithBadgerOptions(bopts))
	if err != nil {
		t.Fatal(err)
	}
	db := raw.(*Database)
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}

	if err := db.Set(ctx, "/after", []byte("v"), true); !errors.Is(err, badger.ErrDBClosed) {
		t.Errorf("batched Set after Close = %v, want badger.ErrDBClosed", err)
	}
	if err := db.Flush(ctx); !errors.Is(err, badger.ErrDBClosed) {
		t.Errorf("Flush after Close = %v, want badger.ErrDBClosed", err)
	}
}
