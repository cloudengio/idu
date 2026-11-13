// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package badgerdb_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/database/badgerdb"
	"github.com/dgraph-io/badger/v4"
)

func TestBatch(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	bopts := badger.DefaultOptions(tmpdir)
	bopts = bopts.WithMemTableSize(1 << 15)
	bopts = bopts.WithValueThreshold(1024)
	db, err := badgerdb.Open(tmpdir, badgerdb.WithBadgerOptions(bopts))
	if err != nil {
		t.Fatal(err)
	}
	bdb := db.(*badgerdb.Database).BadgerDB()
	mc := bdb.MaxBatchCount()
	t.Logf("max batch count: %v", mc)
	for i := int64(0); i < (mc*2)+3; i++ {
		_ = db.Set(ctx, fmt.Sprintf("/%08v", i), []byte(fmt.Sprintf("%08v", i)), true)
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}

	db, err = badgerdb.Open(tmpdir, badgerdb.ReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	i := 0
	_ = db.Scan(ctx, "", func(_ context.Context, k string, v []byte) bool {
		if got, want := k, fmt.Sprintf("/%08v", i); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := string(v), fmt.Sprintf("%08v", i); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		i++
		return true
	})
}

func TestCancelAndDoubleClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tmpdir := t.TempDir()
	db, err := badgerdb.Open(tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(ctx, "/a", []byte("val-a"), true); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := db.Set(ctx, "/b", []byte("val-b"), true); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if err := db.Close(context.Background()); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := db.Close(context.Background()); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}

	rdb, err := badgerdb.Open(tmpdir, badgerdb.ReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(context.Background(), "/c", []byte("val-c"), true); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Fatalf("got %v, want ErrReadOnlyTxn", err)
	}
	if err := rdb.Close(context.Background()); err != nil {
		t.Fatalf("readonly first Close failed: %v", err)
	}
	if err := rdb.Close(context.Background()); err != nil {
		t.Fatalf("readonly second Close failed: %v", err)
	}
}

func TestLastLogAcrossBuckets(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	db, err := badgerdb.Open(tmpdir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)

	if err := db.Set(ctx, "/prefix/a", []byte("1"), false); err != nil {
		t.Fatal(err)
	}
	if err := db.LogError(ctx, "/err/1", time.Now(), []byte("boom")); err != nil {
		t.Fatal(err)
	}
	t1 := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 2, 10, 0, 0, 0, time.UTC)
	if err := db.Log(ctx, t1, t1.Add(time.Minute), []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := db.Log(ctx, t2, t2.Add(time.Minute), []byte("second")); err != nil {
		t.Fatal(err)
	}

	start, stop, detail, err := db.LastLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(t2) || !stop.Equal(t2.Add(time.Minute)) || string(detail) != "second" {
		t.Fatalf("got (%v, %v, %q), want (%v, %v, %q)", start, stop, detail, t2, t2.Add(time.Minute), "second")
	}
}

func TestCompactAndEmptyLastLog(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()

	db, err := badgerdb.Open(tmpdir)
	if err != nil {
		t.Fatal(err)
	}

	// Empty log bucket returns zero times and nil error.
	start, stop, detail, err := db.LastLog(ctx)
	if err != nil || !start.IsZero() || !stop.IsZero() || len(detail) != 0 {
		t.Fatalf("expected empty LastLog without error, got (%v, %v, %v, %v)", start, stop, detail, err)
	}

	for i := range 20 {
		if err := db.Set(ctx, fmt.Sprintf("/k/%02d", i), []byte("val"), true); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.(*badgerdb.Database).Compact(ctx); err != nil {
		t.Fatal(err)
	}
	// Writes after Compact still succeed before Close.
	if err := db.Set(ctx, "/k/after", []byte("after"), true); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Read-only Compact returns badger.ErrReadOnlyTxn.
	rdb, err := badgerdb.Open(tmpdir, badgerdb.ReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close(ctx)
	if err := rdb.(*badgerdb.Database).Compact(ctx); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Fatalf("expected badger.ErrReadOnlyTxn, got %v", err)
	}
}
