// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package badgerdb_test

import (
	"context"
	"fmt"
	"testing"

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
		db.SetBatch(ctx, fmt.Sprintf("/%08v", i), []byte(fmt.Sprintf("%08v", i)))
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
	db.Scan(ctx, "", func(_ context.Context, k string, v []byte) bool {
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
