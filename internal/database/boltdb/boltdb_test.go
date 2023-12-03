// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boltdb_test

import (
	"context"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/database/boltdb"
	bolt "go.etcd.io/bbolt"
)

func listBuckets(db database.DB) ([]string, error) {
	bdb := db.(*boltdb.Database).Bolt()
	buckets := []string{}
	err := bdb.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(tl []byte, b *bolt.Bucket) error {
			buckets = append(buckets, string(tl))
			err := b.ForEachBucket(func(nb []byte) error {
				buckets = append(buckets, string(nb))
				return nil
			})
			return err
		})
	})
	return buckets, err
}

func bucketsForPrefix() []string {
	b := append([]string{}, boltdb.NestedBuckets()...)
	sort.Strings(b)
	return b
}

func TestOpen(t *testing.T) {
	ctx := context.Background()
	dbname := filepath.Join(t.TempDir(), "db")
	db, err := boltdb.Open(dbname)
	if err != nil {
		t.Fatal(err)
	}

	// Will timeout as the database is locked.
	_, err = boltdb.Open(dbname, boltdb.WithTimeout(10*time.Millisecond))
	if err == nil || err.Error() != "timeout" {
		t.Fatal(err)
	}

	db.Close(ctx)

	db, err = boltdb.Open(dbname, boltdb.ReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)

	buckets, err := listBuckets(db)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := buckets, bucketsForPrefix(); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// It's possible to open multiple databases in read-only mode.
	db2, err := boltdb.Open(dbname, boltdb.ReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close(ctx)

	buckets, err = listBuckets(db2)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := buckets, bucketsForPrefix(); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

}
