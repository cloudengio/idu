// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/stats"
)

// waitForGoroutines fails the test if the number of goroutines does not
// return to at most n.
func waitForGoroutines(t *testing.T, n int) {
	t.Helper()
	for range 200 {
		if runtime.NumGoroutine() <= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("goroutine leak: %v goroutines, want at most %v", runtime.NumGoroutine(), n)
}

func summaryKeysFor(ctx context.Context, t *testing.T, db database.DB, exactKey, childPrefix string) []string {
	t.Helper()
	var keys []string
	if err := db.ScanSummaries(ctx, exactKey, childPrefix, func(_ context.Context, k string, _ []byte) bool {
		keys = append(keys, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return keys
}

func checkPrefixEntry(t *testing.T, e *prefixEntry, nfiles int) {
	t.Helper()
	if e.decodeErr != nil {
		t.Errorf("%v: %v", e.prefix, e.decodeErr)
	}
	if e.prefix != stats.DecodeSummaryKey("/", e.summaryKey) || len(e.pi.InfoList()) != nfiles {
		t.Errorf("unexpected entry for %v: %v, %v files", e.summaryKey, e.prefix, len(e.pi.InfoList()))
	}
}

func TestReadPrefixesInOrder(t *testing.T) {
	ctx := context.Background()
	// 1 + 40 + 40*40 = 1641 prefixes spans many batches and exceeds the
	// window for small numbers of readers.
	cfg := createPrefixTreeDB(ctx, t, 40, 3)

	// A summary without any prefix metadata.
	rawDB, err := internal.OpenDatabase(ctx, cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	missingKey, _ := stats.SummaryKeys("/", "/root/d000/missing")
	if err := rawDB.SetSummary(ctx, missingKey, []byte{}, false); err != nil {
		t.Fatal(err)
	}
	_ = rawDB.Close(ctx)

	db, err := internal.OpenDatabase(ctx, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)

	for _, target := range []string{"/root", "/root/d001"} {
		exactKey, childPrefix := stats.SummaryKeys("/", target)
		want := summaryKeysFor(ctx, t, db, exactKey, childPrefix)
		for _, readers := range []int{1, 3, 16} {
			before := runtime.NumGoroutine()
			var got, missing []string
			err := readPrefixesInOrder(ctx, db, "/", exactKey, childPrefix, readers, func(e *prefixEntry) {
				got = append(got, e.summaryKey)
				if e.missing {
					missing = append(missing, e.prefix)
					return
				}
				checkPrefixEntry(t, e, 3)
			})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, want) {
				t.Errorf("%v: readers %v: got %v prefixes, want %v in the same order", target, readers, len(got), len(want))
			}
			wantMissing := []string(nil)
			if target == "/root" {
				wantMissing = []string{"/root/d000/missing"}
			}
			if !slices.Equal(missing, wantMissing) {
				t.Errorf("%v: readers %v: got missing %v, want %v", target, readers, missing, wantMissing)
			}
			waitForGoroutines(t, before)
		}
	}
}

func TestReadPrefixesInOrderCancel(t *testing.T) {
	ctx := context.Background()
	cfg := createPrefixTreeDB(ctx, t, 40, 3)
	db, err := internal.OpenDatabase(ctx, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	exactKey, childPrefix := stats.SummaryKeys("/", "/root")
	before := runtime.NumGoroutine()
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	n := 0
	err = readPrefixesInOrder(cctx, db, "/", exactKey, childPrefix, 4, func(*prefixEntry) {
		if n++; n == 300 {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want %v", err, context.Canceled)
	}
	waitForGoroutines(t, before)
}

type failingGetBatchDB struct {
	database.DB
	calls, failAt int
}

var errGetBatch = errors.New("get batch failed")

func (db *failingGetBatchDB) GetBatch(ctx context.Context, prefixes []string, fn func(int, []byte) error) error {
	// Called concurrently, but only used with a single reader.
	if db.calls++; db.calls == db.failAt {
		return errGetBatch
	}
	return db.DB.GetBatch(ctx, prefixes, fn)
}

func TestReadPrefixesInOrderError(t *testing.T) {
	ctx := context.Background()
	cfg := createPrefixTreeDB(ctx, t, 40, 3)
	rodb, err := internal.OpenDatabase(ctx, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer rodb.Close(ctx)
	exactKey, childPrefix := stats.SummaryKeys("/", "/root")
	before := runtime.NumGoroutine()
	db := &failingGetBatchDB{DB: rodb, failAt: 3}
	n := 0
	err = readPrefixesInOrder(ctx, db, "/", exactKey, childPrefix, 1, func(*prefixEntry) { n++ })
	if !errors.Is(err, errGetBatch) {
		t.Errorf("got %v, want %v", err, errGetBatch)
	}
	// Only the batches preceding the failed one are visited.
	if got, want := n, 2*prefixBatchSize; got != want {
		t.Errorf("visited %v, want %v", got, want)
	}
	waitForGoroutines(t, before)
}
