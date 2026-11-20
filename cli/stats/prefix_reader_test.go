// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
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

func summaryKeysFor(ctx context.Context, t *testing.T, db idudb.Reader, exactKey, childPrefix string) []string {
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
	if e.prefix != stats.DecodeSummaryKey("/", e.summaryKey) || len(e.pi.Entries()) != nfiles {
		t.Errorf("unexpected entry for %v: %v, %v files", e.summaryKey, e.prefix, len(e.pi.Entries()))
	}
}

func TestReadPrefixesInOrder(t *testing.T) {
	ctx := context.Background()
	// 1 + 40 + 40*40 = 1641 prefixes spans many batches and exceeds the
	// window for small numbers of readers.
	cfg := createPrefixTreeDB(ctx, t, 40, 3)

	// A summary without any prefix metadata.
	rawDB, err := clitest.OpenRawDB(ctx, cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	missingKey, _ := stats.SummaryKeys("/", "/root/d000/missing")
	if err := rawDB.SetSummary(ctx, missingKey, []byte{}, false); err != nil {
		t.Fatal(err)
	}
	_ = rawDB.Close(ctx)

	db, err := testEnv.OpenReadOnlyDB(ctx, &cfg.Settings)
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
	db, err := testEnv.OpenReadOnlyDB(ctx, &cfg.Settings)
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
	idudb.Reader
	calls, failAt int
}

var errGetBatch = errors.New("get batch failed")

func (db *failingGetBatchDB) GetBatch(ctx context.Context, prefixes []string, fn func(int, []byte) error) error {
	// Called concurrently, but only used with a single reader.
	if db.calls++; db.calls == db.failAt {
		return errGetBatch
	}
	return db.Reader.GetBatch(ctx, prefixes, fn)
}

func TestReadPrefixesInOrderError(t *testing.T) {
	ctx := context.Background()
	cfg := createPrefixTreeDB(ctx, t, 40, 3)
	rodb, err := testEnv.OpenReadOnlyDB(ctx, &cfg.Settings)
	if err != nil {
		t.Fatal(err)
	}
	defer rodb.Close(ctx)
	exactKey, childPrefix := stats.SummaryKeys("/", "/root")
	before := runtime.NumGoroutine()
	db := &failingGetBatchDB{Reader: rodb, failAt: 3}
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

type failingScanSummariesDB struct {
	idudb.Reader
	failAfter int
	err       error
}

func (db *failingScanSummariesDB) ScanSummaries(ctx context.Context, exactKey, childPrefix string, visitor func(context.Context, string, []byte) bool) error {
	seen := 0
	var aborted bool
	err := db.Reader.ScanSummaries(ctx, exactKey, childPrefix, func(c context.Context, k string, v []byte) bool {
		seen++
		if seen > db.failAfter {
			aborted = true
			return false
		}
		return visitor(c, k, v)
	})
	if err != nil {
		return err
	}
	if aborted {
		return db.err
	}
	return nil
}

func TestReadPrefixesInOrderScanErrorAndDecodeErr(t *testing.T) {
	ctx := context.Background()
	cfg := createPrefixTreeDB(ctx, t, 40, 3)

	t.Run("ScanSummaries mid-stream error", func(t *testing.T) {
		rodb, err := testEnv.OpenReadOnlyDB(ctx, &cfg.Settings)
		if err != nil {
			t.Fatal(err)
		}
		defer rodb.Close(ctx)
		exactKey, childPrefix := stats.SummaryKeys("/", "/root")
		before := runtime.NumGoroutine()
		errScan := errors.New("scan summaries failed")
		db := &failingScanSummariesDB{Reader: rodb, failAfter: prefixBatchSize + 5, err: errScan}
		err = readPrefixesInOrder(ctx, db, "/", exactKey, childPrefix, 2, func(*prefixEntry) {})
		if !errors.Is(err, errScan) {
			t.Errorf("got %v, want %v", err, errScan)
		}
		waitForGoroutines(t, before)
	})

	t.Run("corrupt prefix entry and readers=0", func(t *testing.T) {
		rwdb, err := clitest.OpenRawDB(ctx, cfg.Database)
		if err != nil {
			t.Fatal(err)
		}
		badPrefix := "/root/d000/corrupt"
		badSK, _ := stats.SummaryKeys("/", badPrefix)
		if err := rwdb.Set(ctx, badPrefix, []byte{0xff}, false); err != nil {
			_ = rwdb.Close(ctx)
			t.Fatal(err)
		}
		if err := rwdb.SetSummary(ctx, badSK, []byte{0x01}, false); err != nil {
			_ = rwdb.Close(ctx)
			t.Fatal(err)
		}
		_ = rwdb.Close(ctx)

		rodb, err := testEnv.OpenReadOnlyDB(ctx, &cfg.Settings)
		if err != nil {
			t.Fatal(err)
		}
		defer rodb.Close(ctx)
		exactKey, childPrefix := stats.SummaryKeys("/", "/root/d000")
		before := runtime.NumGoroutine()
		var gotDecodeErr int
		err = readPrefixesInOrder(ctx, rodb, "/", exactKey, childPrefix, 0, func(e *prefixEntry) {
			if e.prefix == badPrefix && e.decodeErr != nil {
				gotDecodeErr++
			}
		})
		if err != nil {
			t.Fatalf("readPrefixesInOrder: %v", err)
		}
		if gotDecodeErr != 1 {
			t.Errorf("gotDecodeErr = %d, want 1", gotDecodeErr)
		}
		waitForGoroutines(t, before)
	})
}
