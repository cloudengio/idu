// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package badgerdb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/idudb/backends/badgerdb"
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
	bdb := db.BadgerDB()
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
	if err := db.Compact(ctx); err != nil {
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
	if err := rdb.Compact(ctx); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Fatalf("expected badger.ErrReadOnlyTxn, got %v", err)
	}
}

func openTestBadger(t *testing.T, dir string, opts ...badgerdb.Option) idudb.Database {
	t.Helper()
	bopts := badger.DefaultOptions(dir).WithLogger(nil)
	allOpts := append([]badgerdb.Option{badgerdb.WithBadgerOptions(bopts)}, opts...)
	db, err := badgerdb.Open(dir, allOpts...)
	if err != nil {
		t.Fatalf("badgerdb.Open(%q): %v", dir, err)
	}
	return db
}

func seedClearDB(ctx context.Context, t *testing.T, db idudb.Database, base time.Time) {
	t.Helper()
	if err := db.Set(ctx, "/keep", []byte("val"), false); err != nil {
		t.Fatal(err)
	}
	if err := db.SetSummary(ctx, "\x00keep", []byte("sum"), false); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		ts := base.Add(time.Duration(i) * time.Minute)
		if err := db.Log(ctx, ts, ts.Add(time.Second), []byte{byte('0' + i)}); err != nil {
			t.Fatal(err)
		}
		if err := db.LogError(ctx, fmt.Sprintf("/err/%d", i+1), ts, []byte("e")); err != nil {
			t.Fatal(err)
		}
	}
}

func assertLogAndErrCounts(ctx context.Context, t *testing.T, db idudb.Database, end time.Time, wantLogs, wantErrs int) {
	t.Helper()
	var logs, errsCount int
	if err := db.VisitLogs(ctx, time.Time{}, end, func(context.Context, time.Time, time.Time, []byte) bool {
		logs++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.VisitErrors(ctx, "", func(context.Context, string, time.Time, []byte) bool {
		errsCount++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if logs != wantLogs || errsCount != wantErrs {
		t.Errorf("got logs=%d errs=%d, want %d, %d", logs, errsCount, wantLogs, wantErrs)
	}
}

func TestClear(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		clearLogs    bool
		clearErrors  bool
		wantLogCount int
		wantErrCount int
	}{
		{name: "clear neither", clearLogs: false, clearErrors: false, wantLogCount: 2, wantErrCount: 2},
		{name: "clear logs only", clearLogs: true, clearErrors: false, wantLogCount: 0, wantErrCount: 2},
		{name: "clear errors only", clearLogs: false, clearErrors: true, wantLogCount: 2, wantErrCount: 0},
		{name: "clear both", clearLogs: true, clearErrors: true, wantLogCount: 0, wantErrCount: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTestBadger(t, t.TempDir())
			defer db.Close(ctx)

			seedClearDB(ctx, t, db, base)
			if err := db.Clear(ctx, tc.clearLogs, tc.clearErrors); err != nil {
				t.Fatalf("Clear(%v, %v): %v", tc.clearLogs, tc.clearErrors, err)
			}
			assertLogAndErrCounts(ctx, t, db, base.Add(time.Hour), tc.wantLogCount, tc.wantErrCount)

			var buf bytes.Buffer
			if err := db.Get(ctx, "/keep", &buf); err != nil || buf.String() != "val" {
				t.Errorf("Get(/keep) = (%q, %v), want (\"val\", nil)", buf.String(), err)
			}
		})
	}

	t.Run("canceled and readonly Clear", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		db := openTestBadger(t, dir)
		seedClearDB(ctx, t, db, base)

		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if err := db.Clear(cctx, true, true); !errors.Is(err, context.Canceled) {
			t.Errorf("canceled Clear = %v, want context.Canceled", err)
		}
		_ = db.Close(ctx)

		rdb := openTestBadger(t, dir, badgerdb.ReadOnly())
		defer rdb.Close(ctx)
		if err := rdb.Clear(ctx, true, true); !errors.Is(err, badger.ErrReadOnlyTxn) {
			t.Errorf("readonly Clear = %v, want ErrReadOnlyTxn", err)
		}
	})
}

func TestDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	db := openTestBadger(t, dir)

	for _, kv := range [][2]string{{"/a", "va"}, {"/a/b", "vab"}} {
		if err := db.Set(ctx, kv[0], []byte(kv[1]), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Delete(ctx, "/a"); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(ctx, "/missing"); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := db.Get(ctx, "/a", &buf); err != nil || buf.Len() != 0 {
		t.Errorf("expected /a deleted, got (%q, %v)", buf.String(), err)
	}
	if err := db.Get(ctx, "/a/b", &buf); err != nil || buf.String() != "vab" {
		t.Errorf("expected /a/b preserved, got (%q, %v)", buf.String(), err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := db.Delete(cctx, "/a/b"); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled Delete = %v, want context.Canceled", err)
	}
	_ = db.Close(ctx)

	rdb := openTestBadger(t, dir, badgerdb.ReadOnly())
	defer rdb.Close(ctx)
	if err := rdb.Delete(ctx, "/a/b"); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly Delete = %v, want ErrReadOnlyTxn", err)
	}
	if err := rdb.DeletePrefix(ctx, "/a/b"); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly DeletePrefix = %v, want ErrReadOnlyTxn", err)
	}
	if err := rdb.DeleteError(ctx, "/e1"); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly DeleteError = %v, want ErrReadOnlyTxn", err)
	}
	if err := rdb.DeleteErrors(ctx, "/e1"); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly DeleteErrors = %v, want ErrReadOnlyTxn", err)
	}
}

func TestDeleteError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestBadger(t, t.TempDir())
	defer db.Close(ctx)

	for _, ek := range []string{"/e1", "/e1/sub"} {
		if err := db.LogError(ctx, ek, time.Now(), []byte("err")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.DeleteError(ctx, "/e1"); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteError(ctx, "/e-missing"); err != nil {
		t.Fatal(err)
	}

	var errKeys []string
	if err := db.VisitErrors(ctx, "", func(_ context.Context, k string, _ time.Time, _ []byte) bool {
		errKeys = append(errKeys, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(errKeys, []string{"/e1/sub"}) {
		t.Errorf("remaining errors = %v, want [/e1/sub]", errKeys)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := db.DeleteError(cctx, "/e1/sub"); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled DeleteError = %v, want context.Canceled", err)
	}
}

func TestDeletePrefixMultiBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tmpdir := t.TempDir()
	bopts := badger.DefaultOptions(tmpdir).
		WithLogger(nil).
		WithMemTableSize(1 << 15).
		WithValueThreshold(1024)
	db, err := badgerdb.Open(tmpdir, badgerdb.WithBadgerOptions(bopts))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)

	mc := db.BadgerDB().MaxBatchCount()
	total := int(mc)*2 + 10
	for i := range total {
		if err := db.Set(ctx, fmt.Sprintf("/big/%06d", i), []byte("v"), true); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Set(ctx, "/keep/1", []byte("ok"), true); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.DeletePrefix(ctx, "/big/"); err != nil {
		t.Fatalf("DeletePrefix: %v", err)
	}
	var remaining []string
	if err := db.Scan(ctx, "", func(_ context.Context, k string, _ []byte) bool {
		remaining = append(remaining, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(remaining, []string{"/keep/1"}) {
		t.Errorf("remaining keys count=%d, want [/keep/1]", len(remaining))
	}
}

func TestFlushAndConcurrentBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	db := openTestBadger(t, dir)

	const workers = 4
	const perWorker = 50
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				key := fmt.Sprintf("/w%d/%03d", w, i)
				if err := db.Set(ctx, key, []byte(key), true); err != nil {
					t.Errorf("Set(%q): %v", key, err)
					return
				}
				if i%10 == 0 {
					if err := db.Flush(ctx); err != nil {
						t.Errorf("Flush: %v", err)
						return
					}
				}
			}
		})
	}
	wg.Wait()
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("final Flush: %v", err)
	}

	var count int
	if err := db.Scan(ctx, "", func(context.Context, string, []byte) bool {
		count++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if count != workers*perWorker {
		t.Errorf("count = %d, want %d", count, workers*perWorker)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := db.Flush(cctx); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled Flush = %v, want context.Canceled", err)
	}
	_ = db.Close(ctx)

	rdb := openTestBadger(t, dir, badgerdb.ReadOnly())
	defer rdb.Close(ctx)
	if err := rdb.Flush(ctx); err != nil {
		t.Errorf("readonly Flush = %v, want nil", err)
	}
}

func TestGetAndGetBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestBadger(t, t.TempDir())
	defer db.Close(ctx)

	for _, k := range []string{"/k1", "/k2", "/k3"} {
		if err := db.Set(ctx, k, []byte("val-"+k), false); err != nil {
			t.Fatal(err)
		}
	}

	var buf bytes.Buffer
	if err := db.Get(ctx, "/k2", &buf); err != nil || buf.String() != "val-/k2" {
		t.Fatalf("Get(/k2) = (%q, %v)", buf.String(), err)
	}
	if err := db.GetBatch(ctx, nil, func(int, []byte) error {
		t.Fatal("unexpected callback for empty keys")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var got []string
	err := db.GetBatch(ctx, []string{"/k1", "/missing", "/k3"}, func(i int, val []byte) error {
		if val == nil {
			got = append(got, fmt.Sprintf("%d:<nil>", i))
		} else {
			got = append(got, fmt.Sprintf("%d:%s", i, val))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"0:val-/k1", "1:<nil>", "2:val-/k3"}; !reflect.DeepEqual(got, want) {
		t.Errorf("GetBatch = %v, want %v", got, want)
	}

	errBoom := errors.New("boom")
	if err := db.GetBatch(ctx, []string{"/k1", "/k2"}, func(i int, _ []byte) error {
		if i == 1 {
			return errBoom
		}
		return nil
	}); !errors.Is(err, errBoom) {
		t.Errorf("GetBatch callback err = %v, want %v", err, errBoom)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := db.Get(cctx, "/k1", &buf); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled Get = %v, want context.Canceled", err)
	}
	if err := db.GetBatch(cctx, []string{"/k1"}, func(int, []byte) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled GetBatch = %v, want context.Canceled", err)
	}
}

func TestDeleteSummaryAndMutationErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	db := openTestBadger(t, dir)

	for _, k := range []string{"\x00s1", "\x00s1\x00c1", "\x00s2", "\x00s2\x00c1"} {
		if err := db.SetSummary(ctx, k, []byte(k), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.DeleteSummary(ctx, "\x00s1", ""); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteSummary(ctx, "", "\x00s2\x00"); err != nil {
		t.Fatal(err)
	}
	var rem []string
	if err := db.ScanSummaries(ctx, "", "", func(_ context.Context, k string, _ []byte) bool {
		rem = append(rem, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"\x00s1\x00c1", "\x00s2"}; !slices.Equal(rem, want) {
		t.Errorf("remaining summaries = %v, want %v", rem, want)
	}
	_ = db.Close(ctx)

	rdb := openTestBadger(t, dir, badgerdb.ReadOnly())
	defer rdb.Close(ctx)
	if err := rdb.SetSummary(ctx, "\x00x", []byte("1"), false); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly SetSummary = %v, want ErrReadOnlyTxn", err)
	}
	if err := rdb.DeleteSummary(ctx, "\x00x", ""); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly DeleteSummary = %v, want ErrReadOnlyTxn", err)
	}
	if err := rdb.SetMeta(ctx, "k", []byte("v")); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly SetMeta = %v, want ErrReadOnlyTxn", err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := rdb.SetSummary(cctx, "\x00x", []byte("1"), false); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled SetSummary = %v, want context.Canceled", err)
	}
	if err := rdb.DeleteSummary(cctx, "\x00x", ""); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled DeleteSummary = %v, want context.Canceled", err)
	}
	if err := rdb.SetMeta(cctx, "k", []byte("v")); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled SetMeta = %v, want context.Canceled", err)
	}
}

func seedScanVisitDB(ctx context.Context, t *testing.T, base time.Time) idudb.Database {
	t.Helper()
	db := openTestBadger(t, t.TempDir())
	for i := range 10 {
		if err := db.Set(ctx, fmt.Sprintf("/p/%02d", i), []byte("v"), false); err != nil {
			t.Fatal(err)
		}
		if err := db.SetSummary(ctx, fmt.Sprintf("\x00p\x00%02d", i), []byte("s"), false); err != nil {
			t.Fatal(err)
		}
		if err := db.LogError(ctx, fmt.Sprintf("/e/%02d", i), base, []byte("err")); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 5 {
		ts := base.Add(time.Duration(i) * time.Minute)
		if err := db.Log(ctx, ts, ts.Add(time.Second), []byte{byte('0' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func TestScanAndStreamEdgeCases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := seedScanVisitDB(ctx, t, time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC))
	defer db.Close(ctx)

	var seen int
	if err := db.Scan(ctx, "", func(context.Context, string, []byte) bool {
		seen++
		return seen < 3
	}); err != nil {
		t.Fatal(err)
	}
	if seen != 3 {
		t.Errorf("seen = %d, want 3", seen)
	}

	cctx, cancel := context.WithCancel(ctx)
	err := db.Scan(cctx, "", func(context.Context, string, []byte) bool {
		cancel()
		return true
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Scan cancel err = %v, want context.Canceled", err)
	}

	var keys []string
	if err := db.ScanSummaries(ctx, "\x00p\x0002", "", func(_ context.Context, k string, _ []byte) bool {
		keys = append(keys, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keys, []string{"\x00p\x0002"}) {
		t.Errorf("ScanSummaries exact-only = %v, want [\x00p\x0002]", keys)
	}
	if err := db.ScanSummaries(cctx, "", "", func(context.Context, string, []byte) bool { return true }); !errors.Is(err, context.Canceled) {
		t.Errorf("ScanSummaries cancel err = %v, want context.Canceled", err)
	}
	if err := db.Stream(cctx, "", func(context.Context, string, []byte) {}); !errors.Is(err, context.Canceled) {
		t.Errorf("Stream cancel = %v, want context.Canceled", err)
	}
}

func TestVisitLogsAndErrorsEdgeCases(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	base := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	db := seedScanVisitDB(ctx, t, base)
	defer db.Close(ctx)

	var got []byte
	err := db.VisitLogs(ctx, base.Add(time.Minute), base.Add(3*time.Minute), func(_ context.Context, _ time.Time, _ time.Time, d []byte) bool {
		got = append(got, d...)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "123" {
		t.Errorf("VisitLogs bounded = %q, want %q", got, "123")
	}

	var n int
	if err := db.VisitLogs(ctx, time.Time{}, base.Add(time.Hour), func(context.Context, time.Time, time.Time, []byte) bool {
		n++
		return false
	}); err != nil || n != 1 {
		t.Errorf("VisitLogs early stop = (%d, %v), want (1, nil)", n, err)
	}

	cctx, cancel := context.WithCancel(ctx)
	err = db.VisitLogs(cctx, time.Time{}, base.Add(time.Hour), func(context.Context, time.Time, time.Time, []byte) bool {
		cancel()
		return true
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("VisitLogs cancel = %v, want context.Canceled", err)
	}

	n = 0
	if err := db.VisitErrors(ctx, "", func(context.Context, string, time.Time, []byte) bool {
		n++
		return false
	}); err != nil || n != 1 {
		t.Errorf("VisitErrors early stop = (%d, %v), want (1, nil)", n, err)
	}
	if err := db.VisitErrors(cctx, "", func(context.Context, string, time.Time, []byte) bool { return true }); !errors.Is(err, context.Canceled) {
		t.Errorf("VisitErrors cancel = %v, want context.Canceled", err)
	}
}

func TestOpenErrorsAndOptions(t *testing.T) {
	t.Parallel()

	t.Run("Open fails on file path", func(t *testing.T) {
		t.Parallel()
		f := filepath.Join(t.TempDir(), "notadir")
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := badgerdb.Open(f); err == nil {
			t.Fatal("expected Open to fail on regular file path")
		}
	})

	t.Run("Open ReadOnly fails on missing database", func(t *testing.T) {
		t.Parallel()
		missing := filepath.Join(t.TempDir(), "missing-db")
		if db, err := badgerdb.Open(missing, badgerdb.ReadOnly()); err == nil {
			_ = db.Close(context.Background())
			t.Fatal("expected Open ReadOnly to fail on non-existent database")
		}
		if _, err := os.Stat(missing); !os.IsNotExist(err) {
			t.Errorf("expected ReadOnly Open on missing DB not to create %q, stat err = %v", missing, err)
		}
	})
}

func TestCanceledScansSkipFirstItem(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := badgerdb.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close(ctx) }()

	now := time.Date(2025, 4, 1, 10, 0, 0, 0, time.UTC)
	if err := db.Set(ctx, "/a", []byte("1"), false); err != nil {
		t.Fatal(err)
	}
	if err := db.Log(ctx, now, now.Add(time.Minute), []byte("log")); err != nil {
		t.Fatal(err)
	}
	if err := db.LogError(ctx, "/a", now, []byte("err")); err != nil {
		t.Fatal(err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()

	scanCalls := 0
	if err := db.Scan(cctx, "/", func(context.Context, string, []byte) bool {
		scanCalls++
		return true
	}); !errors.Is(err, context.Canceled) || scanCalls != 0 {
		t.Errorf("Scan(canceled) = (%v, calls=%d), want (context.Canceled, 0)", err, scanCalls)
	}

	logCalls := 0
	if err := db.VisitLogs(cctx, time.Time{}, now.Add(time.Hour), func(context.Context, time.Time, time.Time, []byte) bool {
		logCalls++
		return true
	}); !errors.Is(err, context.Canceled) || logCalls != 0 {
		t.Errorf("VisitLogs(canceled) = (%v, calls=%d), want (context.Canceled, 0)", err, logCalls)
	}

	errCalls := 0
	if err := db.VisitErrors(cctx, "/", func(context.Context, string, time.Time, []byte) bool {
		errCalls++
		return true
	}); !errors.Is(err, context.Canceled) || errCalls != 0 {
		t.Errorf("VisitErrors(canceled) = (%v, calls=%d), want (context.Canceled, 0)", err, errCalls)
	}
}

func TestOpenWithTimeoutHeldLock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	db1, err := badgerdb.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db1Closed := false
	defer func() {
		if !db1Closed {
			_ = db1.Close(ctx)
		}
	}()

	type result struct {
		db  idudb.Database
		err error
	}
	ch := make(chan result, 1)
	go func() {
		db2, err := badgerdb.Open(dir, badgerdb.WithTimeout(50*time.Millisecond))
		ch <- result{db: db2, err: err}
	}()

	select {
	case res := <-ch:
		if res.err == nil {
			_ = res.db.Close(ctx)
			t.Fatal("expected Open with held lock to time out and return error")
		}
	case <-time.After(300 * time.Millisecond):
		t.Error("badgerdb.Open ignored WithTimeout while lock was held")
		_ = db1.Close(ctx)
		db1Closed = true
		res := <-ch
		if res.db != nil {
			_ = res.db.Close(ctx)
		}
	}
}

func collectLogs(t *testing.T, db idudb.Database, start, stop time.Time) string {
	t.Helper()
	var got []byte
	if err := db.VisitLogs(context.Background(), start, stop, func(_ context.Context, _, _ time.Time, detail []byte) bool {
		got = append(got, detail...)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return string(got)
}

func assertLastLogDetail(t *testing.T, db idudb.Database, want string) {
	t.Helper()
	_, _, last, err := db.LastLog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(last) != want {
		t.Errorf("LastLog detail = %q, want %q", last, want)
	}
}

func TestLogSubsecondAndTimezoneOrdering(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("subsecond entries do not collide", func(t *testing.T) {
		t.Parallel()
		db, err := badgerdb.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close(ctx) }()

		base := time.Date(2025, 3, 9, 12, 0, 0, 0, time.UTC)
		stamps := []time.Time{
			base,
			base.Add(100 * time.Millisecond),
			base.Add(500 * time.Millisecond),
			base.Add(500*time.Millisecond + time.Nanosecond),
		}
		for i, ts := range stamps {
			if err := db.Log(ctx, ts, ts.Add(time.Millisecond), []byte{byte('0' + i)}); err != nil {
				t.Fatal(err)
			}
		}

		if got := collectLogs(t, db, time.Time{}, base.Add(time.Second)); got != "0123" {
			t.Errorf("VisitLogs subsecond = %q, want %q", got, "0123")
		}
		if got := collectLogs(t, db, base, base.Add(200*time.Millisecond)); got != "01" {
			t.Errorf("VisitLogs bounded subsecond = %q, want %q", got, "01")
		}
		assertLastLogDetail(t, db, "3")
	})

	t.Run("mixed timezones sort chronologically", func(t *testing.T) {
		t.Parallel()
		db, err := badgerdb.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close(ctx) }()

		tzMinus8 := time.FixedZone("PST", -8*3600)
		tzMinus7 := time.FixedZone("PDT", -7*3600)
		tzPlus2 := time.FixedZone("CEST", 2*3600)

		entries := []struct {
			ts     time.Time
			detail string
		}{
			{time.Date(2025, 3, 9, 10, 0, 0, 0, time.UTC), "0"}, // 10:00Z
			{time.Date(2025, 3, 9, 2, 30, 0, 0, tzMinus8), "1"}, // 10:30Z
			{time.Date(2025, 3, 9, 13, 0, 0, 0, tzPlus2), "2"},  // 11:00Z
			{time.Date(2025, 3, 9, 4, 30, 0, 0, tzMinus7), "3"}, // 11:30Z
		}
		for _, e := range entries {
			if err := db.Log(ctx, e.ts, e.ts.Add(time.Minute), []byte(e.detail)); err != nil {
				t.Fatal(err)
			}
		}

		if got := collectLogs(t, db, entries[0].ts, entries[3].ts); got != "0123" {
			t.Errorf("VisitLogs mixed tz = %q, want %q", got, "0123")
		}
		assertLastLogDetail(t, db, "3")
	})
}

var _ idudb.Database = (*badgerdb.Database)(nil)

func TestSubtrees(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestBadger(t, t.TempDir())
	defer db.Close(ctx)

	keys := []string{"\x00s1", "\x00s1\x00c1", "\x00s2", "\x00s2\x00c1"}
	for i, k := range keys {
		// Batch half of the writes.
		if err := db.SetSubtree(ctx, k, []byte(k), i%2 == 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// The subtree and summary buckets are distinct.
	if err := db.SetSummary(ctx, "\x00s1", []byte("summary"), false); err != nil {
		t.Fatal(err)
	}
	scan := func(exactKey, childPrefix string) []string {
		var got []string
		if err := db.ScanSubtrees(ctx, exactKey, childPrefix, func(_ context.Context, k string, v []byte) bool {
			if string(v) != k {
				t.Errorf("%q: got %q", k, v)
			}
			got = append(got, k)
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := scan("", ""); !slices.Equal(got, keys) {
		t.Errorf("got %q, want %q", got, keys)
	}
	if got, want := scan("\x00s2", "\x00s2\x00"), keys[2:]; !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}

	checkGetSubtrees(ctx, t, db)

	if err := db.DeleteSubtrees(ctx, "\x00s1", "\x00s1\x00"); err != nil {
		t.Fatal(err)
	}
	if got, want := scan("", ""), keys[2:]; !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
	if err := db.ClearSubtrees(ctx); err != nil {
		t.Fatal(err)
	}
	if got := scan("", ""); len(got) != 0 {
		t.Errorf("got %q", got)
	}
	var buf []string
	if err := db.ScanSummaries(ctx, "", "", func(_ context.Context, k string, _ []byte) bool {
		buf = append(buf, k)
		return true
	}); err != nil || !slices.Equal(buf, []string{"\x00s1"}) {
		t.Errorf("summaries: %q %v", buf, err)
	}
}

func checkGetSubtrees(ctx context.Context, t *testing.T, db idudb.Database) {
	t.Helper()
	var got []string
	if err := db.GetSubtrees(ctx, []string{"\x00s2", "\x00missing", "\x00s1"}, func(_ int, v []byte) error {
		got = append(got, string(v))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"\x00s2", "", "\x00s1"}; !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestSubtreesReadOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	_ = openTestBadger(t, dir).Close(ctx)
	rdb := openTestBadger(t, dir, badgerdb.ReadOnly())
	defer rdb.Close(ctx)
	if err := rdb.SetSubtree(ctx, "\x00x", []byte("1"), false); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly SetSubtree = %v, want ErrReadOnlyTxn", err)
	}
	if err := rdb.DeleteSubtrees(ctx, "\x00x", ""); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly DeleteSubtrees = %v, want ErrReadOnlyTxn", err)
	}
	if err := rdb.ClearSubtrees(ctx); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly ClearSubtrees = %v, want ErrReadOnlyTxn", err)
	}
}

// scanLinks returns up to limit keys with prefix in the link bucket, each
// of which must have itself as its value.
func scanLinks(ctx context.Context, t *testing.T, db idudb.Database, prefix string, limit int) []string {
	t.Helper()
	var got []string
	if err := db.ScanLinks(ctx, prefix, func(_ context.Context, k string, v []byte) bool {
		if string(v) != k {
			t.Errorf("%q: got %q", k, v)
		}
		got = append(got, k)
		return len(got) < limit
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestLinks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestBadger(t, t.TempDir())
	defer db.Close(ctx)

	keys := []string{"fa\x00x", "fa\x00y", "fab", "fb", "d"}
	for i, k := range keys {
		if err := db.SetLink(ctx, k, []byte(k), i%2 == 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// The link and subtree buckets are distinct.
	if err := db.SetSubtree(ctx, "fa", []byte("subtree"), false); err != nil {
		t.Fatal(err)
	}
	scan := func(prefix string, limit int) []string {
		return scanLinks(ctx, t, db, prefix, limit)
	}
	for _, tc := range []struct {
		prefix string
		limit  int
		want   []string
	}{
		{"fa", 10, []string{"fa\x00x", "fa\x00y", "fab"}},
		{"fa\x00", 10, []string{"fa\x00x", "fa\x00y"}},
		{"fa", 1, []string{"fa\x00x"}},
		{"f", 10, []string{"fa\x00x", "fa\x00y", "fab", "fb"}},
		{"", 10, []string{"d", "fa\x00x", "fa\x00y", "fab", "fb"}},
		{"g", 10, nil},
	} {
		if got := scan(tc.prefix, tc.limit); !slices.Equal(got, tc.want) {
			t.Errorf("%q: got %q, want %q", tc.prefix, got, tc.want)
		}
	}
	if err := db.DeleteLink(ctx, "fa\x00y"); err != nil {
		t.Fatal(err)
	}
	// Deleting a missing key is not an error.
	if err := db.DeleteLink(ctx, "missing"); err != nil {
		t.Fatal(err)
	}
	if got, want := scan("fa", 10), []string{"fa\x00x", "fab"}; !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
	if err := db.ClearLinks(ctx); err != nil {
		t.Fatal(err)
	}
	if got := scan("", 10); got != nil {
		t.Errorf("got %q after clearing", got)
	}
	var buf bytes.Buffer
	if err := db.ScanSubtrees(ctx, "fa", "", func(_ context.Context, _ string, v []byte) bool {
		buf.Write(v)
		return false
	}); err != nil || buf.String() != "subtree" {
		t.Errorf("subtree: %q %v", buf.String(), err)
	}
}

func TestLinksReadOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	_ = openTestBadger(t, dir).Close(ctx)
	rdb := openTestBadger(t, dir, badgerdb.ReadOnly())
	defer rdb.Close(ctx)
	if err := rdb.SetLink(ctx, "x", []byte("1"), false); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly SetLink = %v, want ErrReadOnlyTxn", err)
	}
	if err := rdb.DeleteLink(ctx, "x"); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly DeleteLink = %v, want ErrReadOnlyTxn", err)
	}
	if err := rdb.ClearLinks(ctx); !errors.Is(err, badger.ErrReadOnlyTxn) {
		t.Errorf("readonly ClearLinks = %v, want ErrReadOnlyTxn", err)
	}
	if err := rdb.ScanLinks(ctx, "", func(context.Context, string, []byte) bool { return true }); err != nil {
		t.Errorf("readonly ScanLinks: %v", err)
	}
}
