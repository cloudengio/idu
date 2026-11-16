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
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/database"
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

func openTestBadger(t *testing.T, dir string, opts ...badgerdb.Option) database.DB {
	t.Helper()
	bopts := badger.DefaultOptions(dir).WithLogger(nil)
	allOpts := append([]badgerdb.Option{badgerdb.WithBadgerOptions(bopts)}, opts...)
	db, err := badgerdb.Open(dir, allOpts...)
	if err != nil {
		t.Fatalf("badgerdb.Open(%q): %v", dir, err)
	}
	return db
}

func seedClearDB(ctx context.Context, t *testing.T, db database.DB, base time.Time) {
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

func assertLogAndErrCounts(ctx context.Context, t *testing.T, db database.DB, end time.Time, wantLogs, wantErrs int) {
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

	mc := db.(*badgerdb.Database).BadgerDB().MaxBatchCount()
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

func seedScanVisitDB(ctx context.Context, t *testing.T, base time.Time) database.DB {
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

	t.Run("WithTimeout option sets Timeout", func(t *testing.T) {
		t.Parallel()
		var opts database.Options[badgerdb.Options]
		badgerdb.WithTimeout(5 * time.Second)(&opts)
		if opts.Timeout != 5*time.Second {
			t.Errorf("Timeout = %v, want 5s", opts.Timeout)
		}
	})

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
		if runtime.GOOS == "windows" {
			t.Skip("read-only mode is disabled on Windows")
		}
		missing := filepath.Join(t.TempDir(), "missing-db")
		if db, err := badgerdb.Open(missing, badgerdb.ReadOnly()); err == nil {
			_ = db.Close(context.Background())
			t.Fatal("expected Open ReadOnly to fail on non-existent database")
		}
	})
}
