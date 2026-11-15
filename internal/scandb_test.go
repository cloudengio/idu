// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file"
	"cloudeng.io/file/diskusage"
)

type compactSpyDB struct {
	database.DB
	compacted int
}

func (s *compactSpyDB) Compact(ctx context.Context) error {
	s.compacted++
	if c, ok := s.DB.(compactor); ok {
		return c.Compact(ctx)
	}
	return nil
}

func TestScanDBCompactionInterval(t *testing.T) {
	ctx := context.Background()
	origFactory := databaseFactory
	var latestSpy *compactSpyDB
	databaseFactory = func(ctx context.Context, cfg config.Prefix, readonly bool) (database.DB, error) {
		db, err := origFactory(ctx, cfg, readonly)
		if err != nil {
			return nil, err
		}
		latestSpy = &compactSpyDB{DB: db}
		return latestSpy, nil
	}
	defer func() {
		databaseFactory = origFactory
	}()

	seedLog := func(dbPath string, when time.Time) {
		cfg := config.Prefix{Prefix: "/seed", Database: dbPath}
		db, err := origFactory(ctx, cfg, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Log(ctx, when.Add(-time.Minute), when, []byte(`{"op":"seed"}`)); err != nil {
			_ = db.Close(ctx)
			t.Fatal(err)
		}
		if err := db.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}

	runScan := func(cfg config.Prefix) int {
		latestSpy = nil
		sdb, err := NewScanDB(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		if err := sdb.LogAndClose(ctx, now.Add(-time.Second), now, []byte(`{"op":"analyze"}`)); err != nil {
			t.Fatal(err)
		}
		return latestSpy.compacted
	}

	// 1. Zero interval: never compacts even with an old write log.
	dbZero := filepath.Join(t.TempDir(), "db-zero")
	seedLog(dbZero, time.Now().Add(-10*24*time.Hour))
	if got := runScan(config.Prefix{Prefix: "/a", Database: dbZero}); got != 0 {
		t.Fatalf("zero interval: compacted=%d, want 0", got)
	}

	// 2. Non-zero interval on a new DB with no prior writes: does not compact.
	dbFresh := filepath.Join(t.TempDir(), "db-fresh")
	if got := runScan(config.Prefix{Prefix: "/b", Database: dbFresh, CompactInterval: time.Millisecond}); got != 0 {
		t.Fatalf("fresh db: compacted=%d, want 0", got)
	}

	// 3. Recent write within combined interval (1d + 2h = 26h, last write 25h ago): does not compact.
	dbRecent := filepath.Join(t.TempDir(), "db-recent")
	seedLog(dbRecent, time.Now().Add(-25*time.Hour))
	if got := runScan(config.Prefix{
		Prefix:              "/c",
		Database:            dbRecent,
		CompactIntervalDays: 1,
		CompactInterval:     2 * time.Hour,
	}); got != 0 {
		t.Fatalf("recent write (25h < 26h): compacted=%d, want 0", got)
	}

	// 4. Stale write beyond combined interval (1d + 2h = 26h, last write 27h ago): compacts.
	dbStale := filepath.Join(t.TempDir(), "db-stale")
	seedLog(dbStale, time.Now().Add(-27*time.Hour))
	if got := runScan(config.Prefix{
		Prefix:              "/d",
		Database:            dbStale,
		CompactIntervalDays: 1,
		CompactInterval:     2 * time.Hour,
	}); got != 1 {
		t.Fatalf("stale write (27h > 26h): compacted=%d, want 1", got)
	}
}

func makeTestPrefixInfo(size int64) prefixinfo.T {
	dirInfo := file.NewInfo("dir", 4096, 0o755|1<<31, time.Now(), file.XAttr{UID: 1, GID: 1, Blocks: 8})
	pi := prefixinfo.New("dir", dirInfo)
	pi.SetInfoList(file.InfoList{
		file.NewInfo("f1", size, 0o644, time.Now(), file.XAttr{UID: 1, GID: 1, Blocks: size / 512}),
	})
	return pi
}

func readSummaryKeys(ctx context.Context, t *testing.T, cfg config.Prefix) ([]string, database.DB) {
	t.Helper()
	rodb, err := OpenDatabase(ctx, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	if err := rodb.ScanSummaries(ctx, "", "", func(_ context.Context, k string, _ []byte) bool {
		keys = append(keys, stats.DecodeSummaryKey("/", k))
		return true
	}); err != nil {
		_ = rodb.Close(ctx)
		t.Fatal(err)
	}
	return keys, rodb
}

func TestScanDBSummariesAndMeta(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db-fresh")
	cfg := config.Prefix{Prefix: "/a", Database: dbDir, Separator: "/"}

	sdb, err := NewScanDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		key  string
		size int64
	}{
		{"/a", 100},
		{"/a/b", 200},
		{"/a-sib", 300},
	} {
		pi := makeTestPrefixInfo(entry.size)
		if err := sdb.SetPrefixInfo(ctx, entry.key, false, &pi); err != nil {
			t.Fatal(err)
		}
	}
	if err := sdb.Close(ctx); err != nil {
		t.Fatal(err)
	}

	keys, rodb := readSummaryKeys(ctx, t, cfg)
	calcID := diskusage.NewIdentity().String()
	if CheckSummaries(ctx, rodb, calcID, false) != nil || CheckSummaries(ctx, rodb, "block(4096)", false) == nil {
		t.Fatalf("unexpected CheckSummaries state for fresh db")
	}
	if want := []string{"/a", "/a/b", "/a-sib"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("ScanSummaries keys = %v, want %v", keys, want)
	}
	_ = rodb.Close(ctx)

	sdb2, err := NewScanDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb2.DeletePrefix(ctx, "/a/b"); err != nil {
		t.Fatal(err)
	}
	_ = sdb2.Close(ctx)

	keys2, rodb2 := readSummaryKeys(ctx, t, cfg)
	if want := []string{"/a", "/a-sib"}; !reflect.DeepEqual(keys2, want) {
		t.Fatalf("after DeletePrefix keys = %v, want %v", keys2, want)
	}
	_ = rodb2.Close(ctx)
}

func TestScanDBCalculatorChange(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db-calc")
	cfg := config.Prefix{Prefix: "/a", Database: dbDir, Separator: "/"}
	calcID := diskusage.NewIdentity().String()

	sdb, err := NewScanDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	pi := makeTestPrefixInfo(100)
	if err := sdb.SetPrefixInfo(ctx, "/a", false, &pi); err != nil {
		t.Fatal(err)
	}
	_ = sdb.Close(ctx)

	// Simulate the summaries having been computed with another calculator.
	stampStaleSummary(ctx, t, cfg, "/a", "block(4096)")
	readCalcIDs := func() (bool, []string) {
		return readSummaryCalcIDs(ctx, t, cfg, calcID)
	}

	// An unchanged prefix must have its summary recomputed from the stored
	// prefix info, but the summaries are not valid until marked complete.
	sdb2, err := NewScanDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	dirsOnly := prefixinfo.T{}
	if err := sdb2.SetPrefixInfo(ctx, "/a", true, &dirsOnly); err != nil {
		t.Fatal(err)
	}
	_ = sdb2.Close(ctx)
	valid, ids := readCalcIDs()
	if valid {
		t.Fatalf("summaries should not be valid before MarkSummariesComplete")
	}
	if want := []string{calcID}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("calc ids = %v, want %v", ids, want)
	}

	sdb3, err := NewScanDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb3.MarkSummariesComplete(ctx); err != nil {
		t.Fatal(err)
	}
	_ = sdb3.Close(ctx)
	if valid, _ := readCalcIDs(); !valid {
		t.Fatalf("summaries should be valid after MarkSummariesComplete")
	}
}

func stampStaleSummary(ctx context.Context, t *testing.T, cfg config.Prefix, key, staleCalcID string) {
	t.Helper()
	rawDB, err := OpenDatabase(ctx, cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rawDB.Close(ctx)
	if err := rawDB.SetMeta(ctx, metaKeySummaries, []byte(staleCalcID)); err != nil {
		t.Fatal(err)
	}
	stale := stats.PrefixSummary{CalcID: staleCalcID}
	exactKey, _ := stats.SummaryKeys("/", key)
	if err := rawDB.SetSummary(ctx, exactKey, stale.AppendBinary(nil), false); err != nil {
		t.Fatal(err)
	}
}

func readSummaryCalcIDs(ctx context.Context, t *testing.T, cfg config.Prefix, calcID string) (bool, []string) {
	t.Helper()
	rodb, err := OpenDatabase(ctx, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer rodb.Close(ctx)
	var ids []string
	if err := rodb.ScanSummaries(ctx, "", "", func(_ context.Context, _ string, v []byte) bool {
		var ps stats.PrefixSummary
		if err := ps.UnmarshalBinary(v); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, ps.CalcID)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return CheckSummaries(ctx, rodb, calcID, false) == nil, ids
}

func TestLegacyDatabaseRejected(t *testing.T) {
	ctx := context.Background()
	cfg := config.Prefix{Prefix: "/legacy", Database: filepath.Join(t.TempDir(), "db-legacy")}
	rawDB, err := OpenDatabase(ctx, cfg, false)
	if err != nil {
		t.Fatal(err)
	}
	// Prefix data without summary metadata is the legacy format.
	if err := rawDB.Set(ctx, "/legacy", []byte("dummy"), false); err != nil {
		t.Fatal(err)
	}
	_ = rawDB.Close(ctx)

	for _, readonly := range []bool{true, false} {
		if _, err := OpenDatabase(ctx, cfg, readonly); !errors.Is(err, ErrLegacyDatabase) {
			t.Errorf("readonly=%v: got %v, want %v", readonly, err, ErrLegacyDatabase)
		}
	}
	if _, err := NewScanDB(ctx, cfg); !errors.Is(err, ErrLegacyDatabase) {
		t.Errorf("NewScanDB: got %v, want %v", err, ErrLegacyDatabase)
	}
}

func TestScanDBDeletePrefixSiblings(t *testing.T) {
	ctx := context.Background()
	cfg := config.Prefix{Prefix: "/", Database: filepath.Join(t.TempDir(), "db"), Separator: "/"}
	sdb, err := NewScanDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"/a", "/a/b", "/a-b", "/ab"} {
		pi := makeTestPrefixInfo(10)
		if err := sdb.SetPrefixInfo(ctx, key, false, &pi); err != nil {
			t.Fatal(err)
		}
	}
	_ = sdb.Close(ctx)

	sdb, err = NewScanDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"/a", "/a/b", "/a-b", "/ab"} {
		if err := sdb.LogError(ctx, key, time.Now(), []byte("oops")); err != nil {
			t.Fatal(err)
		}
	}
	if err := sdb.DeletePrefix(ctx, "/a"); err != nil {
		t.Fatal(err)
	}
	if err := sdb.DeleteErrors(ctx, "/a"); err != nil {
		t.Fatal(err)
	}
	// A delete must not be undone by a batched write that is still
	// pending when it is issued.
	pi := makeTestPrefixInfo(10)
	if err := sdb.SetPrefixInfo(ctx, "/c", false, &pi); err != nil {
		t.Fatal(err)
	}
	if err := sdb.DeletePrefix(ctx, "/c"); err != nil {
		t.Fatal(err)
	}
	_ = sdb.Close(ctx)

	summaries, prefixes, errs := readAllKeys(ctx, t, cfg)
	want := []string{"/a-b", "/ab"}
	if !reflect.DeepEqual(summaries, want) || !reflect.DeepEqual(prefixes, want) || !reflect.DeepEqual(errs, want) {
		t.Errorf("got summaries %v, prefixes %v, errors %v, want %v", summaries, prefixes, errs, want)
	}
}

// readAllKeys returns the keys in the summary, prefix and error buckets.
func readAllKeys(ctx context.Context, t *testing.T, cfg config.Prefix) (summaries, prefixes, errs []string) {
	t.Helper()
	summaries, rodb := readSummaryKeys(ctx, t, cfg)
	defer rodb.Close(ctx)
	if err := rodb.Scan(ctx, "", func(_ context.Context, k string, _ []byte) bool {
		prefixes = append(prefixes, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := rodb.VisitErrors(ctx, "", func(_ context.Context, k string, _ time.Time, _ []byte) bool {
		errs = append(errs, k)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return summaries, prefixes, errs
}
