// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file"
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

	prefixes, errs := readAllKeys(ctx, t, cfg)
	want := []string{"/a-b", "/ab"}
	if !reflect.DeepEqual(prefixes, want) || !reflect.DeepEqual(errs, want) {
		t.Errorf("got prefixes %v, errors %v, want %v", prefixes, errs, want)
	}
}

// readAllKeys returns the keys in the prefix and error buckets.
func readAllKeys(ctx context.Context, t *testing.T, cfg config.Prefix) (prefixes, errs []string) {
	t.Helper()
	rodb, err := OpenDatabase(ctx, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
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
	return prefixes, errs
}
