// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database"
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
