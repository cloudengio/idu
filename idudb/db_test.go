// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb/backends/badgerdb"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file"
	"cloudeng.io/file/diskusage"
)

type compactSpyDB struct {
	Database
	compacted int
}

func (s *compactSpyDB) Compact(ctx context.Context) error {
	s.compacted++
	if c, ok := s.Database.(compactor); ok {
		return c.Compact(ctx)
	}
	return nil
}

func TestScanDBCompactionInterval(t *testing.T) {
	ctx := context.Background()
	origFactory := databaseFactory
	var latestSpy *compactSpyDB
	databaseFactory = func(ctx context.Context, path string, readonly bool) (Database, error) {
		db, err := origFactory(ctx, path, readonly)
		if err != nil {
			return nil, err
		}
		latestSpy = &compactSpyDB{Database: db}
		return latestSpy, nil
	}
	defer func() {
		databaseFactory = origFactory
	}()

	seedLog := func(dbPath string, when time.Time) {
		db, err := origFactory(ctx, dbPath, false)
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

	runScan := func(cfg iduconfig.Prefix) int {
		latestSpy = nil
		sdb, err := newTestDB(ctx, cfg)
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
	if got := runScan(iduconfig.Prefix{Prefix: "/a", Database: dbZero}); got != 0 {
		t.Fatalf("zero interval: compacted=%d, want 0", got)
	}

	// 2. Non-zero interval on a new DB with no prior writes: does not compact.
	dbFresh := filepath.Join(t.TempDir(), "db-fresh")
	if got := runScan(iduconfig.Prefix{Prefix: "/b", Database: dbFresh, CompactInterval: time.Millisecond}); got != 0 {
		t.Fatalf("fresh db: compacted=%d, want 0", got)
	}

	// 3. Recent write within combined interval (1d + 2h = 26h, last write 25h ago): does not compact.
	dbRecent := filepath.Join(t.TempDir(), "db-recent")
	seedLog(dbRecent, time.Now().Add(-25*time.Hour))
	if got := runScan(iduconfig.Prefix{
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
	if got := runScan(iduconfig.Prefix{
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

func readSummaryKeys(ctx context.Context, t *testing.T, cfg iduconfig.Prefix) ([]string, Database) {
	t.Helper()
	rodb, err := openDatabase(ctx, cfg.Database, true)
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
	cfg := iduconfig.Prefix{Prefix: "/a", Database: dbDir, Separator: "/"}

	sdb, err := newTestDB(ctx, cfg)
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
		if err := sdb.setPrefixInfo(ctx, entry.key, false, &pi); err != nil {
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

	sdb2, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb2.deletePrefix(ctx, "/a/b"); err != nil {
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
	cfg := iduconfig.Prefix{Prefix: "/a", Database: dbDir, Separator: "/"}
	calcID := diskusage.NewIdentity().String()

	sdb, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	pi := makeTestPrefixInfo(100)
	if err := sdb.setPrefixInfo(ctx, "/a", false, &pi); err != nil {
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
	sdb2, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	dirsOnly := prefixinfo.T{}
	if err := sdb2.setPrefixInfo(ctx, "/a", true, &dirsOnly); err != nil {
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

	sdb3, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb3.markSummariesComplete(ctx); err != nil {
		t.Fatal(err)
	}
	_ = sdb3.Close(ctx)
	if valid, _ := readCalcIDs(); !valid {
		t.Fatalf("summaries should be valid after MarkSummariesComplete")
	}
}

func TestScanDBSummaryFormatChange(t *testing.T) {
	ctx := context.Background()
	cfg := iduconfig.Prefix{Prefix: "/a", Database: filepath.Join(t.TempDir(), "db-format"), Separator: "/"}
	calcID := diskusage.NewIdentity().String()

	sdb, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	pi := makeTestPrefixInfo(100)
	if err := sdb.setPrefixInfo(ctx, "/a", false, &pi); err != nil {
		t.Fatal(err)
	}
	_ = sdb.Close(ctx)

	// Simulate summaries written using the previous encoding, whose
	// specification did not include the encoding's version.
	rawDB, err := openDatabase(ctx, cfg.Database, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := rawDB.SetMeta(ctx, metaKeySummaries, []byte(calcID+hardlinksSpecSuffix)); err != nil {
		t.Fatal(err)
	}
	if err := CheckSummaries(ctx, rawDB, calcID, true); !errors.Is(err, ErrStaleSummaries) {
		t.Errorf("got %v, want %v", err, ErrStaleSummaries)
	}
	_ = rawDB.Close(ctx)

	// The next analyze rewrites the summaries, including for unchanged
	// prefixes, after which they are valid.
	sdb, err = newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb.setPrefixInfo(ctx, "/a", true, &pi); err != nil {
		t.Fatal(err)
	}
	if err := sdb.markSummariesComplete(ctx); err != nil {
		t.Fatal(err)
	}
	_ = sdb.Close(ctx)
	if valid, ids := readSummaryCalcIDs(ctx, t, cfg, calcID); !valid || !reflect.DeepEqual(ids, []string{calcID}) {
		t.Errorf("got valid=%v calc ids=%v, want true [%v]", valid, ids, calcID)
	}
}

func stampStaleSummary(ctx context.Context, t *testing.T, cfg iduconfig.Prefix, key, staleCalcID string) {
	t.Helper()
	rawDB, err := openDatabase(ctx, cfg.Database, false)
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

func readSummaryCalcIDs(ctx context.Context, t *testing.T, cfg iduconfig.Prefix, calcID string) (bool, []string) {
	t.Helper()
	rodb, err := openDatabase(ctx, cfg.Database, true)
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
	cfg := iduconfig.Prefix{Prefix: "/legacy", Database: filepath.Join(t.TempDir(), "db-legacy")}
	rawDB, err := openDatabase(ctx, cfg.Database, false)
	if err != nil {
		t.Fatal(err)
	}
	// Prefix data without summary metadata is the legacy format.
	if err := rawDB.Set(ctx, "/legacy", []byte("dummy"), false); err != nil {
		t.Fatal(err)
	}
	_ = rawDB.Close(ctx)

	for _, readonly := range []bool{true, false} {
		if _, err := openDatabase(ctx, cfg.Database, readonly); !errors.Is(err, ErrLegacyDatabase) {
			t.Errorf("readonly=%v: got %v, want %v", readonly, err, ErrLegacyDatabase)
		}
	}
	if _, err := newTestDB(ctx, cfg); !errors.Is(err, ErrLegacyDatabase) {
		t.Errorf("NewScanDB: got %v, want %v", err, ErrLegacyDatabase)
	}
}

func TestScanDBDeletePrefixSiblings(t *testing.T) {
	ctx := context.Background()
	cfg := iduconfig.Prefix{Prefix: "/", Database: filepath.Join(t.TempDir(), "db"), Separator: "/"}
	sdb, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"/a", "/a/b", "/a-b", "/ab"} {
		pi := makeTestPrefixInfo(10)
		if err := sdb.setPrefixInfo(ctx, key, false, &pi); err != nil {
			t.Fatal(err)
		}
	}
	_ = sdb.Close(ctx)

	sdb, err = newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"/a", "/a/b", "/a-b", "/ab"} {
		if err := sdb.logError(ctx, key, time.Now(), []byte("oops")); err != nil {
			t.Fatal(err)
		}
	}
	if err := sdb.deletePrefix(ctx, "/a"); err != nil {
		t.Fatal(err)
	}
	if err := sdb.deleteErrors(ctx, "/a"); err != nil {
		t.Fatal(err)
	}
	// A delete must not be undone by a batched write that is still
	// pending when it is issued.
	pi := makeTestPrefixInfo(10)
	if err := sdb.setPrefixInfo(ctx, "/c", false, &pi); err != nil {
		t.Fatal(err)
	}
	if err := sdb.deletePrefix(ctx, "/c"); err != nil {
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
func readAllKeys(ctx context.Context, t *testing.T, cfg iduconfig.Prefix) (summaries, prefixes, errs []string) {
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

type faultDB struct {
	Database
	setErr           error
	getErr           error
	setSummaryErr    error
	setMetaErr       error
	flushErr         error
	deleteErr        error
	deletePrefixErr  error
	deleteSummaryErr error
	deleteErrorErr   error
	deleteErrorsErr  error
	logErr           error
	compactErr       error
	gcErr            error
	closeErr         error
	closed           int
}

func (f *faultDB) Set(ctx context.Context, prefix string, value []byte, batch bool) error {
	if f.setErr != nil {
		return f.setErr
	}
	return f.Database.Set(ctx, prefix, value, batch)
}

func (f *faultDB) Get(ctx context.Context, prefix string, buf *bytes.Buffer) error {
	if f.getErr != nil {
		return f.getErr
	}
	return f.Database.Get(ctx, prefix, buf)
}

func (f *faultDB) SetSummary(ctx context.Context, key string, value []byte, batch bool) error {
	if f.setSummaryErr != nil {
		return f.setSummaryErr
	}
	return f.Database.SetSummary(ctx, key, value, batch)
}

func (f *faultDB) SetMeta(ctx context.Context, key string, value []byte) error {
	if f.setMetaErr != nil {
		return f.setMetaErr
	}
	return f.Database.SetMeta(ctx, key, value)
}

func (f *faultDB) Flush(ctx context.Context) error {
	if f.flushErr != nil {
		return f.flushErr
	}
	return f.Database.Flush(ctx)
}

func (f *faultDB) Delete(ctx context.Context, prefix string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return f.Database.Delete(ctx, prefix)
}

func (f *faultDB) DeletePrefix(ctx context.Context, prefix string) error {
	if f.deletePrefixErr != nil {
		return f.deletePrefixErr
	}
	return f.Database.DeletePrefix(ctx, prefix)
}

func (f *faultDB) DeleteSummary(ctx context.Context, exactKey, childPrefix string) error {
	if f.deleteSummaryErr != nil {
		return f.deleteSummaryErr
	}
	return f.Database.DeleteSummary(ctx, exactKey, childPrefix)
}

func (f *faultDB) DeleteError(ctx context.Context, key string) error {
	if f.deleteErrorErr != nil {
		return f.deleteErrorErr
	}
	return f.Database.DeleteError(ctx, key)
}

func (f *faultDB) DeleteErrors(ctx context.Context, prefix string) error {
	if f.deleteErrorsErr != nil {
		return f.deleteErrorsErr
	}
	return f.Database.DeleteErrors(ctx, prefix)
}

func (f *faultDB) Log(ctx context.Context, start, stop time.Time, detail []byte) error {
	if f.logErr != nil {
		return f.logErr
	}
	return f.Database.Log(ctx, start, stop, detail)
}

func (f *faultDB) Compact(ctx context.Context) error {
	if f.compactErr != nil {
		return f.compactErr
	}
	if c, ok := f.Database.(compactor); ok {
		return c.Compact(ctx)
	}
	return nil
}

func (f *faultDB) RunValueLogGC(discardRatio float64) error {
	if f.gcErr != nil {
		return f.gcErr
	}
	if gc, ok := f.Database.(valueLogGCRunner); ok {
		return gc.RunValueLogGC(discardRatio)
	}
	return nil
}

func (f *faultDB) Close(ctx context.Context) error {
	f.closed++
	if f.closeErr != nil {
		_ = f.Database.Close(ctx)
		return f.closeErr
	}
	return f.Database.Close(ctx)
}

func newFaultScanDBFactory(t *testing.T) **faultDB {
	t.Helper()
	origFactory := databaseFactory
	var fdb *faultDB
	databaseFactory = func(ctx context.Context, path string, readonly bool) (Database, error) {
		db, err := origFactory(ctx, path, readonly)
		if err != nil {
			return nil, err
		}
		fdb = &faultDB{Database: db}
		return fdb, nil
	}
	t.Cleanup(func() {
		databaseFactory = origFactory
	})
	return &fdb
}

func TestScanDBSummaryInvalidation(t *testing.T) {
	ctx := context.Background()
	fdbPtr := newFaultScanDBFactory(t)

	cfg := iduconfig.Prefix{Prefix: "/a", Database: filepath.Join(t.TempDir(), "db-inv"), Separator: "/"}
	sdb, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close(ctx)
	fdb := *fdbPtr

	pi := makeTestPrefixInfo(100)
	errSummary := errors.New("summary write failed")
	fdb.setSummaryErr = errSummary
	if err := sdb.setPrefixInfo(ctx, "/a", false, &pi); !errors.Is(err, errSummary) {
		t.Fatalf("SetPrefixInfo = %v, want %v", err, errSummary)
	}
	fdb.setSummaryErr = nil

	if err := CheckSummaryKeys(ctx, fdb); !errors.Is(err, ErrStaleSummaries) {
		t.Errorf("CheckSummaryKeys = %v, want ErrStaleSummaries", err)
	}
	if err := CheckSummaries(ctx, fdb, diskusage.NewIdentity().String(), false); !errors.Is(err, ErrStaleSummaries) {
		t.Errorf("CheckSummaries = %v, want ErrStaleSummaries", err)
	}
}

func TestScanDBGetSetPrefixInfoErrors(t *testing.T) {
	ctx := context.Background()
	fdbPtr := newFaultScanDBFactory(t)

	cfg := iduconfig.Prefix{Prefix: "/a", Database: filepath.Join(t.TempDir(), "db-gs"), Separator: "/"}
	sdb, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close(ctx)
	fdb := *fdbPtr

	var out prefixinfo.T
	if ok, err := sdb.getPrefixInfo(ctx, "/missing", &out); ok || err != nil {
		t.Errorf("missing GetPrefixInfo = (%v, %v), want (false, nil)", ok, err)
	}
	if err := fdb.Database.Set(ctx, "/corrupt", []byte{0xff}, false); err != nil {
		t.Fatal(err)
	}
	if ok, err := sdb.getPrefixInfo(ctx, "/corrupt", &out); !ok || err == nil {
		t.Errorf("corrupt GetPrefixInfo = (%v, %v), want (true, err)", ok, err)
	}

	errGet := errors.New("get failed")
	fdb.getErr = errGet
	if ok, err := sdb.getPrefixInfo(ctx, "/a", &out); ok || !errors.Is(err, errGet) {
		t.Errorf("error GetPrefixInfo = (%v, %v), want (false, %v)", ok, err, errGet)
	}
	fdb.getErr = nil

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if ok, err := sdb.getPrefixInfo(cctx, "/a", &out); ok || !errors.Is(err, context.Canceled) {
		t.Errorf("canceled GetPrefixInfo = (%v, %v), want (false, context.Canceled)", ok, err)
	}
	pi := makeTestPrefixInfo(100)
	if err := sdb.setPrefixInfo(cctx, "/a", false, &pi); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled SetPrefixInfo = %v, want context.Canceled", err)
	}

	errSet := errors.New("set failed")
	fdb.setErr = errSet
	if err := sdb.setPrefixInfo(ctx, "/a", false, &pi); !errors.Is(err, errSet) {
		t.Errorf("SetPrefixInfo set err = %v, want %v", err, errSet)
	}
}

func TestScanDBDeletePrefixAndErrorsPropagation(t *testing.T) {
	ctx := context.Background()
	fdbPtr := newFaultScanDBFactory(t)

	cfg := iduconfig.Prefix{Prefix: "/", Database: filepath.Join(t.TempDir(), "db"), Separator: "/"}
	sdb, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close(ctx)
	fdb := *fdbPtr

	pi := makeTestPrefixInfo(50)
	if err := sdb.setPrefixInfo(ctx, "/", false, &pi); err != nil {
		t.Fatal(err)
	}
	if err := sdb.setPrefixInfo(ctx, "/sub", false, &pi); err != nil {
		t.Fatal(err)
	}
	if err := sdb.deletePrefix(ctx, "/"); err != nil {
		t.Fatalf("DeletePrefix(/): %v", err)
	}

	wantErr := errors.New("op failed")
	fdb.flushErr = wantErr
	if err := sdb.deletePrefix(ctx, "/x"); !errors.Is(err, wantErr) {
		t.Errorf("Flush err = %v, want %v", err, wantErr)
	}
	fdb.flushErr = nil

	fdb.deleteErr = wantErr
	if err := sdb.deletePrefix(ctx, "/x"); !errors.Is(err, wantErr) {
		t.Errorf("Delete err = %v, want %v", err, wantErr)
	}
	fdb.deleteErr = nil

	fdb.deletePrefixErr = wantErr
	if err := sdb.deletePrefix(ctx, "/x"); !errors.Is(err, wantErr) {
		t.Errorf("DeletePrefix err = %v, want %v", err, wantErr)
	}
	fdb.deletePrefixErr = nil

	fdb.deleteErrorErr = wantErr
	if err := sdb.deleteErrors(ctx, "/x"); !errors.Is(err, wantErr) {
		t.Errorf("DeleteError err = %v, want %v", err, wantErr)
	}
}

func TestScanDBMarkCompleteAndLogCloseErrors(t *testing.T) {
	ctx := context.Background()
	fdbPtr := newFaultScanDBFactory(t)

	dbDir := filepath.Join(t.TempDir(), "db-close")
	cfg := iduconfig.Prefix{Prefix: "/a", Database: dbDir, Separator: "/"}
	sdb, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb.markSummariesComplete(ctx); err != nil {
		t.Fatalf("MarkSummariesComplete no-op: %v", err)
	}
	if err := (*fdbPtr).Database.SetMeta(ctx, metaKeySummaries, []byte("other")); err != nil {
		t.Fatal(err)
	}
	_ = sdb.Close(ctx)

	sdb2, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer sdb2.Close(ctx)
	fdb := *fdbPtr

	wantErr := errors.New("boom")
	fdb.flushErr = wantErr
	if err := sdb2.markSummariesComplete(ctx); !errors.Is(err, wantErr) {
		t.Errorf("MarkSummariesComplete Flush err = %v, want %v", err, wantErr)
	}
	fdb.flushErr = nil

	fdb.setMetaErr = wantErr
	if err := sdb2.markSummariesComplete(ctx); !errors.Is(err, wantErr) {
		t.Errorf("MarkSummariesComplete SetMeta err = %v, want %v", err, wantErr)
	}
	fdb.setMetaErr = nil

	if err := sdb2.markSummariesComplete(ctx); err != nil {
		t.Fatalf("MarkSummariesComplete: %v", err)
	}

	fdb.logErr = wantErr
	if err := sdb2.LogAndClose(ctx, time.Now(), time.Now(), []byte("{}")); !errors.Is(err, wantErr) {
		t.Errorf("LogAndClose Log err = %v, want %v", err, wantErr)
	}
	if fdb.closed != 1 {
		t.Errorf("LogAndClose Log error did not close DB: closed=%d, want 1", fdb.closed)
	}

	for _, tc := range []struct {
		name string
		set  func(*faultDB)
	}{
		{name: "gc", set: func(f *faultDB) { f.gcErr = wantErr }},
		{name: "close", set: func(f *faultDB) { f.closeErr = wantErr }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := newTestDB(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			f := *fdbPtr
			tc.set(f)
			if err := s.LogAndClose(ctx, time.Now(), time.Now(), []byte("{}")); !errors.Is(err, wantErr) {
				t.Errorf("LogAndClose %s err = %v, want %v", tc.name, err, wantErr)
			}
			if f.closed != 1 {
				t.Errorf("LogAndClose %s error did not close DB: closed=%d, want 1", tc.name, f.closed)
			}
		})
	}
}

func TestScanDBCompactError(t *testing.T) {
	ctx := context.Background()
	origFactory := databaseFactory
	dbDir := filepath.Join(t.TempDir(), "db-compact-err")
	raw, err := origFactory(ctx, dbDir, false)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := raw.Log(ctx, old.Add(-time.Minute), old, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close(ctx)

	fdbPtr := newFaultScanDBFactory(t)
	cfg := iduconfig.Prefix{Prefix: "/a", Database: dbDir, CompactInterval: time.Hour}
	sdb, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close(ctx)

	wantErr := errors.New("compact failed")
	(*fdbPtr).compactErr = wantErr
	if err := sdb.LogAndClose(ctx, time.Now(), time.Now(), []byte("{}")); !errors.Is(err, wantErr) {
		t.Errorf("LogAndClose Compact err = %v, want %v", err, wantErr)
	}
	if (*fdbPtr).closed != 1 {
		t.Errorf("LogAndClose Compact error did not close DB: closed=%d, want 1", (*fdbPtr).closed)
	}
}

func writeScanDBPrefixes(ctx context.Context, t *testing.T, cfg iduconfig.Prefix, unchanged bool, complete bool, keys ...string) {
	t.Helper()
	sdb, err := newTestDB(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		pi := makeTestPrefixInfo(100)
		if err := sdb.setPrefixInfo(ctx, k, unchanged, &pi); err != nil {
			t.Fatal(err)
		}
	}
	if complete {
		if err := sdb.markSummariesComplete(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_ = sdb.Close(ctx)
}

func TestScanDBPartialRewriteInvalidatesOldSpec(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db-partial-rewrite")
	calcID := diskusage.NewIdentity().String()

	// 1. Build a complete DB recording hardlinks.
	cfgOld := iduconfig.Prefix{Prefix: "/a", Database: dbDir, Separator: "/", CountHardlinkAsFiles: false}
	writeScanDBPrefixes(ctx, t, cfgOld, false, true, "/a", "/a/sub")

	// 2. Switch to CountHardlinkAsFiles: true and rewrite only /a/sub
	// (simulating a sub-prefix analyze or an interrupted full analyze).
	cfgNew := iduconfig.Prefix{Prefix: "/a", Database: dbDir, Separator: "/", CountHardlinkAsFiles: true}
	writeScanDBPrefixes(ctx, t, cfgNew, false, false, "/a/sub")

	// 3. The old spec (needLinks=true) and new spec must both be rejected
	// because summaries are now mixed and incomplete.
	rodb, err := openDatabase(ctx, cfgNew.Database, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckSummaries(ctx, rodb, calcID, true); !errors.Is(err, ErrStaleSummaries) {
		t.Errorf("CheckSummaries(needLinks=true) = %v, want ErrStaleSummaries", err)
	}
	if err := CheckSummaries(ctx, rodb, calcID, false); !errors.Is(err, ErrStaleSummaries) {
		t.Errorf("CheckSummaries(needLinks=false) = %v, want ErrStaleSummaries", err)
	}
	if err := CheckSummaryKeys(ctx, rodb); !errors.Is(err, ErrStaleSummaries) {
		t.Errorf("CheckSummaryKeys = %v, want ErrStaleSummaries", err)
	}
	_ = rodb.Close(ctx)

	// 4. Completing a full scan with cfgNew restores valid summaries.
	writeScanDBPrefixes(ctx, t, cfgNew, true, true, "/a", "/a/sub")

	rodb2, err := openDatabase(ctx, cfgNew.Database, true)
	if err != nil {
		t.Fatal(err)
	}
	defer rodb2.Close(ctx)
	if err := CheckSummaries(ctx, rodb2, calcID, false); err != nil {
		t.Errorf("CheckSummaries(needLinks=false) after complete = %v, want nil", err)
	}
	if err := CheckSummaries(ctx, rodb2, calcID, true); !errors.Is(err, ErrStaleSummaries) {
		t.Errorf("CheckSummaries(needLinks=true) after non-hardlink complete = %v, want ErrStaleSummaries", err)
	}
}

// newTestDB opens the database specified by cfg using the options
// derived from cfg.
func newTestDB(ctx context.Context, cfg iduconfig.Prefix) (*DB, error) {
	return openTestDB(ctx, cfg.Database, OptionsFromConfig(cfg)...)
}

// databaseFactory creates the database backend used by tests and may be
// overridden to inject faults.
var databaseFactory = func(_ context.Context, path string, readonly bool) (Database, error) {
	var opts []badgerdb.Option
	if readonly {
		opts = append(opts, badgerdb.ReadOnly())
	}
	return badgerdb.Open(path, opts...)
}

// openDatabase opens a database backend via databaseFactory and checks
// its format.
func openDatabase(ctx context.Context, path string, readonly bool) (Database, error) {
	db, err := databaseFactory(ctx, path, readonly)
	if err != nil {
		return nil, err
	}
	if err := CheckFormat(ctx, db); err != nil {
		_ = db.Close(ctx)
		return nil, err
	}
	return db, nil
}
