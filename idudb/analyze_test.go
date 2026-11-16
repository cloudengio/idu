// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/localfs"
)

// countingProgress is a ScanProgress implementation that counts calls.
type countingProgress struct {
	nullProgress
	mu                                            sync.Mutex
	started, done, errors, parent, children, slow int
	files                                         int64
}

func (c *countingProgress) PrefixStarted() { c.mu.Lock(); c.started++; c.mu.Unlock() }
func (c *countingProgress) Error()         { c.mu.Lock(); c.errors++; c.mu.Unlock() }
func (c *countingProgress) ParentUnchanged() {
	c.mu.Lock()
	c.parent++
	c.mu.Unlock()
}
func (c *countingProgress) ChildrenUnchanged() {
	c.mu.Lock()
	c.children++
	c.mu.Unlock()
}
func (c *countingProgress) SlowScan() { c.mu.Lock(); c.slow++; c.mu.Unlock() }
func (c *countingProgress) PrefixDone(_ int, files int64) {
	c.mu.Lock()
	c.done++
	c.files += files
	c.mu.Unlock()
}

func (c *countingProgress) numErrors() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errors
}

func createRootAndSub(t *testing.T) (root, sub, dbDir string) {
	t.Helper()
	tmpDir := t.TempDir()
	root = filepath.Join(tmpDir, "root")
	sub = filepath.Join(root, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "f0"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "f1"), []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	return root, sub, filepath.Join(tmpDir, "db")
}

func analyzeOnce(ctx context.Context, t *testing.T, dbDir, root string, opts ...AnalyzeOption) {
	t.Helper()
	db, err := openTestDB(ctx, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewAnalyzer(db, root, opts...).Analyze(ctx, root); err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if err := db.LogAndClose(ctx, time.Now(), time.Now(), []byte("{}")); err != nil {
		t.Fatal(err)
	}
}

func verifyAnalyzed(ctx context.Context, t *testing.T, dbDir, root, sub string) {
	t.Helper()
	rdb, err := openDatabase(ctx, dbDir, true)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := rdb.Get(ctx, sub, &buf); err != nil {
		t.Fatal(err)
	}
	var pi prefixinfo.T
	if err := pi.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	if got := pi.InfoList(); len(got) != 1 || got[0].Name() != "f1" {
		t.Errorf("unexpected entries for %v: %v", sub, got)
	}
	if err := CheckSummaries(ctx, rdb, newOptions(nil).calc.String(), false); err != nil {
		t.Errorf("CheckSummaries: %v", err)
	}
	exactKey, childPrefix := stats.SummaryKeys("/", root)
	nsummaries := 0
	if err := rdb.ScanSummaries(ctx, exactKey, childPrefix, func(context.Context, string, []byte) bool {
		nsummaries++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if nsummaries != 2 {
		t.Errorf("got %v summaries, want 2", nsummaries)
	}
	if err := rdb.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAnalyzer(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	if err := os.WriteFile(filepath.Join(sub, "ignored.txt"), []byte("c"), 0o600); err != nil {
		t.Fatal(err)
	}
	ignore := regexp.MustCompile(`ignored\.txt$`)

	pt := &countingProgress{}
	opts := []AnalyzeOption{
		WithFS(localfs.New()),
		WithProgress(pt),
		WithExclusions(ignore.MatchString),
		WithConcurrentScans(2),
		WithScanSize(10),
	}
	analyzeOnce(ctx, t, dbDir, root, opts...)
	if pt.started != 2 || pt.done != 2 || pt.files != 2 || pt.errors != 0 {
		t.Errorf("unexpected progress: %+v", pt)
	}

	verifyAnalyzed(ctx, t, dbDir, root, sub)

	// A second, incremental, analysis should find everything unchanged.
	pt2 := &countingProgress{}
	analyzeOnce(ctx, t, dbDir, root, WithProgress(pt2), WithExclusions(ignore.MatchString))
	if pt2.parent != 2 {
		t.Errorf("got %v unchanged parents, want 2", pt2.parent)
	}

	// Forcing a reanalysis should not reuse any existing entries.
	pt3 := &countingProgress{}
	analyzeOnce(ctx, t, dbDir, root, WithProgress(pt3), WithForce(true))
	if pt3.parent != 0 || pt3.files != 3 {
		t.Errorf("got %v unchanged parents, %v files, want 0, 3", pt3.parent, pt3.files)
	}
}

func TestAnalyzerConfiguration(t *testing.T) {
	a := NewAnalyzer(nil, "/",
		WithScanSize(11),
		WithConcurrentScans(12),
		WithConcurrentStats(13),
		WithConcurrentStatsThreshold(14))
	want := Configuration{ScanSize: 11, ConcurrentScans: 12, ConcurrentStats: 13, ConcurrentStatsThreshold: 14}
	if got := a.Configuration(); got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestAnalyzerPartialScanDoesNotMarkComplete(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	// Analyze without recording hardlinks, then again with recording
	// hardlinks, but only for the subtree: the summaries must not be
	// marked as complete.
	analyzeOnce(ctx, t, dbDir, root)

	db, err := openTestDB(ctx, dbDir, WithRecordHardlinks(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := NewAnalyzer(db, root).Analyze(ctx, sub); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}
	rdb, err := openDatabase(ctx, dbDir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer rdb.Close(ctx)
	if err := CheckSummaries(ctx, rdb, newOptions(nil).calc.String(), false); !errors.Is(err, ErrStaleSummaries) {
		t.Errorf("got %v, want %v", err, ErrStaleSummaries)
	}
}

func TestEraseLogsAndErrors(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	db, err := openTestDB(ctx, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.logError(ctx, "/a", time.Now(), []byte("oops")); err != nil {
		t.Fatal(err)
	}
	if err := db.LogAndClose(ctx, time.Now(), time.Now(), []byte("{}")); err != nil {
		t.Fatal(err)
	}
	count := func() (logs, errs int) {
		rdb, err := openDatabase(ctx, dbDir, true)
		if err != nil {
			t.Fatal(err)
		}
		defer rdb.Close(ctx)
		_ = rdb.VisitLogs(ctx, time.Time{}, time.Now().Add(time.Hour), func(context.Context, time.Time, time.Time, []byte) bool {
			logs++
			return true
		})
		_ = rdb.VisitErrors(ctx, "", func(context.Context, string, time.Time, []byte) bool {
			errs++
			return true
		})
		return
	}
	erase := func(fn func(context.Context, Database) error) error {
		db, err := openDatabase(ctx, dbDir, false)
		if err != nil {
			return err
		}
		if err := fn(ctx, db); err != nil {
			return err
		}
		return db.Close(ctx)
	}
	if l, e := count(); l != 1 || e != 1 {
		t.Fatalf("got %v logs, %v errors, want 1, 1", l, e)
	}
	if err := erase(EraseErrors); err != nil {
		t.Fatal(err)
	}
	if l, e := count(); l != 1 || e != 0 {
		t.Fatalf("got %v logs, %v errors, want 1, 0", l, e)
	}
	if err := erase(EraseLogs); err != nil {
		t.Fatal(err)
	}
	if l, e := count(); l != 0 || e != 0 {
		t.Fatalf("got %v logs, %v errors, want 0, 0", l, e)
	}
}

func TestWalkerContentsErrorPreservesSubtree(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	analyzeOnce(ctx, t, dbDir, root)
	lfs := localfs.New()

	sdb, err := openTestDB(ctx, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer sdb.Close(ctx)

	pt := &countingProgress{}
	w := &walker{root: root, db: sdb, fs: lfs, pt: pt, exclude: func(string) bool { return false }}
	a := NewAnalyzer(sdb, root, WithFS(lfs), WithProgress(pt))
	w.lsi = a.newAsyncStat(w)
	w.fw = filewalk.New(lfs, w)

	var state prefixState
	info, err := lfs.Lstat(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if stop, _, err := w.Prefix(ctx, &state, root, info, nil); stop || err != nil {
		t.Fatalf("Prefix = (%v, %v)", stop, err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	_, cerr := w.Contents(cctx, &state, root, []filewalk.Entry{{Name: "sub", Type: fs.ModeDir}, {Name: "f0"}})
	if cerr == nil {
		t.Fatal("expected Contents to return error when processStats fails")
	}
	if n := pt.numErrors(); n != 1 {
		t.Errorf("errors count = %d, want 1 (no double logging)", n)
	}

	_ = w.Done(ctx, &state, root, cerr)
	if n := pt.numErrors(); n != 1 {
		t.Errorf("errors count after Done = %d, want 1", n)
	}
	if err := sdb.Close(ctx); err != nil {
		t.Fatal(err)
	}

	assertSubtreePreserved(ctx, t, dbDir, root, sub)
}

func assertSubtreePreserved(ctx context.Context, t *testing.T, dbDir, root, sub string) {
	t.Helper()
	rodb, err := openDatabase(ctx, dbDir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer rodb.Close(ctx)
	var buf bytes.Buffer
	if err := rodb.Get(ctx, sub, &buf); err != nil || buf.Len() == 0 {
		t.Errorf("expected %s to be preserved in DB, got (%d bytes, %v)", sub, buf.Len(), err)
	}
	buf.Reset()
	if err := rodb.Get(ctx, root, &buf); err != nil || buf.Len() == 0 {
		t.Fatalf("expected %s in DB, got (%d bytes, %v)", root, buf.Len(), err)
	}
	var pi prefixinfo.T
	if err := pi.UnmarshalBinary(buf.Bytes()); err != nil || len(pi.InfoList()) != 2 {
		t.Errorf("expected %s InfoList len 2 preserved, got err=%v len=%d", root, err, len(pi.InfoList()))
	}
}

type fakeDeletePrefixDB struct {
	scanStore
	deleteErr error
}

func (f *fakeDeletePrefixDB) deletePrefix(context.Context, string) error {
	return f.deleteErr
}

func (f *fakeDeletePrefixDB) logError(context.Context, string, time.Time, []byte) error {
	return nil
}

func TestHandleDeletedOrChangedPrefixes(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	w := &walker{
		fs: localfs.New(),
		pt: &countingProgress{},
		db: &fakeDeletePrefixDB{deleteErr: errors.New("delete failed")},
	}

	dir1 := file.NewInfo("d1", 0, fs.ModeDir|0o755, now, file.XAttr{})
	dir2 := file.NewInfo("d2", 0, fs.ModeDir|0o755, now, file.XAttr{})
	prev := prefixinfo.New("/root", file.NewInfo("root", 0, fs.ModeDir|0o755, now, file.XAttr{}))
	prev.SetInfoList(file.InfoList{dir1})
	cur := prefixinfo.New("/root", file.NewInfo("root", 0, fs.ModeDir|0o755, now, file.XAttr{}))
	cur.SetInfoList(file.InfoList{dir1, dir2})

	n, unchanged, err := w.handleDeletedOrChangedPrefixes(ctx, "/root", true, cur, prev)
	if err != nil || n != 0 || unchanged {
		t.Errorf("added subdir with parentUnchanged=true: got (n=%d, unchanged=%v, err=%v), want (0, false, nil)", n, unchanged, err)
	}

	// When a dir is deleted and DeletePrefix fails, ndeleted must remain 0.
	n, unchanged, err = w.handleDeletedOrChangedPrefixes(ctx, "/root", true, prev, cur)
	if err == nil || unchanged || n != 0 {
		t.Errorf("failed DeletePrefix: got (n=%d, unchanged=%v, err=%v), want (0, false, non-nil)", n, unchanged, err)
	}
}

func openTestDB(ctx context.Context, path string, opts ...Option) (*DB, error) {
	raw, err := openDatabase(ctx, path, false)
	if err != nil {
		return nil, err
	}
	db, err := New(ctx, raw, opts...)
	if err != nil {
		_ = raw.Close(ctx)
		return nil, err
	}
	return db, nil
}
