// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"errors"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"cloudeng.io/cmd/idu/stats"
)

type change struct {
	old, new *stats.Totals
}

type deletion struct {
	path  string
	paths []string
	files int64
}

type recordingObserver struct {
	mu        sync.Mutex
	started   []ScanInfo
	changes   map[string]change
	deletions []deletion
	done      []error
	startErr  error
}

func (r *recordingObserver) ScanStarted(_ context.Context, scan ScanInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started = append(r.started, scan)
	r.changes = map[string]change{}
	r.deletions = nil
	return r.startErr
}

func (r *recordingObserver) PrefixChanged(_ context.Context, path string, old, cur *stats.PrefixSummary) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var c change
	if old != nil {
		o := old.Direct
		c.old = &o
	}
	n := cur.Direct
	c.new = &n
	r.changes[path] = c
}

func (r *recordingObserver) SubtreeDeleted(_ context.Context, path string, summaries iter.Seq2[string, *stats.PrefixSummary]) {
	d := deletion{path: path}
	for p, ps := range summaries {
		d.paths = append(d.paths, p)
		d.files += ps.Direct.Files
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deletions = append(r.deletions, d)
}

func (r *recordingObserver) ScanDone(_ context.Context, scanErr error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done = append(r.done, scanErr)
	return nil
}

func (r *recordingObserver) changed() []string {
	var paths []string
	for p := range r.changes {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	return paths
}

func mkfile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestObserver(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	d, e := filepath.Join(sub, "d"), filepath.Join(sub, "d", "e")
	mkfile(t, filepath.Join(d, "f2"))
	mkfile(t, filepath.Join(e, "f3"))
	obs := &recordingObserver{}

	analyzeOnce(ctx, t, dbDir, root, WithObserver(obs))
	checkInitialChanges(t, obs, root, sub, d, e)

	// Add a file to sub, its old summary must be reported.
	mkfile(t, filepath.Join(sub, "f4"))
	analyzeOnce(ctx, t, dbDir, root, WithObserver(obs))
	checkAddedFile(t, obs, sub, d, e)

	// Delete d and hence e.
	if err := os.RemoveAll(d); err != nil {
		t.Fatal(err)
	}
	analyzeOnce(ctx, t, dbDir, root, WithObserver(obs))
	checkDeletedSubtree(t, obs, sub, d, e)
	if got := readSummaryPaths(ctx, t, dbDir, root); !slices.Equal(got, []string{root, sub}) {
		t.Errorf("summaries: %v", got)
	}
}

// checkInitialChanges checks that every prefix is reported as new.
func checkInitialChanges(t *testing.T, obs *recordingObserver, root, sub, d, e string) {
	t.Helper()
	if want := []ScanInfo{{Root: root, Target: root}}; !slices.Equal(obs.started, want) {
		t.Errorf("got %v, want %v", obs.started, want)
	}
	if got, want := obs.changed(), []string{root, sub, d, e}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	for p, c := range obs.changes {
		if c.old != nil || c.new.Files != 1 {
			t.Errorf("%v: %+v %+v", p, c.old, c.new)
		}
	}
	if len(obs.deletions) != 0 || len(obs.done) != 1 || obs.done[0] != nil {
		t.Errorf("deletions %v, done %v", obs.deletions, obs.done)
	}
}

func checkAddedFile(t *testing.T, obs *recordingObserver, sub string, unchanged ...string) {
	t.Helper()
	c, ok := obs.changes[sub]
	if !ok || c.old == nil || c.old.Files != 1 || c.new.Files != 2 {
		t.Errorf("sub: %v %+v", ok, c)
	}
	for _, p := range unchanged {
		if _, ok := obs.changes[p]; ok {
			t.Errorf("%v: unexpectedly changed", p)
		}
	}
}

func checkDeletedSubtree(t *testing.T, obs *recordingObserver, sub, d, e string) {
	t.Helper()
	want := deletion{path: d, paths: []string{d, e}, files: 2}
	if len(obs.deletions) != 1 || obs.deletions[0].path != want.path ||
		!slices.Equal(obs.deletions[0].paths, want.paths) || obs.deletions[0].files != want.files {
		t.Errorf("got %+v, want %+v", obs.deletions, want)
	}
	if c, ok := obs.changes[sub]; !ok || c.old == nil || c.new.SubPrefixes != 0 || c.old.SubPrefixes != 1 {
		t.Errorf("sub: %v %+v", ok, c)
	}
	if len(obs.done) != 3 || obs.done[2] != nil {
		t.Errorf("done: %v", obs.done)
	}
}

func readSummaryPaths(ctx context.Context, t *testing.T, dbDir, root string) []string {
	t.Helper()
	db, err := openTestDB(ctx, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	sep := string(filepath.Separator)
	exactKey, childPrefix := stats.SummaryKeys(sep, root)
	var paths []string
	if err := db.db.ScanSummaries(ctx, exactKey, childPrefix, func(_ context.Context, key string, _ []byte) bool {
		paths = append(paths, stats.DecodeSummaryKey(sep, key))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return paths
}

func TestObserverRewriteAndStartError(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	analyzeOnce(ctx, t, dbDir, root)

	// A configuration change rewrites every summary.
	obs := &recordingObserver{}
	db, err := openTestDB(ctx, dbDir, WithRecordHardlinks(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	if err := NewAnalyzer(db, root, WithObserver(obs)).Analyze(ctx, root); err != nil {
		t.Fatal(err)
	}
	if len(obs.started) != 1 || !obs.started[0].Rewrite {
		t.Errorf("started: %v", obs.started)
	}
	if got, want := obs.changed(), []string{root, sub}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	for p, c := range obs.changes {
		if c.old == nil {
			t.Errorf("%v: missing old summary", p)
		}
	}

	// An error from ScanStarted prevents the analysis.
	obs.startErr = errors.New("oops")
	if err := NewAnalyzer(db, root, WithObserver(obs)).Analyze(ctx, root); !errors.Is(err, obs.startErr) {
		t.Errorf("got %v, want %v", err, obs.startErr)
	}
	if len(obs.changes) != 0 || len(obs.done) != 1 {
		t.Errorf("changes %v, done %v", obs.changes, obs.done)
	}
	if db.observer != nil {
		t.Errorf("observer not cleared")
	}
}
