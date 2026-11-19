// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/topn"
	"cloudeng.io/cmd/idu/stats"
)

type storedTopN struct {
	state    topn.State
	prefixes topn.Rankings
	users    map[int64]*topn.Rankings
}

func readTopN(ctx context.Context, t *testing.T, dbDir string) storedTopN {
	t.Helper()
	db, err := openTestDB(ctx, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	var st storedTopN
	var ok bool
	if st.state, ok, err = getTopNState(ctx, db.db); err != nil || !ok {
		t.Fatalf("state: %v %v", ok, err)
	}
	if ok, err := getRankings(ctx, db.db, topn.PrefixesKey, &st.prefixes); err != nil || !ok {
		t.Fatalf("prefixes: %v %v", ok, err)
	}
	st.users = map[int64]*topn.Rankings{}
	for _, uid := range st.state.Users {
		var r topn.Rankings
		if ok, err := getRankings(ctx, db.db, topn.UserKey(uid), &r); err != nil || !ok {
			t.Fatalf("user %v: %v %v", uid, ok, err)
		}
		st.users[uid] = &r
	}
	return st
}

func analyzeTopN(ctx context.Context, t *testing.T, dbDir, root string, dbOpts []Option, opts ...AnalyzeOption) {
	t.Helper()
	db, err := openTestDB(ctx, dbDir, dbOpts...)
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

func refreshTopNFor(ctx context.Context, t *testing.T, dbDir string, dbOpts []Option) {
	t.Helper()
	db, err := openTestDB(ctx, dbDir, dbOpts...)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	if err := db.RefreshTopN(ctx); err != nil {
		t.Fatal(err)
	}
}

func topKeys(r *topn.Rankings, m stats.Metric) []string {
	var keys []string
	for _, e := range r[m].Top(0) {
		keys = append(keys, e.Key)
	}
	return keys
}

// sameRankings checks that the incrementally maintained rankings match
// those computed from scratch.
func sameRankings(t *testing.T, incremental, refreshed storedTopN) {
	t.Helper()
	for _, m := range stats.Metrics() {
		if got, want := incremental.prefixes[m].Top(0), refreshed.prefixes[m].Top(0); !slices.Equal(got, want) {
			t.Errorf("%v: got %v, want %v", m, got, want)
		}
	}
	if got, want := incremental.state.Users, refreshed.state.Users; !slices.Equal(got, want) {
		t.Errorf("users: got %v, want %v", got, want)
	}
	for uid, r := range refreshed.users {
		if got, want := incremental.users[uid][stats.MetricBytes].Top(0), r[stats.MetricBytes].Top(0); !slices.Equal(got, want) {
			t.Errorf("user %v: got %v, want %v", uid, got, want)
		}
	}
}

func writeSized(t *testing.T, path string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestTopNIncremental(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	d := filepath.Join(sub, "d")
	writeSized(t, filepath.Join(d, "big"), 10000)
	dbOpts := []Option{WithTopN(10, 20, 5, 10)}

	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	st := readTopN(ctx, t, dbDir)
	if !st.state.Valid || st.state.N != 10 || st.state.Capacity != 20 || len(st.state.Users) != 1 {
		t.Errorf("state: %+v", st.state)
	}
	if got, want := topKeys(&st.prefixes, stats.MetricFiles), []string{root, sub, d}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got := topKeys(&st.prefixes, stats.MetricPrefixes); !slices.Equal(got, []string{root, sub}) {
		t.Errorf("got %v", got)
	}

	// Add a large file to root, which must become the top prefix by
	// file bytes, and delete d.
	writeSized(t, filepath.Join(root, "bigger"), 20000)
	if err := os.RemoveAll(d); err != nil {
		t.Fatal(err)
	}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	incremental := readTopN(ctx, t, dbDir)
	if !incremental.state.Valid {
		t.Errorf("state: %+v", incremental.state)
	}
	if got := topKeys(&incremental.prefixes, stats.MetricFiles); !slices.Equal(got, []string{root, sub}) {
		t.Errorf("got %v", got)
	}
	if top := incremental.prefixes[stats.MetricBytes].Top(1); len(top) != 1 || top[0].Key != root {
		t.Errorf("got %v", top)
	}
	refreshTopNFor(ctx, t, dbDir, dbOpts)
	sameRankings(t, incremental, readTopN(ctx, t, dbDir))
}

func TestTopNRefresh(t *testing.T) {
	ctx := context.Background()
	root, _, dbDir := createRootAndSub(t)
	var dirs []string
	for _, name := range []string{"a", "b", "c", "d"} {
		dirs = append(dirs, filepath.Join(root, name))
	}
	for i, d := range dirs {
		writeSized(t, filepath.Join(d, "f"), (i+1)*1000)
	}
	// Room for only the top 2 of the 6 prefixes.
	dbOpts := []Option{WithTopN(1, 2, 1, 2)}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	st := readTopN(ctx, t, dbDir)
	h := st.prefixes[stats.MetricBytes]
	if got := topKeys(&st.prefixes, stats.MetricBytes); !slices.Equal(got, []string{dirs[3], dirs[2]}) {
		t.Errorf("got %v", got)
	}
	if h.Threshold() == 0 || h.Certified() != 2 {
		t.Errorf("threshold %v, certified %v", h.Threshold(), h.Certified())
	}

	// Deleting the top two leaves the heap with no certified entries and
	// hence must trigger a refresh.
	for _, d := range dirs[2:] {
		if err := os.RemoveAll(d); err != nil {
			t.Fatal(err)
		}
	}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	st = readTopN(ctx, t, dbDir)
	if got := topKeys(&st.prefixes, stats.MetricBytes); !slices.Equal(got, []string{dirs[1], dirs[0]}) {
		t.Errorf("got %v", got)
	}
	if !st.state.Valid || st.prefixes[stats.MetricBytes].Certified() != 2 {
		t.Errorf("state %+v, certified %v", st.state, st.prefixes[stats.MetricBytes].Certified())
	}
}

func TestTopNConfigChange(t *testing.T) {
	ctx := context.Background()
	root, _, dbDir := createRootAndSub(t)
	analyzeTopN(ctx, t, dbDir, root, []Option{WithTopN(10, 20, 5, 10)})
	analyzeTopN(ctx, t, dbDir, root, []Option{WithTopN(3, 3, 2, 2)})
	st := readTopN(ctx, t, dbDir)
	if st.state.N != 3 || st.state.Capacity != 3 || st.prefixes[stats.MetricBytes].Capacity() != 3 {
		t.Errorf("state %+v", st.state)
	}
	for uid, r := range st.users {
		if r[stats.MetricBytes].Capacity() != 2 {
			t.Errorf("user %v: capacity %v", uid, r[stats.MetricBytes].Capacity())
		}
	}
}

func TestTopNFailedScan(t *testing.T) {
	ctx := context.Background()
	root, _, dbDir := createRootAndSub(t)
	dbOpts := []Option{WithTopN(10, 20, 5, 10)}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)

	db, err := openTestDB(ctx, dbDir, dbOpts...)
	if err != nil {
		t.Fatal(err)
	}
	obs := newTopNObserver(db)
	if err := obs.ScanStarted(ctx, ScanInfo{Root: root, Target: root}); err != nil {
		t.Fatal(err)
	}
	if obs.refresh {
		t.Errorf("unexpected refresh")
	}
	if err := obs.ScanDone(ctx, errors.New("interrupted")); err != nil {
		t.Fatal(err)
	}
	if s, ok, err := getTopNState(ctx, db.db); err != nil || !ok || s.Valid {
		t.Errorf("state: %+v %v %v", s, ok, err)
	}
	// The next analysis must refresh the rankings.
	obs = newTopNObserver(db)
	if err := obs.ScanStarted(ctx, ScanInfo{Root: root, Target: root}); err != nil {
		t.Fatal(err)
	}
	if !obs.refresh {
		t.Errorf("expected refresh")
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}

	db, err = openTestDB(ctx, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	if err := db.RefreshTopN(ctx); !errors.Is(err, ErrTopNDisabled) {
		t.Errorf("got %v, want %v", err, ErrTopNDisabled)
	}
}

// injectSentinel adds an entry for a nonexistent prefix to the stored
// prefix rankings; it is only removed by a refresh.
func injectSentinel(ctx context.Context, t *testing.T, dbDir string, dbOpts []Option, corrupt string) {
	t.Helper()
	db, err := openTestDB(ctx, dbDir, dbOpts...)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	r := topn.NewRankings(0)
	if _, err := getRankings(ctx, db.db, topn.PrefixesKey, r); err != nil {
		t.Fatal(err)
	}
	r.Set("/sentinel", stats.Totals{Files: 1 << 40})
	if err := db.db.SetMeta(ctx, topn.PrefixesKey, r.AppendBinary(nil)); err != nil {
		t.Fatal(err)
	}
	if corrupt != "" {
		exactKey, _ := stats.SummaryKeys(db.sep, corrupt)
		if err := db.db.SetSummary(ctx, exactKey, []byte{0xff}, false); err != nil {
			t.Fatal(err)
		}
	}
}

func hasSentinel(st storedTopN) bool {
	_, ok := st.prefixes[stats.MetricFiles].Value("/sentinel")
	return ok
}

func TestTopNUnreadablePreviousSummary(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	dbOpts := []Option{WithTopN(10, 20, 5, 10)}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)

	// An incremental analysis retains the sentinel.
	injectSentinel(ctx, t, dbDir, dbOpts, "")
	writeSized(t, filepath.Join(sub, "f2"), 10)
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	if st := readTopN(ctx, t, dbDir); !hasSentinel(st) || !st.state.Valid {
		t.Fatalf("sentinel missing or invalid state: %+v", st.state)
	}

	// An unreadable previous summary forces a refresh.
	injectSentinel(ctx, t, dbDir, dbOpts, sub)
	writeSized(t, filepath.Join(sub, "f3"), 10)
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	st := readTopN(ctx, t, dbDir)
	if hasSentinel(st) || !st.state.Valid {
		t.Errorf("sentinel present or invalid state: %+v", st.state)
	}
	if got := topKeys(&st.prefixes, stats.MetricFiles); !slices.Equal(got, []string{sub, root}) {
		t.Errorf("got %v", got)
	}
}

func TestRefreshTopNFailureInvalidatesState(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	dbOpts := []Option{WithTopN(10, 20, 5, 10)}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)

	// Corrupt a summary so RefreshTopN fails mid-refresh.
	db, err := openTestDB(ctx, dbDir, dbOpts...)
	if err != nil {
		t.Fatal(err)
	}
	exactKey, _ := stats.SummaryKeys(db.sep, sub)
	if err := db.db.SetSummary(ctx, exactKey, []byte{0xff}, false); err != nil {
		t.Fatal(err)
	}
	if err := db.RefreshTopN(ctx); err == nil {
		t.Fatalf("expected RefreshTopN error for corrupt summary")
	}
	st, ok, err := getTopNState(ctx, db.db)
	_ = db.Close(ctx)
	if err != nil || !ok || st.Valid {
		t.Errorf("expected Valid=false after failed RefreshTopN, got %+v ok=%v err=%v", st, ok, err)
	}
}
