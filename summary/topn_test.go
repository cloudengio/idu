// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package summary_test

import (
	"context"
	"errors"
	"iter"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmd/idu/summary"
)

func rankedPaths(tn summary.TopN) []string {
	var paths []string
	for _, e := range tn.Entries {
		paths = append(paths, e.Path)
	}
	return paths
}

func TestTopPrefixes(t *testing.T) {
	ctx := context.Background()
	root, cfg := createTree(t, false)
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)

	tn, err := sr.TopPrefixes(ctx, stats.MetricPrefixes, 0)
	if err != nil {
		t.Fatal(err)
	}
	a := filepath.Join(root, "a")
	if got, want := rankedPaths(tn), []string{root, a}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if tn.Certified != 2 || tn.N != iduconfig.DefaultTopN || tn.Entries[0].Value != 2 {
		t.Errorf("got %+v", tn)
	}

	tn, err = sr.TopPrefixes(ctx, stats.MetricBytes, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := rankedPaths(tn), []string{filepath.Join(root, "c")}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	if _, err := sr.TopPrefixes(ctx, stats.Metric(100), 1); err == nil {
		t.Errorf("expected an error for an invalid metric")
	}
}

func TestTopPrefixesForIDs(t *testing.T) {
	ctx := context.Background()
	root, cfg := createTree(t, false)
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)
	a := filepath.Join(root, "a")

	users, groups, err := sr.TopNIDs(ctx)
	if err != nil || len(users) != 1 || len(groups) != 1 {
		t.Fatalf("got %v %v %v", users, groups, err)
	}
	if runtime.GOOS != "windows" && users[0] != int64(os.Getuid()) {
		t.Errorf("got %v, want %v", users[0], os.Getuid())
	}
	tn, err := sr.TopPrefixesForUser(ctx, users[0], stats.MetricPrefixes, 0)
	if err != nil || tn.N != iduconfig.DefaultTopNPerID {
		t.Fatalf("got %+v %v", tn, err)
	}
	if got, want := rankedPaths(tn), []string{root, a}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	tn, err = sr.TopPrefixesForGroup(ctx, groups[0], stats.MetricFiles, 0)
	if err != nil || len(tn.Entries) != 4 {
		t.Errorf("got %+v %v", tn, err)
	}
	if tn, err := sr.TopPrefixesForUser(ctx, users[0]+1, stats.MetricBytes, 0); err != nil || len(tn.Entries) != 0 {
		t.Errorf("got %+v %v", tn, err)
	}
}

type failStartObserver struct{}

func (failStartObserver) ScanStarted(context.Context, idudb.ScanInfo) error {
	return errors.New("abort scan")
}
func (failStartObserver) PrefixChanged(context.Context, string, *stats.PrefixSummary, *stats.PrefixSummary) {
}
func (failStartObserver) SubtreeDeleted(context.Context, string, iter.Seq2[string, *stats.PrefixSummary]) {
}
func (failStartObserver) ScanDone(context.Context, error) error { return nil }

func TestTopPrefixesUnavailable(t *testing.T) {
	ctx := context.Background()
	root, cfg := createTree(t, false)
	cfg.TopN = -1 // disables the rankings, normally rejected by Validate
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)
	if _, err := sr.TopPrefixes(ctx, stats.MetricBytes, 1); !errors.Is(err, summary.ErrNoTopN) {
		t.Errorf("got %v, want %v", err, summary.ErrNoTopN)
	}

	// An interrupted analysis leaves top-N rankings marked invalid.
	root2, cfg2 := createTree(t, false)
	db, err := idudb.OpenWritable(ctx, cfg2, []idudb.OpenOption{withBadger})
	if err != nil {
		t.Fatal(err)
	}
	if err := idudb.NewAnalyzer(db, root2, idudb.WithObserver(failStartObserver{})).Analyze(ctx, root2); err == nil {
		t.Fatal("expected error from failStartObserver")
	}
	if err := db.LogAndClose(ctx, time.Now(), time.Now(), []byte("{}")); err != nil {
		t.Fatal(err)
	}
	sr2, err := summary.Open(ctx, cfg2, withBadger)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sr2.Close(ctx) }()
	if _, err := sr2.TopPrefixes(ctx, stats.MetricBytes, 1); !errors.Is(err, summary.ErrStaleTopN) {
		t.Errorf("got %v, want %v", err, summary.ErrStaleTopN)
	}
}

func TestTopSubtrees(t *testing.T) {
	ctx := context.Background()
	root, cfg := createTree(t, false)
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)
	a, b, c := filepath.Join(root, "a"), filepath.Join(root, "a", "b"), filepath.Join(root, "c")

	tn, err := sr.TopSubtrees(ctx, stats.MetricFiles, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := rankedPaths(tn), []string{root, a, b, c}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if tn.Entries[0].Value != 4 || tn.Entries[1].Value != 2 || tn.Certified != 4 {
		t.Errorf("got %+v", tn)
	}

	for _, tc := range []struct {
		path string
		n    int
		want []string
	}{
		{root, 10, []string{root, a, b, c}},
		{root, 2, []string{root, a}},
		{a, 10, []string{a, b}},
		{b, 10, []string{b}},
		{filepath.Join(root, "missing"), 10, nil},
		{root, 0, nil},
	} {
		within, err := sr.TopSubtreesWithin(ctx, tc.path, stats.MetricFiles, tc.n)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, e := range within {
			got = append(got, e.Path)
			tot, ok, err := sr.Subtree(ctx, e.Path)
			if err != nil || !ok || tot.Files != e.Value {
				t.Errorf("%v: got %+v %v %v, want %v files", e.Path, tot, ok, err, e.Value)
			}
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("%v %v: got %v, want %v", tc.path, tc.n, got, tc.want)
		}
	}
}

// The best-first search and the stored ranking must agree.
func TestTopSubtreesAgree(t *testing.T) {
	ctx := context.Background()
	root, cfg := createTree(t, false)
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)
	within, err := sr.TopSubtreesWithin(ctx, root, stats.MetricBytes, 10)
	if err != nil {
		t.Fatal(err)
	}
	tn, err := sr.TopSubtrees(ctx, stats.MetricBytes, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(within, tn.Entries) {
		t.Errorf("got %v, want %v", within, tn.Entries)
	}
}

func TestTopIDs(t *testing.T) {
	ctx := context.Background()
	root, cfg := createTree(t, false)
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)
	users, err := sr.TopUsers(ctx, stats.MetricFiles, 0)
	if err != nil || len(users) != 1 || users[0].Value != 4 || users[0].Totals.Files != 4 {
		t.Fatalf("got %+v %v", users, err)
	}
	if runtime.GOOS != "windows" && users[0].ID != int64(os.Getuid()) {
		t.Errorf("got %v, want %v", users[0].ID, os.Getuid())
	}
	groups, err := sr.TopGroups(ctx, stats.MetricFiles, 1)
	if err != nil || len(groups) != 1 || groups[0].Value != 4 {
		t.Errorf("got %+v %v", groups, err)
	}
	all, found, err := sr.Subtree(ctx, root)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	if users[0].Totals.Bytes != all.Bytes {
		t.Errorf("got %v, want %v", users[0].Totals.Bytes, all.Bytes)
	}
}

func TestSubtreesUnavailable(t *testing.T) {
	ctx := context.Background()
	root, cfg := createTree(t, false)
	cfg.TopN = -1 // disables the derived state, normally rejected by Validate
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)
	if _, _, err := sr.Subtree(ctx, root); !errors.Is(err, idudb.ErrStaleSubtrees) {
		t.Errorf("got %v, want %v", err, idudb.ErrStaleSubtrees)
	}
	if _, err := sr.TopSubtreesWithin(ctx, root, stats.MetricBytes, 1); !errors.Is(err, idudb.ErrStaleSubtrees) {
		t.Errorf("got %v, want %v", err, idudb.ErrStaleSubtrees)
	}
	if _, err := sr.TopUsers(ctx, stats.MetricBytes, 1); !errors.Is(err, summary.ErrNoTopN) {
		t.Errorf("got %v, want %v", err, summary.ErrNoTopN)
	}

	// Missing SubtreesKey on a database with valid State must return ErrNoTopN.
	root2, cfg2 := createTree(t, false)
	analyze(ctx, t, root2, cfg2)
	raw, err := idudb.OpenBackend(ctx, cfg2.Database, false, withBadger)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.SetMeta(ctx, "topn/v2/subtrees", nil); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(ctx); err != nil {
		t.Fatal(err)
	}
	sr2, err := summary.Open(ctx, cfg2, withBadger)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sr2.Close(ctx) }()
	if _, err := sr2.TopSubtrees(ctx, stats.MetricBytes, 1); !errors.Is(err, summary.ErrNoTopN) {
		t.Errorf("got %v, want %v", err, summary.ErrNoTopN)
	}
}
