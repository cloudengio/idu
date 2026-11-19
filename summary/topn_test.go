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
