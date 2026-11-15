// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/file/localfs"
)

// createHardlinkTree creates a tree with hard links to files in sibling
// directories whose names share a string prefix (a, a-b, ab).
func createHardlinkTree(t *testing.T, root string) {
	t.Helper()
	for _, d := range []string{"a", "ab", "a-b", "b/c"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, size := range map[string]int{"a/f1": 1000, "a/f2": 500, "ab/g": 300, "b/h": 200} {
		if err := os.WriteFile(filepath.Join(root, name), make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for link, target := range map[string]string{"ab/l1": "a/f1", "a-b/l2": "a/f1", "b/c/l3": "a/f2"} {
		if err := os.Link(filepath.Join(root, target), filepath.Join(root, link)); err != nil {
			t.Fatal(err)
		}
	}
}

func writeHardlinkConfig(t *testing.T, dir, root string, dedup config.HardlinkDedup) config.T {
	t.Helper()
	cfgFile := filepath.Join(dir, fmt.Sprintf("config-%v.yaml", dedup))
	yaml := fmt.Sprintf("filesystems:\n  - prefix: %s\n    database: %s\n    count_hardlinks_as_files: false\n    hardlink_dedup: %s\n",
		root, filepath.Join(dir, "db"), dedup)
	if err := os.WriteFile(cfgFile, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.ReadConfig(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func analyzeWithConfig(ctx context.Context, t *testing.T, cfg config.T, root string) {
	t.Helper()
	globalConfig = cfg
	alz := &analyzeCmd{}
	if err := alz.analyzeFS(ctx, localfs.New(), &analyzeFlags{}, []string{root}); err != nil {
		t.Fatal(err)
	}
}

type statsTotals struct {
	Files, Bytes, StorageBytes, Prefixes, Hardlinks, HardlinkDirs int64
	UserBytes, UserFiles                                          int64
}

func totalsFor(sdb *reports.AllStats) statsTotals {
	return statsTotals{
		Files:        sdb.Prefix.TotalFiles,
		Bytes:        sdb.Prefix.TotalBytes,
		StorageBytes: sdb.Prefix.TotalStorageBytes,
		Prefixes:     sdb.Prefix.TotalPrefixes,
		Hardlinks:    sdb.Prefix.TotalHardlinks,
		HardlinkDirs: sdb.Prefix.TotalHardlinkDirs,
		UserBytes:    sdb.ByUser.TotalBytes,
		UserFiles:    sdb.ByUser.TotalFiles,
	}
}

func hardlinkMatcher(ctx context.Context, t *testing.T) boolexpr.Matcher {
	t.Helper()
	fs := localfs.New()
	m, err := boolexpr.CreateMatcher(boolexpr.NewParser(ctx, fs),
		boolexpr.WithEmptyEntryValue(true),
		boolexpr.WithFilewalkFS(fs),
		boolexpr.WithHardlinkHandling(true))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// computeBoth computes stats for target using both the summaries and a
// rescan of all of the file metadata.
func computeBoth(ctx context.Context, t *testing.T, cfg config.T, target string) (fromSummaries *reports.AllStats, fromPrefixes *reports.AllStats, summaryErr error) {
	t.Helper()
	_, pcfg, _, db, err := internal.OpenPrefixAndDatabase(ctx, cfg, target, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	st := &statsCmds{}
	fromSummaries, summaryErr = st.computeFromSummaries(ctx, db, &pcfg, target, 10, false)
	fromPrefixes, err = st.computeFromPrefixes(ctx, db, &pcfg, hardlinkMatcher(ctx, t), target, 10, false)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func TestHardlinkDedupSummariesMatchRescan(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	root, err := filepath.EvalSymlinks(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	internal.LogDir = filepath.Join(root, "logs")
	if err := os.MkdirAll(internal.LogDir, 0o700); err != nil {
		t.Fatal(err)
	}
	root = filepath.Join(root, "tree")
	createHardlinkTree(t, root)

	verify := func(cfg config.T, rootFiles, rootHardlinks, aFiles int64) {
		t.Helper()
		for _, tc := range []struct {
			target           string
			files, hardlinks int64
			prefixes         int64
		}{
			{root, rootFiles, rootHardlinks, 6},
			// Siblings a-b and ab must not be included.
			{filepath.Join(root, "a"), aFiles, 0, 1},
		} {
			fromSummaries, fromPrefixes, err := computeBoth(ctx, t, cfg, tc.target)
			if err != nil {
				t.Fatalf("%v: computeFromSummaries: %v", tc.target, err)
			}
			got, want := totalsFor(fromSummaries), totalsFor(fromPrefixes)
			if got != want {
				t.Errorf("%v: summaries: %+v, rescan: %+v", tc.target, got, want)
			}
			// Both visit prefixes in the same order and hence attribute
			// hardlinks to the same prefixes.
			if g, w := fromSummaries.Prefix.Merge(20), fromPrefixes.Prefix.Merge(20); !reflect.DeepEqual(g, w) {
				t.Errorf("%v: per prefix: summaries: %+v, rescan: %+v", tc.target, g, w)
			}
			if got.Files != tc.files || got.Hardlinks != tc.hardlinks || got.Prefixes != tc.prefixes {
				t.Errorf("%v: got files=%v hardlinks=%v prefixes=%v, want %v %v %v",
					tc.target, got.Files, got.Hardlinks, got.Prefixes, tc.files, tc.hardlinks, tc.prefixes)
			}
		}
	}

	// Freshly analyzed, potential hardlinks are recorded using their link counts.
	summariesCfg := writeHardlinkConfig(t, tmpDir, root, config.HardlinkDedupSummaries)
	analyzeWithConfig(ctx, t, summariesCfg, root)
	// f1, f2, g, h are counted once, l1, l2, l3 are hardlinks.
	verify(summariesCfg, 4, 3, 2)

	// Create a hardlink in a, which is rescanned, to b/h, a file in b, which
	// is not rescanned since it is unchanged, and which had a link count of
	// 1 when last scanned. a/hl precedes b/h and is counted, b/h is now a
	// hardlink.
	if err := os.Link(filepath.Join(root, "b", "h"), filepath.Join(root, "a", "hl")); err != nil {
		t.Fatal(err)
	}
	analyzeWithConfig(ctx, t, summariesCfg, root)
	verify(summariesCfg, 4, 4, 3)

	// Switching to rescan rewrites the summaries without hardlink information.
	rescanCfg := writeHardlinkConfig(t, tmpDir, root, config.HardlinkDedupRescan)
	analyzeWithConfig(ctx, t, rescanCfg, root)
	if _, _, err := computeBoth(ctx, t, rescanCfg, root); !errors.Is(err, internal.ErrStaleSummaries) {
		t.Fatalf("expected %v when summaries lack hardlinks, got %v", internal.ErrStaleSummaries, err)
	}

	// Switching back without analyzing again is an error.
	if _, _, err := computeBoth(ctx, t, summariesCfg, root); !errors.Is(err, internal.ErrStaleSummaries) {
		t.Fatalf("expected %v before reanalyzing, got %v", internal.ErrStaleSummaries, err)
	}

	// Reanalyzing rewrites the summaries for the unchanged prefixes from the
	// stored metadata, which lacks link counts, and must yield the same results.
	analyzeWithConfig(ctx, t, summariesCfg, root)
	verify(summariesCfg, 4, 4, 3)
}
