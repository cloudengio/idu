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

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/cmd/idu/stats"
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

func writeHardlinkConfig(t *testing.T, dir, root string, dedup iduconfig.HardlinkDedup) iduconfig.T {
	t.Helper()
	cfgFile := filepath.Join(dir, fmt.Sprintf("config-%v.yaml", dedup))
	yaml := fmt.Sprintf("filesystems:\n  - prefix: %s\n    database: %s\n    count_hardlinks_as_files: false\n    hardlink_dedup: %s\n",
		root, filepath.Join(dir, "db"), dedup)
	if err := os.WriteFile(cfgFile, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := iduconfig.ReadConfig(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func analyzeWithConfig(ctx context.Context, t *testing.T, cfg iduconfig.T, root string) {
	t.Helper()
	globalConfig = cfg
	alz := &analyzeCmd{}
	if err := alz.analyzeFS(ctx, localfs.New(), &analyzeFlags{}, []string{root}); err != nil {
		t.Fatal(err)
	}
}

type statsTotals struct {
	Files, Bytes, StorageBytes, Prefixes, Hardlinks, HardlinkDirs int64
	SubtreeFiles, SubtreeBytes, UserBytes, UserFiles              int64
	SparseFiles, SparseBytes, SubtreeSparse, UserSparse           int64
}

func totalsFor(sdb *reports.AllStats) statsTotals {
	return statsTotals{
		Files:         sdb.Prefix.TotalFiles,
		Bytes:         sdb.Prefix.TotalBytes,
		StorageBytes:  sdb.Prefix.TotalStorageBytes,
		Prefixes:      sdb.Prefix.TotalPrefixes,
		Hardlinks:     sdb.Prefix.TotalHardlinks,
		HardlinkDirs:  sdb.Prefix.TotalHardlinkDirs,
		SubtreeFiles:  sdb.Subtree.TotalFiles,
		SubtreeBytes:  sdb.Subtree.TotalBytes,
		UserBytes:     sdb.ByUser.TotalBytes,
		UserFiles:     sdb.ByUser.TotalFiles,
		SparseFiles:   sdb.Prefix.TotalSparseFiles,
		SparseBytes:   sdb.Prefix.TotalSparseBytes,
		SubtreeSparse: sdb.Subtree.TotalSparseBytes,
		UserSparse:    sdb.ByUser.TotalSparseBytes,
	}
}

func hardlinkMatcher(ctx context.Context, t testing.TB) boolexpr.Matcher {
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
func computeBoth(ctx context.Context, t *testing.T, cfg iduconfig.T, target string) (fromSummaries *reports.AllStats, fromPrefixes *reports.AllStats, summaryErr error) {
	t.Helper()
	_, pcfg, _, db, err := openPrefixReadOnly(ctx, cfg, target)
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

	verify := func(cfg iduconfig.T, rootFiles, rootHardlinks, aFiles int64) {
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
			if g, w := fromSummaries.Subtree.Merge(20), fromPrefixes.Subtree.Merge(20); !reflect.DeepEqual(g, w) {
				t.Errorf("%v: per subtree: summaries: %+v, rescan: %+v", tc.target, g, w)
			}
			if got.Files != tc.files || got.Hardlinks != tc.hardlinks || got.Prefixes != tc.prefixes {
				t.Errorf("%v: got files=%v hardlinks=%v prefixes=%v, want %v %v %v",
					tc.target, got.Files, got.Hardlinks, got.Prefixes, tc.files, tc.hardlinks, tc.prefixes)
			}
		}
	}

	// Freshly analyzed, potential hardlinks are recorded using their link counts.
	summariesCfg := writeHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
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
	rescanCfg := writeHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupRescan)
	analyzeWithConfig(ctx, t, rescanCfg, root)
	if _, _, err := computeBoth(ctx, t, rescanCfg, root); !errors.Is(err, idudb.ErrStaleSummaries) {
		t.Fatalf("expected %v when summaries lack hardlinks, got %v", idudb.ErrStaleSummaries, err)
	}

	// Switching back without analyzing again is an error.
	if _, _, err := computeBoth(ctx, t, summariesCfg, root); !errors.Is(err, idudb.ErrStaleSummaries) {
		t.Fatalf("expected %v before reanalyzing, got %v", idudb.ErrStaleSummaries, err)
	}

	// Reanalyzing rewrites the summaries for the unchanged prefixes from the
	// stored metadata, which lacks link counts, and must yield the same results.
	analyzeWithConfig(ctx, t, summariesCfg, root)
	verify(summariesCfg, 4, 4, 3)
}

// createSparseFile creates a file of size bytes with a hole after its
// first byte and returns the number of its bytes that are expected to be
// reported as sparse, which will be zero if the filesystem does not
// support sparse files.
func createSparseFile(ctx context.Context, t *testing.T, name string, size int64) int64 {
	t.Helper()
	f, err := os.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	fs := localfs.New()
	fi, err := fs.Lstat(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	xattr, err := fs.XAttr(ctx, name, fi)
	if err != nil {
		t.Fatal(err)
	}
	return stats.SparseBytes(fi.Size(), xattr.Blocks)
}

// createSparseTree creates a tree containing a sparse file, a dense file
// and a hardlink to the sparse file in a sibling directory, and returns
// its root and the number of the sparse file's bytes that are expected to
// be reported as sparse.
func createSparseTree(ctx context.Context, t *testing.T, dir string) (string, int64) {
	t.Helper()
	root, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	internal.LogDir = filepath.Join(root, "logs")
	if err := os.MkdirAll(internal.LogDir, 0o700); err != nil {
		t.Fatal(err)
	}
	root = filepath.Join(root, "tree")
	for _, d := range []string{"s", "t"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	sparse := createSparseFile(ctx, t, filepath.Join(root, "s", "sparse"), 64<<20)
	if err := os.WriteFile(filepath.Join(root, "s", "dense"), make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	// A hardlink to the sparse file must only be counted once.
	if err := os.Link(filepath.Join(root, "s", "sparse"), filepath.Join(root, "t", "link")); err != nil {
		t.Fatal(err)
	}
	return root, sparse
}

func TestSparseFilesSummariesMatchRescan(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	root, sparse := createSparseTree(ctx, t, tmpDir)
	wantFiles := int64(0)
	if sparse > 0 {
		wantFiles = 1
	}

	cfg := writeHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
	analyzeWithConfig(ctx, t, cfg, root)
	fromSummaries, fromPrefixes, err := computeBoth(ctx, t, cfg, root)
	if err != nil {
		t.Fatal(err)
	}
	got, want := totalsFor(fromSummaries), totalsFor(fromPrefixes)
	if got != want {
		t.Errorf("summaries: %+v, rescan: %+v", got, want)
	}
	if got.SparseFiles != wantFiles || got.SparseBytes != sparse || got.SubtreeSparse != sparse || got.UserSparse != sparse {
		t.Errorf("got sparse files=%v bytes=%v subtree=%v user=%v, want %v %v",
			got.SparseFiles, got.SparseBytes, got.SubtreeSparse, got.UserSparse, wantFiles, sparse)
	}
	merged := fromSummaries.Prefix.Merge(20)
	if got := merged[filepath.Join(root, "s")].SparseBytes; got != sparse {
		t.Errorf("got %v sparse bytes for s, want %v", got, sparse)
	}
	if got := merged[filepath.Join(root, "t")].SparseBytes; got != 0 {
		t.Errorf("got %v sparse bytes for t, want 0", got)
	}
}
