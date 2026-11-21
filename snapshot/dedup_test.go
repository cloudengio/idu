// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmd/idu/summary"
	"cloudeng.io/file/localfs"
)

func analyzeWithConfig(ctx context.Context, t *testing.T, cfg iduconfig.T, root string) {
	t.Helper()
	analyzeWithFlags(ctx, t, cfg, root, &analyze.Flags{})
}

func analyzeWithFlags(ctx context.Context, t *testing.T, cfg iduconfig.T, root string, af *analyze.Flags) {
	t.Helper()
	testEnv.Config = cfg
	alz := analyze.New(testEnv)
	if err := alz.Analyze(ctx, af, []string{root}); err != nil {
		t.Fatal(err)
	}
}

type statsTotals struct {
	Files, Bytes, StorageBytes, Prefixes, Hardlinks, HardlinkDirs int64
	SubtreeFiles, SubtreeBytes, UserBytes, UserFiles              int64
	SparseFiles, SparseBytes, SubtreeSparse, UserSparse           int64
}

func totalsFor(sdb *report.AllStats) statsTotals {
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

func hardlinkMatcher(ctx context.Context, t testing.TB) stats.Matcher {
	t.Helper()
	fs := localfs.New()
	m, err := stats.NewMatcher(stats.NewParser(ctx, fs),
		stats.WithEmptyValue(true),
		stats.WithFilewalkFS(fs),
		stats.WithHardlinkHandling(true))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// computeBoth computes stats for target using both the summaries and a
// rescan of all of the file metadata.
func computeBoth(ctx context.Context, t *testing.T, cfg iduconfig.T, target string) (fromSummaries *report.AllStats, fromPrefixes *report.AllStats, summaryErr error) {
	t.Helper()
	_, pcfg, _, db, err := openPrefixReadOnly(ctx, cfg, target)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	st := Computer{}
	fromSummaries, summaryErr = st.FromSummaries(ctx, db, pcfg, target, 10)
	fromPrefixes, err = st.FromPrefixes(ctx, db, pcfg, hardlinkMatcher(ctx, t), target, 10)
	if err != nil {
		t.Fatal(err)
	}
	return
}

// verifyHardlinkTree verifies the stats for the tree created by
// createHardlinkTree, and for its prefix a, computed from the summaries.
// If exact is true they must match those computed by a rescan.
func verifyHardlinkTree(ctx context.Context, t *testing.T, cfg iduconfig.T, root string, exact bool, rootFiles, rootHardlinks, aFiles int64) {
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
		if exact {
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
		}
		if tc.target == root {
			checkStoredSubtree(ctx, t, cfg, root, fromSummaries)
		}
		if got.Files != tc.files || got.Hardlinks != tc.hardlinks || got.Prefixes != tc.prefixes {
			t.Errorf("%v: got files=%v hardlinks=%v prefixes=%v, want %v %v %v",
				tc.target, got.Files, got.Hardlinks, got.Prefixes, tc.files, tc.hardlinks, tc.prefixes)
		}
	}
}

// checkStoredSubtree checks that the recursive totals for root maintained
// by analyze deduplicate hardlinks in the same way as computing them from
// the summaries.
func checkStoredSubtree(ctx context.Context, t *testing.T, cfg iduconfig.T, root string, fromSummaries *report.AllStats) {
	t.Helper()
	_, pcfg, _, db, err := openPrefixReadOnly(ctx, cfg, root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	stored, ok, err := summary.New(db, pcfg).Subtree(ctx, root)
	if err != nil || !ok {
		t.Fatalf("%v: %v %v", root, ok, err)
	}
	want := fromSummaries.Subtree
	if stored.Files != want.TotalFiles || stored.Bytes != want.TotalBytes || stored.Hardlinks != want.TotalHardlinks {
		t.Errorf("%v: stored %+v, from summaries: files %v, bytes %v, hardlinks %v", root, stored, want.TotalFiles, want.TotalBytes, want.TotalHardlinks)
	}
}

func TestHardlinkDedupSummariesMatchRescan(t *testing.T) {
	ctx := context.Background()
	tmpDir, root := clitest.NewHardlinkTree(t)

	// Freshly analyzed, potential hardlinks are recorded using their link counts.
	summariesCfg := clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
	analyzeWithConfig(ctx, t, summariesCfg, root)
	// f1, f2, g, h are counted once, l1, l2, l3 are hardlinks.
	verifyHardlinkTree(ctx, t, summariesCfg, root, true, 4, 3, 2)

	// The link count stored for b/h is stale, it is not recorded and hence
	// is counted twice: a/hl and b/h are both counted.
	clitest.LinkUnchanged(t, root)
	analyzeWithConfig(ctx, t, summariesCfg, root)
	verifyHardlinkTree(ctx, t, summariesCfg, root, false, 5, 3, 3)

	// Forcing a reanalysis refreshes the link counts.
	analyzeWithFlags(ctx, t, summariesCfg, root, &analyze.Flags{Force: true})
	verifyHardlinkTree(ctx, t, summariesCfg, root, true, 4, 4, 3)

	// Switching to rescan rewrites the summaries without hardlink information.
	rescanCfg := clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupRescan)
	analyzeWithConfig(ctx, t, rescanCfg, root)
	if _, _, err := computeBoth(ctx, t, rescanCfg, root); !errors.Is(err, idudb.ErrStaleSummaries) {
		t.Fatalf("expected %v when summaries lack hardlinks, got %v", idudb.ErrStaleSummaries, err)
	}

	// Switching back without analyzing again is an error.
	if _, _, err := computeBoth(ctx, t, summariesCfg, root); !errors.Is(err, idudb.ErrStaleSummaries) {
		t.Fatalf("expected %v before reanalyzing, got %v", idudb.ErrStaleSummaries, err)
	}

	// Reanalyzing rewrites the summaries for the unchanged prefixes from the
	// stored metadata, and must yield the same results.
	analyzeWithConfig(ctx, t, summariesCfg, root)
	verifyHardlinkTree(ctx, t, summariesCfg, root, true, 4, 4, 3)
}

func TestHardlinkDedupSummariesAllMatchRescan(t *testing.T) {
	ctx := context.Background()
	tmpDir, root := clitest.NewHardlinkTree(t)

	allCfg := clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummariesAll)
	analyzeWithConfig(ctx, t, allCfg, root)
	verifyHardlinkTree(ctx, t, allCfg, root, true, 4, 3, 2)

	// Every file is recorded, so the stale link count of b/h does not matter.
	clitest.LinkUnchanged(t, root)
	analyzeWithConfig(ctx, t, allCfg, root)
	verifyHardlinkTree(ctx, t, allCfg, root, true, 4, 4, 3)

	// Switching from summaries to summaries-all requires reanalyzing.
	summariesCfg := clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
	analyzeWithConfig(ctx, t, summariesCfg, root)
	verifyHardlinkTree(ctx, t, summariesCfg, root, false, 5, 3, 3)
	if _, _, err := computeBoth(ctx, t, allCfg, root); !errors.Is(err, idudb.ErrStaleSummaries) {
		t.Fatalf("expected %v before reanalyzing with summaries-all, got %v", idudb.ErrStaleSummaries, err)
	}
	analyzeWithConfig(ctx, t, allCfg, root)
	verifyHardlinkTree(ctx, t, allCfg, root, true, 4, 4, 3)
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

	cfg := clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
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
