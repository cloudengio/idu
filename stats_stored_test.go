// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/report"
)

// computeStoredAndSummaries computes stats for target from the stored
// state and from the summaries.
func computeStoredAndSummaries(ctx context.Context, t *testing.T, cfg iduconfig.T, target string, n int) (stored, fromSummaries *report.AllStats, storedErr error) {
	t.Helper()
	_, pcfg, _, db, err := openPrefixReadOnly(ctx, cfg, target)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	st := &statsCmds{}
	stored, _, storedErr = st.computeFromStored(ctx, db, pcfg, target, n)
	fromSummaries, err = st.computeFromSummaries(ctx, db, pcfg, target, n, false)
	if err != nil {
		t.Fatal(err)
	}
	return
}

// compareHeaps compares got and want, after applying adjust, if any, to the
// merged entries of want.
func compareHeaps[T comparable](t *testing.T, what string, got, want *report.Heaps[T], adjust ...func(map[T]report.MergedStats)) {
	t.Helper()
	type totals struct {
		bytes, storage, files, prefixes, prefixBytes, hardlinks, hardlinkDirs, sparseFiles, sparseBytes int64
	}
	tot := func(h *report.Heaps[T]) totals {
		return totals{h.TotalBytes, h.TotalStorageBytes, h.TotalFiles, h.TotalPrefixes, h.TotalPrefixBytes,
			h.TotalHardlinks, h.TotalHardlinkDirs, h.TotalSparseFiles, h.TotalSparseBytes}
	}
	if g, w := tot(got), tot(want); g != w {
		t.Errorf("%v: totals: got %+v, want %+v", what, g, w)
	}
	g, w := got.Merge(0), want.Merge(0)
	for _, fn := range adjust {
		fn(w)
	}
	// The maintained rankings omit entries whose metrics are all zero, eg.
	// directories containing only hardlinks on filesystems where directories
	// report a size of zero.
	for k, v := range w {
		if v == (report.MergedStats{}) {
			delete(w, k)
		}
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("%v: got %+v, want %+v", what, g, w)
	}
}

func TestStatsFromStored(t *testing.T) {
	ctx := context.Background()
	tmpDir, root := newHardlinkTree(t)
	createSparseFile(ctx, t, filepath.Join(root, "b", "sparse"), 1<<20)
	cfg := writeHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
	analyzeWithConfig(ctx, t, cfg, root)

	stored, fromSummaries, err := computeStoredAndSummaries(ctx, t, cfg, root, 10)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Distributed != nil {
		t.Errorf("distributed stats should not be computed from the stored state")
	}
	compareHeaps(t, "prefix", stored.Prefix, fromSummaries.Prefix)
	compareHeaps(t, "subtree", stored.Subtree, fromSummaries.Subtree, func(m map[string]report.MergedStats) {
		// The stored ranking of subtrees by the number of prefixes they
		// contain omits those that contain no other prefixes.
		for k, v := range m {
			if v.Prefixes == 1 {
				v.Prefixes = 0
				m[k] = v
			}
		}
	})
	compareHeaps(t, "by user", stored.ByUser, fromSummaries.ByUser)
	compareHeaps(t, "by group", stored.ByGroup, fromSummaries.ByGroup)
	for _, per := range []struct {
		what      string
		got, want report.PerIDStats
	}{{"per user", stored.PerUser, fromSummaries.PerUser}, {"per group", stored.PerGroup, fromSummaries.PerGroup}} {
		if len(per.got.ByPrefix) != len(per.want.ByPrefix) {
			t.Errorf("%v: got %v ids, want %v", per.what, len(per.got.ByPrefix), len(per.want.ByPrefix))
		}
		for id, want := range per.want.ByPrefix {
			if got, ok := per.got.ByPrefix[id]; !ok {
				t.Errorf("%v: %v: missing", per.what, id)
			} else {
				compareHeaps(t, per.what, got, want)
			}
		}
	}
	if stored.Prefix.TotalHardlinks == 0 {
		t.Errorf("no hardlinks were deduplicated")
	}

	// More entries than are maintained.
	if _, _, err := computeStoredAndSummaries(ctx, t, cfg, root, 100000); !errors.Is(err, errStoredUnusable) {
		t.Errorf("n=100000: got %v, want %v", err, errStoredUnusable)
	}

	_, pcfg, _, db, err := openPrefixReadOnly(ctx, cfg, root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	for _, n := range []int{0, -1} {
		if _, _, err := (&statsCmds{}).computeFromStored(ctx, db, pcfg, root, n); !errors.Is(err, errStoredUnusable) {
			t.Errorf("computeFromStored(n=%d): got %v, want %v", n, err, errStoredUnusable)
		}
	}

	// Stored deduplicated state must not be used when CountHardlinkAsFiles is enabled.
	countedCfg := *pcfg
	countedCfg.CountHardlinkAsFiles = true
	if _, _, err := (&statsCmds{}).computeFromStored(ctx, db, &countedCfg, root, 10); !errors.Is(err, errStoredUnusable) {
		t.Errorf("CountHardlinkAsFiles: got %v, want %v", err, errStoredUnusable)
	}
}

func TestStatsSource(t *testing.T) {
	ctx := context.Background()
	tmpDir, root := newHardlinkTree(t)
	cfg := writeHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
	analyzeWithConfig(ctx, t, cfg, root)
	_, pcfg, _, db, err := openPrefixReadOnly(ctx, cfg, root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	st := &statsCmds{}
	match := hardlinkMatcher(ctx, t)
	for _, tc := range []struct {
		target    string
		useStored bool
		n         int
		want      string
		wantPerID int
	}{
		{root, true, 10, statsFromStored, iduconfig.DefaultTopNPerID},
		{root, false, 10, statsFromSummaries, 0},
		// Too many entries are requested.
		{root, true, 100000, statsFromSummaries, 0},
	} {
		_, source, perIDN, err := st.computeStats(ctx, db, pcfg, match, tc.target, tc.useStored, tc.n, false)
		if err != nil || source != tc.want || perIDN != tc.wantPerID {
			t.Errorf("%v %v %v: got %q perID=%v %v, want %q perID=%v", tc.target, tc.useStored, tc.n, source, perIDN, err, tc.want, tc.wantPerID)
		}
	}
	// PerIDN must reflect the stored DB limit even if config changes afterwards.
	changedCfg := *pcfg
	changedCfg.TopNPerID = pcfg.TopNPerIDOrDefault() + 50
	if _, source, perIDN, err := st.computeStats(ctx, db, &changedCfg, match, root, true, 10, false); err != nil || source != statsFromStored || perIDN != iduconfig.DefaultTopNPerID {
		t.Errorf("changed config: got %q perID=%v %v, want %q perID=%v", source, perIDN, err, statsFromStored, iduconfig.DefaultTopNPerID)
	}
	for target, want := range map[string]bool{
		root:                              true,
		root + string(filepath.Separator): true,
		filepath.Join(root, "a"):          false,
		filepath.Join(root, "a-b"):        false,
	} {
		if got := isFilesystemRoot(pcfg, root, target); got != want {
			t.Errorf("%v: got %v, want %v", target, got, want)
		}
	}
}
