// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package reports_test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"sort"
	"testing"

	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/cmd/idu/stats"
)

// dirEntry holds direct directory totals paired with its summary key.
type dirEntry struct {
	SummaryKey string
	Direct     stats.Totals
}

// rollup sorts entries into DFS pre-order by summary key and feeds them
// through sa.
func rollup(sa *reports.SubtreeAggregator, entries []dirEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].SummaryKey < entries[j].SummaryKey
	})
	for _, e := range entries {
		sa.Step(e.SummaryKey, stats.DecodeSummaryKey("/", e.SummaryKey), e.Direct)
	}
	sa.Finish()
}

func addDirEntry(entries *[]dirEntry, path string, bytes, storage, files, prefixBytes int64) {
	ek, _ := stats.SummaryKeys("/", path)
	*entries = append(*entries, dirEntry{
		SummaryKey: ek,
		Direct: stats.Totals{
			Prefix:       1,
			Files:        files,
			Bytes:        bytes,
			StorageBytes: storage,
			PrefixBytes:  prefixBytes,
		},
	})
}

func buildFanoutEntries() []dirEntry {
	var entries []dirEntry
	addDirEntry(&entries, "/root", 0, 0, 0, 4096)
	addDirEntry(&entries, "/root/wrapper", 0, 0, 0, 4096)
	addDirEntry(&entries, "/root/wrapper/sprawl", 0, 0, 0, 4096)
	for b := 0; b < 4; b++ {
		bPath := fmt.Sprintf("/root/wrapper/sprawl/b%d", b)
		addDirEntry(&entries, bPath, 0, 0, 0, 4096)
		for c := 0; c < 4; c++ {
			cPath := fmt.Sprintf("%s/c%d", bPath, c)
			addDirEntry(&entries, cPath, 1<<20, 1<<20, 10, 4096)
		}
	}
	addDirEntry(&entries, "/root/bigleaf", 10<<20, 10<<20, 1, 4096)
	addDirEntry(&entries, "/root-other", 50<<20, 50<<20, 5, 4096)
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	return entries
}

func assertSubtreeGobRoundTrip(t *testing.T, agg *reports.SubtreeAggregator) {
	t.Helper()
	all := reports.NewAllStats("/root", 20)
	all.Subtree = agg.Subtree
	all.Distributed = agg.Distributed
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(all); err != nil {
		t.Fatalf("gob encode AllStats: %v", err)
	}
	var decoded reports.AllStats
	if err := gob.NewDecoder(&buf).Decode(&decoded); err != nil {
		t.Fatalf("gob decode AllStats: %v", err)
	}
	if got := len(reports.TopDistributed(decoded.Distributed.Bytes, 10)); got == 0 {
		t.Fatalf("expected non-empty decoded Distributed stats")
	}
}

func assertDistributedFanout(t *testing.T, agg *reports.SubtreeAggregator) {
	t.Helper()
	dist := reports.TopDistributed(agg.Distributed.Bytes, 20)
	if dist2 := reports.TopDistributed(agg.Distributed.Bytes, 20); len(dist2) != len(dist) {
		t.Fatalf("TopDistributed mutated heap: %d vs %d", len(dist), len(dist2))
	}
	byDistPath := map[string]reports.DistributedStats{}
	for _, d := range dist {
		byDistPath[d.Prefix] = d
	}
	if _, ok := byDistPath["/root/wrapper"]; ok {
		t.Errorf("/root/wrapper should have been suppressed due to MaxChildShare=1.0")
	}
	if _, ok := byDistPath["/root/bigleaf"]; ok {
		t.Errorf("/root/bigleaf is a single leaf directory and should not be in Distributed")
	}
	sprawl, ok := byDistPath["/root/wrapper/sprawl"]
	if !ok {
		t.Fatalf("expected /root/wrapper/sprawl in Distributed, got keys: %v", byDistPath)
	}
	if sprawl.SubtreeBytes != 16<<20 || sprawl.MaxLeafBytes != 1<<20 || sprawl.DistributedBytes != 15<<20 || sprawl.SubtreeDirs != 21 {
		t.Errorf("sprawl stats unexpected: %+v", sprawl)
	}
	if math.Abs(sprawl.BytesAmplification-16.0) > 0.01 || math.Abs(sprawl.MaxChildShare-0.25) > 0.01 {
		t.Errorf("sprawl ratios unexpected: %+v", sprawl)
	}
}

func TestSubtreeAggregatorFanoutDetection(t *testing.T) {
	entries := buildFanoutEntries()
	agg := reports.NewSubtreeAggregator("/root", 20)
	rollup(agg, entries)

	if got, want := agg.Subtree.TotalBytes, int64(76<<20); got != want {
		t.Fatalf("Subtree.TotalBytes = %d, want %d", got, want)
	}
	if got, want := agg.Subtree.TotalFiles, int64(160+1+5); got != want {
		t.Fatalf("Subtree.TotalFiles = %d, want %d", got, want)
	}

	subtreeByPath := map[string]int64{}
	for _, z := range reports.ZipN(agg.Subtree.Bytes, 10) {
		subtreeByPath[z.V] = z.K
	}
	if got, want := subtreeByPath["/root"], int64(26<<20); got != want {
		t.Errorf("/root subtree bytes = %d, want %d", got, want)
	}
	if got, want := subtreeByPath["/root/wrapper/sprawl"], int64(16<<20); got != want {
		t.Errorf("/root/wrapper/sprawl subtree bytes = %d, want %d", got, want)
	}

	assertDistributedFanout(t, agg)
	assertSubtreeGobRoundTrip(t, agg)
}

func TestSubtreeAggregatorMissingIntermediateDir(t *testing.T) {
	// If /a/missing is excluded/absent, /a/missing/c1 and /a/missing/c2 must still
	// fold directly into /a without failing stack invariants.
	var entries []dirEntry
	addDirEntry(&entries, "/a", 100, 100, 1, 4096)
	addDirEntry(&entries, "/a/missing/c1", 500, 500, 2, 4096)
	addDirEntry(&entries, "/a/missing/c2", 500, 500, 2, 4096)

	agg := reports.NewSubtreeAggregator("/a", 10)
	rollup(agg, entries)

	if got, want := agg.Subtree.TotalBytes, int64(1100); got != want {
		t.Fatalf("TotalBytes = %d, want %d", got, want)
	}
	if got, want := agg.Subtree.TotalFiles, int64(5); got != want {
		t.Fatalf("TotalFiles = %d, want %d", got, want)
	}
	dist := reports.TopDistributed(agg.Distributed.Bytes, 10)
	if len(dist) != 1 || dist[0].Prefix != "/a" {
		t.Fatalf("expected /a in Distributed with missing intermediate collapsed, got %+v", dist)
	}
}
