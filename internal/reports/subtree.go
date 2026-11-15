// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package reports

import (
	"sort"
	"strings"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/stats"
)

const (
	defaultMaxChildShare = 0.85
	minAmplification     = 2.0
)

// DistributedStats describes a directory whose subtree accumulates substantial
// usage across multiple descendant directories where neither a single directory
// nor a single child dominates (for example, modest-fanout hierarchies whose
// leaves each hold modest data).
type DistributedStats struct {
	Prefix               string  `json:"prefix"`
	SubtreeBytes         int64   `json:"subtree_bytes"`
	SubtreeStorage       int64   `json:"subtree_storage"`
	SubtreeFiles         int64   `json:"subtree_files"`
	SubtreeDirs          int64   `json:"subtree_dirs"`
	SubtreePrefixBytes   int64   `json:"subtree_prefix_bytes"`
	DirectBytes          int64   `json:"direct_bytes"`
	DirectStorage        int64   `json:"direct_storage"`
	DirectFiles          int64   `json:"direct_files"`
	MaxLeafBytes         int64   `json:"max_leaf_bytes"`
	MaxLeafStorage       int64   `json:"max_leaf_storage"`
	MaxLeafFiles         int64   `json:"max_leaf_files"`
	MaxChildShare        float64 `json:"max_child_share"`
	BytesAmplification   float64 `json:"bytes_amplification"`
	StorageAmplification float64 `json:"storage_amplification"`
	FilesAmplification   float64 `json:"files_amplification"`
	DistributedBytes     int64   `json:"distributed_bytes"`
	DistributedStorage   int64   `json:"distributed_storage"`
	DistributedFiles     int64   `json:"distributed_files"`
}

// DistributedHeaps maintains bounded top-N min-max heaps of DistributedStats
// ordered by distributed bytes, distributed storage bytes, and distributed files.
type DistributedHeaps struct {
	MaxN         int
	Bytes        *heap.MinMax[int64, DistributedStats]
	StorageBytes *heap.MinMax[int64, DistributedStats]
	Files        *heap.MinMax[int64, DistributedStats]
}

// NewDistributedHeaps creates a new DistributedHeaps bounded to n entries per heap.
func NewDistributedHeaps(n int) *DistributedHeaps {
	return &DistributedHeaps{
		MaxN:         n,
		Bytes:        heap.NewMinMax[int64, DistributedStats](),
		StorageBytes: heap.NewMinMax[int64, DistributedStats](),
		Files:        heap.NewMinMax[int64, DistributedStats](),
	}
}

// Push records ds into whichever distributed metrics satisfy the amplification
// threshold (subtree total >= 2x single largest directory).
func (dh *DistributedHeaps) Push(ds DistributedStats) {
	if dh == nil {
		return
	}
	if ds.DistributedBytes > 0 && ds.BytesAmplification >= minAmplification {
		dh.Bytes.PushMaxN(ds.DistributedBytes, ds, dh.MaxN)
	}
	if ds.DistributedStorage > 0 && ds.StorageAmplification >= minAmplification {
		dh.StorageBytes.PushMaxN(ds.DistributedStorage, ds, dh.MaxN)
	}
	if ds.DistributedFiles > 0 && ds.FilesAmplification >= minAmplification {
		dh.Files.PushMaxN(ds.DistributedFiles, ds, dh.MaxN)
	}
}

// TopDistributed returns up to n entries from h in descending order. It
// leaves h unchanged on return by popping and then restoring its entries,
// and hence must not be called concurrently with any other use of h.
func TopDistributed(h *heap.MinMax[int64, DistributedStats], n int) []DistributedStats {
	if h == nil || h.Len() == 0 {
		return nil
	}
	type popped struct {
		k int64
		v DistributedStats
	}
	var restore []popped
	var out []DistributedStats
	for h.Len() > 0 {
		if n > 0 && len(out) >= n {
			break
		}
		k, v := h.PopMax()
		restore = append(restore, popped{k: k, v: v})
		out = append(out, v)
	}
	for _, p := range restore {
		h.Push(p.k, p.v)
	}
	return out
}

type subtreeFrame struct {
	path                     string
	summaryKey               string
	direct                   stats.Totals
	subtree                  stats.Totals
	subtreeDirs              int64
	maxDescendantDirectBytes int64
	maxDescendantDirectStor  int64
	maxDescendantDirectFiles int64
	maxChildSubtreeBytes     int64
	maxChildSubtreeStor      int64
	maxChildSubtreeFiles     int64
}

// SubtreeAggregator computes recursive subtree totals and modest-fanout
// distributed hotspots. Directories must be supplied in DFS pre-order
// (sorted by normalized summary key), in which case it requires O(depth)
// memory.
type SubtreeAggregator struct {
	Subtree       *Heaps[string]
	Distributed   *DistributedHeaps
	maxChildShare float64
	stack         []subtreeFrame
}

// NewSubtreeAggregator creates a SubtreeAggregator rooted at prefix with
// top-N heap limit n.
func NewSubtreeAggregator(prefix string, n int) *SubtreeAggregator {
	return &SubtreeAggregator{
		Subtree:       newHeaps[string](prefix, n),
		Distributed:   NewDistributedHeaps(n),
		maxChildShare: defaultMaxChildShare,
	}
}

func isSummaryDescendant(parentKey, childKey string) bool {
	if parentKey == "" {
		return false
	}
	// Only the root key (e.g. "/" encoded as "\x00") ends with the separator,
	// in which case every longer key with that prefix is a descendant.
	if strings.HasSuffix(parentKey, "\x00") {
		return len(childKey) > len(parentKey) && strings.HasPrefix(childKey, parentKey)
	}
	return len(childKey) > len(parentKey)+1 &&
		strings.HasPrefix(childKey, parentKey) &&
		childKey[len(parentKey)] == '\x00'
}

// Step feeds the next directory in DFS pre-order (summaryKey ordered) into
// the aggregator.
func (sa *SubtreeAggregator) Step(summaryKey, path string, direct stats.Totals) {
	for len(sa.stack) > 0 && !isSummaryDescendant(sa.stack[len(sa.stack)-1].summaryKey, summaryKey) {
		sa.popOne()
	}
	frame := subtreeFrame{
		path:                     path,
		summaryKey:               summaryKey,
		direct:                   direct,
		subtree:                  direct,
		subtreeDirs:              1,
		maxDescendantDirectBytes: direct.Bytes,
		maxDescendantDirectStor:  direct.StorageBytes,
		maxDescendantDirectFiles: direct.Files,
	}
	sa.stack = append(sa.stack, frame)
}

// Finish finalizes any remaining directory frames on the stack.
func (sa *SubtreeAggregator) Finish() {
	for len(sa.stack) > 0 {
		sa.popOne()
	}
}

func amplification(total, maxLeaf int64) float64 {
	if total <= 0 {
		return 0
	}
	if maxLeaf <= 0 {
		return float64(total)
	}
	return float64(total) / float64(maxLeaf)
}

func (sa *SubtreeAggregator) maxChildDominance(f *subtreeFrame) float64 {
	switch {
	case f.subtree.StorageBytes > 0:
		return float64(f.maxChildSubtreeStor) / float64(f.subtree.StorageBytes)
	case f.subtree.Bytes > 0:
		return float64(f.maxChildSubtreeBytes) / float64(f.subtree.Bytes)
	case f.subtree.Files > 0:
		return float64(f.maxChildSubtreeFiles) / float64(f.subtree.Files)
	default:
		return 0
	}
}

func (sa *SubtreeAggregator) popOne() {
	n := len(sa.stack)
	f := sa.stack[n-1]
	sa.stack = sa.stack[:n-1]

	sa.Subtree.pushMax(f.path,
		f.subtree.Bytes,
		f.subtree.StorageBytes,
		f.subtree.PrefixBytes,
		f.subtree.Files,
		f.subtreeDirs)

	if f.subtreeDirs > 1 {
		childShare := sa.maxChildDominance(&f)
		if childShare <= sa.maxChildShare {
			ds := DistributedStats{
				Prefix:               f.path,
				SubtreeBytes:         f.subtree.Bytes,
				SubtreeStorage:       f.subtree.StorageBytes,
				SubtreeFiles:         f.subtree.Files,
				SubtreeDirs:          f.subtreeDirs,
				SubtreePrefixBytes:   f.subtree.PrefixBytes,
				DirectBytes:          f.direct.Bytes,
				DirectStorage:        f.direct.StorageBytes,
				DirectFiles:          f.direct.Files,
				MaxLeafBytes:         f.maxDescendantDirectBytes,
				MaxLeafStorage:       f.maxDescendantDirectStor,
				MaxLeafFiles:         f.maxDescendantDirectFiles,
				MaxChildShare:        childShare,
				BytesAmplification:   amplification(f.subtree.Bytes, f.maxDescendantDirectBytes),
				StorageAmplification: amplification(f.subtree.StorageBytes, f.maxDescendantDirectStor),
				FilesAmplification:   amplification(f.subtree.Files, f.maxDescendantDirectFiles),
				DistributedBytes:     f.subtree.Bytes - f.maxDescendantDirectBytes,
				DistributedStorage:   f.subtree.StorageBytes - f.maxDescendantDirectStor,
				DistributedFiles:     f.subtree.Files - f.maxDescendantDirectFiles,
			}
			sa.Distributed.Push(ds)
		}
	}

	if len(sa.stack) == 0 {
		sa.Subtree.TotalBytes += f.subtree.Bytes
		sa.Subtree.TotalStorageBytes += f.subtree.StorageBytes
		sa.Subtree.TotalPrefixBytes += f.subtree.PrefixBytes
		sa.Subtree.TotalFiles += f.subtree.Files
		sa.Subtree.TotalPrefixes += f.subtree.Prefix
		sa.Subtree.TotalHardlinks += f.subtree.Hardlinks
		sa.Subtree.TotalHardlinkDirs += f.subtree.HardlinkDirs
		return
	}

	parent := &sa.stack[len(sa.stack)-1]
	parent.subtree.Add(f.subtree)
	parent.subtreeDirs += f.subtreeDirs
	if f.maxDescendantDirectBytes > parent.maxDescendantDirectBytes {
		parent.maxDescendantDirectBytes = f.maxDescendantDirectBytes
	}
	if f.maxDescendantDirectStor > parent.maxDescendantDirectStor {
		parent.maxDescendantDirectStor = f.maxDescendantDirectStor
	}
	if f.maxDescendantDirectFiles > parent.maxDescendantDirectFiles {
		parent.maxDescendantDirectFiles = f.maxDescendantDirectFiles
	}
	if f.subtree.Bytes > parent.maxChildSubtreeBytes {
		parent.maxChildSubtreeBytes = f.subtree.Bytes
	}
	if f.subtree.StorageBytes > parent.maxChildSubtreeStor {
		parent.maxChildSubtreeStor = f.subtree.StorageBytes
	}
	if f.subtree.Files > parent.maxChildSubtreeFiles {
		parent.maxChildSubtreeFiles = f.subtree.Files
	}
}

// MergeDistributed returns up to n unique DistributedStats entries across
// the storage, bytes, and files heaps, ordered by DistributedStorage,
// DistributedBytes, and DistributedFiles descending.
func MergeDistributed(dh *DistributedHeaps, n int) []DistributedStats {
	if dh == nil {
		return nil
	}
	seen := make(map[string]DistributedStats)
	for _, list := range [][]DistributedStats{
		TopDistributed(dh.StorageBytes, n),
		TopDistributed(dh.Bytes, n),
		TopDistributed(dh.Files, n),
	} {
		for _, ds := range list {
			seen[ds.Prefix] = ds
		}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]DistributedStats, 0, len(seen))
	for _, ds := range seen {
		out = append(out, ds)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DistributedStorage != out[j].DistributedStorage {
			return out[i].DistributedStorage > out[j].DistributedStorage
		}
		if out[i].DistributedBytes != out[j].DistributedBytes {
			return out[i].DistributedBytes > out[j].DistributedBytes
		}
		if out[i].DistributedFiles != out[j].DistributedFiles {
			return out[i].DistributedFiles > out[j].DistributedFiles
		}
		return out[i].Prefix < out[j].Prefix
	})
	return out
}
