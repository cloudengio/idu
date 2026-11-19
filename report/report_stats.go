// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package report

import (
	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/stats"
)

// Heaps is a collection of heap data structures for determining the top N
// values for a set of statistics and also for computing the total for those
// statistics. The Prefix refers to the root of the hierarchy for which the
// statistics are being computed.
type Heaps[T comparable] struct {
	MaxN                          int
	Prefix                        string
	TotalBytes, TotalStorageBytes int64
	TotalFiles, TotalPrefixes     int64
	TotalPrefixBytes              int64
	TotalHardlinks                int64
	TotalHardlinkDirs             int64
	TotalSparseFiles              int64
	TotalSparseBytes              int64
	Bytes                         *heap.MinMax[int64, T]
	StorageBytes                  *heap.MinMax[int64, T]
	PrefixBytes                   *heap.MinMax[int64, T]
	Files                         *heap.MinMax[int64, T]
	Prefixes                      *heap.MinMax[int64, T]
	// SparseBytes will be nil for stats computed before sparse files
	// were tracked.
	SparseBytes *heap.MinMax[int64, T]
}

// PerIDStats is a collection of statistics on a per user/group basis.
type PerIDStats struct {
	Prefix   string
	MaxN     int
	ByPrefix map[int64]*Heaps[string]
}

// AllStats is a collection of statistics for a given prefix and includes:
// - the top N values for each statistic by prefix
// - the total for each statistic
// - the top N values for/per each statistic by user/group
// - the topN user/groups by each statistic
type AllStats struct {
	MaxN        int
	Prefix      *Heaps[string]
	Subtree     *Heaps[string]
	Distributed *DistributedHeaps
	PerUser     PerIDStats
	PerGroup    PerIDStats
	ByUser      *Heaps[int64]
	ByGroup     *Heaps[int64]

	userTotals  map[int64]stats.Totals
	groupTotals map[int64]stats.Totals
}

func newHeaps[T comparable](prefix string, n int) *Heaps[T] {
	h := &Heaps[T]{
		MaxN:         n,
		Prefix:       prefix,
		Bytes:        heap.NewMinMax[int64, T](),
		PrefixBytes:  heap.NewMinMax[int64, T](),
		Files:        heap.NewMinMax[int64, T](),
		Prefixes:     heap.NewMinMax[int64, T](),
		StorageBytes: heap.NewMinMax[int64, T](),
		SparseBytes:  heap.NewMinMax[int64, T](),
	}
	return h
}

// pushMax pushes item into the top N heaps using t and children as the
// number of prefixes/directories it contains.
func (h *Heaps[T]) pushMax(item T, t stats.Totals, children int64) {
	h.Bytes.PushMaxN(t.Bytes, item, h.MaxN)
	h.StorageBytes.PushMaxN(t.StorageBytes, item, h.MaxN)
	h.Files.PushMaxN(t.Files, item, h.MaxN)
	h.Prefixes.PushMaxN(children, item, h.MaxN)
	h.PrefixBytes.PushMaxN(t.PrefixBytes, item, h.MaxN)
	if t.SparseBytes > 0 {
		h.SparseBytes.PushMaxN(t.SparseBytes, item, h.MaxN)
	}
}

// addTotals adds t, other than its hardlink counts, to the totals for h.
func (h *Heaps[T]) addTotals(t stats.Totals) {
	h.TotalBytes += t.Bytes
	h.TotalStorageBytes += t.StorageBytes
	h.TotalPrefixBytes += t.PrefixBytes
	h.TotalFiles += t.Files
	h.TotalPrefixes += t.Prefix
	h.TotalSparseFiles += t.SparseFiles
	h.TotalSparseBytes += t.SparseBytes
}

// PushValue pushes item, whose value for m is v, into the top N heap for
// m. It is used to construct h from existing rankings, eg. those
// maintained by analyze, rather than from the totals of every item, and
// hence does not update the totals for h, see SetTotals.
func (h *Heaps[T]) PushValue(m stats.Metric, item T, v int64) {
	var hp *heap.MinMax[int64, T]
	switch m {
	case stats.MetricBytes:
		hp = h.Bytes
	case stats.MetricStorageBytes:
		hp = h.StorageBytes
	case stats.MetricFiles:
		hp = h.Files
	case stats.MetricPrefixes:
		hp = h.Prefixes
	case stats.MetricPrefixBytes:
		hp = h.PrefixBytes
	case stats.MetricSparseBytes:
		if v <= 0 {
			return
		}
		hp = h.SparseBytes
	}
	if hp != nil {
		hp.PushMaxN(v, item, h.MaxN)
	}
}

// SetTotals sets the totals for h to t, other than its hardlink counts,
// as if the totals of every item had been pushed, see PushValue.
func (h *Heaps[T]) SetTotals(t stats.Totals) {
	h.TotalBytes, h.TotalStorageBytes, h.TotalPrefixBytes = 0, 0, 0
	h.TotalFiles, h.TotalPrefixes = 0, 0
	h.TotalSparseFiles, h.TotalSparseBytes = 0, 0
	h.addTotals(t)
}

// Push pushes item into the top N heaps and adds t, other than its
// hardlink counts, to the totals for h.
func (h *Heaps[T]) Push(item T, t stats.Totals) {
	h.pushMax(item, t, t.SubPrefixes)
	h.addTotals(t)
}

func PopN[T comparable](heap *heap.MinMax[int64, T], n int) (keys []int64, vals []T) {
	i := 0
	for heap.Len() > 0 {
		if i++; n > 0 && i > n {
			break
		}
		k, v := heap.PopMax()
		keys = append(keys, k)
		vals = append(vals, v)
	}
	return
}

type Zipped[T comparable] struct {
	K int64
	V T
}

func ZipN[T comparable](h *heap.MinMax[int64, T], n int) (z []Zipped[T]) {
	if h == nil {
		return nil
	}
	i := 0
	for h.Len() > 0 {
		if i++; n > 0 && i > n {
			break
		}
		k, v := h.PopMax()
		z = append(z, Zipped[T]{k, v})
	}
	return
}

type MergedStats struct {
	Prefix      string `json:"prefix,omitempty"`
	ID          int64  `json:"id,omitempty"`
	IDName      string `json:"name,omitempty"`
	Bytes       int64  `json:"bytes"`
	Storage     int64  `json:"storage,omitempty"`
	Files       int64  `json:"files"`
	Prefixes    int64  `json:"prefixes"`
	PrefixBytes int64  `json:"prefix_bytes"`
	SparseBytes int64  `json:"sparse_bytes,omitempty"`
}

func mergesStats[T comparable](n int, merged map[T]MergedStats, keys []int64, prefixes []T, setter func(MergedStats, int64) MergedStats) {
	for i, prefix := range prefixes {
		if n > 0 && i >= n {
			break
		}
		merged[prefix] = setter(merged[prefix], keys[i])
	}
}

func (h *Heaps[T]) Merge(n int) map[T]MergedStats {
	merged := make(map[T]MergedStats)
	b, bp := PopN(h.Bytes, n)
	mergesStats(n, merged, b, bp, func(m MergedStats, v int64) MergedStats {
		m.Bytes = v
		return m
	})
	if h.StorageBytes != nil {
		sb, sbp := PopN(h.StorageBytes, n)
		mergesStats(n, merged, sb, sbp, func(m MergedStats, v int64) MergedStats {
			m.Storage = v
			return m
		})
	}
	fb, fbp := PopN(h.Files, n)
	mergesStats(n, merged, fb, fbp, func(m MergedStats, v int64) MergedStats {
		m.Files = v
		return m
	})
	db, dbp := PopN(h.Prefixes, n)
	mergesStats(n, merged, db, dbp, func(m MergedStats, v int64) MergedStats {
		m.Prefixes = v
		return m
	})
	pbb, pbp := PopN(h.PrefixBytes, n)
	mergesStats(n, merged, pbb, pbp, func(m MergedStats, v int64) MergedStats {
		m.PrefixBytes = v
		return m
	})
	if h.SparseBytes != nil {
		spb, spp := PopN(h.SparseBytes, n)
		mergesStats(n, merged, spb, spp, func(m MergedStats, v int64) MergedStats {
			m.SparseBytes = v
			return m
		})
	}
	return merged
}

func newPerIDStats(prefix string, n int) PerIDStats {
	return PerIDStats{
		Prefix:   prefix,
		MaxN:     n,
		ByPrefix: make(map[int64]*Heaps[string]),
	}
}

// ForID returns the heaps for id, creating them if necessary.
func (s *PerIDStats) ForID(id int64) *Heaps[string] {
	if _, ok := s.ByPrefix[id]; !ok {
		s.ByPrefix[id] = newHeaps[string](s.Prefix, s.MaxN)
	}
	return s.ByPrefix[id]
}

func (s *PerIDStats) Push(id int64, prefix string, t stats.Totals) {
	s.ForID(id).Push(prefix, t)
}

func NewAllStats(prefix string, n int) *AllStats {
	return &AllStats{
		MaxN:        n,
		Prefix:      newHeaps[string](prefix, n),
		Subtree:     newHeaps[string](prefix, n),
		Distributed: NewDistributedHeaps(n),
		PerUser:     newPerIDStats(prefix, n),
		PerGroup:    newPerIDStats(prefix, n),
		ByUser:      newHeaps[int64](prefix, n),
		ByGroup:     newHeaps[int64](prefix, n),
		userTotals:  map[int64]stats.Totals{},
		groupTotals: map[int64]stats.Totals{},
	}
}

func addToMap(stats map[int64]stats.Totals, id int64, t stats.Totals) {
	s := stats[id]
	s.Add(t)
	stats[id] = s
}

func (s *AllStats) PushPerUserStats(prefix string, us stats.PerIDTotals) {
	for _, u := range us {
		s.PerUser.Push(u.ID, prefix, u)
		addToMap(s.userTotals, u.ID, u)
	}
}

func (s *AllStats) PushPerGroupStats(prefix string, ug stats.PerIDTotals) {
	for _, g := range ug {
		s.PerGroup.Push(g.ID, prefix, g)
		addToMap(s.groupTotals, g.ID, g)
	}
}

func (s *AllStats) Finalize() {
	for id, stats := range s.userTotals {
		s.ByUser.Push(id, stats)
	}
	for id, stats := range s.groupTotals {
		s.ByGroup.Push(id, stats)
	}
}

// RecordDirect pushes precomputed direct directory totals and per-ID
// breakdowns into s.
func (s *AllStats) RecordDirect(prefix string, totals stats.Totals, users, groups stats.PerIDTotals) {
	s.Prefix.Push(prefix, totals)
	s.Prefix.TotalHardlinks += totals.Hardlinks
	s.Prefix.TotalHardlinkDirs += totals.HardlinkDirs
	s.PushPerUserStats(prefix, users)
	s.PushPerGroupStats(prefix, groups)
}
