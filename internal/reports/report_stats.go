// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package reports

import (
	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file/diskusage"
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
	Bytes                         *heap.MinMax[int64, T]
	StorageBytes                  *heap.MinMax[int64, T]
	PrefixBytes                   *heap.MinMax[int64, T]
	Files                         *heap.MinMax[int64, T]
	Prefixes                      *heap.MinMax[int64, T]
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
	MaxN     int
	Prefix   *Heaps[string]
	PerUser  PerIDStats
	PerGroup PerIDStats
	ByUser   *Heaps[int64]
	ByGroup  *Heaps[int64]

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
	}
	return h
}

func (h *Heaps[T]) Push(item T, bytes, storageBytes, prefixBytes, files, prefixes int64) {
	h.Bytes.PushMaxN(bytes, item, h.MaxN)
	if h.StorageBytes != nil {
		h.StorageBytes.PushMaxN(storageBytes, item, h.MaxN)
	}
	h.Files.PushMaxN(files, item, h.MaxN)
	h.Prefixes.PushMaxN(prefixes, item, h.MaxN)
	h.PrefixBytes.PushMaxN(prefixBytes, item, h.MaxN)
	h.TotalBytes += bytes
	h.TotalStorageBytes += storageBytes
	h.TotalPrefixBytes += prefixBytes
	h.TotalFiles += files
	h.TotalPrefixes += 1
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
		m.Prefixes = v
		return m
	})
	return merged
}

func newPerIDStats(prefix string, n int) PerIDStats {
	return PerIDStats{
		Prefix:   prefix,
		MaxN:     n,
		ByPrefix: make(map[int64]*Heaps[string]),
	}
}

func (s *PerIDStats) Push(id int64, prefix string, size, storageBytes, prefixBytes, files, children int64) {
	if _, ok := s.ByPrefix[id]; !ok {
		s.ByPrefix[id] = newHeaps[string](s.Prefix, s.MaxN)
	}
	s.ByPrefix[id].Push(prefix, size, storageBytes, prefixBytes, files, children)
}

func NewAllStats(prefix string, n int) *AllStats {
	return &AllStats{
		MaxN:        n,
		Prefix:      newHeaps[string](prefix, n),
		PerUser:     newPerIDStats(prefix, n),
		PerGroup:    newPerIDStats(prefix, n),
		ByUser:      newHeaps[int64](prefix, n),
		ByGroup:     newHeaps[int64](prefix, n),
		userTotals:  map[int64]stats.Totals{},
		groupTotals: map[int64]stats.Totals{},
	}
}

func addToMap(stats map[int64]stats.Totals, uid int64, size, storageBytes, prefixBytes, files, children int64) {
	s := stats[uid]
	s.Bytes += size
	s.StorageBytes += storageBytes
	s.Files += files
	s.SubPrefixes += children
	s.PrefixBytes += prefixBytes
	stats[uid] = s
}

func (s *AllStats) PushPerUserStats(prefix string, us stats.PerIDTotals) {
	for _, u := range us {
		s.PerUser.Push(u.ID, prefix, u.Bytes, u.StorageBytes, u.PrefixBytes, u.Files, u.SubPrefixes)
		addToMap(s.userTotals, u.ID, u.Bytes, u.StorageBytes, u.PrefixBytes, u.Files, u.SubPrefixes)
	}
}

func (s *AllStats) PushPerGroupStats(prefix string, ug stats.PerIDTotals) {
	for _, g := range ug {
		s.PerGroup.Push(g.ID, prefix, g.Bytes, g.StorageBytes, g.PrefixBytes, g.Files, g.SubPrefixes)
		addToMap(s.groupTotals, g.ID, g.Bytes, g.StorageBytes, g.PrefixBytes, g.Files, g.SubPrefixes)
	}
}

func (s *AllStats) Finalize() {
	for id, stats := range s.userTotals {
		s.ByUser.Push(id, stats.Bytes, stats.StorageBytes, stats.PrefixBytes, stats.Files, stats.SubPrefixes)
	}
	for id, stats := range s.groupTotals {
		s.ByGroup.Push(id, stats.Bytes, stats.StorageBytes, stats.PrefixBytes, stats.Files, stats.SubPrefixes)
	}
}

func (s *AllStats) Update(prefix string, pi prefixinfo.T, calc diskusage.Calculator, matcher boolexpr.Matcher) error {
	totals, users, groups := stats.ComputeTotals(prefix, &pi, calc, matcher)
	s.Prefix.Push(prefix,
		totals.Bytes,
		totals.StorageBytes,
		totals.PrefixBytes,
		totals.Files,
		totals.SubPrefixes)
	s.Prefix.TotalHardlinks += totals.Hardlinks
	s.Prefix.TotalHardlinkDirs += totals.HardlinkDirs
	s.PushPerUserStats(prefix, users)
	s.PushPerGroupStats(prefix, groups)
	return nil
}
