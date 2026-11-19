// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package summary

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"

	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/topn"
	"cloudeng.io/cmd/idu/stats"
)

var (
	// ErrNoTopN is returned when the database contains no top-N rankings,
	// ie. when it was not analyzed with them enabled.
	ErrNoTopN = errors.New("the database contains no top-N rankings: rerun analyze")
	// ErrStaleTopN is returned when the stored top-N rankings may be out
	// of date because the most recent analysis was interrupted or failed,
	// or did not cover the entire filesystem after a configuration change.
	ErrStaleTopN = errors.New("the top-N rankings may be out of date: rerun analyze for the entire prefix")
)

// Ranked is a single entry in a ranking.
type Ranked struct {
	Path  string
	Value int64
}

// TopN is a ranking of prefixes by a single stats.Metric, as maintained by
// analyze. Hardlinks are deduplicated, see Subtree.
type TopN struct {
	// Entries are in descending order of Value.
	Entries []Ranked
	// Certified is the number of leading Entries that are known to be
	// ranked exactly. The remaining entries may be out of order with
	// respect to prefixes not included in the ranking. It is normally
	// equal to len(Entries).
	Certified int
	// N is the number of entries that analyze maintains for this ranking,
	// requests for more than N entries may return fewer, uncertified,
	// entries.
	N int
}

func (r *Reader) topNState(ctx context.Context) (topn.State, error) {
	if r.err != nil {
		return topn.State{}, r.err
	}
	var buf bytes.Buffer
	if err := r.db.GetMeta(ctx, topn.StateKey, &buf); err != nil {
		return topn.State{}, err
	}
	if buf.Len() == 0 {
		return topn.State{}, ErrNoTopN
	}
	var s topn.State
	if err := s.UnmarshalBinary(buf.Bytes()); err != nil {
		return topn.State{}, err
	}
	if !s.Valid {
		return s, ErrStaleTopN
	}
	return s, nil
}

func checkMetric(metric stats.Metric) error {
	if metric < 0 || int(metric) >= stats.NumMetrics {
		return fmt.Errorf("invalid metric: %v", metric)
	}
	return nil
}

func (r *Reader) topN(ctx context.Context, key string, metric stats.Metric, n, maintained int) (TopN, error) {
	if err := checkMetric(metric); err != nil {
		return TopN{}, err
	}
	res := TopN{N: maintained}
	var buf bytes.Buffer
	if err := r.db.GetMeta(ctx, key, &buf); err != nil {
		return TopN{}, err
	}
	if buf.Len() == 0 {
		return TopN{}, ErrNoTopN
	}
	var rk topn.Rankings
	if err := rk.UnmarshalBinary(buf.Bytes()); err != nil {
		return TopN{}, err
	}
	h := rk[metric]
	top := h.Top(n)
	res.Entries = make([]Ranked, 0, len(top))
	for _, e := range top {
		res.Entries = append(res.Entries, Ranked{Path: e.Key, Value: e.Value})
	}
	res.Certified = min(h.Certified(), len(res.Entries))
	return res, nil
}

// TopPrefixes returns the top n prefixes by their own, ie. not recursive,
// value for metric. n of zero or less returns all of the stored entries.
// It returns ErrNoTopN or ErrStaleTopN if the rankings are not available
// or may be out of date.
func (r *Reader) TopPrefixes(ctx context.Context, metric stats.Metric, n int) (TopN, error) {
	s, err := r.topNState(ctx)
	if err != nil {
		return TopN{}, err
	}
	return r.topN(ctx, topn.PrefixesKey, metric, n, s.N)
}

// TopPrefixesForUser is like TopPrefixes but ranks the prefixes by the
// value for metric of the files, and the prefixes themselves, owned by uid.
func (r *Reader) TopPrefixesForUser(ctx context.Context, uid int64, metric stats.Metric, n int) (TopN, error) {
	s, err := r.topNState(ctx)
	if err != nil {
		return TopN{}, err
	}
	if !s.HasUser(uid) {
		return TopN{N: s.PerIDN}, nil
	}
	return r.topN(ctx, topn.UserKey(uid), metric, n, s.PerIDN)
}

// TopPrefixesForGroup is like TopPrefixesForUser but for the group gid.
func (r *Reader) TopPrefixesForGroup(ctx context.Context, gid int64, metric stats.Metric, n int) (TopN, error) {
	s, err := r.topNState(ctx)
	if err != nil {
		return TopN{}, err
	}
	if !s.HasGroup(gid) {
		return TopN{N: s.PerIDN}, nil
	}
	return r.topN(ctx, topn.GroupKey(gid), metric, n, s.PerIDN)
}

// TopNIDs returns the users and groups that have rankings, ie. those
// that own any of the files or prefixes in the database, in ascending
// order.
func (r *Reader) TopNIDs(ctx context.Context) (users, groups []int64, err error) {
	s, err := r.topNState(ctx)
	if err != nil {
		return nil, nil, err
	}
	return s.Users, s.Groups, nil
}

// TopSubtrees is like TopPrefixes but ranks the prefixes by their
// recursive value for metric, ie. that of the prefix and all of its
// descendants. See TopSubtreesWithin for the top subtrees within a prefix.
func (r *Reader) TopSubtrees(ctx context.Context, metric stats.Metric, n int) (TopN, error) {
	s, err := r.topNState(ctx)
	if err != nil {
		return TopN{}, err
	}
	return r.topN(ctx, topn.SubtreesKey, metric, n, s.N)
}

// RankedID is a single entry in a ranking of users or groups.
type RankedID struct {
	ID     int64
	Value  int64
	Totals stats.Totals
}

func (r *Reader) idTotals(ctx context.Context) (*topn.IDTotals, error) {
	if _, err := r.topNState(ctx); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := r.db.GetMeta(ctx, topn.IDTotalsKey, &buf); err != nil {
		return nil, err
	}
	if buf.Len() == 0 {
		return nil, ErrNoTopN
	}
	ids := &topn.IDTotals{}
	if err := ids.UnmarshalBinary(buf.Bytes()); err != nil {
		return nil, err
	}
	return ids, nil
}

func rankIDs(m map[int64]stats.Totals, metric stats.Metric, n int) []RankedID {
	sorted := topn.Sorted(m, metric)
	if n > 0 && n < len(sorted) {
		sorted = sorted[:n]
	}
	out := make([]RankedID, len(sorted))
	for i, t := range sorted {
		out[i] = RankedID{ID: t.ID, Value: metric.Value(t), Totals: t}
	}
	return out
}

// TopUsers returns the top n users, or all users if n is zero or less, by
// their value for metric across the entire filesystem, in descending order
// of Value. Unlike the rankings of prefixes, the totals for every user are
// stored and hence the result is always exact. Hardlinks are deduplicated,
// see Subtree.
func (r *Reader) TopUsers(ctx context.Context, metric stats.Metric, n int) ([]RankedID, error) {
	if err := checkMetric(metric); err != nil {
		return nil, err
	}
	ids, err := r.idTotals(ctx)
	if err != nil {
		return nil, err
	}
	return rankIDs(ids.Users, metric, n), nil
}

// TopGroups is like TopUsers but for groups.
func (r *Reader) TopGroups(ctx context.Context, metric stats.Metric, n int) ([]RankedID, error) {
	if err := checkMetric(metric); err != nil {
		return nil, err
	}
	ids, err := r.idTotals(ctx)
	if err != nil {
		return nil, err
	}
	return rankIDs(ids.Groups, metric, n), nil
}

// Subtree returns the recursive totals for path, ie. the sum of the direct
// totals of path and all of its descendants, if path exists. It returns
// idudb.ErrStaleSubtrees if the recursive totals are missing or may be out
// of date.
//
// The recursive totals, and the rankings and per-id totals, deduplicate
// hardlinks across the entire filesystem, as Walk does when walking from
// its root: each file is attributed to the first prefix, in depth-first
// pre-order, that records it (see stats.HardlinkRecording), and counted as
// a hardlink by any other prefixes that record it. They may therefore
// differ from the totals computed by walking from path, which attributes
// files only amongst the prefixes beneath path. Prefixes that are
// duplicates of preceding ones, eg. bind mounts, are only recognized when
// the recursive totals are rebuilt, eg. by analyze --refresh-topn, rather
// than as soon as they are created.
func (r *Reader) Subtree(ctx context.Context, path string) (stats.Totals, bool, error) {
	t, ok, err := r.Subtrees(ctx, []string{path})
	if err != nil {
		return stats.Totals{}, false, err
	}
	return t[0], ok[0], nil
}

// Subtrees is like Subtree but returns the recursive totals for each of
// paths, and whether each was found, reading them all at once.
func (r *Reader) Subtrees(ctx context.Context, paths []string) ([]stats.Totals, []bool, error) {
	if r.err != nil {
		return nil, nil, r.err
	}
	if err := idudb.CheckSubtrees(ctx, r.db); err != nil {
		return nil, nil, err
	}
	normalized := make([]string, len(paths))
	for i, p := range paths {
		normalized[i] = stats.NormalizePath(r.sep, p)
	}
	return r.subtrees(ctx, normalized)
}

// subtrees returns the recursive totals for each of paths and whether
// each was found.
func (r *Reader) subtrees(ctx context.Context, paths []string) ([]stats.Totals, []bool, error) {
	keys := make([]string, len(paths))
	for i, p := range paths {
		keys[i], _ = stats.SummaryKeys(r.sep, p)
	}
	totals := make([]stats.Totals, len(paths))
	found := make([]bool, len(paths))
	err := r.db.GetSubtrees(ctx, keys, func(i int, val []byte) error {
		if val == nil {
			return nil
		}
		if err := totals[i].UnmarshalBinary(val); err != nil {
			return fmt.Errorf("%v: recursive totals: %w", paths[i], err)
		}
		found[i] = true
		return nil
	})
	return totals, found, err
}

// rankedHeap is a max-heap of Ranked, ordered by descending Value with
// ties broken by ascending Path.
type rankedHeap []Ranked

func (h rankedHeap) Len() int { return len(h) }
func (h rankedHeap) Less(i, j int) bool {
	if h[i].Value != h[j].Value {
		return h[i].Value > h[j].Value
	}
	return h[i].Path < h[j].Path
}
func (h rankedHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *rankedHeap) Push(x any)   { *h = append(*h, x.(Ranked)) }
func (h *rankedHeap) Pop() any {
	old := *h
	n := len(old) - 1
	x := old[n]
	*h = old[:n]
	return x
}

// TopSubtreesWithin returns the top n prefixes, path itself included, by
// their recursive value for metric among path and its descendants, in
// descending order of Value with ties broken by Path. Since a subtree's
// value can never exceed that of its parent, it performs a best-first
// search of the stored recursive totals whose cost is proportional to n
// times the fanout of the prefixes visited, rather than to the size of the
// subtree, and its results are always exact. Prefixes with a zero value
// are omitted. It returns idudb.ErrStaleSubtrees if the recursive totals
// are missing or may be out of date.
func (r *Reader) TopSubtreesWithin(ctx context.Context, path string, metric stats.Metric, n int) ([]Ranked, error) {
	if r.err != nil {
		return nil, r.err
	}
	if err := checkMetric(metric); err != nil {
		return nil, err
	}
	if err := idudb.CheckSubtrees(ctx, r.db); err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	frontier := &rankedHeap{}
	push := func(paths []string) error {
		totals, found, err := r.subtrees(ctx, paths)
		if err != nil {
			return err
		}
		for i, p := range paths {
			if v := metric.Value(totals[i]); found[i] && v > 0 {
				heap.Push(frontier, Ranked{Path: p, Value: v})
			}
		}
		return nil
	}
	if err := push([]string{stats.NormalizePath(r.sep, path)}); err != nil {
		return nil, err
	}
	var out []Ranked
	for len(out) < n && frontier.Len() > 0 {
		next := heap.Pop(frontier).(Ranked)
		out = append(out, next)
		if len(out) == n {
			break
		}
		children, err := r.childPaths(ctx, next.Path)
		if err != nil {
			return nil, err
		}
		if err := push(children); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// childPaths returns the paths of the immediate child prefixes of path.
func (r *Reader) childPaths(ctx context.Context, path string) ([]string, error) {
	p, ok, err := r.Prefix(ctx, path)
	if err != nil || !ok {
		return nil, err
	}
	prefixes := p.pi.PrefixesOnly()
	children := make([]string, 0, len(prefixes))
	for _, fi := range prefixes {
		children = append(children, join(r.sep, p.Path(), fi.Name()))
	}
	return children, nil
}
