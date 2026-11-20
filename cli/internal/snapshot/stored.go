// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package snapshot

import (
	"context"
	"errors"
	"fmt"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmd/idu/summary"
)

// ErrStoredUnusable is returned by FromStored when the stored state
// cannot be used and the stats must be computed from the summaries instead.
var ErrStoredUnusable = errors.New("the stored top-N rankings cannot be used")

// FromStored constructs stats for the entire filesystem, rooted at
// root, from the recursive totals and rankings maintained by analyze, also
// returning the per-ID ranking limit maintained in the stored state. It
// returns an error that wraps ErrStoredUnusable if they are missing, may be
// out of date, or do not contain the top n entries.
func (c Computer) FromStored(ctx context.Context, db idudb.Reader, cfg *iduconfig.Settings, root string, n int) (*report.AllStats, int, error) {
	if cfg.CountHardlinkAsFiles && idudb.CheckSummaryRecording(ctx, db, cfg.CalculatorOrDefault().String(), stats.RecordLinkedFiles) == nil {
		// The stored recursive totals and rankings deduplicate hardlinks
		// whenever the summaries recorded them, so they cannot be used
		// when counting hardlinks as separate files.
		return nil, 0, fmt.Errorf("%w: stored totals deduplicate hardlinks", ErrStoredUnusable)
	}
	sr := summary.New(db, cfg)
	if err := c.check(ctx, sr); err != nil {
		return nil, 0, storedUnusable(err)
	}
	total, ok, err := sr.Subtree(ctx, root)
	if err != nil {
		return nil, 0, storedUnusable(err)
	}
	if !ok {
		return nil, 0, fmt.Errorf("%w: no recursive totals for %v", ErrStoredUnusable, root)
	}
	sdb := report.NewAllStats(root, n)
	// The distributed subtree statistics require a pass over all of the
	// summaries.
	sdb.Distributed = nil
	for _, h := range []*report.Heaps[string]{sdb.Prefix, sdb.Subtree} {
		h.SetTotals(total)
		h.TotalHardlinks = total.Hardlinks
		h.TotalHardlinkDirs = total.HardlinkDirs
	}
	if err := storedPrefixes(ctx, sr, sdb.Prefix, n); err != nil {
		return nil, 0, err
	}
	if err := storedSubtrees(ctx, sr, sdb.Subtree, n); err != nil {
		return nil, 0, err
	}
	if err := storedIDs(ctx, sr, sdb); err != nil {
		return nil, 0, err
	}
	perIDN, err := storedPerID(ctx, sr, sdb, n)
	if err != nil {
		return nil, 0, err
	}
	if perIDN == 0 {
		perIDN = cfg.TopNPerIDOrDefault()
	}
	// The rankings maintained by analyze do not include the number of
	// hardlinks.
	sdb.ClearHardlinkRankings()
	return sdb, perIDN, nil
}

func storedUnusable(err error) error {
	for _, target := range []error{summary.ErrNoTopN, summary.ErrStaleTopN, idudb.ErrStaleSubtrees, idudb.ErrStaleSummaries} {
		if errors.Is(err, target) {
			return fmt.Errorf("%w: %w", ErrStoredUnusable, err)
		}
	}
	return err
}

// exact returns an error wrapping ErrStoredUnusable unless top contains
// the exact top n entries.
func exact(what string, m stats.Metric, top summary.TopN, n int) error {
	if n <= 0 || n > top.N {
		return fmt.Errorf("%w: %v entries requested but only %v are maintained", ErrStoredUnusable, n, top.N)
	}
	if top.Certified < len(top.Entries) {
		return fmt.Errorf("%w: the %v ranking by %v is not exact", ErrStoredUnusable, what, m)
	}
	return nil
}

func storedPrefixes(ctx context.Context, sr *summary.Reader, h *report.Heaps[string], n int) error {
	for _, m := range stats.Metrics() {
		top, err := sr.TopPrefixes(ctx, m, n)
		if err != nil {
			return storedUnusable(err)
		}
		if err := exact("prefix", m, top, n); err != nil {
			return err
		}
		for _, e := range top.Entries {
			h.PushValue(m, e.Path, e.Value)
		}
	}
	return nil
}

func storedSubtrees(ctx context.Context, sr *summary.Reader, h *report.Heaps[string], n int) error {
	for _, m := range stats.Metrics() {
		top, err := sr.TopSubtrees(ctx, m, n)
		if err != nil {
			return storedUnusable(err)
		}
		if err := exact("subtree", m, top, n); err != nil {
			return err
		}
		if m != stats.MetricPrefixes || len(top.Entries) == 0 {
			for _, e := range top.Entries {
				h.PushValue(m, e.Path, e.Value)
			}
			continue
		}
		// The number of prefixes in a subtree, as computed from the
		// summaries, includes the subtree's own prefix, as do the
		// recursive totals' Prefix values, whereas the ranking is of the
		// number of prefixes beneath it.
		paths := make([]string, len(top.Entries))
		for i, e := range top.Entries {
			paths[i] = e.Path
		}
		totals, found, err := sr.Subtrees(ctx, paths)
		if err != nil {
			return storedUnusable(err)
		}
		for i, p := range paths {
			if found[i] {
				h.PushValue(m, p, totals[i].Prefix)
			}
		}
	}
	return nil
}

// storedIDs records the totals for every user and group.
func storedIDs(ctx context.Context, sr *summary.Reader, sdb *report.AllStats) error {
	users, err := sr.TopUsers(ctx, stats.MetricBytes, 0)
	if err != nil {
		return storedUnusable(err)
	}
	groups, err := sr.TopGroups(ctx, stats.MetricBytes, 0)
	if err != nil {
		return storedUnusable(err)
	}
	for _, u := range users {
		sdb.ByUser.Push(u.ID, u.Totals)
	}
	for _, g := range groups {
		sdb.ByGroup.Push(g.ID, g.Totals)
	}
	// The totals of the rankings of the prefixes for each user and group
	// are those of the user or group.
	for _, ids := range []struct {
		ranked []summary.RankedID
		per    *report.PerIDStats
	}{{users, &sdb.PerUser}, {groups, &sdb.PerGroup}} {
		for _, id := range ids.ranked {
			ids.per.ForID(id.ID).SetTotals(id.Totals)
		}
	}
	return nil
}

// storedPerID records the rankings of the prefixes for every user and
// group, which contain at most topn_per_id entries, and returns that limit.
func storedPerID(ctx context.Context, sr *summary.Reader, sdb *report.AllStats, n int) (int, error) {
	users, groups, err := sr.TopNIDs(ctx)
	if err != nil {
		return 0, storedUnusable(err)
	}
	perIDN := 0
	for _, ids := range []struct {
		ids  []int64
		per  *report.PerIDStats
		rank func(context.Context, int64, stats.Metric, int) (summary.TopN, error)
	}{
		{users, &sdb.PerUser, sr.TopPrefixesForUser},
		{groups, &sdb.PerGroup, sr.TopPrefixesForGroup},
	} {
		for _, id := range ids.ids {
			h := ids.per.ForID(id)
			for _, m := range stats.Metrics() {
				top, err := ids.rank(ctx, id, m, n)
				if err != nil {
					return 0, storedUnusable(err)
				}
				perIDN = top.N
				for _, e := range top.Entries {
					h.PushValue(m, e.Path, e.Value)
				}
			}
		}
	}
	return perIDN, nil
}
