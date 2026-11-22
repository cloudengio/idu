// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package query

import (
	"context"
	"errors"
	"fmt"
	"io"

	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/snapshot"
	"cloudeng.io/cmd/idu/summary"
	"cloudeng.io/file/localfs"
)

// pass computes the stats for p.within by a pass over the summaries
// beneath it or, if p has a filter, over the metadata of the files beneath
// it. It returns an error, explained by reason, if the Engine is
// restricted to lookups.
func (e *Engine) pass(ctx context.Context, p plan, reason string) (*report.AllStats, string, error) {
	if e.lookupsOnly {
		return nil, "", unsupported("%v", reason)
	}
	c := snapshot.Computer{Log: io.Discard}
	if p.Filter == "" {
		sdb, err := c.FromSummaries(ctx, e.db, e.cfg, p.within, p.limit)
		return sdb, SourcePassOf + "the summaries beneath " + p.within, err
	}
	match, err := snapshot.NewMatcher(ctx, localfs.New(), e.cfg, p.Filter)
	if err != nil {
		return nil, "", err
	}
	sdb, _, _, err := c.Compute(ctx, e.db, e.cfg, match, p.within, false, p.limit)
	return sdb, fmt.Sprintf("%vthe files beneath %v that match %v", SourcePassOf, p.within, match.String()), err
}

// passRanking ranks the prefixes beneath p.within by a pass.
func (e *Engine) passRanking(ctx context.Context, p plan, res *Result) error {
	reason := fmt.Sprintf("rankings of prefixes by a direct_ column are only maintained for the entire filesystem, use scope %v, or order by a total_ column, for %v", Children, p.within)
	switch {
	case p.Filter != "":
		reason = "filter expressions require a pass over the metadata of all of the files beneath " + p.within
	case p.UID != nil || p.GID != nil:
		reason = "rankings of prefixes for a user or group are only maintained for the entire filesystem"
	case p.orderBy.Recursive:
		reason = "the recursive totals are missing or may be out of date: rerun analyze for the entire filesystem"
	}
	sdb, source, err := e.pass(ctx, p, reason)
	if err != nil {
		return err
	}
	res.Source = source
	h := sdb.Prefix
	switch {
	case p.UID != nil:
		h = sdb.PerUser.ForID(*p.UID)
	case p.GID != nil:
		h = sdb.PerGroup.ForID(*p.GID)
	case p.orderBy.Recursive:
		h = sdb.Subtree
	}
	values, paths := popN(heapFor(h, p.orderBy.Metric), p.limit)
	return e.fillValues(ctx, p, paths, values, res)
}

// passIDs ranks the users or groups by their usage beneath p.within by a
// pass.
func (e *Engine) passIDs(ctx context.Context, p plan, res *Result) error {
	reason := fmt.Sprintf("the totals for users and groups are only maintained for the entire filesystem, not for %v, use scope %v and a direct_ column for its own files", p.within, Self)
	if p.Filter != "" {
		reason = "filter expressions require a pass over the metadata of all of the files beneath " + p.within
	}
	sdb, source, err := e.pass(ctx, p, reason)
	if err != nil {
		return err
	}
	res.Source = source
	appendIDRows(res, idHeaps(sdb, p.Table), p)
	return nil
}

func idHeaps(sdb *report.AllStats, table Table) *report.Heaps[int64] {
	if table == Groups {
		return sdb.ByGroup
	}
	return sdb.ByUser
}

// appendIDRows appends the top rows in h, whose values are those of the
// recursive totals.
func appendIDRows(res *Result, h *report.Heaps[int64], p plan) {
	values, ids := popN(heapFor(h, p.orderBy.Metric), p.limit)
	res.Rows = make([]Row, 0, len(values))
	for i := range values {
		res.Rows = append(res.Rows, Row{ID: new(ids[i]), Value: values[i]})
	}
}

// fillValues is like fill, but sets the value of each row to the
// corresponding element of values. The totals are omitted when there is a
// filter since they are those of all of the files, not just those that
// match it.
func (e *Engine) fillValues(ctx context.Context, p plan, paths []string, values []int64, res *Result) error {
	if p.Filter != "" {
		res.Rows = make([]Row, len(paths))
		for i, path := range paths {
			res.Rows[i] = e.newRow(path)
			res.Rows[i].Value = values[i]
		}
		return nil
	}
	if err := e.fill(ctx, p, paths, res); err != nil {
		return err
	}
	for i := range res.Rows {
		res.Rows[i].Value = values[i]
	}
	return nil
}

// level returns the prefixes at depth p.Level below the root and beneath
// p.within, ordered by p.orderBy, by walking the summaries beneath
// p.within.
func (e *Engine) level(ctx context.Context, p plan, res *Result) error {
	if e.lookupsOnly {
		return unsupported("grouping by level %v requires a pass over the summaries beneath %v, only the level of its children, %v, is supported", p.Level, p.within, e.depth(p.within)+1)
	}
	res.Source = SourcePassOf + "the summaries beneath " + p.within
	rel := p.Level - e.depth(p.within)
	var entries []summary.Entry
	var paths []string
	for entry, err := range e.sr.Walk(ctx, p.within, summary.WithMaxDepth(rel)) {
		if err != nil {
			return err
		}
		if entry.Depth == rel {
			entries = append(entries, entry)
			paths = append(paths, entry.Path)
		}
	}
	totals, found, err := e.sr.Subtrees(ctx, paths)
	if err != nil {
		if p.orderBy.Recursive || !errors.Is(err, idudb.ErrStaleSubtrees) {
			return err
		}
		found = make([]bool, len(paths))
	}
	rows := make([]Row, len(entries))
	for i, entry := range entries {
		rows[i] = e.newRow(entry.Path)
		direct := entry.Summary.Direct
		rows[i].Direct = &direct
		if found[i] {
			rows[i].Total = &totals[i]
		}
		rows[i].Value = value(p.orderBy, rows[i])
	}
	sortRows(rows)
	res.Rows = rows[:min(len(rows), p.limit)]
	return nil
}
