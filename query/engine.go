// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package query

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/snapshot"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmd/idu/summary"
)

// MaxLimit is the maximum number of rows that a query may return.
const MaxLimit = 10000

// The sources of the rows in a Result.
const (
	SourcePrefix     = "the summary and recursive totals of the prefix"
	SourceChildren   = "the summaries and recursive totals of the prefix's children"
	SourceBestFirst  = "a best-first search of the recursive totals"
	SourceRankings   = "the rankings maintained by analyze"
	SourceIDTotals   = "the per-user and per-group totals maintained by analyze"
	SourceSnapshotOf = "the snapshot "
)

// Engine answers queries for a single filesystem.
type Engine struct {
	f           iduconfig.Filesystem
	cfg         *iduconfig.Settings
	db          idudb.Reader
	sr          *summary.Reader
	root, sep   string
	snapshotDir string
}

// Option configures an Engine.
type Option func(*Engine)

// WithSnapshotDir sets the directory in which snapshots are looked for,
// the filesystem's snapshot_dir by default. An empty dir means that no
// snapshots are used.
func WithSnapshotDir(dir string) Option {
	return func(e *Engine) {
		e.snapshotDir = dir
	}
}

// New returns an Engine for the filesystem f whose database, opened for
// reading, is db.
func New(f iduconfig.Filesystem, db idudb.Reader, opts ...Option) *Engine {
	cfg := f.Config()
	e := &Engine{
		f:           f,
		cfg:         cfg,
		db:          db,
		sr:          summary.New(db, cfg),
		root:        f.Location(),
		sep:         cfg.SeparatorOrDefault(),
		snapshotDir: cfg.SnapshotDir,
	}
	for _, fn := range opts {
		fn(e)
	}
	return e
}

// plan is a normalized query.
type plan struct {
	Query
	within  string
	orderBy Column
	limit   int
	isRoot  bool
}

func (e *Engine) normalize(q Query) (plan, error) {
	p := plan{Query: q}
	p.Table = cmp.Or(q.Table, Prefixes)
	p.Scope = cmp.Or(q.Scope, Subtree)
	limit, err := normalizeLimit(q.Limit)
	if err != nil {
		return p, err
	}
	p.limit = limit
	_, within, err := iduconfig.T{Filesystems: iduconfig.Filesystems{e.f}}.Lookup("", q.Within)
	if err != nil {
		return p, err
	}
	p.within = within
	p.isRoot = snapshot.IsFilesystemRoot(e.cfg, e.root, within)
	if err := e.checkGrouping(&p); err != nil {
		return p, err
	}
	p.orderBy = Total(stats.MetricStorageBytes)
	if (p.Table == Users || p.Table == Groups) && p.Scope != Subtree {
		p.orderBy = Direct(stats.MetricStorageBytes)
	}
	if q.OrderBy != nil {
		p.orderBy = *q.OrderBy
	}
	return p, checkIDs(p)
}

func normalizeLimit(limit int) (int, error) {
	switch {
	case limit < 0:
		return 0, fmt.Errorf("invalid limit: %v", limit)
	case limit == 0:
		return DefaultLimit, nil
	}
	return min(limit, MaxLimit), nil
}

// checkGrouping rejects filters and levels other than that of the
// children of p.within, which it converts to the Children scope.
func (e *Engine) checkGrouping(p *plan) error {
	if p.Filter != "" {
		return unsupported("filter expressions require a pass over all of the prefixes beneath %v, use idu find or idu stats view with an expression instead", p.within)
	}
	if p.Level == 0 {
		return nil
	}
	children := e.depth(p.within) + 1
	if p.Level != children || p.Scope == Self {
		return unsupported("grouping by level %v requires a pass over all of the prefixes beneath %v, only the level of its children, %v, is supported", p.Level, p.within, children)
	}
	p.Scope = Children
	return nil
}

// checkIDs rejects a user or group for anything other than the rankings
// of prefixes that are maintained per user and group.
func checkIDs(p plan) error {
	if p.UID == nil && p.GID == nil {
		return nil
	}
	if p.Table != Prefixes || p.Scope != Subtree || p.orderBy.Recursive || !p.isRoot {
		return unsupported("a user or group may only be specified for a ranking of the prefixes, by a direct column, for the entire filesystem, since only those rankings are maintained per user and group")
	}
	if p.UID != nil && p.GID != nil {
		return unsupported("only one of a user or a group may be specified")
	}
	return nil
}

// Run runs q. It returns an error that wraps ErrUnsupported if q cannot be
// answered by looking up the data maintained by analyze.
func (e *Engine) Run(ctx context.Context, q Query) (Result, error) {
	p, err := e.normalize(q)
	if err != nil {
		return Result{}, err
	}
	res := Result{OrderBy: p.orderBy.String(), Exact: true}
	switch p.Table {
	case Prefixes:
		err = e.prefixes(ctx, p, &res)
	case Users, Groups:
		err = e.ids(ctx, p, &res)
	case Hotspots:
		err = e.hotspots(ctx, p, &res)
	default:
		err = fmt.Errorf("unknown table %q, it must be one of %v, %v, %v or %v", p.Table, Prefixes, Users, Groups, Hotspots)
	}
	if err != nil {
		return Result{}, err
	}
	return res, nil
}

func (e *Engine) prefixes(ctx context.Context, p plan, res *Result) error {
	switch p.Scope {
	case Self:
		res.Source = SourcePrefix
		row, err := e.prefixRow(ctx, p.within)
		if err != nil {
			return err
		}
		row.Value = value(p.orderBy, row)
		res.Rows = []Row{row}
		return nil
	case Children:
		res.Source = SourceChildren
		return e.children(ctx, p, res)
	case Subtree:
		if p.orderBy.Recursive {
			res.Source = SourceBestFirst
			ranked, err := e.sr.TopSubtreesWithin(ctx, p.within, p.orderBy.Metric, p.limit)
			if err != nil {
				return err
			}
			paths := make([]string, len(ranked))
			for i, r := range ranked {
				paths[i] = r.Path
			}
			return e.fill(ctx, p, paths, res)
		}
		return e.directRanking(ctx, p, res)
	}
	return fmt.Errorf("unknown scope %q, it must be one of %v, %v or %v", p.Scope, Self, Children, Subtree)
}

func (e *Engine) directRanking(ctx context.Context, p plan, res *Result) error {
	if !p.isRoot {
		return unsupported("rankings of prefixes by their own files are only maintained for the entire filesystem, use scope %v, or order by a total_ column, for %v", Children, p.within)
	}
	res.Source = SourceRankings
	var top summary.TopN
	var err error
	switch {
	case p.UID != nil:
		top, err = e.sr.TopPrefixesForUser(ctx, *p.UID, p.orderBy.Metric, p.limit)
	case p.GID != nil:
		top, err = e.sr.TopPrefixesForGroup(ctx, *p.GID, p.orderBy.Metric, p.limit)
	default:
		top, err = e.sr.TopPrefixes(ctx, p.orderBy.Metric, p.limit)
	}
	if isStaleTopN(err) && p.UID == nil && p.GID == nil {
		return e.fromSnapshot(ctx, p, res, err, func(sf snapshot.File) []string {
			_, paths := popN(heapFor(sf.Stats.Prefix, p.orderBy.Metric), p.limit)
			return paths
		})
	}
	if err != nil {
		return err
	}
	entries := top.Entries[:min(len(top.Entries), p.limit)]
	paths := make([]string, len(entries))
	for i, r := range entries {
		paths[i] = r.Path
	}
	res.Exact = top.Certified >= len(entries)
	if err := e.fill(ctx, p, paths, res); err != nil {
		return err
	}
	if p.UID != nil || p.GID != nil {
		// The values are those of the user's or group's files.
		for i := range res.Rows {
			res.Rows[i].Value = entries[i].Value
		}
	}
	return nil
}

func isStaleTopN(err error) bool {
	return errors.Is(err, summary.ErrNoTopN) || errors.Is(err, summary.ErrStaleTopN)
}

// fromSnapshot answers a query using the most recent snapshot that is
// newer than the last analysis, if there is one, and otherwise returns
// err, the reason that the data maintained by analyze cannot be used.
func (e *Engine) fromSnapshot(ctx context.Context, p plan, res *Result, err error, paths func(snapshot.File) []string) error {
	sf, path, ok, serr := e.freshSnapshot(ctx)
	if serr != nil || !ok {
		return err
	}
	res.Source, res.Exact = SourceSnapshotOf+path, true
	return e.fill(ctx, p, paths(sf), res)
}

func (e *Engine) children(ctx context.Context, p plan, res *Result) error {
	entries, _, err := e.sr.Children(ctx, p.within)
	if err != nil {
		return err
	}
	paths := make([]string, len(entries))
	for i, c := range entries {
		paths[i] = c.Path
	}
	totals, found, err := e.sr.Subtrees(ctx, paths)
	if err != nil {
		if p.orderBy.Recursive || !errors.Is(err, idudb.ErrStaleSubtrees) {
			return err
		}
		found = make([]bool, len(entries))
	}
	rows := make([]Row, len(entries))
	for i, c := range entries {
		rows[i] = e.newRow(c.Path)
		direct := c.Summary.Direct
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

func sortRows(rows []Row) {
	slices.SortStableFunc(rows, func(a, b Row) int {
		return cmp.Or(cmp.Compare(b.Value, a.Value), cmp.Compare(a.Path, b.Path))
	})
}

func (e *Engine) ids(ctx context.Context, p plan, res *Result) error {
	switch p.Scope {
	case Self:
		if p.orderBy.Recursive {
			return unsupported("the recursive totals for users and groups are only maintained for the entire filesystem, use scope %v for the root, or order by a direct_ column", Subtree)
		}
		res.Source = SourcePrefix
		ps, ok, err := e.sr.Get(ctx, p.within)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%v: no such prefix in the database", p.within)
		}
		perID := ps.PerUser
		if p.Table == Groups {
			perID = ps.PerGroup
		}
		for _, t := range perID {
			row := Row{ID: new(t.ID), Direct: &t}
			row.Value = p.orderBy.Metric.Value(t)
			res.Rows = append(res.Rows, row)
		}
		slices.SortStableFunc(res.Rows, func(a, b Row) int {
			return cmp.Or(cmp.Compare(b.Value, a.Value), cmp.Compare(*a.ID, *b.ID))
		})
		res.Rows = res.Rows[:min(len(res.Rows), p.limit)]
		return nil
	case Subtree:
		if !p.isRoot {
			return unsupported("the totals for users and groups are only maintained for the entire filesystem, not for %v, use scope %v for its own files", p.within, Self)
		}
		if !p.orderBy.Recursive {
			return unsupported("the users and groups of the entire filesystem are ranked by their total usage, order by a total_ column, or use scope %v for the files of the root itself", Self)
		}
		return e.idRanking(ctx, p, res)
	}
	return unsupported("scope %v is not supported for %v, use %v or %v", p.Scope, p.Table, Self, Subtree)
}

func (e *Engine) idRanking(ctx context.Context, p plan, res *Result) error {
	rank := e.sr.TopUsers
	if p.Table == Groups {
		rank = e.sr.TopGroups
	}
	res.Source = SourceIDTotals
	ranked, err := rank(ctx, p.orderBy.Metric, p.limit)
	if isStaleTopN(err) {
		sf, path, ok, serr := e.freshSnapshot(ctx)
		if serr != nil || !ok {
			return err
		}
		h := sf.Stats.ByUser
		if p.Table == Groups {
			h = sf.Stats.ByGroup
		}
		values, ids := popN(heapFor(h, p.orderBy.Metric), p.limit)
		res.Source = SourceSnapshotOf + path
		for i := range values {
			res.Rows = append(res.Rows, Row{ID: new(ids[i]), Value: values[i]})
		}
		return nil
	}
	if err != nil {
		return err
	}
	for _, r := range ranked {
		res.Rows = append(res.Rows, Row{ID: new(r.ID), Total: &r.Totals, Value: r.Value})
	}
	return nil
}

func (e *Engine) hotspots(ctx context.Context, p plan, res *Result) error {
	if p.Scope != Subtree || !p.isRoot {
		return unsupported("hotspots are only available for the entire filesystem, use scope %v for its root", Subtree)
	}
	var get func(*report.DistributedHeaps) *heap.MinMax[int64, report.DistributedStats]
	switch p.orderBy.Metric {
	case stats.MetricBytes:
		get = func(h *report.DistributedHeaps) *heap.MinMax[int64, report.DistributedStats] { return h.Bytes }
	case stats.MetricStorageBytes:
		get = func(h *report.DistributedHeaps) *heap.MinMax[int64, report.DistributedStats] { return h.StorageBytes }
	case stats.MetricFiles:
		get = func(h *report.DistributedHeaps) *heap.MinMax[int64, report.DistributedStats] { return h.Files }
	default:
		return unsupported("hotspots are only ranked by bytes, storage-bytes or files")
	}
	res.OrderBy = "distributed_" + strings.ReplaceAll(p.orderBy.Metric.String(), "-", "_")
	sf, path, ok, err := e.freshSnapshot(ctx)
	if err != nil {
		return err
	}
	if !ok || sf.Stats.Distributed == nil {
		where := "no snapshot directory is configured"
		if e.snapshotDir != "" {
			where = "there is no snapshot that includes them, newer than the last analysis, in " + e.snapshotDir
		}
		return unsupported("hotspots require a pass over all of the summaries and %v, use idu stats compute --hotspots to write one", where)
	}
	res.Source = SourceSnapshotOf + path
	for _, ds := range report.TopDistributed(get(sf.Stats.Distributed), p.limit) {
		row := e.newRow(ds.Prefix)
		row.Hotspot = &ds
		switch p.orderBy.Metric {
		case stats.MetricBytes:
			row.Value = ds.DistributedBytes
		case stats.MetricStorageBytes:
			row.Value = ds.DistributedStorage
		case stats.MetricFiles:
			row.Value = ds.DistributedFiles
		}
		res.Rows = append(res.Rows, row)
	}
	return nil
}

// freshSnapshot returns the most recent snapshot of the entire filesystem
// that is newer than the last analysis, if any.
func (e *Engine) freshSnapshot(ctx context.Context) (snapshot.File, string, bool, error) {
	if e.snapshotDir == "" {
		return snapshot.File{}, "", false, nil
	}
	start, stop, _, err := e.db.LastLog(ctx)
	if err != nil {
		return snapshot.File{}, "", false, err
	}
	updated := cmp.Or(stop, start)
	if updated.IsZero() {
		return snapshot.File{}, "", false, nil
	}
	return snapshot.NewStore(e.snapshotDir).FreshFor(e.sep, e.root, updated, nil)
}

// newRow returns a Row for path with its Parent, Name and Depth set.
func (e *Engine) newRow(path string) Row {
	row := Row{Path: path, Depth: e.depth(path)}
	if row.Depth > 0 {
		trimmed := strings.TrimSuffix(path, e.sep)
		if i := strings.LastIndex(trimmed, e.sep); i >= 0 {
			row.Parent, row.Name = trimmed[:i], trimmed[i+len(e.sep):]
			if row.Parent == "" {
				row.Parent = e.sep
			}
		}
	}
	return row
}

// depth returns the number of components of path below the root.
func (e *Engine) depth(path string) int {
	rel, ok := strings.CutPrefix(path, e.root)
	if !ok {
		return 0
	}
	rel = strings.Trim(rel, e.sep)
	if rel == "" {
		return 0
	}
	return strings.Count(rel, e.sep) + 1
}

// prefixRow returns the Row for path with its direct and, if available,
// recursive totals.
func (e *Engine) prefixRow(ctx context.Context, path string) (Row, error) {
	ps, ok, err := e.sr.Get(ctx, path)
	if err != nil {
		return Row{}, err
	}
	if !ok {
		return Row{}, fmt.Errorf("%v: no such prefix in the database", path)
	}
	row := e.newRow(path)
	row.Direct = &ps.Direct
	t, ok, err := e.sr.Subtree(ctx, path)
	if err != nil && !errors.Is(err, idudb.ErrStaleSubtrees) {
		return Row{}, err
	}
	if ok {
		row.Total = &t
	}
	return row, nil
}

// fill sets res.Rows to the rows for paths, in order, with their direct
// and recursive totals.
func (e *Engine) fill(ctx context.Context, p plan, paths []string, res *Result) error {
	totals, found, err := e.sr.Subtrees(ctx, paths)
	if err != nil {
		if !errors.Is(err, idudb.ErrStaleSubtrees) {
			return err
		}
		found = make([]bool, len(paths))
	}
	res.Rows = make([]Row, 0, len(paths))
	for i, path := range paths {
		row := e.newRow(path)
		ps, ok, err := e.sr.Get(ctx, path)
		if err != nil {
			return err
		}
		if ok {
			row.Direct = &ps.Direct
		}
		if found[i] {
			row.Total = &totals[i]
		}
		row.Value = value(p.orderBy, row)
		res.Rows = append(res.Rows, row)
	}
	return nil
}

// value returns the value of c for row, or zero if it is not known.
func value(c Column, row Row) int64 {
	t := row.Direct
	if c.Recursive {
		t = row.Total
	}
	if t == nil {
		return 0
	}
	return c.Metric.Value(*t)
}

func popN[T comparable](h *heap.MinMax[int64, T], n int) ([]int64, []T) {
	if h == nil {
		return nil, nil
	}
	return report.PopN(h, n)
}

// heapFor returns the heap in h for metric.
func heapFor[T comparable](h *report.Heaps[T], metric stats.Metric) *heap.MinMax[int64, T] {
	if h == nil {
		return nil
	}
	switch metric {
	case stats.MetricBytes:
		return h.Bytes
	case stats.MetricStorageBytes:
		return h.StorageBytes
	case stats.MetricFiles:
		return h.Files
	case stats.MetricPrefixes:
		return h.Prefixes
	case stats.MetricPrefixBytes:
		return h.PrefixBytes
	case stats.MetricSparseBytes:
		return h.SparseBytes
	}
	return nil
}
