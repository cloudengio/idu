// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package model provides the data displayed by the tui: the tree of
// prefixes, their summaries and the top-N rankings beneath them. It is
// independent of the widgets used to display them and is safe for
// concurrent use from background goroutines.
package model

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/cli/internal/snapshot"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmd/idu/summary"
)

// Node is a prefix in the tree.
type Node struct {
	Path, Name string
	// Direct contains the totals for the prefix's own files.
	Direct stats.Totals
	// Subtree contains the recursive totals for the prefix and everything
	// beneath it, it is only valid if HasSubtree is true.
	Subtree    stats.Totals
	HasSubtree bool
}

// HasChildren returns true if the prefix has any child prefixes.
func (n Node) HasChildren() bool {
	return n.Direct.SubPrefixes > 0
}

// Totals returns the subtree's totals, if known, and the prefix's own
// totals otherwise.
func (n Node) Totals() stats.Totals {
	if n.HasSubtree {
		return n.Subtree
	}
	return n.Direct
}

// Size returns the storage used by the subtree, if known, and by the
// prefix's own files otherwise.
func (n Node) Size() int64 {
	return n.Totals().StorageBytes
}

// Summary describes a single prefix.
type Summary struct {
	Node
	Users, Groups stats.PerIDTotals
	Info          summary.Info
	HasInfo       bool
}

// Ranked is an entry in a ranking.
type Ranked[T any] struct {
	Value int64
	Item  T
}

// TopN contains the top-N rankings beneath a prefix. Unlike the heaps in
// report.AllStats, from which they are constructed, they may be read
// repeatedly and concurrently.
type TopN struct {
	Path string
	N    int
	// Source describes where the rankings came from.
	Source                         string
	Bytes, Storage, Files, Subdirs []Ranked[string]
	// Users and Groups are ranked by the storage that they use, ie.
	// StorageBytes, rather than the size of their files.
	Users, Groups []Ranked[int64]
	// Hotspots are the distributed subtree hotspots ranked by bytes, they
	// are only valid if HasHotspots is true.
	Hotspots    []report.DistributedStats
	HasHotspots bool
}

// sorted returns the top n entries in h, which is unchanged.
func sorted[T any](h *heap.MinMax[int64, T], n int) []Ranked[T] {
	if h == nil || h.Len() <= 0 {
		return nil
	}
	r := make([]Ranked[T], 0, h.Len())
	for i := 1; i < len(h.Keys); i++ {
		r = append(r, Ranked[T]{Value: h.Keys[i], Item: h.Vals[i]})
	}
	slices.SortStableFunc(r, func(a, b Ranked[T]) int { return cmp.Compare(b.Value, a.Value) })
	return r[:min(len(r), n)]
}

// newTopN constructs a TopN from sdb, which must not be used concurrently.
func newTopN(path, source string, sdb *report.AllStats, n int) TopN {
	t := TopN{
		Path:    path,
		N:       n,
		Source:  source,
		Bytes:   sorted(sdb.Prefix.Bytes, n),
		Storage: sorted(sdb.Prefix.StorageBytes, n),
		Files:   sorted(sdb.Prefix.Files, n),
		Subdirs: sorted(sdb.Prefix.Prefixes, n),
		Users:   sorted(sdb.ByUser.StorageBytes, n),
		Groups:  sorted(sdb.ByGroup.StorageBytes, n),
	}
	if sdb.Distributed != nil {
		t.Hotspots, t.HasHotspots = report.TopDistributed(sdb.Distributed.Bytes, n), true
	}
	return t
}

// Source provides the data displayed by the tui.
type Source interface {
	// Root returns the root of the filesystem.
	Root() string
	// Children returns the child prefixes of path, those using the most
	// storage first.
	Children(ctx context.Context, path string) ([]Node, error)
	// Summary returns the summary for path.
	Summary(ctx context.Context, path string) (Summary, error)
	// TopN returns the top n entries beneath path.
	TopN(ctx context.Context, path string, n int) (TopN, error)
	// LastUpdate returns the time of the most recent analysis, if known.
	LastUpdate(ctx context.Context) time.Time
	// Warning returns a description of any limitations of the data, eg.
	// that hardlinks may be counted more than once, or the empty string,
	// and whether analyzing the filesystem would remove them.
	Warning() (msg string, analyzeFixes bool)
	// Close releases any resources, eg. the database.
	Close(ctx context.Context) error
}

// ErrNotAnalyzed is returned by Open if the filesystem has not been
// analyzed.
var ErrNotAnalyzed = errors.New("the filesystem has not been analyzed")

// DB is a Source backed by a filesystem's database.
type DB struct {
	f        iduconfig.Filesystem
	db       idudb.Reader
	sr       *summary.Reader
	dir      string
	computer snapshot.Computer
	warning  string
	fixable  bool

	mu    sync.Mutex
	cache map[string]TopN
}

// Open opens the database for f, read-only, as a Source. Snapshots are
// read from, and full passes saved to, snapshotDir if set. Notes from
// computing the rankings are written to log. It returns ErrNotAnalyzed,
// with the database closed, if the database has never been analyzed.
func Open(ctx context.Context, env *cli.Env, f iduconfig.Filesystem, snapshotDir string, log io.Writer) (*DB, error) {
	cfg := f.Config()
	db, err := env.OpenReadOnlyDB(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open database for %v in %v: %w", iduconfig.Label(f), cfg.Database, err)
	}
	if start, _, _, err := db.LastLog(ctx); err != nil || start.IsZero() {
		db.Close(ctx)
		return nil, ErrNotAnalyzed
	}
	sr := summary.New(db, cfg)
	// Summaries that do not record what is needed to deduplicate
	// hardlinks as configured can still be used, but may count hardlinks
	// more than once.
	if err := sr.CheckCompatible(ctx); err != nil {
		db.Close(ctx)
		return nil, err
	}
	s := &DB{
		f:        f,
		db:       db,
		sr:       sr,
		dir:      snapshotDir,
		computer: snapshot.Computer{Log: log},
		cache:    map[string]TopN{},
	}
	if err := sr.Check(ctx); err != nil {
		s.computer.Lenient = true
		if cfg.RecordHardlinks() {
			// The next analysis will rewrite the summaries as configured.
			s.warning, s.fixable = fmt.Sprintf("Hardlinks may be counted more than once since the %v", err), true
		} else if !cfg.CountHardlinkAsFiles {
			s.warning = "Hardlinks may be counted more than once since hardlink_dedup is rescan and hence the summaries, which the tui uses, do not record hardlinks"
		}
	}
	return s, nil
}

// Warning implements Source.
func (s *DB) Warning() (string, bool) {
	return s.warning, s.fixable
}

// Root implements Source.
func (s *DB) Root() string {
	return s.f.Location()
}

// LastUpdate returns the time of the most recent analysis.
func (s *DB) LastUpdate(ctx context.Context) time.Time {
	_, stop, _, _ := s.db.LastLog(ctx)
	return stop
}

// Children implements Source.
func (s *DB) Children(ctx context.Context, path string) ([]Node, error) {
	entries, _, err := s.sr.Children(ctx, path)
	if err != nil {
		return nil, err
	}
	paths := make([]string, len(entries))
	nodes := make([]Node, len(entries))
	for i, e := range entries {
		paths[i] = e.Path
		nodes[i] = Node{Path: e.Path, Name: lastElement(path, e.Path), Direct: e.Summary.Direct}
	}
	// The recursive totals are unavailable if they are stale or being rebuilt.
	totals, found, err := s.sr.Subtrees(ctx, paths)
	if err == nil {
		for i := range nodes {
			nodes[i].Subtree, nodes[i].HasSubtree = totals[i], found[i]
		}
	} else if !errors.Is(err, idudb.ErrStaleSubtrees) {
		return nil, err
	}
	slices.SortStableFunc(nodes, func(a, b Node) int {
		if c := cmp.Compare(b.Size(), a.Size()); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})
	return nodes, nil
}

func lastElement(parent, path string) string {
	if len(path) > len(parent) {
		name := path[len(parent):]
		for len(name) > 0 && (name[0] == '/' || name[0] == '\\') {
			name = name[1:]
		}
		return name
	}
	return path
}

// byStorage returns a copy of t sorted by the storage used, largest first.
func byStorage(t stats.PerIDTotals) stats.PerIDTotals {
	s := slices.Clone(t)
	slices.SortStableFunc(s, func(a, b stats.Totals) int {
		if c := cmp.Compare(b.StorageBytes, a.StorageBytes); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return s
}

// Summary implements Source.
func (s *DB) Summary(ctx context.Context, path string) (Summary, error) {
	ps, ok, err := s.sr.Get(ctx, path)
	if err != nil {
		return Summary{}, err
	}
	if !ok {
		return Summary{}, fmt.Errorf("%v: no summary", path)
	}
	sum := Summary{
		Node:   Node{Path: path, Direct: ps.Direct},
		Users:  byStorage(ps.PerUser),
		Groups: byStorage(ps.PerGroup),
	}
	if t, ok, err := s.sr.Subtree(ctx, path); err == nil && ok {
		sum.Subtree, sum.HasSubtree = t, true
	}
	if p, ok, err := s.sr.Prefix(ctx, path); err == nil && ok {
		sum.Info, sum.HasInfo = p.Info(), true
	}
	return sum, nil
}

// TopN implements Source. For the root they are the current stats, ie.
// those displayed by stats view; for any other prefix they are computed by
// a pass over the summaries beneath it. Results are cached.
func (s *DB) TopN(ctx context.Context, path string, n int) (TopN, error) {
	s.mu.Lock()
	cached, ok := s.cache[path]
	s.mu.Unlock()
	if ok && cached.N >= n {
		return cached, nil
	}
	var top TopN
	if snapshot.IsFilesystemRoot(s.f.Config(), s.Root(), path) {
		sf, snap, err := s.computer.CurrentFrom(ctx, s.db, s.f, s.dir, n)
		if err != nil {
			return TopN{}, err
		}
		top = newTopN(path, describe(sf, snap), sf.Stats, n)
	} else {
		sdb, err := s.computer.FromSummaries(ctx, s.db, s.f.Config(), path, n)
		if err != nil {
			return TopN{}, err
		}
		top = newTopN(path, "computed from the summaries beneath "+path, sdb, n)
	}
	s.mu.Lock()
	s.cache[path] = top
	s.mu.Unlock()
	return top, nil
}

func describe(sf snapshot.File, path string) string {
	if path == "" {
		return "computed from the database at " + sf.Date.Format(time.DateTime)
	}
	return "snapshot " + path
}

// Close implements Source.
func (s *DB) Close(ctx context.Context) error {
	return s.db.Close(ctx)
}
