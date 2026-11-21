// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package mcpserver

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/snapshot"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmd/idu/summary"
	"cloudeng.io/cmd/idu/usernames"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Limits on the number of entries returned by the tools.
const (
	defaultN = 20
	maxN     = 1000
)

// Totals are the totals for a prefix, or for a user or group.
type Totals struct {
	Files        int64 `json:"files"`
	Prefixes     int64 `json:"prefixes" jsonschema:"the number of prefixes (directories) beneath: the immediate children for direct totals and all descendants for recursive totals"`
	Bytes        int64 `json:"bytes" jsonschema:"the total size of the files"`
	StorageBytes int64 `json:"storage_bytes" jsonschema:"the space used on disk by the files"`
	PrefixBytes  int64 `json:"prefix_bytes" jsonschema:"the total size of the prefixes (directories) themselves"`
	Hardlinks    int64 `json:"hardlinks" jsonschema:"the number of links to files that are counted elsewhere"`
	SparseFiles  int64 `json:"sparse_files"`
	SparseBytes  int64 `json:"sparse_bytes" jsonschema:"the size of sparse files that is not allocated on disk"`
}

// newTotals returns the Totals for t.
func newTotals(t stats.Totals) Totals {
	return Totals{
		Files:        t.Files,
		Prefixes:     t.SubPrefixes,
		Bytes:        t.Bytes,
		StorageBytes: t.StorageBytes,
		PrefixBytes:  t.PrefixBytes,
		Hardlinks:    t.Hardlinks,
		SparseFiles:  t.SparseFiles,
		SparseBytes:  t.SparseBytes,
	}
}

var (
	readOnly = &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true, OpenWorldHint: new(false)}
	metrics  = func() string {
		names := make([]string, 0, stats.NumMetrics)
		for _, m := range stats.Metrics() {
			names = append(names, m.String())
		}
		return strings.Join(names, ", ")
	}()
)

func (s *Server) addTools() {
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "list_filesystems",
		Description: "List the filesystems that idu is configured to analyze, with the location of each, ie. its root, and when it was last analyzed.",
		Annotations: readOnly,
	}, s.listFilesystems)
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "usage",
		Description: "The disk usage of a prefix (directory): the recursive totals for it and everything beneath it, and the totals for its own files.",
		Annotations: readOnly,
	}, s.usage)
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "children",
		Description: "The immediate children of a prefix (directory) with the recursive totals for each, ordered by a metric, largest first: " + metrics + ".",
		Annotations: readOnly,
	}, s.children)
	mcp.AddTool(s.srv, &mcp.Tool{
		Name: "top_n",
		Description: "The top n entries beneath a prefix, ordered by a metric (" + metrics + "), largest first. " +
			"by is one of: subtree, the prefixes ranked by their recursive totals, ie. where the space is used; " +
			"prefix, the prefixes ranked by their own files only; user or group, the users or groups ranked by their usage. " +
			"For by=prefix, user may be set to rank the prefixes by the usage of that user's files, and similarly for group; this is only supported for the entire filesystem.",
		Annotations: readOnly,
	}, s.topN)
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "errors",
		Description: "The errors, eg. permission denied, recorded by the most recent analyses for a prefix and everything beneath it, in path order.",
		Annotations: readOnly,
	}, s.errors)
}

// FSArgs identify a filesystem and a prefix within it.
type FSArgs struct {
	FS     string `json:"fs,omitempty" jsonschema:"the name, or root, of the filesystem, see list_filesystems; if omitted, an absolute prefix selects the filesystem that contains it and otherwise the default filesystem is used"`
	Prefix string `json:"prefix,omitempty" jsonschema:"a path, either absolute or relative to the filesystem's root; omit it for the entire filesystem"`
}

// Filesystem describes a configured filesystem.
type Filesystem struct {
	Name         string    `json:"name,omitempty"`
	Location     string    `json:"location" jsonschema:"the root of the filesystem, or the range of keys for a key oriented store"`
	Database     string    `json:"database"`
	LastAnalyzed time.Time `json:"last_analyzed,omitzero"`
	Status       string    `json:"status" jsonschema:"ok, not analyzed, or why the database could not be read, eg. because it is locked by an analysis"`
	Default      string    `json:"default,omitempty" jsonschema:"set, to why, for the filesystem used when a tool call specifies neither fs nor an absolute prefix"`
}

// Filesystems is the result of list_filesystems.
type Filesystems struct {
	Filesystems []Filesystem `json:"filesystems"`
}

func (s *Server) listFilesystems(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, Filesystems, error) {
	out := Filesystems{Filesystems: make([]Filesystem, len(s.cfg.Filesystems))}
	def, why, _ := s.defaultFS()
	var wg sync.WaitGroup
	for i, f := range s.cfg.Filesystems {
		wg.Go(func() {
			cfg := f.Config()
			fs := Filesystem{Name: cfg.Name, Location: f.Location(), Database: cfg.Database, Status: "ok"}
			if f == def {
				fs.Default = why
			}
			err := s.withDB(ctx, f, func(db idudb.Reader, _ *summary.Reader) error {
				start, stop, _, err := db.LastLog(ctx)
				if err != nil {
					return err
				}
				fs.LastAnalyzed = cmp.Or(stop, start)
				if fs.LastAnalyzed.IsZero() {
					fs.Status = "not analyzed"
				}
				return nil
			})
			if err != nil {
				fs.Status = err.Error()
			}
			out.Filesystems[i] = fs
		})
	}
	wg.Wait()
	return nil, out, nil
}

// Usage is the result of usage.
type Usage struct {
	Path string `json:"path"`
	// Recursive is omitted if the recursive totals are not available.
	Recursive *Totals `json:"recursive,omitempty" jsonschema:"the totals for the prefix and everything beneath it"`
	Direct    Totals  `json:"direct" jsonschema:"the totals for the prefix's own files"`
	Note      string  `json:"note,omitempty"`
}

func (s *Server) usage(ctx context.Context, _ *mcp.CallToolRequest, args FSArgs) (*mcp.CallToolResult, Usage, error) {
	f, path, err := s.lookup(args.FS, args.Prefix)
	if err != nil {
		return nil, Usage{}, err
	}
	out := Usage{Path: path}
	err = s.withDB(ctx, f, func(_ idudb.Reader, sr *summary.Reader) error {
		ps, ok, err := sr.Get(ctx, path)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%v: no such prefix in the database", path)
		}
		out.Direct = newTotals(ps.Direct)
		t, ok, err := sr.Subtree(ctx, path)
		switch {
		case errors.Is(err, idudb.ErrStaleSubtrees):
			out.Note = err.Error()
		case err != nil:
			return err
		case ok:
			rt := newTotals(t)
			out.Recursive = &rt
		}
		return nil
	})
	return nil, out, err
}

// ChildrenArgs are the arguments to children.
type ChildrenArgs struct {
	FSArgs
	Metric string `json:"metric,omitempty" jsonschema:"the metric to order the children by, storage_bytes by default"`
	N      int    `json:"n,omitempty" jsonschema:"the maximum number of children to return, 20 by default"`
}

// Child is a child of a prefix.
type Child struct {
	Path string `json:"path"`
	// Recursive is omitted if the recursive totals are not available.
	Recursive *Totals `json:"recursive,omitempty"`
	Direct    Totals  `json:"direct"`
}

// Children is the result of children.
type Children struct {
	Path     string  `json:"path"`
	Metric   string  `json:"metric"`
	Total    int     `json:"total" jsonschema:"the number of children, which may be more than are returned"`
	Children []Child `json:"children"`
	Note     string  `json:"note,omitempty"`
}

// parseMetric parses name, which may use underscores rather than hyphens.
func parseMetric(name string) (stats.Metric, error) {
	if name == "" {
		return stats.MetricStorageBytes, nil
	}
	m, err := stats.ParseMetric(strings.ReplaceAll(name, "_", "-"))
	if err != nil {
		return 0, fmt.Errorf("%w, it must be one of: %v", err, metrics)
	}
	return m, nil
}

func limit(n int) int {
	if n <= 0 {
		return defaultN
	}
	return min(n, maxN)
}

func (s *Server) children(ctx context.Context, _ *mcp.CallToolRequest, args ChildrenArgs) (*mcp.CallToolResult, Children, error) {
	f, path, err := s.lookup(args.FS, args.Prefix)
	if err != nil {
		return nil, Children{}, err
	}
	metric, err := parseMetric(args.Metric)
	if err != nil {
		return nil, Children{}, err
	}
	out := Children{Path: path, Metric: metric.String()}
	err = s.withDB(ctx, f, func(_ idudb.Reader, sr *summary.Reader) error {
		entries, _, err := sr.Children(ctx, path)
		if err != nil {
			return err
		}
		paths := make([]string, len(entries))
		for i, e := range entries {
			paths[i] = e.Path
		}
		totals, found, err := sr.Subtrees(ctx, paths)
		if errors.Is(err, idudb.ErrStaleSubtrees) {
			out.Note = "the children are ordered by their own files only: " + err.Error()
			totals, found = make([]stats.Totals, len(entries)), make([]bool, len(entries))
		} else if err != nil {
			return err
		}
		order := make([]int, len(entries))
		values := make([]int64, len(entries))
		for i, e := range entries {
			order[i] = i
			if found[i] {
				values[i] = metric.Value(totals[i])
			} else {
				values[i] = metric.Value(e.Summary.Direct)
			}
		}
		slices.SortStableFunc(order, func(a, b int) int {
			return cmp.Or(cmp.Compare(values[b], values[a]), cmp.Compare(entries[a].Path, entries[b].Path))
		})
		out.Total = len(order)
		keep := min(len(order), limit(args.N))
		out.Children = make([]Child, 0, keep)
		for _, i := range order[:keep] {
			e := entries[i]
			c := Child{Path: e.Path, Direct: newTotals(e.Summary.Direct)}
			if found[i] {
				rt := newTotals(totals[i])
				c.Recursive = &rt
			}
			out.Children = append(out.Children, c)
		}
		return nil
	})
	return nil, out, err
}

// TopNArgs are the arguments to top_n.
type TopNArgs struct {
	FSArgs
	Metric string `json:"metric,omitempty" jsonschema:"the metric to rank by, storage_bytes by default"`
	By     string `json:"by,omitempty" jsonschema:"what to rank: subtree (the default), prefix, user or group"`
	User   string `json:"user,omitempty" jsonschema:"for by=prefix, rank the prefixes by the usage of this user's files, a name or numeric id"`
	Group  string `json:"group,omitempty" jsonschema:"for by=prefix, rank the prefixes by the usage of this group's files, a name or numeric id"`
	N      int    `json:"n,omitempty" jsonschema:"the number of entries to return, 20 by default"`
}

// Entry is an entry in a ranking.
type Entry struct {
	Path  string `json:"path,omitempty"`
	User  string `json:"user,omitempty"`
	Group string `json:"group,omitempty"`
	ID    *int64 `json:"id,omitempty" jsonschema:"the numeric id of the user or group"`
	Value int64  `json:"value"`
}

// TopN is the result of top_n.
type TopN struct {
	Path    string  `json:"path"`
	Metric  string  `json:"metric"`
	By      string  `json:"by"`
	Entries []Entry `json:"entries"`
	Exact   bool    `json:"exact" jsonschema:"true if the ranking is known to be exact"`
	Source  string  `json:"source" jsonschema:"how the ranking was obtained"`
}

const (
	sourceStored   = "the rankings maintained by analyze"
	sourceComputed = "computed by a pass over all of the summaries beneath the prefix"
)

func (s *Server) topN(ctx context.Context, _ *mcp.CallToolRequest, args TopNArgs) (*mcp.CallToolResult, TopN, error) {
	f, path, err := s.lookup(args.FS, args.Prefix)
	if err != nil {
		return nil, TopN{}, err
	}
	metric, err := parseMetric(args.Metric)
	if err != nil {
		return nil, TopN{}, err
	}
	by := cmp.Or(args.By, "subtree")
	n := limit(args.N)
	out := TopN{Path: path, Metric: metric.String(), By: by, Exact: true, Source: sourceStored}
	isRoot := snapshot.IsFilesystemRoot(f.Config(), f.Location(), path)
	if args.User != "" && args.Group != "" {
		return nil, TopN{}, fmt.Errorf("user and group cannot both be set")
	}
	if (args.User != "" || args.Group != "") && (by != "prefix" || !isRoot) {
		return nil, TopN{}, fmt.Errorf("user and group may only be set for by=prefix for the entire filesystem")
	}
	err = s.withDB(ctx, f, func(db idudb.Reader, sr *summary.Reader) error {
		switch by {
		case "subtree":
			return s.topSubtrees(ctx, db, sr, f, path, metric, n, &out)
		case "prefix":
			if isRoot {
				return s.topPrefixes(ctx, db, sr, f, path, metric, args, n, &out)
			}
		case "user", "group":
			if isRoot {
				return s.topIDs(ctx, db, sr, f, path, metric, n, &out)
			}
		default:
			return fmt.Errorf("unknown value for by: %q, it must be one of subtree, prefix, user or group", by)
		}
		return s.computed(ctx, db, f, path, metric, n, &out)
	})
	return nil, out, err
}

func (s *Server) topSubtrees(ctx context.Context, db idudb.Reader, sr *summary.Reader, f iduconfig.Filesystem, path string, metric stats.Metric, n int, out *TopN) error {
	ranked, err := sr.TopSubtreesWithin(ctx, path, metric, n)
	if errors.Is(err, idudb.ErrStaleSubtrees) {
		return s.computed(ctx, db, f, path, metric, n, out)
	}
	if err != nil {
		return err
	}
	out.Entries = make([]Entry, 0, len(ranked))
	for _, r := range ranked {
		out.Entries = append(out.Entries, Entry{Path: r.Path, Value: r.Value})
	}
	return nil
}

func (s *Server) topPrefixes(ctx context.Context, db idudb.Reader, sr *summary.Reader, f iduconfig.Filesystem, path string, metric stats.Metric, args TopNArgs, n int, out *TopN) error {
	var top summary.TopN
	var err error
	switch {
	case args.User != "":
		uid, uerr := usernames.Manager.UIDForName(args.User)
		if uerr != nil {
			return fmt.Errorf("unknown user: %q", args.User)
		}
		top, err = sr.TopPrefixesForUser(ctx, uid, metric, n)
	case args.Group != "":
		gid, gerr := usernames.Manager.GIDForName(args.Group)
		if gerr != nil {
			return fmt.Errorf("unknown group: %q", args.Group)
		}
		top, err = sr.TopPrefixesForGroup(ctx, gid, metric, n)
	default:
		top, err = sr.TopPrefixes(ctx, metric, n)
	}
	if (errors.Is(err, summary.ErrNoTopN) || errors.Is(err, summary.ErrStaleTopN)) && args.User == "" && args.Group == "" {
		return s.computed(ctx, db, f, path, metric, n, out)
	}
	if err != nil {
		return err
	}
	keep := min(len(top.Entries), n)
	out.Entries = make([]Entry, 0, keep)
	for _, r := range top.Entries[:keep] {
		out.Entries = append(out.Entries, Entry{Path: r.Path, Value: r.Value})
	}
	out.Exact = top.Certified >= len(out.Entries)
	return nil
}

func (s *Server) topIDs(ctx context.Context, db idudb.Reader, sr *summary.Reader, f iduconfig.Filesystem, path string, metric stats.Metric, n int, out *TopN) error {
	rank := sr.TopUsers
	if out.By == "group" {
		rank = sr.TopGroups
	}
	ranked, err := rank(ctx, metric, n)
	if errors.Is(err, summary.ErrNoTopN) || errors.Is(err, summary.ErrStaleTopN) {
		return s.computed(ctx, db, f, path, metric, n, out)
	}
	if err != nil {
		return err
	}
	out.Entries = make([]Entry, 0, len(ranked))
	for _, r := range ranked {
		out.Entries = append(out.Entries, s.idEntry(out.By, r.ID, r.Value))
	}
	return nil
}

func (s *Server) idEntry(by string, id, value int64) Entry {
	e := Entry{ID: &id, Value: value}
	if by == "group" {
		e.Group = usernames.Manager.NameForGID(id)
	} else {
		e.User = usernames.Manager.NameForUID(id)
	}
	return e
}

// computed ranks the entries beneath path by a pass over its summaries,
// whose cost is proportional to the size of the subtree rooted at path.
func (s *Server) computed(ctx context.Context, db idudb.Reader, f iduconfig.Filesystem, path string, metric stats.Metric, n int, out *TopN) error {
	sdb, err := snapshot.Computer{}.FromSummaries(ctx, db, f.Config(), path, n)
	if err != nil {
		return err
	}
	out.Source, out.Exact = sourceComputed, true
	switch out.By {
	case "subtree":
		appendPaths(out, heapFor(sdb.Subtree, metric), n)
	case "prefix":
		appendPaths(out, heapFor(sdb.Prefix, metric), n)
	case "user":
		appendIDs(s, out, heapFor(sdb.ByUser, metric), n)
	case "group":
		appendIDs(s, out, heapFor(sdb.ByGroup, metric), n)
	}
	return nil
}

func appendPaths(out *TopN, h *heap.MinMax[int64, string], n int) {
	if h == nil {
		return
	}
	values, paths := report.PopN(h, n)
	out.Entries = make([]Entry, 0, len(values))
	for i := range values {
		out.Entries = append(out.Entries, Entry{Path: paths[i], Value: values[i]})
	}
}

func appendIDs(s *Server, out *TopN, h *heap.MinMax[int64, int64], n int) {
	if h == nil {
		return
	}
	values, ids := report.PopN(h, n)
	out.Entries = make([]Entry, 0, len(values))
	for i := range values {
		out.Entries = append(out.Entries, s.idEntry(out.By, ids[i], values[i]))
	}
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

// ErrorsArgs are the arguments to errors.
type ErrorsArgs struct {
	FSArgs
	N int `json:"n,omitempty" jsonschema:"the maximum number of errors to return, 20 by default"`
}

// ErrorEntry is an error recorded by analyze.
type ErrorEntry struct {
	Path    string    `json:"path"`
	When    time.Time `json:"when"`
	Message string    `json:"message"`
}

// Errors is the result of errors.
type Errors struct {
	Path   string       `json:"path"`
	Errors []ErrorEntry `json:"errors"`
	More   bool         `json:"more" jsonschema:"true if there are more errors than were returned"`
}

func (s *Server) errors(ctx context.Context, _ *mcp.CallToolRequest, args ErrorsArgs) (*mcp.CallToolResult, Errors, error) {
	f, path, err := s.lookup(args.FS, args.Prefix)
	if err != nil {
		return nil, Errors{}, err
	}
	out := Errors{Path: path, Errors: []ErrorEntry{}}
	n := limit(args.N)
	sep := f.Config().SeparatorOrDefault()
	err = s.withDB(ctx, f, func(db idudb.Reader, _ *summary.Reader) error {
		return db.VisitErrors(ctx, path, func(_ context.Context, key string, when time.Time, detail []byte) bool {
			// Errors are visited in key order, starting at path.
			match, stop := internal.SubtreeMatch(key, path, sep)
			if stop {
				return false
			}
			if !match {
				return true
			}
			if len(out.Errors) == n {
				out.More = true
				return false
			}
			out.Errors = append(out.Errors, ErrorEntry{Path: key, When: when, Message: string(detail)})
			return true
		})
	})
	return nil, out, err
}
