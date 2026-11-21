// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package mcpserver

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/query"
	"cloudeng.io/cmd/idu/report"
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
			"prefix, the prefixes ranked by their own files only; user or group, the users or groups ranked by their usage; hotspot, prefixes whose subtrees accumulate substantial usage spread across many descendants. " +
			"Rankings by subtree, and the others for the entire filesystem, use the rankings maintained by analyze; the others beneath any other prefix require a pass over the summaries beneath it. " +
			"For by=prefix, user may be set to rank the prefixes by the usage of that user's files, and similarly for group.",
		Annotations: readOnly,
	}, s.topN)
	mcp.AddTool(s.srv, &mcp.Tool{
		Name:        "query",
		Description: queryDescription,
		Annotations: readOnly,
	}, s.query)
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

// run runs q, with Within set to prefix, against the filesystem fs.
func (s *Server) run(ctx context.Context, fs, prefix string, q query.Query) (query.Result, string, error) {
	f, path, err := s.lookup(fs, prefix)
	if err != nil {
		return query.Result{}, "", err
	}
	q.Within = path
	var res query.Result
	err = s.withDB(ctx, f, func(db idudb.Reader, _ *summary.Reader) error {
		var err error
		res, err = query.New(f, db).Run(ctx, q)
		return err
	})
	return res, path, err
}

func (s *Server) usage(ctx context.Context, _ *mcp.CallToolRequest, args FSArgs) (*mcp.CallToolResult, Usage, error) {
	res, path, err := s.run(ctx, args.FS, args.Prefix, query.Query{Scope: query.Self})
	if err != nil {
		return nil, Usage{}, err
	}
	row := res.Rows[0]
	out := Usage{Path: path, Direct: newTotals(*row.Direct)}
	if row.Total != nil {
		rt := newTotals(*row.Total)
		out.Recursive = &rt
	} else {
		out.Note = idudb.ErrStaleSubtrees.Error()
	}
	return nil, out, nil
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
	metric, err := parseMetric(args.Metric)
	if err != nil {
		return nil, Children{}, err
	}
	// All of the children are requested so that their number is known.
	q := query.Query{Scope: query.Children, OrderBy: new(query.Total(metric)), Limit: query.MaxLimit}
	res, path, err := s.run(ctx, args.FS, args.Prefix, q)
	note := ""
	if errors.Is(err, idudb.ErrStaleSubtrees) {
		note = "the children are ordered by their own files only: " + err.Error()
		q.OrderBy = new(query.Direct(metric))
		res, path, err = s.run(ctx, args.FS, args.Prefix, q)
	}
	if err != nil {
		return nil, Children{}, err
	}
	out := Children{Path: path, Metric: metric.String(), Total: len(res.Rows), Note: note}
	rows := res.Rows[:min(len(res.Rows), limit(args.N))]
	out.Children = make([]Child, 0, len(rows))
	for _, r := range rows {
		c := Child{Path: r.Path, Direct: newTotals(*r.Direct)}
		if r.Total != nil {
			rt := newTotals(*r.Total)
			c.Recursive = &rt
		}
		out.Children = append(out.Children, c)
	}
	return nil, out, nil
}

// TopNArgs are the arguments to top_n.
type TopNArgs struct {
	FSArgs
	Metric string `json:"metric,omitempty" jsonschema:"the metric to rank by, storage_bytes by default"`
	By     string `json:"by,omitempty" jsonschema:"what to rank: subtree (the default), prefix, user, group or hotspot"`
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

// ids returns the uid and gid for the user and group names, either of
// which may be empty.
func ids(user, group string) (uid, gid *int64, err error) {
	if user != "" {
		id, err := usernames.Manager.UIDForName(user)
		if err != nil {
			return nil, nil, fmt.Errorf("unknown user: %q", user)
		}
		uid = &id
	}
	if group != "" {
		id, err := usernames.Manager.GIDForName(group)
		if err != nil {
			return nil, nil, fmt.Errorf("unknown group: %q", group)
		}
		gid = &id
	}
	return uid, gid, nil
}

func (s *Server) topN(ctx context.Context, _ *mcp.CallToolRequest, args TopNArgs) (*mcp.CallToolResult, TopN, error) {
	metric, err := parseMetric(args.Metric)
	if err != nil {
		return nil, TopN{}, err
	}
	by := cmp.Or(args.By, "subtree")
	q := query.Query{Scope: query.Subtree, OrderBy: new(query.Total(metric)), Limit: limit(args.N)}
	switch by {
	case "subtree":
	case "prefix":
		q.OrderBy = new(query.Direct(metric))
	case "user":
		q.Table = query.Users
	case "group":
		q.Table = query.Groups
	case "hotspot":
		q.Table = query.Hotspots
	default:
		return nil, TopN{}, fmt.Errorf("unknown value for by: %q, it must be one of subtree, prefix, user, group or hotspot", by)
	}
	if q.UID, q.GID, err = ids(args.User, args.Group); err != nil {
		return nil, TopN{}, err
	}
	res, path, err := s.run(ctx, args.FS, args.Prefix, q)
	if err != nil {
		return nil, TopN{}, err
	}
	out := TopN{Path: path, Metric: metric.String(), By: by, Exact: res.Exact, Source: res.Source, Entries: make([]Entry, 0, len(res.Rows))}
	for _, r := range res.Rows {
		e := Entry{Path: r.Path, ID: r.ID, Value: r.Value}
		s.setName(q.Table, r.ID, &e.User, &e.Group)
		out.Entries = append(out.Entries, e)
	}
	return nil, out, nil
}

// setName sets user or group to the name of id, if any, according to table.
func (s *Server) setName(table query.Table, id *int64, user, group *string) {
	switch {
	case id == nil:
	case table == query.Groups:
		*group = usernames.Manager.NameForGID(*id)
	default:
		*user = usernames.Manager.NameForUID(*id)
	}
}

// queryDescription describes the query tool, see the query package.
var queryDescription = "Query the rows of the row model for a filesystem: prefixes (directories), users, groups and hotspots, each with its direct totals, for its own files, and recursive totals, for everything beneath it. " +
	"Rows are ordered by a column, largest first, one of: " + strings.Join(query.Columns(), ", ") + ". " +
	"Queries are answered by looking up the totals, rankings and snapshots maintained by analyze when possible, so that their cost is proportional to the rows returned, and otherwise by a pass over the summaries beneath the prefix, whose cost is proportional to the size of the subtree; source says which. " +
	"table=prefixes: scope=self, the prefix itself; scope=children (or level set to the depth of its children), its children, ordered by any column; " +
	"scope=subtree, the prefixes beneath the prefix with the largest value of the column: a total_ column for where the space is used, a direct_ column for the prefixes' own files, optionally for a single user or group; " +
	"level set to a depth below the children, the prefixes at that depth. " +
	"table=users or groups: scope=self with a direct_ column, the users or groups of the prefix's own files; scope=subtree with a total_ column, their usage beneath the prefix. " +
	"table=hotspots, scope=subtree: prefixes whose subtrees accumulate substantial usage spread across many descendants, where neither a single directory nor a single child dominates, ordered by their distributed bytes, storage bytes or files (use a total_bytes, total_storage_bytes or total_files column); each row includes the hotspot's subtree, direct and largest leaf totals and its amplification. " +
	"filter is an expression, as described by idu expression-syntax, eg. user=<name>, file-larger=<size>, name=<glob>, newer=<time>, combined with && || !; it requires a pass over the metadata of the files beneath the prefix, is only supported for scope=subtree, and since directories must match for their files to be counted, use eg. type=d || file-larger=1GB. " +
	"Rows for individual files are not supported."

// QueryArgs are the arguments to query.
type QueryArgs struct {
	FSArgs
	Table   string `json:"table,omitempty" jsonschema:"prefixes (the default), users, groups or hotspots"`
	Scope   string `json:"scope,omitempty" jsonschema:"self, children or subtree (the default), relative to the prefix"`
	OrderBy string `json:"order_by,omitempty" jsonschema:"the column to order the rows by, largest first, total_storage_bytes by default, or direct_storage_bytes for the users or groups of a prefix"`
	User    string `json:"user,omitempty" jsonschema:"rank prefixes by the usage of this user's files, a name or numeric id"`
	Group   string `json:"group,omitempty" jsonschema:"rank prefixes by the usage of this group's files, a name or numeric id"`
	Level   int    `json:"level,omitempty" jsonschema:"group by the prefixes at this depth below the root, only the depth of the prefix's children is supported"`
	Filter  string `json:"filter,omitempty" jsonschema:"a filter expression, not supported"`
	N       int    `json:"n,omitempty" jsonschema:"the maximum number of rows to return, 20 by default"`
}

// QueryRow is a row returned by query.
type QueryRow struct {
	Path    string                   `json:"path,omitempty"`
	Parent  string                   `json:"parent,omitempty"`
	Name    string                   `json:"name,omitempty"`
	Depth   int                      `json:"depth"`
	ID      *int64                   `json:"id,omitempty" jsonschema:"the numeric id of the user or group"`
	User    string                   `json:"user,omitempty"`
	Group   string                   `json:"group,omitempty"`
	Direct  *Totals                  `json:"direct,omitempty" jsonschema:"the totals for the prefix's, user's or group's own files, if known"`
	Total   *Totals                  `json:"total,omitempty" jsonschema:"the recursive totals, if known"`
	Hotspot *report.DistributedStats `json:"hotspot,omitempty"`
	Value   int64                    `json:"value" jsonschema:"the value of the column that the rows are ordered by"`
}

// QueryResult is the result of query.
type QueryResult struct {
	Path    string     `json:"path"`
	Table   string     `json:"table"`
	Scope   string     `json:"scope"`
	OrderBy string     `json:"order_by"`
	Rows    []QueryRow `json:"rows"`
	Exact   bool       `json:"exact" jsonschema:"true if the rows are known to be exactly the top rows"`
	Source  string     `json:"source" jsonschema:"where the rows came from"`
}

func (s *Server) query(ctx context.Context, _ *mcp.CallToolRequest, args QueryArgs) (*mcp.CallToolResult, QueryResult, error) {
	q := query.Query{
		Table:  query.Table(args.Table),
		Scope:  query.Scope(args.Scope),
		Level:  args.Level,
		Filter: args.Filter,
		Limit:  limit(args.N),
	}
	if args.OrderBy != "" {
		c, err := query.ParseColumn(args.OrderBy)
		if err != nil {
			return nil, QueryResult{}, err
		}
		q.OrderBy = &c
	}
	var err error
	if q.UID, q.GID, err = ids(args.User, args.Group); err != nil {
		return nil, QueryResult{}, err
	}
	res, path, err := s.run(ctx, args.FS, args.Prefix, q)
	if err != nil {
		return nil, QueryResult{}, err
	}
	out := QueryResult{
		Path:    path,
		Table:   string(cmp.Or(q.Table, query.Prefixes)),
		Scope:   string(cmp.Or(q.Scope, query.Subtree)),
		OrderBy: res.OrderBy,
		Rows:    make([]QueryRow, 0, len(res.Rows)),
		Exact:   res.Exact,
		Source:  res.Source,
	}
	for _, r := range res.Rows {
		qr := QueryRow{Path: r.Path, Parent: r.Parent, Name: r.Name, Depth: r.Depth, ID: r.ID, Hotspot: r.Hotspot, Value: r.Value}
		s.setName(q.Table, r.ID, &qr.User, &qr.Group)
		if r.Direct != nil {
			qr.Direct = new(newTotals(*r.Direct))
		}
		if r.Total != nil {
			qr.Total = new(newTotals(*r.Total))
		}
		out.Rows = append(out.Rows, qr)
	}
	return nil, out, nil
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
