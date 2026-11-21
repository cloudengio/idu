// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package query defines the row model for the data that idu maintains for
// an analyzed filesystem, and queries that return rows.
//
// There are three kinds of row: prefixes, ie. directories, users and
// groups, which have direct totals, for their own files, and recursive
// totals, for everything beneath them, and hotspots, prefixes whose
// subtrees accumulate substantial usage spread across many descendants.
//
// Queries are answered only by looking up the data that analyze maintains,
// rather than by a pass over all of the prefixes beneath the one being
// queried or by an export of every row: the summaries and recursive totals
// of individual prefixes, the top-N rankings and per-user and per-group
// totals maintained by analyze, and the most recent snapshot. A query that
// would require a pass, or an export, returns an error that wraps
// ErrUnsupported and says why. The cost of every supported query is
// therefore proportional to the number of rows it returns, or the number
// of children of a prefix, rather than to the size of the filesystem.
//
// The supported queries are:
//
//   - Prefixes, Self: the row for a single prefix.
//   - Prefixes, Children: the immediate children of a prefix, ordered by
//     any direct or recursive column.
//   - Prefixes, Subtree, ordered by a recursive column: the prefixes
//     beneath any prefix with the largest recursive totals, ie. where the
//     space is used, which is always exact.
//   - Prefixes, Subtree, ordered by a direct column: the prefixes with the
//     largest totals for their own files, for the entire filesystem only,
//     optionally for the files of a single user or group.
//   - Users or Groups, Self: the users or groups of the files in a single
//     prefix, not those beneath it.
//   - Users or Groups, Subtree: the users or groups with the largest
//     totals, for the entire filesystem only.
//   - Hotspots, Subtree: the hotspots for the entire filesystem, from a
//     snapshot that includes them, see idu stats compute --hotspots.
//
// Filter expressions, grouping by levels of the hierarchy other than a
// prefix's children, and rows for individual files are not supported.
package query

import (
	"errors"
	"fmt"
	"strings"

	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/stats"
)

// Table identifies the kind of rows returned by a query.
type Table string

// The tables.
const (
	Prefixes Table = "prefixes"
	Users    Table = "users"
	Groups   Table = "groups"
	Hotspots Table = "hotspots"
)

// Scope identifies the rows, relative to Query.Within, that a query ranges
// over.
type Scope string

// The scopes.
const (
	// Self is the prefix itself, or for users and groups, the users or
	// groups of its own files.
	Self Scope = "self"
	// Children are the prefix's immediate children.
	Children Scope = "children"
	// Subtree is the prefix and everything beneath it.
	Subtree Scope = "subtree"
)

// Column is a column that rows can be ordered by: a metric of either the
// direct or the recursive totals.
type Column struct {
	Metric    stats.Metric
	Recursive bool
}

// Direct returns the Column for m of the direct totals.
func Direct(m stats.Metric) Column { return Column{Metric: m} }

// Total returns the Column for m of the recursive totals.
func Total(m stats.Metric) Column { return Column{Metric: m, Recursive: true} }

// String returns the name of the column, eg. total_storage_bytes or
// direct_files.
func (c Column) String() string {
	kind := "direct_"
	if c.Recursive {
		kind = "total_"
	}
	return kind + strings.ReplaceAll(c.Metric.String(), "-", "_")
}

// Columns returns the names of all of the columns.
func Columns() []string {
	var names []string
	for _, recursive := range []bool{true, false} {
		for _, m := range stats.Metrics() {
			names = append(names, Column{m, recursive}.String())
		}
	}
	return names
}

// ParseColumn parses the name of a column, see Column.String.
func ParseColumn(name string) (Column, error) {
	var c Column
	rest, ok := strings.CutPrefix(name, "total_")
	if ok {
		c.Recursive = true
	} else if rest, ok = strings.CutPrefix(name, "direct_"); !ok {
		return Column{}, fmt.Errorf("unknown column %q, it must be one of: %v", name, strings.Join(Columns(), ", "))
	}
	m, err := stats.ParseMetric(strings.ReplaceAll(rest, "_", "-"))
	if err != nil {
		return Column{}, fmt.Errorf("unknown column %q, it must be one of: %v", name, strings.Join(Columns(), ", "))
	}
	c.Metric = m
	return c, nil
}

// DefaultLimit is the number of rows returned by a query that does not
// specify a limit.
const DefaultLimit = 20

// Query describes the rows to return.
type Query struct {
	// Table is the kind of rows, Prefixes if not set.
	Table Table
	// Within is the prefix that the query is relative to, the root of the
	// filesystem if empty; it is a path, not relative to the root.
	Within string
	// Scope is the rows, relative to Within, that the query ranges over,
	// Subtree if not set.
	Scope Scope
	// OrderBy is the column that the rows are ordered by, largest first.
	// It defaults to total_storage_bytes, or for Self and Children of
	// users and groups, direct_storage_bytes.
	OrderBy *Column
	// UID and GID restrict a ranking of prefixes to the usage of the files
	// owned by a single user or group.
	UID, GID *int64
	// Limit is the maximum number of rows to return, DefaultLimit if zero.
	Limit int
	// Filter is an expression, as accepted by idu find and stats, that
	// rows must match. Filters are not supported.
	Filter string
	// Level groups the rows by the prefixes at the specified depth below
	// the root. Only the depth of Within's children is supported, which is
	// equivalent to Scope Children.
	Level int
}

// Row is a row of the row model.
type Row struct {
	// Path, Parent, Name and Depth are set for prefixes and hotspots.
	// Depth is the number of path components below the filesystem's root.
	Path   string `json:"path,omitempty"`
	Parent string `json:"parent,omitempty"`
	Name   string `json:"name,omitempty"`
	Depth  int    `json:"depth"`
	// ID is set for users and groups.
	ID *int64 `json:"id,omitempty"`
	// Direct and Total are the direct and recursive totals; either may be
	// nil if not known, eg. the recursive totals are not maintained per
	// prefix for users and groups.
	Direct *stats.Totals `json:"direct,omitempty"`
	Total  *stats.Totals `json:"total,omitempty"`
	// Hotspot is set for hotspots.
	Hotspot *report.DistributedStats `json:"hotspot,omitempty"`
	// Value is the value of the column that the rows are ordered by.
	Value int64 `json:"value"`
}

// Result is the result of a query.
type Result struct {
	// Rows are in descending order of Value.
	Rows []Row
	// OrderBy is the column, or for hotspots, the metric, that the rows
	// are ordered by.
	OrderBy string
	// Exact is true if the rows are known to be exactly the top rows; it
	// may be false for rankings maintained by analyze whose lower entries
	// are not certified, see summary.TopN.
	Exact bool
	// Source describes where the rows came from.
	Source string
}

// ErrUnsupported is wrapped by the errors returned for queries that cannot
// be answered by looking up the data maintained by analyze.
var ErrUnsupported = errors.New("unsupported query")

// UnsupportedError is returned for a query that cannot be answered by
// looking up the data maintained by analyze.
type UnsupportedError struct {
	// Reason explains why, and what is supported instead.
	Reason string
}

func (e *UnsupportedError) Error() string {
	return "unsupported query: " + e.Reason
}

// Unwrap returns ErrUnsupported.
func (e *UnsupportedError) Unwrap() error {
	return ErrUnsupported
}

func unsupported(format string, args ...any) error {
	return &UnsupportedError{Reason: fmt.Sprintf(format, args...)}
}
