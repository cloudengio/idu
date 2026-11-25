# Package [cloudeng.io/cmd/idu/query](https://pkg.go.dev/cloudeng.io/cmd/idu/query?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/query)](https://goreportcard.com/report/cloudeng.io/cmd/idu/query)

```go
import cloudeng.io/cmd/idu/query
```

Package query defines the row model for the data that idu maintains for an
analyzed filesystem, and queries that return rows.

There are three kinds of row: prefixes, ie. directories, users and groups,
which have direct totals, for their own files, and recursive totals, for
everything beneath them, and hotspots, prefixes whose subtrees accumulate
substantial usage spread across many descendants.

The rows are not stored, or exported, as such: queries are answered
by looking up the data that analyze maintains whenever possible, ie.
the summaries and recursive totals of individual prefixes, the top-N
rankings and per-user and per-group totals, and the most recent snapshot,
so that their cost is proportional to the number of rows returned, or the
number of children of a prefix. Otherwise, they are answered by a pass over
the summaries beneath the prefix being queried, or, for a filter expression,
over the metadata of its files, whose cost is proportional to the size of
the subtree; WithLookupsOnly restricts an Engine to lookups, in which case
such queries fail with an error that wraps ErrUnsupported and says why.
Result.Source says how the rows were obtained.

The queries answered by lookups are:

  - Prefixes, Self: the row for a single prefix.
  - Prefixes, Children, or Level equal to the depth of the children:
    the immediate children of a prefix, ordered by any column.
  - Prefixes, Subtree, ordered by a recursive column: the prefixes beneath
    any prefix with the largest recursive totals, ie. where the space is
    used, by a best-first search that is always exact.
  - Prefixes, Subtree, ordered by a direct column: the prefixes with the
    largest totals for their own files, for the entire filesystem.
  - Users or Groups, Self, ordered by a direct column: the users or groups
    of the files in a single prefix.
  - Users or Groups, Subtree: the users or groups with the largest totals,
    for the entire filesystem.
  - Hotspots, Subtree: the hotspots for the entire filesystem, from a
    snapshot that includes them, see idu stats compute --hotspots.

Those that require a pass are: rankings of prefixes by a direct column,
or for a single user or group, and of users, groups and hotspots, beneath a
prefix other than the root; grouping by a level below that of the children;
and any query with a filter, which is only supported for rankings, ie.
scope Subtree. Rows for individual files are not supported.

## Constants
### SourcePrefix, SourceChildren, SourceBestFirst, SourceRankings, SourceIDTotals, SourceSnapshotOf, SourcePassOf
```go
const (
	SourcePrefix     = "the summary and recursive totals of the prefix"
	SourceChildren   = "the summaries and recursive totals of the prefix's children"
	SourceBestFirst  = "a best-first search of the recursive totals"
	SourceRankings   = "the rankings maintained by analyze"
	SourceIDTotals   = "the per-user and per-group totals maintained by analyze"
	SourceSnapshotOf = "the snapshot "
	// SourcePassOf prefixes the descriptions of the rows computed by a
	// pass over the summaries, or, for a filter, the metadata of the files
	// beneath a prefix.
	SourcePassOf = "a pass over "
)
```
The sources of the rows in a Result.

### DefaultLimit
```go
const DefaultLimit = 20
```
DefaultLimit is the number of rows returned by a query that does not specify
a limit.

### MaxLimit
```go
const MaxLimit = 10000
```
MaxLimit is the maximum number of rows that a query may return.



## Variables
### ErrUnsupported
```go
ErrUnsupported = errors.New("unsupported query")

```
ErrUnsupported is wrapped by the errors returned for queries that cannot be
answered by looking up the data maintained by analyze.



## Functions
### Func Columns
```go
func Columns() []string
```
Columns returns the names of all of the columns.



## Types
### Type Column
```go
type Column struct {
	Metric    stats.Metric
	Recursive bool
}
```
Column is a column that rows can be ordered by: a metric of either the
direct or the recursive totals.

### Functions

```go
func Direct(m stats.Metric) Column
```
Direct returns the Column for m of the direct totals.


```go
func ParseColumn(name string) (Column, error)
```
ParseColumn parses the name of a column, see Column.String.


```go
func Total(m stats.Metric) Column
```
Total returns the Column for m of the recursive totals.



### Methods

```go
func (c Column) String() string
```
String returns the name of the column, eg. total_storage_bytes or
direct_files.




### Type Engine
```go
type Engine struct {
	// contains filtered or unexported fields
}
```
Engine answers queries for a single filesystem.

### Functions

```go
func New(f iduconfig.Filesystem, db idudb.Reader, opts ...Option) *Engine
```
New returns an Engine for the filesystem f whose database, opened for
reading, is db.



### Methods

```go
func (e *Engine) Run(ctx context.Context, q Query) (Result, error)
```
Run runs q. Queries are answered by looking up the data maintained
by analyze whenever possible and otherwise, unless WithLookupsOnly is
specified, by a pass over the summaries, or the file metadata, beneath
q.Within. It returns an error that wraps ErrUnsupported for queries that
cannot be answered.




### Type Option
```go
type Option func(*Engine)
```
Option configures an Engine.

### Functions

```go
func WithLookupsOnly() Option
```
WithLookupsOnly restricts the Engine to the queries that can be answered
by looking up the data maintained by analyze, the others fail with an error
that wraps ErrUnsupported rather than performing a pass over the summaries,
or file metadata, beneath the prefix being queried.


```go
func WithSnapshotDir(dir string) Option
```
WithSnapshotDir sets the directory in which snapshots are looked for, the
filesystem's snapshot_dir by default. An empty dir means that no snapshots
are used.




### Type Query
```go
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
	// files must match; it is only supported for rankings, ie. Scope
	// Subtree without Level.
	Filter string
	// Level groups the rows by the prefixes at the specified depth below
	// the root. Only depths below Within are supported; the depth of
	// Within's children is equivalent to Scope Children.
	Level int
}
```
Query describes the rows to return.


### Type Result
```go
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
```
Result is the result of a query.


### Type Row
```go
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
```
Row is a row of the row model.


### Type Scope
```go
type Scope string
```
Scope identifies the rows, relative to Query.Within, that a query ranges
over.

### Constants
### Self, Children, Subtree
```go
const (
	// Self is the prefix itself, or for users and groups, the users or
	// groups of its own files.
	Self Scope = "self"
	// Children are the prefix's immediate children.
	Children Scope = "children"
	// Subtree is the prefix and everything beneath it.
	Subtree Scope = "subtree"
)
```
The scopes.




### Type Table
```go
type Table string
```
Table identifies the kind of rows returned by a query.

### Constants
### Prefixes, Users, Groups, Hotspots
```go
const (
	Prefixes Table = "prefixes"
	Users    Table = "users"
	Groups   Table = "groups"
	Hotspots Table = "hotspots"
)
```
The tables.




### Type UnsupportedError
```go
type UnsupportedError struct {
	// Reason explains why, and what is supported instead.
	Reason string
}
```
UnsupportedError is returned for a query that cannot be answered by looking
up the data maintained by analyze.

### Methods

```go
func (e *UnsupportedError) Error() string
```


```go
func (e *UnsupportedError) Unwrap() error
```
Unwrap returns ErrUnsupported.







