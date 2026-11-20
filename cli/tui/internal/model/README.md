# Package [cloudeng.io/cmd/idu/cli/tui/internal/model](https://pkg.go.dev/cloudeng.io/cmd/idu/cli/tui/internal/model?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/cli/tui/internal/model)](https://goreportcard.com/report/cloudeng.io/cmd/idu/cli/tui/internal/model)

```go
import cloudeng.io/cmd/idu/cli/tui/internal/model
```

Package model provides the data displayed by the tui: the tree of prefixes,
their summaries and the top-N rankings beneath them. It is independent
of the widgets used to display them and is safe for concurrent use from
background goroutines.

## Variables
### ErrNotAnalyzed
```go
ErrNotAnalyzed = errors.New("the filesystem has not been analyzed")

```
ErrNotAnalyzed is returned by Open if the filesystem has not been analyzed.



## Types
### Type DB
```go
type DB struct {
	// contains filtered or unexported fields
}
```
DB is a Source backed by a filesystem's database.

### Functions

```go
func Open(ctx context.Context, env *cli.Env, f iduconfig.Filesystem, snapshotDir string, log io.Writer) (*DB, error)
```
Open opens the database for f, read-only, as a Source. Snapshots are read
from, and full passes saved to, snapshotDir if set. Notes from computing the
rankings are written to log. It returns ErrNotAnalyzed, with the database
closed, if the database has never been analyzed.



### Methods

```go
func (s *DB) Children(ctx context.Context, path string) ([]Node, error)
```
Children implements Source.


```go
func (s *DB) Close(ctx context.Context) error
```
Close implements Source.


```go
func (s *DB) Errors(ctx context.Context, path string, limit int) ([]ErrorEntry, bool, error)
```
Errors implements Source.


```go
func (s *DB) LastUpdate(ctx context.Context) time.Time
```
LastUpdate returns the time of the most recent analysis.


```go
func (s *DB) Root() string
```
Root implements Source.


```go
func (s *DB) Summary(ctx context.Context, path string) (Summary, error)
```
Summary implements Source.


```go
func (s *DB) TopN(ctx context.Context, path string, n int) (TopN, error)
```
TopN implements Source. For the root they are the current stats, ie.
those displayed by stats view; for any other prefix they are computed by a
pass over the summaries beneath it. Results are cached.


```go
func (s *DB) Warning() (string, bool)
```
Warning implements Source.




### Type ErrorEntry
```go
type ErrorEntry struct {
	// Path is the file or prefix that the error relates to.
	Path    string
	When    time.Time
	Message string
}
```
ErrorEntry is an error recorded by an analysis.


### Type Node
```go
type Node struct {
	Path, Name string
	// Direct contains the totals for the prefix's own files.
	Direct stats.Totals
	// Subtree contains the recursive totals for the prefix and everything
	// beneath it, it is only valid if HasSubtree is true.
	Subtree    stats.Totals
	HasSubtree bool
}
```
Node is a prefix in the tree.

### Methods

```go
func (n Node) HasChildren() bool
```
HasChildren returns true if the prefix has any child prefixes.


```go
func (n Node) Size() int64
```
Size returns the storage used by the subtree, if known, and by the prefix's
own files otherwise.


```go
func (n Node) Totals() stats.Totals
```
Totals returns the subtree's totals, if known, and the prefix's own totals
otherwise.




### Type Ranked
```go
type Ranked[T any] struct {
	Value int64
	Item  T
}
```
Ranked is an entry in a ranking.


### Type Source
```go
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
	// Errors returns the errors recorded by the most recent analyses for
	// path and the files and prefixes beneath it, in key order, up to
	// limit of them, and whether there were more than limit.
	Errors(ctx context.Context, path string, limit int) ([]ErrorEntry, bool, error)
	// LastUpdate returns the time of the most recent analysis, if known.
	LastUpdate(ctx context.Context) time.Time
	// Warning returns a description of any limitations of the data, eg.
	// that hardlinks may be counted more than once, or the empty string,
	// and whether analyzing the filesystem would remove them.
	Warning() (msg string, analyzeFixes bool)
	// Close releases any resources, eg. the database.
	Close(ctx context.Context) error
}
```
Source provides the data displayed by the tui.


### Type Summary
```go
type Summary struct {
	Node
	Users, Groups stats.PerIDTotals
	Info          summary.Info
	HasInfo       bool
}
```
Summary describes a single prefix.


### Type TopN
```go
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
```
TopN contains the top-N rankings beneath a prefix. Unlike the heaps
in report.AllStats, from which they are constructed, they may be read
repeatedly and concurrently.





