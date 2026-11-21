# Package [cloudeng.io/cmd/idu/snapshot](https://pkg.go.dev/cloudeng.io/cmd/idu/snapshot?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/snapshot)](https://goreportcard.com/report/cloudeng.io/cmd/idu/snapshot)

```go
import cloudeng.io/cmd/idu/snapshot
```

Package snapshot provides the statistics, ie. the totals and top-N
rankings described by report.AllStats, for a filesystem analyzed by idu,
and the snapshots in which they are stored. It is used by idu's stats,
reports and tui commands and may be used by other commands to implement
their own displays or reports of the same statistics.

A snapshot, see File, is written by analyze --snapshot and stats compute to
a filesystem's snapshot_dir, see Store, in which each snapshot is named by
the time it was created and a link, named latest, refers to the most recent.

Computer.Current returns the current statistics for a filesystem:
the most recent snapshot that is newer than the last update to its database,
otherwise statistics constructed from the totals and rankings maintained by
analyze, otherwise statistics computed by a full pass over the summaries,
which are saved as a new snapshot. For example:

    cfg, _, err := iduconfig.Load(ctx, &cmdconfig.FilesFlag{})
    f := cfg.Filesystems[0]
    sf, path, err := snapshot.Computer{}.Current(ctx, f,
    	f.Config().SnapshotDir, 100, defaultdb.OpenOptions()...)
    keys, prefixes := report.PopN(sf.Stats.Subtree.Bytes, 10)

Note that the heaps in report.AllStats are consumed as they are read.

## Constants
### FromSummariesSource, FromStoredSource
```go
const (
	// FromSummariesSource denotes stats computed by a pass over all of the
	// summaries, or the file metadata, of the prefix.
	FromSummariesSource = "summaries"
	// FromStoredSource denotes stats constructed from the recursive totals
	// and rankings maintained by analyze. They do not include the
	// distributed subtree statistics, and the rankings of prefixes for each
	// user and group are limited to topn_per_id entries.
	FromStoredSource = "stored"
)
```
The sources of the statistics in a stats file.

### DefaultN
```go
const DefaultN = 2000
```
DefaultN is the number of top entries computed for a snapshot that requires
a full pass over the summaries, so that it can be used for most subsequent
views and reports.

### Ext
```go
const Ext = ".idustats"
```
Ext is the extension of snapshot files.



## Variables
### ErrStoredUnusable
```go
ErrStoredUnusable = errors.New("the stored top-N rankings cannot be used")

```
ErrStoredUnusable is returned by FromStored when the stored state cannot be
used and the stats must be computed from the summaries instead.



## Functions
### Func IsFilesystemRoot
```go
func IsFilesystemRoot(cfg *iduconfig.Settings, root, target string) bool
```
IsFilesystemRoot returns true if target is the root of the filesystem,
which is the only prefix for which the stored rankings and recursive totals
can be used, since they deduplicate hardlinks across the entire filesystem.

### Func NewMatcher
```go
func NewMatcher(ctx context.Context, fwfs filewalk.FS, cfg *iduconfig.Settings, expr ...string) (stats.Matcher, error)
```
NewMatcher returns the matcher for expr, which may be empty, configured to
deduplicate hardlinks as specified by cfg.



## Types
### Type Computer
```go
type Computer struct {
	// Readers is the number of goroutines used to read prefix metadata
	// concurrently, defaultPrefixReaders if zero.
	Readers int
	// Progress requests that progress be displayed.
	Progress bool
	// Log is where notes, eg. why a full pass is required, and errors in
	// the metadata of individual prefixes are written, os.Stderr if nil.
	Log io.Writer
	// Lenient allows summaries that do not record the information needed
	// to deduplicate hardlinks as configured, see summary.Reader.Check, to
	// be used, in which case hardlinks may be counted more than once.
	Lenient bool
	// Hotspots and Hardlinks require that the stats returned by Current
	// include the distributed subtree hotspots and the rankings by number
	// of hardlinks respectively. Neither is included in the stats
	// constructed from the rankings maintained by analyze and hence either
	// requires a full pass over the database.
	Hotspots, Hardlinks bool
}
```
Computer computes stats from a database.

### Methods

```go
func (c Computer) Compute(ctx context.Context, db idudb.Reader, cfg *iduconfig.Settings, match stats.Matcher, prefix string, useStored bool, topN int) (*report.AllStats, string, int, error)
```
Compute computes stats for prefix and returns them together with their
source and, when FromStoredSource is used, the stored per-ID ranking limit.
If useStored is true, and prefix is the root of the filesystem, they are
constructed from the rankings and recursive totals maintained by analyze
when those are available and up to date. Otherwise they are computed from
the precomputed summaries unless they cannot be used, in which case all
of the file metadata is rescanned: summaries are computed without any
filter expression and only record what's needed to deduplicate hardlinks if
configured to do so.


```go
func (c Computer) Current(ctx context.Context, f iduconfig.Filesystem, dir string, n int, opts ...idudb.OpenOption) (File, string, error)
```
Current returns stats, with at least the top n entries, for the entire
filesystem f. They are, in order of preference:

 1. the most recent snapshot of the entire filesystem in dir, if dir is set,
    that was created after the database was last updated;
 2. stats constructed, in memory, from the rankings and recursive totals
    maintained by analyze, if they are usable;
 3. stats computed by a full pass over the summaries, which, since they
    are expensive to compute, are saved as a new snapshot in dir, if set,
    with at least DefaultN entries.

It returns the path of the snapshot used or saved, which is empty for
stats constructed in memory or not saved. The database is opened,
for reading only, using opts, which must include idudb.WithBackendFactory,
see defaultdb.OpenOptions.


```go
func (c Computer) CurrentFrom(ctx context.Context, db idudb.Reader, f iduconfig.Filesystem, dir string, n int) (File, string, error)
```
CurrentFrom is like Current but uses db, the open database for f.


```go
func (c Computer) FromPrefixes(ctx context.Context, db idudb.Reader, cfg *iduconfig.Settings, match stats.Matcher, prefix string, topN int) (*report.AllStats, error)
```
FromPrefixes computes stats from all of the file metadata stored for each
prefix. The prefixes are visited in DFS pre-order using the summary keys,
which exist for every prefix, so that memory usage is proportional to the
depth of the tree and hardlinks are attributed to the same prefixes as
when using the summaries. The metadata is read and decoded concurrently,
but the stats, including hardlink deduplication, are computed in order.


```go
func (c Computer) FromStored(ctx context.Context, db idudb.Reader, cfg *iduconfig.Settings, root string, n int) (*report.AllStats, int, error)
```
FromStored constructs stats for the entire filesystem, rooted at root,
from the recursive totals and rankings maintained by analyze, also returning
the per-ID ranking limit maintained in the stored state. It returns an
error that wraps ErrStoredUnusable if they are missing, may be out of date,
or do not contain the top n entries.


```go
func (c Computer) FromSummaries(ctx context.Context, db idudb.Reader, cfg *iduconfig.Settings, prefix string, topN int) (*report.AllStats, error)
```
FromSummaries computes stats for prefix from the precomputed summaries.


```go
func (c Computer) Satisfies(sf File) bool
```
Satisfies returns true if sf includes everything required by c.


```go
func (c Computer) Take(ctx context.Context, f iduconfig.Filesystem, dir string, opts ...idudb.OpenOption) (string, error)
```
Take computes stats for the entire filesystem f from its database and
saves them as a new snapshot in dir, returning its path. The stats are
constructed from the rankings and recursive totals maintained by analyze,
with topn entries, if they are usable, and by a full pass over the summaries
otherwise. The database is opened as for Current.




### Type File
```go
type File struct {
	Prefix     string
	Date       time.Time
	Expression string
	Stats      *report.AllStats
	// Source is FromSummariesSource or FromStoredSource, it is empty for
	// files written before stats could be constructed from the stored
	// state, which were always computed from the summaries.
	Source string
	// Whole is true if the stats are for the entire filesystem, ie. its
	// root, without a filter expression, and hence may be used in place
	// of the database by stats view and reports generate. It is false for
	// files written before it was introduced.
	Whole bool
	// PerIDN is the maximum number of entries in the rankings of prefixes
	// for each user and group when Source is FromStoredSource.
	PerIDN int
}
```
File is the format of a snapshot, ie. the files written by analyze
--snapshot and stats compute, and read by stats view and reports generate.

### Functions

```go
func Decode(rd io.Reader) (File, error)
```
Decode decodes a stats file from rd.


```go
func Load(filename string) (File, error)
```
Load reads a stats file, - denotes os.Stdin.



### Methods

```go
func (sf File) DescribeSource() string
```
DescribeSource describes the source of the stats in sf.


```go
func (sf File) Encode(w io.Writer) error
```
Encode writes sf to w.




### Type Store
```go
type Store struct {
	// contains filtered or unexported fields
}
```
Store is a directory of snapshots, each named by the time it was created,
with a link, named latest, to the most recent.

### Functions

```go
func NewStore(dir string) Store
```
NewStore returns the Store for dir.



### Methods

```go
func (s Store) Dir() string
```
Dir returns the directory of the store.


```go
func (s Store) FreshFor(sep, root string, since time.Time, accept func(File) bool) (File, string, bool, error)
```
FreshFor returns the most recent snapshot of the entire filesystem rooted at
root, ie. one for which Whole is true, that was created after since and for
which accept, if not nil, returns true, if there is one.


```go
func (s Store) LatestPath() string
```
LatestPath returns the path of the link to the most recent snapshot.


```go
func (s Store) Save(f File) (string, error)
```
Save writes f as a new snapshot, named by f.Date, and makes it the latest.







