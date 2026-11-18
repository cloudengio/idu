# Package [cloudeng.io/cmd/idu/summary](https://pkg.go.dev/cloudeng.io/cmd/idu/summary?tab=doc)

```go
import cloudeng.io/cmd/idu/summary
```

Package summary provides typed, read-only, access to an idu database:
to the precomputed per-prefix summaries (Get, Walk, Children) and to all
of the metadata recorded for each prefix and its files (Prefix, Prefixes).
It hides the encoding of the database's keys and values and checks that the
summaries were computed with the configuration that the database is being
read with.

## Types
### Type Entry
```go
type Entry struct {
	// Path is the filesystem path, or key prefix, of the summary.
	Path string
	// Key is the database key of the summary. Keys sort in depth-first
	// pre-order and can be used with report.SubtreeAggregator.
	Key string
	// Depth is the number of path components below the prefix that was
	// passed to Walk, ie. 0 for that prefix itself, 1 for its children etc.
	Depth   int
	Summary stats.PrefixSummary
}
```
Entry is a single prefix summary.


### Type Info
```go
type Info struct {
	file.Info
	// XAttr contains the user, group, device, inode etc. information.
	XAttr file.XAttr
}
```
Info describes a prefix or one of its entries (a file or a prefix). It
implements fs.FileInfo, note that its Sys method returns an implementation
specific value, use XAttr instead.


### Type Prefix
```go
type Prefix struct {
	// contains filtered or unexported fields
}
```
Prefix is a read-only view of all of the metadata recorded for a single
prefix (directory) by the most recent analysis, ie. the prefix itself and
all of its files and immediate child prefixes.

### Methods

```go
func (p *Prefix) Decode(path string, val []byte) error
```
Decode decodes val, the metadata stored for path as returned by the Get,
GetBatch or Scan methods of idudb.Reader, into p. It copies everything
that it needs from val. It is intended for use by callers that read the
metadata for many prefixes directly, eg. concurrently, see Reader.Prefix and
Reader.Prefixes for simpler alternatives.


```go
func (p *Prefix) Entries() []Info
```
Entries returns the files and immediate child prefixes, ie. those for which
IsDir is true, of the prefix in the order in which they were recorded.


```go
func (p *Prefix) Info() Info
```
Info returns information on the prefix itself. Since a prefix's name is
implied by its path, the returned Info's Name is the prefix's full path.


```go
func (p *Prefix) Match(m stats.Matcher) bool
```
Match reports whether the prefix itself matches m.


```go
func (p *Prefix) MatchEntry(m stats.Matcher, e Info) bool
```
MatchEntry reports whether e, which must have been returned by Entries,
matches m.


```go
func (p *Prefix) Path() string
```
Path returns the path of the prefix.


```go
func (p *Prefix) Totals(calc diskusage.Calculator, m stats.Matcher) (totals stats.Totals, perUser, perGroup stats.PerIDTotals)
```
Totals returns the totals for the prefix and its files, overall and per
user and group, computed using calc for those entries that match m.
Unlike the precomputed summaries, it can be used with arbitrary calculators
and matchers.




### Type Reader
```go
type Reader struct {
	// contains filtered or unexported fields
}
```
Reader provides access to the summaries stored in a database.

### Functions

```go
func New(db idudb.Reader, cfg *iduconfig.Settings) *Reader
```
New returns a Reader for db, which must contain the database specified by
cfg. Hardlinks are deduplicated by Walk unless cfg.CountHardlinkAsFiles is
set.


```go
func Open(ctx context.Context, cfg *iduconfig.Settings, opts ...idudb.OpenOption) (*Reader, error)
```
Open opens the database specified by cfg for reading only and returns
a Reader for it. The options must include idudb.WithBackendFactory.
The database is closed by calling Close.



### Methods

```go
func (r *Reader) Check(ctx context.Context) error
```
Check returns idudb.ErrStaleSummaries unless the database contains a
complete set of summaries computed with the configuration the Reader was
created with.


```go
func (r *Reader) Children(ctx context.Context, path string) ([]Entry, int, error)
```
Children returns the summaries for the immediate children of path, in the
order in which they were recorded by the most recent analysis, along with
the number of children that have no summary. Its cost is proportional to
the number of children rather than the size of the subtree rooted at path.
Hardlinks are not deduplicated, see Get.


```go
func (r *Reader) Close(ctx context.Context) error
```
Close closes the underlying database.


```go
func (r *Reader) DB() idudb.Reader
```
DB returns the underlying database.


```go
func (r *Reader) Get(ctx context.Context, path string) (stats.PrefixSummary, bool, error)
```
Get returns the summary for path, if there is one. Hardlinks are not
deduplicated since doing so requires visiting every summary that precedes
path, see Walk.


```go
func (r *Reader) Prefix(ctx context.Context, path string) (*Prefix, bool, error)
```
Prefix returns the metadata recorded for path, if any.


```go
func (r *Reader) Prefixes(ctx context.Context, path string) iter.Seq2[*Prefix, error]
```
Prefixes returns an iterator over the metadata recorded for path and all of
the prefixes beneath it, in lexicographic order of their paths. Note that
this is not necessarily depth-first order, eg. /a-b sorts between /a and
/a/b, but /a-b itself is not visited when path is /a. Iteration stops at the
first error, which is returned as the final value of the iterator.


```go
func (r *Reader) Walk(ctx context.Context, path string, opts ...WalkOption) iter.Seq2[Entry, error]
```
Walk returns an iterator over the summaries for path and all of its
descendants in depth-first pre-order. Unless the Reader was created for a
configuration that counts hardlinks as files, hardlinks are deduplicated
such that the first link encountered in that order is the one that is
counted. Iteration stops at the first error, which is returned as the final
value of the iterator.




### Type WalkOption
```go
type WalkOption func(*walkOptions)
```
WalkOption configures Walk.

### Functions

```go
func WithMaxDepth(depth int) WalkOption
```
WithMaxDepth limits Walk to summaries that are at most depth levels below
the prefix being walked, a negative value, the default, means no limit.
Note that the cost of a walk is proportional to the size of the entire
subtree regardless of depth.







