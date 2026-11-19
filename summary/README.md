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

## Variables
### ErrNoTopN, ErrStaleTopN
```go
// ErrNoTopN is returned when the database contains no top-N rankings,
// ie. when it was not analyzed with them enabled.
ErrNoTopN = errors.New("the database contains no top-N rankings: rerun analyze")
// ErrStaleTopN is returned when the stored top-N rankings may be out
// of date because the most recent analysis was interrupted or failed,
// or did not cover the entire filesystem after a configuration change.
ErrStaleTopN = errors.New("the top-N rankings may be out of date: rerun analyze for the entire prefix")

```



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




### Type Ranked
```go
type Ranked struct {
	Path  string
	Value int64
}
```
Ranked is a single entry in a ranking.


### Type RankedID
```go
type RankedID struct {
	ID     int64
	Value  int64
	Totals stats.Totals
}
```
RankedID is a single entry in a ranking of users or groups.


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
func (r *Reader) Subtree(ctx context.Context, path string) (stats.Totals, bool, error)
```
Subtree returns the recursive totals for path, ie. the sum of the direct
totals of path and all of its descendants, if path exists. It returns
idudb.ErrStaleSubtrees if the recursive totals are missing or may be out of
date.

The recursive totals, and the rankings and per-id totals, deduplicate
hardlinks across the entire filesystem, as Walk does when walking from its
root: each file is attributed to the first prefix, in depth-first pre-order,
that records it (see stats.HardlinkRecording), and counted as a hardlink by
any other prefixes that record it. They may therefore differ from the totals
computed by walking from path, which attributes files only amongst the
prefixes beneath path. Prefixes that are duplicates of preceding ones, eg.
bind mounts, are only recognized when the recursive totals are rebuilt, eg.
by analyze --refresh-topn, rather than as soon as they are created.


```go
func (r *Reader) Subtrees(ctx context.Context, paths []string) ([]stats.Totals, []bool, error)
```
Subtrees is like Subtree but returns the recursive totals for each of paths,
and whether each was found, reading them all at once.


```go
func (r *Reader) TopGroups(ctx context.Context, metric stats.Metric, n int) ([]RankedID, error)
```
TopGroups is like TopUsers but for groups.


```go
func (r *Reader) TopNIDs(ctx context.Context) (users, groups []int64, err error)
```
TopNIDs returns the users and groups that have rankings, ie. those that own
any of the files or prefixes in the database, in ascending order.


```go
func (r *Reader) TopPrefixes(ctx context.Context, metric stats.Metric, n int) (TopN, error)
```
TopPrefixes returns the top n prefixes by their own, ie. not recursive,
value for metric. n of zero or less returns all of the stored entries.
It returns ErrNoTopN or ErrStaleTopN if the rankings are not available or
may be out of date.


```go
func (r *Reader) TopPrefixesForGroup(ctx context.Context, gid int64, metric stats.Metric, n int) (TopN, error)
```
TopPrefixesForGroup is like TopPrefixesForUser but for the group gid.


```go
func (r *Reader) TopPrefixesForUser(ctx context.Context, uid int64, metric stats.Metric, n int) (TopN, error)
```
TopPrefixesForUser is like TopPrefixes but ranks the prefixes by the value
for metric of the files, and the prefixes themselves, owned by uid.


```go
func (r *Reader) TopSubtrees(ctx context.Context, metric stats.Metric, n int) (TopN, error)
```
TopSubtrees is like TopPrefixes but ranks the prefixes by their recursive
value for metric, ie. that of the prefix and all of its descendants.
See TopSubtreesWithin for the top subtrees within a prefix.


```go
func (r *Reader) TopSubtreesWithin(ctx context.Context, path string, metric stats.Metric, n int) ([]Ranked, error)
```
TopSubtreesWithin returns the top n prefixes, path itself included,
by their recursive value for metric among path and its descendants,
in descending order of Value with ties broken by Path. Since a subtree's
value can never exceed that of its parent, it performs a best-first search
of the stored recursive totals whose cost is proportional to n times the
fanout of the prefixes visited, rather than to the size of the subtree,
and its results are always exact. Prefixes with a zero value are omitted.
It returns idudb.ErrStaleSubtrees if the recursive totals are missing or may
be out of date.


```go
func (r *Reader) TopUsers(ctx context.Context, metric stats.Metric, n int) ([]RankedID, error)
```
TopUsers returns the top n users, or all users if n is zero or less,
by their value for metric across the entire filesystem, in descending order
of Value. Unlike the rankings of prefixes, the totals for every user are
stored and hence the result is always exact. Hardlinks are deduplicated,
see Subtree.


```go
func (r *Reader) Walk(ctx context.Context, path string, opts ...WalkOption) iter.Seq2[Entry, error]
```
Walk returns an iterator over the summaries for path and all of its
descendants in depth-first pre-order. Unless the Reader was created for a
configuration that counts hardlinks as files, hardlinks are deduplicated
such that the first link encountered in that order is the one that is
counted. Iteration stops at the first error, which is returned as the final
value of the iterator.




### Type TopN
```go
type TopN struct {
	// Entries are in descending order of Value.
	Entries []Ranked
	// Certified is the number of leading Entries that are known to be
	// ranked exactly. The remaining entries may be out of order with
	// respect to prefixes not included in the ranking. It is normally
	// equal to len(Entries).
	Certified int
	// N is the number of entries that analyze maintains for this ranking,
	// requests for more than N entries may return fewer, uncertified,
	// entries.
	N int
}
```
TopN is a ranking of prefixes by a single stats.Metric, as maintained by
analyze. Hardlinks are deduplicated, see Subtree.


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







