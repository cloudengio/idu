# Package [cloudeng.io/cmd/idu/idudb](https://pkg.go.dev/cloudeng.io/cmd/idu/idudb?tab=doc)

```go
import cloudeng.io/cmd/idu/idudb
```

Package idudb provides support for building and maintaining a database of
filesystem metadata that can be used for incrementally computing disk usage
statistics. The database itself is accessed via the Database and Reader
interfaces, implementations of which (e.g. backends/badgerdb) are created
by a BackendFactory. Open and OpenWritable open the database specified
by an iduconfig.Settings for reading and for analysis respectively.
All operations that modify the database are implemented by this package;
other packages should only read the database via the Reader interface,
or the higher level APIs provided by the summary and report packages.
Log messages are written to the logger, if any, in the context supplied to
each method, see cloudeng.io/logging/ctxlog.

## Constants
### DefaultSlowScanThreshold
```go
const DefaultSlowScanThreshold = 10 * time.Second
```
DefaultSlowScanThreshold is the default duration after which scans are
reported as being slow.



## Variables
### ErrLegacyDatabase
```go
ErrLegacyDatabase = errors.New("legacy database format without summaries: delete it and rerun analyze")

```
ErrLegacyDatabase is returned for a database that was created before
precomputed summaries were introduced. Such databases are not supported and
must be deleted and recreated by rerunning analyze.

### ErrNoBackend
```go
ErrNoBackend = errors.New("no database backend specified, see idudb.WithBackendFactory")

```
ErrNoBackend is returned when a database is opened without specifying a
BackendFactory, see WithBackendFactory.

### ErrStaleSubtrees
```go
ErrStaleSubtrees = errors.New("the recursive totals are missing or may be out of date: rerun analyze for the entire prefix")

```
ErrStaleSubtrees is returned when the recursive totals are missing or may be
out of date.

### ErrStaleSummaries
```go
ErrStaleSummaries = errors.New("summaries are incomplete or were computed with a different configuration (layout or hardlink settings): rerun analyze for the entire prefix")

```
ErrStaleSummaries is returned when the precomputed summaries are incomplete
or were computed using a different configuration.

### ErrTopNDisabled
```go
ErrTopNDisabled = errors.New("top-N rankings are not enabled for this database")

```
ErrTopNDisabled is returned by RefreshTopN if the DB was not created with
WithTopN.



## Functions
### Func CheckFormat
```go
func CheckFormat(ctx context.Context, db Reader) error
```
CheckFormat returns ErrLegacyDatabase if db contains prefix data but
no summary metadata, i.e. it was created by an older version of idu.
It should be called on every newly opened database before it is used; New,
EraseLogs and EraseErrors call it automatically.

### Func CheckSubtrees
```go
func CheckSubtrees(ctx context.Context, db Reader) error
```
CheckSubtrees returns ErrStaleSubtrees unless db contains complete, up to
date, recursive totals.

### Func CheckSummaries
```go
func CheckSummaries(ctx context.Context, db Reader, calcID string, needLinks bool) error
```
CheckSummaries returns ErrStaleSummaries unless db contains complete
precomputed summaries computed with calcID that also record hardlinks,
either the files that may be linked or all files, if needLinks is true.

### Func CheckSummaryKeys
```go
func CheckSummaryKeys(ctx context.Context, db Reader) error
```
CheckSummaryKeys returns ErrStaleSummaries if a summary could not be written
and hence the summary keys may not include every prefix.

### Func CheckSummaryRecording
```go
func CheckSummaryRecording(ctx context.Context, db Reader, calcID string, rec stats.HardlinkRecording) error
```
CheckSummaryRecording returns ErrStaleSummaries unless db contains complete
precomputed summaries computed with calcID that satisfy the hardlink
recording requirement rec. RecordAllFiles satisfies RecordLinkedFiles,
and both satisfy RecordNoFiles.

### Func EraseErrors
```go
func EraseErrors(ctx context.Context, db Database) error
```
EraseErrors deletes all of the error entries in db. It does not close db.

### Func EraseLogs
```go
func EraseLogs(ctx context.Context, db Database) error
```
EraseLogs deletes all of the log entries in db. It does not close db.

### Func HardlinkRecording
```go
func HardlinkRecording(cfg *iduconfig.Settings) stats.HardlinkRecording
```
HardlinkRecording returns the files that the summaries record, as specified
by cfg, so that hardlinks can be deduplicated.



## Types
### Type AnalyzeOption
```go
type AnalyzeOption func(*analyzeOptions)
```
AnalyzeOption configures an Analyzer.

### Functions

```go
func AnalyzeOptionsFromConfig(cfg *iduconfig.Settings) []AnalyzeOption
```
AnalyzeOptionsFromConfig returns the analyze options specified by cfg.
The filesystem (WithFS) and any options not represented in the configuration
(eg. WithForce, WithProgress) must be supplied separately.


```go
func WithConcurrentScans(n int) AnalyzeOption
```
WithConcurrentScans sets the maximum number of concurrent scan operations,
zero selects filewalk's default.


```go
func WithConcurrentStats(n int) AnalyzeOption
```
WithConcurrentStats sets the maximum number of concurrent stat operations,
zero selects asyncstat's default.


```go
func WithConcurrentStatsThreshold(n int) AnalyzeOption
```
WithConcurrentStatsThreshold sets the minimum number of files before
stats are performed concurrently, the value is passed directly to
asyncstat.WithAsyncThreshold.


```go
func WithExclusions(exclude func(path string) bool) AnalyzeOption
```
WithExclusions sets a function that is called for every prefix and file
encountered; those for which it returns true are ignored.


```go
func WithFS(fs filewalk.FS) AnalyzeOption
```
WithFS sets the filesystem to be analyzed, it defaults to the local
filesystem.


```go
func WithForce(v bool) AnalyzeOption
```
WithForce requests that all prefixes be reanalyzed even if they are
unchanged since they were last analyzed.


```go
func WithObserver(o Observer) AnalyzeOption
```
WithObserver sets the Observer to be notified of all changes made to the
summaries by the Analyzer.


```go
func WithProgress(p ScanProgress) AnalyzeOption
```
WithProgress sets the ScanProgress implementation to be notified of the
progress of the analysis.


```go
func WithScanSize(n int) AnalyzeOption
```
WithScanSize sets the maximum number of items to fetch from the filesystem
in a single operation, zero selects filewalk's default.


```go
func WithSlowScanThreshold(d time.Duration) AnalyzeOption
```
WithSlowScanThreshold sets the duration after which a scan is reported as
slow, it defaults to DefaultSlowScanThreshold.




### Type Analyzer
```go
type Analyzer struct {
	// contains filtered or unexported fields
}
```
Analyzer scans a filesystem and records the metadata for every prefix
(directory) and file in a DB. Subsequent analyses are incremental, in that
only prefixes that have changed since the previous analysis are rescanned.

### Functions

```go
func NewAnalyzer(db *DB, root string, opts ...AnalyzeOption) *Analyzer
```
NewAnalyzer returns a new Analyzer for the filesystem rooted at root that
records its results in db. AnalyzeOptionsFromConfig can be used to create
options from an iduconfig.Settings.



### Methods

```go
func (a *Analyzer) Analyze(ctx context.Context, target string) error
```
Analyze scans target, which must be the root or a prefix beneath it, and
updates the database accordingly. Any errors previously recorded for target
and its descendants are deleted before the scan starts. The precomputed
summaries are only marked as consistent with the current configuration once
the entire root has been scanned successfully. Analyze returns when all
filesystem and database operations have completed, including when ctx is
canceled.


```go
func (a *Analyzer) Configuration() Configuration
```
Configuration returns the effective configuration used by the Analyzer,
that is, including any default values.




### Type BackendFactory
```go
type BackendFactory func(ctx context.Context, path string, readonly bool) (Database, error)
```
BackendFactory creates the database backend for the database stored at path,
opening it for reading only if readonly is true.

### Functions

```go
func NewBackendFactory[T Database](fn func(context.Context, string, bool) (T, error)) BackendFactory
```
NewBackendFactory adapts a typed backend constructor, such as
badgerdb.NewBackend, into a BackendFactory while ensuring a nil concrete
pointer is not returned inside a non-nil Database interface on error.




### Type Configuration
```go
type Configuration struct {
	ScanSize                 int
	ConcurrentScans          int
	ConcurrentStats          int
	ConcurrentStatsThreshold int
}
```
Configuration describes the effective configuration of an Analyzer.


### Type DB
```go
type DB struct {
	// contains filtered or unexported fields
}
```
DB represents a writable idu database. A DB is not safe for use by multiple,
concurrent, Analyzers.

### Functions

```go
func New(ctx context.Context, db Database, opts ...Option) (*DB, error)
```
New returns a DB that uses the supplied, writable, database backend.
The returned DB takes ownership of db, which will be closed by calling
either Close or LogAndClose. If New returns an error, db is not closed and
remains the responsibility of the caller. New returns ErrLegacyDatabase for
databases created by older versions of idu, see CheckFormat.


```go
func OpenWritable(ctx context.Context, cfg *iduconfig.Settings, opts []OpenOption, dbOpts ...Option) (*DB, error)
```
OpenWritable opens, or creates, the database specified by cfg for use by an
Analyzer, configured using OptionsFromConfig(cfg) followed by any supplied
dbOpts.



### Methods

```go
func (sdb *DB) Close(ctx context.Context) error
```
Close closes the database.


```go
func (sdb *DB) LogAndClose(ctx context.Context, start, stop time.Time, detail []byte) error
```
LogAndClose records a log entry for an operation that ran from start to
stop with the supplied detail, performs any required maintenance (such as
compaction or garbage collection) and closes the database.


```go
func (sdb *DB) RefreshTopN(ctx context.Context) error
```
RefreshTopN rebuilds the link index and the recursive totals, and recomputes
all of the top-N rankings and per-id totals, from the summaries stored
in the database. Prefixes that are duplicates of preceding ones, eg.
bind mounts, are only recognized by a rebuild. It returns ErrStaleSummaries
if the summaries are incomplete or were computed with a different
configuration, ie. if the entire filesystem needs to be analyzed first.




### Type Database
```go
type Database interface {
	Reader
	Writer
}
```
Database represents a database backend that supports both read and write
operations. Only the idudb package should use the Writer methods.

### Functions

```go
func OpenBackend(ctx context.Context, path string, readonly bool, opts ...OpenOption) (Database, error)
```
OpenBackend opens the database backend for the database stored at path,
waiting until it is opened or ctx is canceled. If ctx is canceled first then
any database that is subsequently opened is closed. OpenBackend does not
check the format of the database, see CheckFormat.




### Type Observer
```go
type Observer interface {
	// ScanStarted is called before the analysis starts. If it returns an
	// error the analysis is not performed and ScanDone is not called.
	ScanStarted(ctx context.Context, scan ScanInfo) error

	// PrefixChanged is called after the summary for path has been written,
	// with the summary that it replaced, old, if any, and the new summary, cur.
	// It is only called for prefixes whose summary was rewritten, ie. those
	// that changed since the previous analysis or all prefixes if
	// ScanInfo.Rewrite is true. old is nil if there was no previous
	// summary or it could not be read. Neither old nor cur may be retained
	// after PrefixChanged returns.
	PrefixChanged(ctx context.Context, path string, old, cur *stats.PrefixSummary)

	// SubtreeDeleted is called before the prefix path, and all of the
	// prefixes beneath it, are deleted from the database. summaries
	// iterates over the summaries being deleted, path's own included, in
	// depth-first pre-order; it reads them from the database and hence
	// incurs no cost if not used. It is only valid for the duration of the
	// call and the summaries it returns may not be retained. If it cannot
	// read or decode a summary then iteration stops and the deletion fails
	// and is reported as an error by the analysis, and hence to ScanDone.
	SubtreeDeleted(ctx context.Context, path string, summaries iter.Seq2[string, *stats.PrefixSummary])

	// ScanDone is called once the analysis has completed and all of the
	// changes it made have been persisted. scanErr is nil if the entire
	// Target was analyzed successfully, otherwise the analysis was
	// incomplete or interrupted and the changes reported may not include
	// all of those made to the database.
	ScanDone(ctx context.Context, scanErr error) error
}
```
Observer is notified of every change that an Analyzer makes to the summaries
stored in a DB. It allows for state derived from the summaries, such as
aggregates, indexes or exports, to be maintained incrementally.

PrefixChanged and SubtreeDeleted may be called concurrently, by multiple
goroutines, between the calls to ScanStarted and ScanDone. They deliberately
do not return errors: an observer cannot abort an analysis once it has
started, since the database of filesystem metadata is the primary record
and is always kept as complete as possible, whereas any state derived from
it can be rebuilt. An observer that fails to maintain its state should
record the failure, return it from ScanDone and rebuild that state, eg.
on the next analysis. Only ScanStarted can prevent an analysis, by returning
an error before it begins.


### Type OpenOption
```go
type OpenOption func(*openOptions)
```
OpenOption configures OpenBackend, Open and OpenWritable.

### Functions

```go
func WithBackendFactory(f BackendFactory) OpenOption
```
WithBackendFactory sets the factory used to create the database backend,
eg. one that opens a badger database using badgerdb.NewBackend. It must be
specified, otherwise ErrNoBackend is returned.


```go
func WithOpenProgress(w io.Writer) OpenOption
```
WithOpenProgress requests that a message be written to w, once a second,
while waiting for a database to open, eg. because another process holds its
lock. By default no messages are written.




### Type Option
```go
type Option func(*options)
```
Option configures a DB created by New.

### Functions

```go
func OptionsFromConfig(cfg *iduconfig.Settings) []Option
```
OptionsFromConfig returns the database options specified by cfg.


```go
func WithCalculator(calc diskusage.Calculator) Option
```
WithCalculator sets the disk usage calculator used for the precomputed
summaries. It defaults to diskusage.NewIdentity().


```go
func WithCompactionInterval(d time.Duration) Option
```
WithCompactionInterval requests that the database be compacted when it is
closed via LogAndClose if more than the specified duration has elapsed since
it was last written. A zero value disables compaction.


```go
func WithHardlinkRecording(r stats.HardlinkRecording) Option
```
WithHardlinkRecording determines which files, if any, the precomputed
summaries record so that hardlinks can be deduplicated without rescanning
the database. It defaults to stats.RecordNoFiles.


```go
func WithSeparator(sep string) Option
```
WithSeparator sets the filename separator, it defaults to
filepath.Separator.


```go
func WithTopN(n, capacity, perIDN, perIDCapacity int) Option
```
WithTopN requests that the top n prefixes, by each stats.Metric,
be maintained incrementally by every analysis, both overall and for each
user and group, using heaps that hold capacity and perIDCapacity entries
respectively. The capacities should exceed n and perIDN so that entries
that shrink or are deleted can be replaced without recomputing the heaps,
see iduconfig.Settings.TopNCapacity. A value of zero for n disables the
maintenance of the rankings.




### Type Reader
```go
type Reader interface {
	// Get retrieves the value associated with prefix storing
	// its contents in the supplied bytes.Buffer.
	Get(ctx context.Context, prefix string, buf *bytes.Buffer) error

	// GetBatch calls fn with the value associated with each of prefixes,
	// or nil if there is none, in order. val is only valid for the duration
	// of the call to fn. It is more efficient than calling Get for each
	// prefix.
	GetBatch(ctx context.Context, prefixes []string, fn func(i int, val []byte) error) error

	// Scan can be used to iterate over all keys in the database starting at
	// the specified key.
	Scan(ctx context.Context, key string, visitor func(ctx context.Context, key string, val []byte) bool) error

	// Stream can be used to iterate over all keys in the database concurrently
	// that have the specified prefix.
	Stream(ctx context.Context, prefix string, visitor func(ctx context.Context, key string, val []byte)) error

	// LastLog returns the most recently recorded log entry.
	LastLog(ctx context.Context) (start, stop time.Time, detail []byte, err error)

	// VisitLogs calls visitor for every log entry between start and stop. The
	// visitor func should return false if it wants to stop the iteration over
	// log entries.
	VisitLogs(ctx context.Context, start, stop time.Time,
		visitor func(ctx context.Context, begin, end time.Time, detail []byte) bool) error

	// VisitErrors calls visitor for every error starting at key. The
	// visitor func should return false if it wants to stop the iteration over
	// errors.
	VisitErrors(ctx context.Context, key string, visitor func(ctx context.Context, key string, when time.Time, val []byte) bool) error

	// ScanSummaries iterates over summary entries in lexicographical key order,
	// starting at exactKey and stopping once keys no longer match exactKey or
	// childPrefix (when either is non-empty).
	ScanSummaries(ctx context.Context, exactKey, childPrefix string, visitor func(ctx context.Context, key string, val []byte) bool) error

	// GetSubtrees calls fn with the recursive totals stored in the
	// subtree bucket for each of keys, which are summary keys, or nil if
	// there are none, in order. val is only valid for the duration of the
	// call to fn.
	GetSubtrees(ctx context.Context, keys []string, fn func(i int, val []byte) error) error

	// ScanSubtrees iterates over the recursive totals in the subtree bucket
	// in lexicographical key order, starting at exactKey and stopping once
	// keys no longer match exactKey or childPrefix (when either is
	// non-empty).
	ScanSubtrees(ctx context.Context, exactKey, childPrefix string, visitor func(ctx context.Context, key string, val []byte) bool) error

	// ScanLinks iterates over the entries in the link bucket whose keys
	// start with prefix, in lexicographical key order. The link bucket
	// contains the index used to deduplicate hardlinks, whose keys and
	// values are defined by the idudb package.
	ScanLinks(ctx context.Context, prefix string, visitor func(ctx context.Context, key string, val []byte) bool) error

	// GetMeta retrieves a metadata value from the metadata bucket into buf.
	GetMeta(ctx context.Context, key string, buf *bytes.Buffer) error

	// Close closes the database.
	Close(context.Context) error
}
```
Reader represents the read-only operations supported by a database backend,
such as the one implemented by backends/badgerdb.

### Functions

```go
func Open(ctx context.Context, cfg *iduconfig.Settings, opts ...OpenOption) (Reader, error)
```
Open opens the database specified by cfg for reading only and checks its
format, see CheckFormat.




### Type ScanInfo
```go
type ScanInfo struct {
	// Root is the root of the filesystem being analyzed.
	Root string
	// Target is the prefix being analyzed, ie. Root or a prefix beneath it.
	Target string
	// Rewrite is true if the stored summaries were computed with a
	// different configuration (eg. calculator or hardlink settings) and
	// hence all of the summaries for Target will be rewritten. The old
	// summaries reported by PrefixChanged are then not comparable with
	// the new ones and any state derived from them should be rebuilt.
	Rewrite bool
}
```
ScanInfo describes an analysis, see Observer.ScanStarted.


### Type ScanProgress
```go
type ScanProgress interface {
	// asyncstat.LatencyTracker is used to track the latency of
	// filesystem stat operations.
	asyncstat.LatencyTracker
	// PrefixStarted is called when the scan of a prefix starts.
	PrefixStarted()
	// PrefixDone is called when the scan of a prefix completes with the
	// number of child prefixes deleted and files found.
	PrefixDone(deleted int, files int64)
	// Error is called whenever an error is logged to the database.
	Error()
	// ParentUnchanged is called when a prefix is found to be unchanged.
	ParentUnchanged()
	// ChildrenUnchanged is called when all of the immediate child
	// prefixes of a prefix are found to be unchanged.
	ChildrenUnchanged()
	// SlowScan is called when a scan exceeds the slow scan threshold.
	SlowScan()
	// SyncScans is called with the running total of synchronous scans.
	SyncScans(n int64)
}
```
ScanProgress is notified of the progress of an analysis. Its methods may be
called concurrently.


### Type Writer
```go
type Writer interface {
	// Set stores the value associated with prefix. If batch is true then
	// calls from multiple goroutines may be merged together. This is only
	// useful when Set is called from multiple goroutines.
	Set(ctx context.Context, prefix string, value []byte, batch bool) error

	// Delete deletes the value associated with exactly prefix, if any.
	Delete(ctx context.Context, prefix string) error

	// DeletePrefix deletes all keys that have the specified prefix.
	DeletePrefix(ctx context.Context, prefix string) error

	// Flush persists any pending batched writes. It is safe to call
	// concurrently with batched writes.
	Flush(ctx context.Context) error

	// DeleteError deletes the error logged for exactly key, if any.
	DeleteError(ctx context.Context, key string) error

	// DeleteErrors deletes all errors that have the specified prefix.
	DeleteErrors(ctx context.Context, prefix string) error

	// Log should be used to record the start and stop time for
	// a given operation and associated details/description.
	Log(ctx context.Context, start, stop time.Time, detail []byte) error

	// LogError records an error.
	LogError(ctx context.Context, key string, when time.Time, detail []byte) error

	// Clear clears all of the log or error entries.
	Clear(ctx context.Context, logs, errors bool) error

	// SetSummary stores a precomputed summary record for key in the summary bucket.
	SetSummary(ctx context.Context, key string, value []byte, batch bool) error

	// DeleteSummary deletes the summary entry matching exactKey and any
	// descendant summary entries matching childPrefix.
	DeleteSummary(ctx context.Context, exactKey, childPrefix string) error

	// SetSubtree stores the recursive totals for key in the subtree bucket.
	SetSubtree(ctx context.Context, key string, value []byte, batch bool) error

	// DeleteSubtrees deletes the subtree entry matching exactKey and any
	// descendant subtree entries matching childPrefix.
	DeleteSubtrees(ctx context.Context, exactKey, childPrefix string) error

	// ClearSubtrees deletes all of the entries in the subtree bucket.
	ClearSubtrees(ctx context.Context) error

	// SetLink stores value for key in the link bucket. If batch is true
	// the write may be merged with others, as for Set.
	SetLink(ctx context.Context, key string, value []byte, batch bool) error

	// DeleteLink deletes exactly key, if present, from the link bucket.
	DeleteLink(ctx context.Context, key string) error

	// ClearLinks deletes all of the entries in the link bucket.
	ClearLinks(ctx context.Context) error

	// SetMeta stores a metadata value in the metadata bucket.
	SetMeta(ctx context.Context, key string, value []byte) error
}
```
Writer represents the operations that modify a database.





