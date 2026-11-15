# Package [cloudeng.io/cmd/idu/internal](https://pkg.go.dev/cloudeng.io/cmd/idu/internal?tab=doc)

```go
import cloudeng.io/cmd/idu/internal
```


## Constants
### LogPrefix, LogProgress, LogError
```go
const (
	LogPrefix   = slog.Level(-1)
	LogProgress = slog.LevelInfo
	LogError    = slog.LevelError
)
```



## Variables
### Verbosity, LogDir
```go
Verbosity = LogError
LogDir string

```

### ErrLegacyDatabase
```go
ErrLegacyDatabase = errors.New("legacy database format without summaries: delete it and rerun analyze")

```
ErrLegacyDatabase is returned when opening a database that was created
before precomputed summaries were introduced. Such databases are not
supported and must be deleted and recreated by rerunning analyze.

### ErrStaleSummaries
```go
ErrStaleSummaries = errors.New("summaries are incomplete or were computed with a different configuration (layout or hardlink settings): rerun analyze for the entire prefix")

```
ErrStaleSummaries is returned when the precomputed summaries are incomplete
or were computed using a different configuration.



## Functions
### Func CheckSummaries
```go
func CheckSummaries(ctx context.Context, db database.DB, calcID string, needLinks bool) error
```
CheckSummaries returns ErrStaleSummaries unless db contains complete
precomputed summaries computed with calcID that also record hardlinks if
needLinks is true.

### Func CheckSummaryKeys
```go
func CheckSummaryKeys(ctx context.Context, db database.DB) error
```
CheckSummaryKeys returns ErrStaleSummaries if a summary could not be written
and hence the summary keys may not include every prefix.

### Func Log
```go
func Log(ctx context.Context, level slog.Level, msg string, args ...interface{})
```

### Func LookupPrefix
```go
func LookupPrefix(ctx context.Context, all config.T, target string) (context.Context, config.Prefix, string, error)
```
LookupPrefix resolves target (either a configuration name or a filesystem
path) to the matching Prefix configuration and the filesystem path to
operate on (Prefix.Prefix when matched by name, or the resolved path when
matched by prefix).

### Func OpenDatabase
```go
func OpenDatabase(ctx context.Context, cfg config.Prefix, readonly bool) (database.DB, error)
```

### Func OpenPrefixAndDatabase
```go
func OpenPrefixAndDatabase(ctx context.Context, all config.T, prefix string, readonly bool) (context.Context, config.Prefix, string, database.DB, error)
```

### Func PrefixInfoAsFSInfo
```go
func PrefixInfoAsFSInfo(pi prefixinfo.T, name string) fs.FileInfo
```
PrefixInfoAsFSInfo returns a fs.FileInfo for the supplied PrefixInfo.

### Func SummarySpec
```go
func SummarySpec(cfg *config.Prefix) string
```
SummarySpec returns the specification that the precomputed summaries must be
created with for cfg: they depend on the disk usage calculator and whether
hardlinks are recorded.

### Func UseBadgerDB
```go
func UseBadgerDB()
```



## Types
### Type ScanDB
```go
type ScanDB interface {
	GetPrefixInfo(ctx context.Context, key string, pi *prefixinfo.T) (bool, error)
	SetPrefixInfo(ctx context.Context, key string, unchanged bool, pi *prefixinfo.T) error
	LogError(ctx context.Context, key string, when time.Time, detail []byte) error
	LogAndClose(ctx context.Context, start, stop time.Time, detail []byte) error
	DeletePrefix(ctx context.Context, prefix string) error
	DeleteErrors(ctx context.Context, prefix string) error
	// MarkSummariesComplete records that every summary in the database
	// was computed with the current configuration. It must only be called
	// after a complete, uninterrupted scan of the entire configured prefix.
	MarkSummariesComplete(ctx context.Context) error
	Close(ctx context.Context) error
}
```

### Functions

```go
func NewScanDB(ctx context.Context, cfg config.Prefix) (ScanDB, error)
```




### Type TimeRangeFlags
```go
type TimeRangeFlags struct {
	Since time.Duration `subcmd:"since,0s,'display entries since the specified duration, it takes precedence over from/to'"`
	From  flags.Time    `subcmd:"from,,display entries starting at this time/date"`
	To    flags.Time    `subcmd:"to,,display entries ending at this time/date"`
}
```

### Methods

```go
func (tr *TimeRangeFlags) FromTo() (from, to time.Time, set bool, err error)
```







