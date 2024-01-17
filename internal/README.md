# Package [cloudeng.io/cmd/idu/internal](https://pkg.go.dev/cloudeng.io/cmd/idu/internal?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal)

```go
import cloudeng.io/cmd/idu/internal
```


## Constants
### LogPrefix, LogProgress, LogError
```go
LogPrefix = slog.Level(-1)
LogProgress = slog.LevelInfo
LogError = slog.LevelError

```



## Variables
### LogDir
```go
LogDir string

```

### Verbosity
```go
Verbosity slog.Level = LogError

```



## Functions
### Func Log
```go
func Log(ctx context.Context, level slog.Level, msg string, args ...interface{})
```

### Func LookupPrefix
```go
func LookupPrefix(ctx context.Context, all config.T, prefix string) (context.Context, config.Prefix, error)
```

### Func OpenDatabase
```go
func OpenDatabase(ctx context.Context, cfg config.Prefix, readonly bool) (database.DB, error)
```

### Func OpenPrefixAndDatabase
```go
func OpenPrefixAndDatabase(ctx context.Context, all config.T, prefix string, readonly bool) (context.Context, config.Prefix, database.DB, error)
```

### Func PrefixInfoAsFSInfo
```go
func PrefixInfoAsFSInfo(pi prefixinfo.T, name string) fs.FileInfo
```
PrefixInfoAsFSInfo returns a fs.FileInfo for the supplied PrefixInfo.

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







