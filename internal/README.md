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



## Functions
### Func Log
```go
func Log(ctx context.Context, level slog.Level, msg string, args ...interface{})
```

### Func LookupPrefix
```go
func LookupPrefix(ctx context.Context, all iduconfig.T, target string) (context.Context, iduconfig.Prefix, string, error)
```
LookupPrefix resolves target (either a configuration name or a filesystem
path) to the matching Prefix configuration and the filesystem path to
operate on (Prefix.Prefix when matched by name, or the resolved path when
matched by prefix).

### Func PrefixInfoAsFSInfo
```go
func PrefixInfoAsFSInfo(pi prefixinfo.T, name string) fs.FileInfo
```
PrefixInfoAsFSInfo returns a fs.FileInfo for the supplied PrefixInfo.



## Types
### Type BadgerLogger
```go
type BadgerLogger struct{}
```
BadgerLogger implements badger.Logger by writing to a dedicated log file
subject to the configured Verbosity.

### Methods

```go
func (l *BadgerLogger) Debugf(f string, a ...interface{})
```


```go
func (l *BadgerLogger) Errorf(f string, a ...interface{})
```


```go
func (l *BadgerLogger) Infof(f string, a ...interface{})
```


```go
func (l *BadgerLogger) Warningf(f string, a ...interface{})
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







