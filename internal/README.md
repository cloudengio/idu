# Package [cloudeng.io/cmd/idu/internal](https://pkg.go.dev/cloudeng.io/cmd/idu/internal?tab=doc)

```go
import cloudeng.io/cmd/idu/internal
```


## Functions
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







