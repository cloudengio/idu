# Package [cloudeng.io/cmd/idu/internal](https://pkg.go.dev/cloudeng.io/cmd/idu/internal?tab=doc)

```go
import cloudeng.io/cmd/idu/internal
```


## Functions
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







