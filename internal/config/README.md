# Package [cloudeng.io/cmd/idu/internal/config](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/config?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/config)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/config)

```go
import cloudeng.io/cmd/idu/internal/config
```


## Variables
### DefaultConcurrentStats, DefaultConcurrentStatsThreshold, DefaultConcurrentScans, DefaultScanSize
```go
DefaultConcurrentStats = 0
DefaultConcurrentStatsThreshold = 0
DefaultConcurrentScans = 0
DefaultScanSize = 0

```



## Functions
### Func Documentation
```go
func Documentation() string
```
Documentation will return a description of the format of the yaml
configuration file.



## Types
### Type Block
```go
type Block struct {
	BlockSize int64 `yaml:"size" cmd:"block size used by this filesystem"`
}
```


### Type Prefix
```go
type Prefix struct {
	Prefix                   string   `yaml:"prefix" cmd:"the prefix to be analyzed"`
	Database                 string   `yaml:"database" cmd:"the location of the database to use for this prefix"`
	Separator                string   `yaml:"separator" cmd:"filename separator to use, defaults to /"`
	ConcurrentScans          int      `yaml:"concurrent_scans" cmd:"maximum number of concurrent scan operations"`
	ConcurrentStats          int      `yaml:"concurrent_stats" cmd:"maximum number of concurrent stat operations"`
	ConcurrentStatsThreshold int      `yaml:"concurrent_stats_threshold" cmd:"minimum number of files before stats are performed concurrently"`
	SetMaxThreads            int      `yaml:"set_max_threads" cmd:"if non-zero used for debug.SetMaxThreads"`
	ScanSize                 int      `yaml:"scan_size" cmd:"maximum number of items to fetch from the filesystem in a single operation"`
	Exclusions               []string `yaml:"exclusions" cmd:"prefixes and files matching these regular expressions will be ignored when building a dataase"`
	CountHardlinkAsFiles     bool     `yaml:"count_hardlinks_as_files" cmd:"if true, hardlinks will be counted as separate files"`

	Layout layout `yaml:"layout" cmd:"the filesystem layout to use for calculating raw bytes used"`
	// contains filtered or unexported fields
}
```

### Methods

```go
func (p *Prefix) Calculator() diskusage.Calculator
```


```go
func (p *Prefix) Exclude(path string) bool
```
Exclude returns true if path should be excluded/ignored.




### Type RAID0
```go
type RAID0 struct {
	StripeSize int64 `yaml:"stripe_size" cmd:"the size of the raid0 stripes"`
	NumStripes int   `yaml:"num_stripes" cmd:"the number of stripes used"`
}
```


### Type T
```go
type T struct {
	Prefixes []Prefix `yaml:"prefixes" cmd:"the prefixes to be analyzed"`
}
```

### Functions

```go
func ParseConfig(buf []byte) (T, error)
```
ParseConfig will parse a yaml config from the supplied byte slice.


```go
func ReadConfig(filename string) (T, error)
```
ReadConfig will read a yaml config from the specified file.



### Methods

```go
func (t T) ForPrefix(path string) (Prefix, bool)
```
ForPrefix returns the prefix configuration that corresponds to path. The
prefix is the longest matching prefix in the configuration and the returned
string is the path relative to that prefix. The boolean return value is true
if a match is found.







