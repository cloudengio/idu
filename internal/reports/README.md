# Package [cloudeng.io/cmd/idu/internal/reports](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/reports?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/reports)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/reports)

```go
import cloudeng.io/cmd/idu/internal/reports
```


## Functions
### Func PopN
```go
func PopN[T comparable](heap *heap.MinMax[int64, T], n int) (keys []int64, vals []T)
```



## Types
### Type AllStats
```go
type AllStats struct {
	MaxN     int
	Prefix   *Heaps[string]
	PerUser  PerIDStats
	PerGroup PerIDStats
	ByUser   *Heaps[int64]
	ByGroup  *Heaps[int64]
	// contains filtered or unexported fields
}
```
AllStats is a collection of statistics for a given prefix and includes:
- the top N values for each statistic by prefix - the total for each
statistic - the top N values for/per each statistic by user/group - the topN
user/groups by each statistic

### Functions

```go
func NewAllStats(prefix string, n int) *AllStats
```



### Methods

```go
func (s *AllStats) Finalize()
```


```go
func (s *AllStats) PushPerGroupStats(prefix string, ug stats.PerIDTotals)
```


```go
func (s *AllStats) PushPerUserStats(prefix string, us stats.PerIDTotals)
```


```go
func (s *AllStats) Update(prefix string, pi prefixinfo.T, calc diskusage.Calculator, matcher boolexpr.Matcher) error
```




### Type Heaps
```go
type Heaps[T comparable] struct {
	MaxN                          int
	Prefix                        string
	TotalBytes, TotalStorageBytes int64
	TotalFiles, TotalPrefixes     int64
	TotalPrefixBytes              int64
	TotalHardlinks                int64
	TotalHardlinkDirs             int64
	Bytes                         *heap.MinMax[int64, T]
	StorageBytes                  *heap.MinMax[int64, T]
	PrefixBytes                   *heap.MinMax[int64, T]
	Files                         *heap.MinMax[int64, T]
	Prefixes                      *heap.MinMax[int64, T]
}
```
Heaps is a collection of heap data structures for determining the top N
values for a set of statistics and also for computing the total for those
statistics. The Prefix refers to the root of the hierarchy for which the
statistics are being computed.

### Methods

```go
func (h *Heaps[T]) Merge(n int) map[T]MergedStats
```


```go
func (h *Heaps[T]) Push(item T, bytes, storageBytes, prefixBytes, files, prefixes, children int64)
```




### Type MergedStats
```go
type MergedStats struct {
	Prefix      string `json:"prefix,omitempty"`
	ID          int64  `json:"id,omitempty"`
	IDName      string `json:"name,omitempty"`
	Bytes       int64  `json:"bytes"`
	Storage     int64  `json:"storage,omitempty"`
	Files       int64  `json:"files"`
	Prefixes    int64  `json:"prefixes"`
	PrefixBytes int64  `json:"prefix_bytes"`
}
```


### Type PerIDStats
```go
type PerIDStats struct {
	Prefix   string
	MaxN     int
	ByPrefix map[int64]*Heaps[string]
}
```
PerIDStats is a collection of statistics on a per user/group basis.

### Methods

```go
func (s *PerIDStats) Push(id int64, prefix string, size, storageBytes, prefixBytes, files, prefixCount, children int64)
```




### Type Zipped
```go
type Zipped[T comparable] struct {
	K int64
	V T
}
```

### Functions

```go
func ZipN[T comparable](h *heap.MinMax[int64, T], n int) (z []Zipped[T])
```







