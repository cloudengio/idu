# Package [cloudeng.io/cmd/idu/internal/reports](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/reports?tab=doc)

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
	MaxN        int
	Prefix      *Heaps[string]
	Subtree     *Heaps[string]
	Distributed *DistributedHeaps
	PerUser     PerIDStats
	PerGroup    PerIDStats
	ByUser      *Heaps[int64]
	ByGroup     *Heaps[int64]
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
func (s *AllStats) RecordDirect(prefix string, totals stats.Totals, users, groups stats.PerIDTotals)
```
RecordDirect pushes precomputed direct directory totals and per-ID
breakdowns into s.


```go
func (s *AllStats) Update(prefix string, pi prefixinfo.T, calc diskusage.Calculator, matcher boolexpr.Matcher) error
```




### Type DistributedHeaps
```go
type DistributedHeaps struct {
	MaxN         int
	Bytes        *heap.MinMax[int64, DistributedStats]
	StorageBytes *heap.MinMax[int64, DistributedStats]
	Files        *heap.MinMax[int64, DistributedStats]
}
```
DistributedHeaps maintains bounded top-N min-max heaps of DistributedStats
ordered by distributed bytes, distributed storage bytes, and distributed
files.

### Functions

```go
func NewDistributedHeaps(n int) *DistributedHeaps
```
NewDistributedHeaps creates a new DistributedHeaps bounded to n entries per
heap.



### Methods

```go
func (dh *DistributedHeaps) Push(ds DistributedStats)
```
Push records ds into whichever distributed metrics satisfy the amplification
threshold (subtree total >= 2x single largest directory).




### Type DistributedStats
```go
type DistributedStats struct {
	Prefix               string  `json:"prefix"`
	SubtreeBytes         int64   `json:"subtree_bytes"`
	SubtreeStorage       int64   `json:"subtree_storage"`
	SubtreeFiles         int64   `json:"subtree_files"`
	SubtreeDirs          int64   `json:"subtree_dirs"`
	SubtreePrefixBytes   int64   `json:"subtree_prefix_bytes"`
	DirectBytes          int64   `json:"direct_bytes"`
	DirectStorage        int64   `json:"direct_storage"`
	DirectFiles          int64   `json:"direct_files"`
	MaxLeafBytes         int64   `json:"max_leaf_bytes"`
	MaxLeafStorage       int64   `json:"max_leaf_storage"`
	MaxLeafFiles         int64   `json:"max_leaf_files"`
	MaxChildShare        float64 `json:"max_child_share"`
	BytesAmplification   float64 `json:"bytes_amplification"`
	StorageAmplification float64 `json:"storage_amplification"`
	FilesAmplification   float64 `json:"files_amplification"`
	DistributedBytes     int64   `json:"distributed_bytes"`
	DistributedStorage   int64   `json:"distributed_storage"`
	DistributedFiles     int64   `json:"distributed_files"`
}
```
DistributedStats describes a directory whose subtree accumulates
substantial usage across multiple descendant directories where neither a
single directory nor a single child dominates (for example, modest-fanout
hierarchies whose leaves each hold modest data).

### Functions

```go
func MergeDistributed(dh *DistributedHeaps, n int) []DistributedStats
```
MergeDistributed returns up to n unique DistributedStats entries across
the storage, bytes, and files heaps, ordered by DistributedStorage,
DistributedBytes, and DistributedFiles descending.


```go
func TopDistributed(h *heap.MinMax[int64, DistributedStats], n int) []DistributedStats
```
TopDistributed returns up to n entries from h in descending order.
It leaves h unchanged on return by popping and then restoring its entries,
and hence must not be called concurrently with any other use of h.




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
	TotalSparseFiles              int64
	TotalSparseBytes              int64
	Bytes                         *heap.MinMax[int64, T]
	StorageBytes                  *heap.MinMax[int64, T]
	PrefixBytes                   *heap.MinMax[int64, T]
	Files                         *heap.MinMax[int64, T]
	Prefixes                      *heap.MinMax[int64, T]
	// SparseBytes will be nil for stats computed before sparse files
	// were tracked.
	SparseBytes *heap.MinMax[int64, T]
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
func (h *Heaps[T]) Push(item T, t stats.Totals)
```
Push pushes item into the top N heaps and adds t, other than its hardlink
counts, to the totals for h.




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
	SparseBytes int64  `json:"sparse_bytes,omitempty"`
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
func (s *PerIDStats) Push(id int64, prefix string, t stats.Totals)
```




### Type SubtreeAggregator
```go
type SubtreeAggregator struct {
	Subtree     *Heaps[string]
	Distributed *DistributedHeaps
	// contains filtered or unexported fields
}
```
SubtreeAggregator computes recursive subtree totals and modest-fanout
distributed hotspots. Directories must be supplied in DFS pre-order (sorted
by normalized summary key), in which case it requires O(depth) memory.

### Functions

```go
func NewSubtreeAggregator(prefix string, n int) *SubtreeAggregator
```
NewSubtreeAggregator creates a SubtreeAggregator rooted at prefix with top-N
heap limit n.



### Methods

```go
func (sa *SubtreeAggregator) Finish()
```
Finish finalizes any remaining directory frames on the stack.


```go
func (sa *SubtreeAggregator) Step(summaryKey, path string, direct stats.Totals)
```
Step feeds the next directory in DFS pre-order (summaryKey ordered) into the
aggregator.




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







