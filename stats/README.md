# Package [cloudeng.io/cmd/idu/stats](https://pkg.go.dev/cloudeng.io/cmd/idu/stats?tab=doc)

```go
import cloudeng.io/cmd/idu/stats
```


## Functions
### Func ComputeTotals
```go
func ComputeTotals(prefix string, pi *prefixinfo.T, du diskusage.Calculator, match boolexpr.Matcher) (totals Totals, perUser, perGroup PerIDTotals)
```
ComputeTotals computes the totals for the prefix itself and any
non-directory contents. Hardlinks are handled as per match.IsHardlink.
Note that:
 1. Prefixes is one if the prefix matched the expression and zero otherwise.
 2. SubPrefixes is the number of prefixes this prefix contains
 3. The size of this prefix is included in the totals for the prefix,
    but the sizes of prefixes it contains are not.

### Func DecodeSummaryKey
```go
func DecodeSummaryKey(sep, key string) string
```
DecodeSummaryKey reverses SummaryKeys back to a filesystem path.

### Func SummaryKeys
```go
func SummaryKeys(sep, path string) (exactKey, childPrefix string)
```
SummaryKeys converts a filesystem path and separator into the exact key
and child-prefix key used by the ordered summary bucket. Replacing the
path separator with NUL ('\x00') ensures lexicographical ordering matches
DFS pre-order and prevents sibling names like "/a-b" from sorting inside
"/a/...".



## Types
### Type FileEntry
```go
type FileEntry struct {
	FileID       uint64
	UID, GID     int64
	Bytes        int64
	StorageBytes int64
}
```
FileEntry records the contribution of a file to the totals of the prefix
that contains it.


### Type PerIDTotals
```go
type PerIDTotals []Totals
```

### Methods

```go
func (pid PerIDTotals) AppendBinary(data []byte) []byte
```


```go
func (pid *PerIDTotals) DecodeBinary(data []byte) []byte
```


```go
func (pid PerIDTotals) MarshalBinary() (data []byte, err error)
```


```go
func (pid *PerIDTotals) UnmarshalBinary(data []byte) error
```




### Type PrefixSummary
```go
type PrefixSummary struct {
	CalcID   string
	Direct   Totals
	PerUser  PerIDTotals
	PerGroup PerIDTotals

	// HardlinkInfo is true if the summary records the information needed
	// to deduplicate hardlinks, i.e. Device, FileID and Files, via
	// ApplyHardlinks.
	HardlinkInfo bool
	// Device and FileID identify the prefix itself.
	Device, FileID uint64
	// Files lists every file counted in Direct. All files are recorded,
	// rather than only those with a link count greater than one, since
	// a hardlink may be created to a file in a prefix that is not rescanned
	// because it is unchanged.
	Files []FileEntry
}
```
PrefixSummary stores precomputed direct (non-recursive) statistics for a
single prefix along with the calculator identifier used for StorageBytes.

### Functions

```go
func ComputeSummary(prefix string, pi *prefixinfo.T, du diskusage.Calculator, match boolexpr.Matcher, hardlinkInfo bool) PrefixSummary
```
ComputeSummary computes a PrefixSummary for a single directory/prefix.
If hardlinkInfo is true the summary records the information required for
hardlinks to be deduplicated by ApplyHardlinks. match should not perform
hardlink handling itself.



### Methods

```go
func (ps *PrefixSummary) AppendBinary(data []byte) []byte
```
AppendBinary appends the binary encoding of ps to data and returns the
extended slice.


```go
func (ps *PrefixSummary) ApplyHardlinks(hl *hardlinks.Incremental)
```
ApplyHardlinks adjusts ps to exclude the prefix itself, or any of its files,
that have already been seen by hl, recording them as hardlinks instead.
Applying it to summaries in the same order as ComputeTotals is called with
a hardlink-handling matcher yields identical results. It has no effect if
HardlinkInfo is false.


```go
func (ps *PrefixSummary) MarshalBinary() ([]byte, error)
```
MarshalBinary implements encoding.BinaryMarshaler.


```go
func (ps *PrefixSummary) UnmarshalBinary(data []byte) error
```
UnmarshalBinary implements encoding.BinaryUnmarshaler.




### Type Totals
```go
type Totals struct {
	ID           int64
	Files        int64 // number of files
	Prefix       int64 // will be 1 or 0
	SubPrefixes  int64 // number of prefixes/directories
	Bytes        int64 // total size of files
	StorageBytes int64 // total size of files on disk
	PrefixBytes  int64 // total size of prefixes
	Hardlinks    int64 // number of hardlinks
	HardlinkDirs int64 // number of hardlinks to directories
}
```
TODO: parametize the ID type to be int64 or string for non-posix system.

### Methods

```go
func (t *Totals) Add(other Totals)
```
Add accumulates the counts and byte totals from other into t.


```go
func (t *Totals) AppendBinary(data []byte) []byte
```


```go
func (t *Totals) DecodeBinary(data []byte) []byte
```


```go
func (t *Totals) MarshalBinary() (data []byte, err error)
```


```go
func (t *Totals) UnmarshalBinary(data []byte) error
```







