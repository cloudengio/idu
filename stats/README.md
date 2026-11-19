# Package [cloudeng.io/cmd/idu/stats](https://pkg.go.dev/cloudeng.io/cmd/idu/stats?tab=doc)

```go
import cloudeng.io/cmd/idu/stats
```


## Constants
### NumMetrics
```go
const NumMetrics = int(MetricSparseBytes) + 1
```
NumMetrics is the number of defined Metrics.

### PrefixSummaryVersion
```go
const PrefixSummaryVersion = 0x3
```
PrefixSummaryVersion is the version of the binary encoding of PrefixSummary,
which includes that of Totals.

### SparseThreshold
```go
const SparseThreshold = 64 * 1024
```
SparseThreshold is the minimum number of bytes of a file's size that must
not be allocated on disk for it to be considered sparse. It excludes small
files whose data is stored inline in their inode or directory entry and
hence have no allocated blocks, as well as platforms, such as Windows,
that derive the number of blocks from the file's size.



## Functions
### Func ComputeTotals
```go
func ComputeTotals(prefix string, pi PrefixInfo, du diskusage.Calculator, match Matcher) (totals Totals, perUser, perGroup PerIDTotals)
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

### Func IsSummaryDescendant
```go
func IsSummaryDescendant(parentKey, childKey string) bool
```
IsSummaryDescendant returns true if childKey is the summary key of a
descendant of the prefix with summary key parentKey.

### Func NewParser
```go
func NewParser(ctx context.Context, fs filewalk.FS, opts ...ParserOption) *boolexpr.Parser
```
NewParser returns a parser for expressions that can be used with NewMatcher.
It supports all of the operands provided by cloudeng.io/file/matcher as
well as user and group operands that accept user/group names or IDs,
and a hardlink operand that uses fs to determine the file referred to.
Additional operands may be registered using WithOperand.

### Func NormalizePath
```go
func NormalizePath(sep, path string) string
```
NormalizePath trims any trailing path separator from path using sep
(defaulting to "/"), while preserving filesystem root paths such as "/" or
"C:\".

### Func ParentSummaryKey
```go
func ParentSummaryKey(key string) (string, bool)
```
ParentSummaryKey returns the summary key of the parent of the prefix with
summary key key, or false if key is that of a filesystem root.

### Func SparseBytes
```go
func SparseBytes(size, blocks int64) int64
```
SparseBytes returns the number of bytes of a file of size bytes that
occupies blocks blocks that are not allocated on disk, or zero if that
is less than SparseThreshold. Note that compressed files will also appear
sparse.

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
	SparseBytes  int64
}
```
FileEntry records the contribution of a file to the totals of the prefix
that contains it.


### Type HardlinkRecording
```go
type HardlinkRecording int
```
HardlinkRecording determines which files, if any, a PrefixSummary records so
that hardlinks can be deduplicated by ApplyHardlinks.

### Constants
### RecordNoFiles, RecordLinkedFiles, RecordAllFiles
```go
const (
	// RecordNoFiles records no hardlink information.
	RecordNoFiles HardlinkRecording = iota
	// RecordLinkedFiles records the files that may be hardlinks, ie. those
	// whose link count is greater than one or is unknown (zero). Link
	// counts can be stale: a file's link count changes when a link to it
	// is created, but its directory does not, and hence a file may still
	// be recorded as having a single link if its directory has not been
	// rescanned since. Such a file is then not deduplicated.
	RecordLinkedFiles
	// RecordAllFiles records every file, and hence deduplication does
	// not depend on the link counts being up to date.
	RecordAllFiles
)
```



### Methods

```go
func (r HardlinkRecording) String() string
```
String implements fmt.Stringer.




### Type MatchEntry
```go
type MatchEntry struct {
	// contains filtered or unexported fields
}
```
MatchEntry is the value that expressions are evaluated against for each
entry (file or directory) within a prefix.

### Methods

```go
func (e MatchEntry) Info() file.Info
```
Info returns the file.Info for the entry.


```go
func (e MatchEntry) ModTime() time.Time
```
ModTime returns the modification time of the entry.


```go
func (e MatchEntry) Mode() fs.FileMode
```
Mode returns the mode of the entry.


```go
func (e MatchEntry) Name() string
```
Name returns the name of the entry within its prefix.


```go
func (e MatchEntry) Path() string
```
Path returns the full path name of the entry.


```go
func (e MatchEntry) Prefix() PrefixInfo
```
Prefix returns the metadata for the prefix that contains the entry.


```go
func (e MatchEntry) Size() int64
```
Size returns the size of the entry in bytes.


```go
func (e MatchEntry) Type() fs.FileMode
```
Type returns the type bits of the entry.


```go
func (e MatchEntry) XAttr() file.XAttr
```
XAttr returns the extended attributes (e.g. user and group) for the entry.




### Type MatchPrefix
```go
type MatchPrefix struct {
	PrefixInfo
	// contains filtered or unexported fields
}
```
MatchPrefix is the value that expressions are evaluated against for each
prefix (directory).

### Methods

```go
func (p MatchPrefix) Name() string
```
Name returns the base name of the prefix.


```go
func (p MatchPrefix) NumEntries() int64
```
NumEntries returns the number of entries in the prefix.


```go
func (p MatchPrefix) Path() string
```
Path returns the full path name of the prefix.




### Type Matcher
```go
type Matcher struct {
	// contains filtered or unexported fields
}
```
Matcher evaluates a boolean expression against prefixes and the entries
within them.

### Functions

```go
func AlwaysMatch() Matcher
```
AlwaysMatch returns a Matcher that matches all prefixes and entries.


```go
func NewMatcher(parser *boolexpr.Parser, opts ...MatcherOption) (Matcher, error)
```
NewMatcher returns a Matcher for the expression specified via
WithExpression, parsed using parser. The parser may be nil if no expression
is specified.



### Methods

```go
func (m Matcher) Entry(prefix string, pi PrefixInfo, fi file.Info) bool
```
Entry evaluates the expression against the entry fi within prefix.


```go
func (m Matcher) HasExpression() bool
```
HasExpression reports whether an expression was specified.


```go
func (m Matcher) IsHardlink(xattr file.XAttr) bool
```
IsHardlink returns true if hardlink handling is enabled and the file
identified by xattr has been seen before. Files whose inode number is zero,
ie. unknown, are never hardlinks.


```go
func (m Matcher) Prefix(prefix string, pi PrefixInfo) bool
```
Prefix evaluates the expression against prefix.


```go
func (m Matcher) String() string
```




### Type MatcherOption
```go
type MatcherOption func(o *matcherOptions)
```
MatcherOption represents an option to NewMatcher.

### Functions

```go
func WithEmptyValue(v bool) MatcherOption
```
WithEmptyValue specifies the value to be returned when no expression is
specified.


```go
func WithExpression(expr ...string) MatcherOption
```
WithExpression specifies the expression to be evaluated. Multiple arguments
are joined using spaces.


```go
func WithFilewalkFS(fs filewalk.FS) MatcherOption
```
WithFilewalkFS specifies the filesystem used to construct the names and
paths of prefixes and entries.


```go
func WithHardlinkHandling(v bool) MatcherOption
```
WithHardlinkHandling enables incremental detection of hardlinks so as to
avoid counting the second and subsequent file system entries that represent
the same file. With this option enabled, IsHardlink will return true for any
file that has already been seen (based on its device and inode numbers).




### Type Metric
```go
type Metric int
```
Metric identifies one of the values recorded in Totals that prefixes,
users and groups can be ranked by.

### Constants
### MetricBytes, MetricStorageBytes, MetricFiles, MetricPrefixes, MetricPrefixBytes, MetricSparseBytes
```go
const (
	// MetricBytes ranks by Totals.Bytes.
	MetricBytes Metric = iota
	// MetricStorageBytes ranks by Totals.StorageBytes.
	MetricStorageBytes
	// MetricFiles ranks by Totals.Files.
	MetricFiles
	// MetricPrefixes ranks by Totals.SubPrefixes, ie. the number of
	// prefixes (directories) contained in a prefix.
	MetricPrefixes
	// MetricPrefixBytes ranks by Totals.PrefixBytes.
	MetricPrefixBytes
	// MetricSparseBytes ranks by Totals.SparseBytes.
	MetricSparseBytes
)
```



### Functions

```go
func Metrics() []Metric
```
Metrics returns all of the defined Metrics.


```go
func ParseMetric(name string) (Metric, error)
```
ParseMetric returns the Metric whose String value is name.



### Methods

```go
func (m Metric) String() string
```
String implements fmt.Stringer.


```go
func (m Metric) Value(t Totals) int64
```
Value returns the value of m in t.




### Type OperandFactory
```go
type OperandFactory func(name, value string) boolexpr.Operand
```
OperandFactory creates an operand for use in an expression, see
boolexpr.Parser.RegisterOperand.


### Type ParserOption
```go
type ParserOption func(o *parserOptions)
```
ParserOption represents an option to NewParser.

### Functions

```go
func WithOperand(name string, factory OperandFactory) ParserOption
```
WithOperand registers an additional operand with the parser created by
NewParser. It may also be used to replace one of the default operands by
specifying the same name. The value passed to the operand's Eval method will
be either a MatchPrefix or a MatchEntry and operands should return false for
any other type.




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
func (pid *PerIDTotals) DecodeBinaryChecked(data []byte) ([]byte, error)
```
DecodeBinaryChecked decodes PerIDTotals encoded by AppendBinary from data
and returns the remaining data.


```go
func (pid PerIDTotals) MarshalBinary() (data []byte, err error)
```


```go
func (pid *PerIDTotals) UnmarshalBinary(data []byte) error
```
UnmarshalBinary decodes PerIDTotals encoded by AppendBinary, returning an
error, rather than panicking, if data is invalid.




### Type PrefixInfo
```go
type PrefixInfo interface {
	// Size returns the size of the prefix itself.
	Size() int64
	// Blocks returns the number of blocks allocated to the prefix itself.
	Blocks() int64
	// Mode returns the mode of the prefix.
	Mode() fs.FileMode
	// Type returns the type bits of the prefix's mode.
	Type() fs.FileMode
	// ModTime returns the modification time of the prefix.
	ModTime() time.Time
	// UserGroup returns the user and group IDs of the prefix.
	UserGroup() (uid, gid int64)
	// DevIno returns the device and inode numbers of the prefix.
	DevIno() (device, inode uint64)
	// XAttr returns the user, group, device, inode etc. of the prefix.
	XAttr() file.XAttr
	// InfoList returns the prefix's entries, ie. its files and its
	// immediate child prefixes, for which IsDir returns true.
	InfoList() file.InfoList
	// FilesOnly returns the entries that are files, ie. not prefixes.
	FilesOnly() file.InfoList
	// PrefixesOnly returns the entries that are prefixes.
	PrefixesOnly() file.InfoList
	// XAttrInfo returns the user, group, device, inode etc. of fi, which
	// must be one of the entries returned by InfoList, FilesOnly or
	// PrefixesOnly.
	XAttrInfo(fi file.Info) file.XAttr
}
```
PrefixInfo represents the metadata recorded for a single prefix (directory)
that is needed to compute its statistics and to evaluate expressions
against it and its entries. It is implemented by the representation used
for prefixes in the database. The MatchPrefix values that expressions
are evaluated against embed a PrefixInfo, and MatchEntry values provide
access to the PrefixInfo for the entry's prefix, so that custom operands,
see WithOperand, can use any of its methods.


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
	// Files lists the files counted in Direct that are recorded so that
	// hardlinks can be deduplicated, see HardlinkRecording.
	Files []FileEntry
}
```
PrefixSummary stores precomputed direct (non-recursive) statistics for a
single prefix along with the calculator identifier used for StorageBytes.

### Functions

```go
func ComputeSummary(prefix string, pi PrefixInfo, du diskusage.Calculator, match Matcher, rec HardlinkRecording) PrefixSummary
```
ComputeSummary computes a PrefixSummary for a single directory/prefix.
Unless rec is RecordNoFiles (or unrecognized) the summary records the
information required for hardlinks to be deduplicated by ApplyHardlinks.
match should not perform hardlink handling itself.



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
a hardlink-handling matcher yields identical results. Prefixes and files
whose inode number is zero, ie. unknown, are never treated as hardlinks.
It has no effect if HardlinkInfo is false.


```go
func (ps *PrefixSummary) Effective(dup bool, owns func(fileID uint64) bool) PrefixSummary
```
Effective returns a copy of ps adjusted, as ApplyHardlinks would be,
to exclude the prefix itself if dup is true, ie. if it is a duplicate of
a prefix that precedes it, such as a bind mount or a directory hardlink,
or otherwise to exclude the files that it does not own, recording them as
hardlinks instead. owns is called with the inode number of each file that
the prefix records, other than repeated ones, which are always hardlinks,
and those whose inode number is zero, which never are; it should return true
if the prefix is the first to contain that file. ps itself is not modified.
It returns a copy of ps if HardlinkInfo is false.


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
	SparseFiles  int64 // number of sparse files, see SparseBytes
	SparseBytes  int64 // total size of sparse files not allocated on disk
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
func (t Totals) IsNegative() bool
```
IsNegative returns true if any of the counts or byte totals in t are
negative, which can only be the result of subtracting totals that were never
added.


```go
func (t Totals) IsZero() bool
```
IsZero returns true if all of the counts and byte totals in t, ie. ignoring
its ID, are zero.


```go
func (t *Totals) MarshalBinary() (data []byte, err error)
```


```go
func (t *Totals) Sub(other Totals)
```
Sub subtracts the counts and byte totals in other from t.


```go
func (t *Totals) UnmarshalBinary(data []byte) error
```
UnmarshalBinary decodes Totals encoded by AppendBinary, returning an error,
rather than panicking, if data is invalid.







