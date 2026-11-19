# Package [cloudeng.io/cmd/idu/internal/topn](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/topn?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/topn)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/topn)

```go
import cloudeng.io/cmd/idu/internal/topn
```

Package topn provides bounded, incrementally maintainable, top-N heaps and
their encoding for storage in an idu database.

A Heap holds at most capacity entries, typically an oversubscribed
multiple of the N entries of interest, so that entries which shrink or
are removed can be replaced without recomputing the heap from scratch.
It also records a threshold, τ, that is an upper bound on the value of every
key that is not in the heap: whenever a key is evicted, or is not admitted
because the heap is full, τ is raised to cover its value. Since a key whose
value grows beyond the heap's minimum is always admitted, every entry with
a value greater than τ is guaranteed to be in its exact position relative
to all keys, including those not in the heap. The number of such entries is
reported by Certified.

## Constants
### StateKey, PrefixesKey, SubtreesKey, IDTotalsKey
```go
const (
	StateKey    = keyPrefix + "state"
	PrefixesKey = keyPrefix + "prefixes"
	// SubtreesKey is the key for the rankings of the prefixes by their
	// recursive totals.
	SubtreesKey = keyPrefix + "subtrees"
	// IDTotalsKey is the key for the IDTotals.
	IDTotalsKey = keyPrefix + "ids"
)
```
The keys, in the database's metadata bucket, used to store the state and the
rankings.



## Functions
### Func GroupKey
```go
func GroupKey(gid int64) string
```
GroupKey returns the key for the rankings of the prefixes used by gid.

### Func Sorted
```go
func Sorted(m map[int64]stats.Totals, metric stats.Metric) []stats.Totals
```
Sorted returns the totals in m in descending order of metric, with ties
broken by ascending id. Ids with a zero value for metric are omitted.

### Func UserKey
```go
func UserKey(uid int64) string
```
UserKey returns the key for the rankings of the prefixes used by uid.



## Types
### Type Entry
```go
type Entry struct {
	Key   string
	Value int64
}
```
Entry is a single key and its value.


### Type Heap
```go
type Heap struct {
	// contains filtered or unexported fields
}
```
Heap is a bounded min-heap of entries indexed by key. It is not safe for
concurrent use.

### Functions

```go
func New(capacity int) *Heap
```
New returns a Heap that holds at most capacity entries.



### Methods

```go
func (h *Heap) AppendBinary(data []byte) []byte
```
AppendBinary appends the binary encoding of h to data.


```go
func (h *Heap) Capacity() int
```
Capacity returns the maximum number of entries held by the heap.


```go
func (h *Heap) Certified() int
```
Certified returns the number of entries whose value is greater than the
threshold and which are hence known to be ranked exactly.


```go
func (h *Heap) DecodeBinary(data []byte) ([]byte, error)
```
DecodeBinary decodes a Heap encoded by AppendBinary from data and returns
the remaining data.


```go
func (h *Heap) Delete(key string)
```
Delete removes key from the heap, if present. This does not change the
threshold since key no longer has a value.


```go
func (h *Heap) Len() int
```
Len returns the number of entries in the heap.


```go
func (h *Heap) NeedsRefresh(n int) bool
```
NeedsRefresh returns true if fewer than n entries are known to be ranked
exactly, ie. if the heap can no longer be trusted for a top-n and must be
recomputed. A heap whose threshold is zero has never had a key excluded and
is therefore always exact.


```go
func (h *Heap) Set(key string, v int64)
```
Set records that the value of key is now v. Keys with values that are zero
or less are removed since they are never of interest.


```go
func (h *Heap) Threshold() int64
```
Threshold returns τ, an upper bound on the value of every key that is not in
the heap.


```go
func (h *Heap) Top(n int) []Entry
```
Top returns up to n entries, or all entries if n is zero or less,
in descending order of value, with ties broken by key.


```go
func (h *Heap) Value(key string) (int64, bool)
```
Value returns the value of key if it is in the heap.




### Type IDTotals
```go
type IDTotals struct {
	Users, Groups map[int64]stats.Totals
}
```
IDTotals records the totals, across an entire filesystem, for every user and
group that owns any of its files or prefixes. They are exact, rather than
bounded rankings, since the number of ids is small compared with the number
of prefixes.

### Functions

```go
func NewIDTotals() *IDTotals
```
NewIDTotals returns an empty IDTotals.



### Methods

```go
func (t *IDTotals) Add(users, groups stats.PerIDTotals)
```
Add adds the per-user and per-group totals for a prefix.


```go
func (t *IDTotals) AppendBinary(data []byte) []byte
```
AppendBinary appends the binary encoding of t to data.


```go
func (t *IDTotals) IsNegative() bool
```
IsNegative returns true if any of the totals are negative, ie. if more was
subtracted than was ever added.


```go
func (t *IDTotals) Sub(users, groups stats.PerIDTotals)
```
Sub subtracts the per-user and per-group totals for a prefix.


```go
func (t *IDTotals) UnmarshalBinary(data []byte) error
```
UnmarshalBinary decodes IDTotals encoded by AppendBinary.




### Type Rankings
```go
type Rankings [stats.NumMetrics]*Heap
```
Rankings contains a Heap for each stats.Metric.

### Functions

```go
func NewRankings(capacity int) *Rankings
```
NewRankings returns Rankings whose heaps hold at most capacity entries.



### Methods

```go
func (r *Rankings) AppendBinary(data []byte) []byte
```
AppendBinary appends the binary encoding of r to data.


```go
func (r *Rankings) Delete(key string)
```
Delete removes key from all of the heaps.


```go
func (r *Rankings) Empty() bool
```
Empty returns true if none of the heaps contain any entries and none has
ever excluded a key, ie. if they convey no information.


```go
func (r *Rankings) NeedsRefresh(n int) bool
```
NeedsRefresh returns true if any of the heaps need to be refreshed for a
top-n, see Heap.NeedsRefresh.


```go
func (r *Rankings) Set(key string, t stats.Totals)
```
Set records that the totals for key are now t.


```go
func (r *Rankings) UnmarshalBinary(data []byte) error
```
UnmarshalBinary decodes Rankings encoded by AppendBinary.




### Type State
```go
type State struct {
	// Valid is false while an analysis is updating the rankings, and
	// remains false if that analysis is interrupted or fails, until
	// the rankings are refreshed.
	Valid bool
	// N and Capacity are the number of entries of interest and the
	// number held by each heap of the prefix rankings.
	N, Capacity int
	// PerIDN and PerIDCapacity are the number of entries of interest and
	// the number held by each heap of the per-user and per-group rankings.
	PerIDN, PerIDCapacity int
	// Users and Groups list the ids that have stored rankings, in
	// ascending order.
	Users, Groups []int64
}
```
State records the configuration and validity of the stored rankings.

### Methods

```go
func (s *State) AppendBinary(data []byte) []byte
```
AppendBinary appends the binary encoding of s to data.


```go
func (s *State) HasGroup(gid int64) bool
```
HasGroup returns true if gid has stored rankings.


```go
func (s *State) HasUser(uid int64) bool
```
HasUser returns true if uid has stored rankings.


```go
func (s *State) UnmarshalBinary(data []byte) error
```
UnmarshalBinary decodes a State encoded by AppendBinary.







