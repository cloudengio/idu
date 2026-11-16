# Package [cloudeng.io/cmd/idu/idudb/backends/badgerdb](https://pkg.go.dev/cloudeng.io/cmd/idu/idudb/backends/badgerdb?tab=doc)

```go
import cloudeng.io/cmd/idu/idudb/backends/badgerdb
```


## Types
### Type Database
```go
type Database struct {
	// contains filtered or unexported fields
}
```
Database represents a badger database.

### Functions

```go
func Open(location string, opts ...Option) (*Database, error)
```
Open opens the specified database. If the database does not exist it will be
created unless opened read-only.



### Methods

```go
func (db *Database) BadgerDB() *badger.DB
```


```go
func (db *Database) Clear(ctx context.Context, logs, errors bool) error
```


```go
func (db *Database) Close(_ context.Context) error
```
Close closes the database.


```go
func (db *Database) Compact(ctx context.Context) error
```
Compact flushes pending batched writes, flattens Badger's LSM tree, and runs
value log garbage collection.


```go
func (db *Database) Delete(ctx context.Context, prefix string) error
```
Delete deletes the value stored for exactly prefix.


```go
func (db *Database) DeleteError(ctx context.Context, key string) error
```
DeleteError deletes the error logged for exactly key.


```go
func (db *Database) DeleteErrors(ctx context.Context, prefix string) error
```


```go
func (db *Database) DeletePrefix(ctx context.Context, prefix string) error
```


```go
func (db *Database) DeleteSummary(ctx context.Context, exactKey, childPrefix string) error
```
DeleteSummary deletes exactKey and any descendant keys matching childPrefix
from the summary bucket without affecting sibling prefixes.


```go
func (db *Database) Flush(ctx context.Context) error
```
Flush persists any pending batched writes. It is safe to call concurrently
with batched writes.


```go
func (db *Database) Get(ctx context.Context, prefix string, buf *bytes.Buffer) error
```


```go
func (db *Database) GetBatch(ctx context.Context, keys []string, fn func(i int, val []byte) error) error
```
GetBatch calls fn with the value stored for each of keys, or nil if there is
none, reading all of them within a single read transaction since creating a
transaction per value is expensive.


```go
func (db *Database) GetMeta(ctx context.Context, key string, buf *bytes.Buffer) error
```
GetMeta retrieves a metadata entry from the metadata bucket into buf.


```go
func (db *Database) LastLog(ctx context.Context) (start, stop time.Time, detail []byte, err error)
```


```go
func (db *Database) Log(ctx context.Context, start, stop time.Time, detail []byte) error
```


```go
func (db *Database) LogError(ctx context.Context, key string, when time.Time, detail []byte) error
```


```go
func (db *Database) RunValueLogGC(discardRatio float64) error
```
RunValueLogGC runs Badger's value log garbage collection repeatedly until no
more files meet discardRatio.


```go
func (db *Database) Scan(ctx context.Context, path string, visitor func(ctx context.Context, key string, val []byte) bool) error
```


```go
func (db *Database) ScanSummaries(ctx context.Context, exactKey, childPrefix string, visitor func(ctx context.Context, key string, val []byte) bool) error
```
ScanSummaries iterates over entries in the summary bucket starting at
exactKey and stopping once keys no longer match exactKey or childPrefix.


```go
func (db *Database) Set(ctx context.Context, prefix string, val []byte, batch bool) error
```


```go
func (db *Database) SetMeta(ctx context.Context, key string, val []byte) error
```
SetMeta stores a metadata entry in the metadata bucket.


```go
func (db *Database) SetSummary(ctx context.Context, key string, val []byte, batch bool) error
```
SetSummary stores a precomputed summary value in the summary bucket.


```go
func (db *Database) Stream(ctx context.Context, path string, visitor func(ctx context.Context, key string, val []byte)) error
```


```go
func (db *Database) VisitErrors(ctx context.Context, key string,
	visitor func(ctx context.Context, key string, when time.Time, detail []byte) bool) error
```


```go
func (db *Database) VisitLogs(ctx context.Context, start, stop time.Time, visitor func(ctx context.Context, begin, end time.Time, detail []byte) bool) error
```




### Type Option
```go
type Option func(o *options)
```
Option represents a specific option accepted by Open.

### Functions

```go
func ReadOnly() Option
```
ReadOnly requests that the database is opened in read-only mode.


```go
func WithBadgerOptions(opts badger.Options) Option
```
WithBadgerOptions specifies the options to be used when opening the
database. Note, that it overrides all other badger specific options when
used.


```go
func WithTimeout(t time.Duration) Option
```
WithTimeout requests that the database be opened with the specified timeout.




### Type Options
```go
type Options struct {
	badger.Options
}
```





