# Package [cloudeng.io/cmd/idu/internal/database/badgerdb](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/database/badgerdb?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/database/badgerdb)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/database/badgerdb)

```go
import cloudeng.io/cmd/idu/internal/database/badgerdb
```


## Variables
### ReadOnly
```go
ReadOnly = database.ReadOnly[Options]

```

### WithTimeout
```go
WithTimeout = database.WithTimeout[Options]

```



## Functions
### Func Open
```go
func Open[T Options](location string, opts ...Option) (database.DB, error)
```
Open opens the specified database. If the database does not exist it will be
created.



## Types
### Type Database
```go
type Database struct {
	database.Options[Options]
	// contains filtered or unexported fields
}
```
Database represents a badger database.

### Methods

```go
func (db *Database) BadgerDB() *badger.DB
```


```go
func (db *Database) Clear(_ context.Context, logs, errors bool) error
```


```go
func (db *Database) Close(ctx context.Context) error
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
type Option func(o *database.Options[Options])
```
Option represents a specific option accepted by Open.

### Functions

```go
func WithBadgerOptions(opts badger.Options) Option
```
WithBadgerOptions specifies the options to be used when opening the
database. Note, that it overrides all other badger specific options when
used.




### Type Options
```go
type Options struct {
	badger.Options
}
```





