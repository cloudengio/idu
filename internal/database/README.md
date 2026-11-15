# Package [cloudeng.io/cmd/idu/internal/database](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/database?tab=doc)

```go
import cloudeng.io/cmd/idu/internal/database
```


## Functions
### Func ReadOnly
```go
func ReadOnly[T any]() func(o *Options[T])
```
ReadOnly requests that the database is opened in read-only mode.

### Func WithTimeout
```go
func WithTimeout[T any](t time.Duration) func(o *Options[T])
```
WithTimeout requests that the database be opened with the specified timeout.



## Types
### Type DB
```go
type DB interface {
	// Set stores the value associated with prefix. If batch is true then
	// calls from multiple goroutines may be merged together. This is only
	// useful when Set is called from multiple goroutines.
	Set(ctx context.Context, prefix string, value []byte, batch bool) error

	// Get retrieves the value associated with prefix storing
	// its contents in the supplied bytes.Buffer.
	Get(ctx context.Context, prefix string, buf *bytes.Buffer) error

	// GetBatch calls fn with the value associated with each of prefixes,
	// or nil if there is none, in order. val is only valid for the duration
	// of the call to fn. It is more efficient than calling Get for each
	// prefix.
	GetBatch(ctx context.Context, prefixes []string, fn func(i int, val []byte) error) error

	// Delete deletes the value associated with exactly prefix, if any.
	Delete(ctx context.Context, prefix string) error

	// DeletePrefix deletes all keys that have the specified prefix.
	DeletePrefix(ctx context.Context, prefix string) error

	// Flush persists any pending batched writes. It is safe to call
	// concurrently with batched writes.
	Flush(ctx context.Context) error

	// DeleteError deletes the error logged for exactly key, if any.
	DeleteError(ctx context.Context, key string) error

	// DeleteErrors deletes all errors that have the specified prefix.
	DeleteErrors(ctx context.Context, prefix string) error

	// Scan can be used to iterate over all keys in the database starting at
	// the specified key.
	Scan(ctx context.Context, key string, visitor func(ctx context.Context, key string, val []byte) bool) error

	// Stream can be used to iterate over all keys in the database concurrently
	// that have the specified prefix.
	Stream(ctx context.Context, prefix string, visitor func(ctx context.Context, key string, val []byte)) error

	// Log should be used to record the start and stop time for
	// a given operation and associated details/description.
	Log(ctx context.Context, start, stop time.Time, detail []byte) error

	// LastLog returns the most recently recorded log entry.
	LastLog(ctx context.Context) (start, stop time.Time, detail []byte, err error)

	// VisitLogs calls visitor for every log entry between start and stop. The
	// visitor func should return false if it wants to stop the iteration over
	// log entries.
	VisitLogs(ctx context.Context, start, stop time.Time,
		visitor func(ctx context.Context, begin, end time.Time, detail []byte) bool) error

	// LogError records an error.
	LogError(ctx context.Context, key string, when time.Time, detail []byte) error

	// VisitErrors calls visitor for every error starting at key. The
	// visitor func should return false if it wants to stop the iteration over
	// errors.
	VisitErrors(ctx context.Context, key string, visitor func(ctx context.Context, key string, when time.Time, val []byte) bool) error

	// Clear clears all of the log or error entries.
	Clear(ctx context.Context, logs, errors bool) error

	// SetSummary stores a precomputed summary record for key in the summary bucket.
	SetSummary(ctx context.Context, key string, value []byte, batch bool) error

	// DeleteSummary deletes the summary entry matching exactKey and any
	// descendant summary entries matching childPrefix.
	DeleteSummary(ctx context.Context, exactKey, childPrefix string) error

	// ScanSummaries iterates over summary entries in lexicographical key order,
	// starting at exactKey and stopping once keys no longer match exactKey or
	// childPrefix (when either is non-empty).
	ScanSummaries(ctx context.Context, exactKey, childPrefix string, visitor func(ctx context.Context, key string, val []byte) bool) error

	// SetMeta stores a metadata value in the metadata bucket.
	SetMeta(ctx context.Context, key string, value []byte) error

	// GetMeta retrieves a metadata value from the metadata bucket into buf.
	GetMeta(ctx context.Context, key string, buf *bytes.Buffer) error

	// Close closes the database.
	Close(context.Context) error
}
```
DB represents a database.


### Type Options
```go
type Options[T any] struct {
	ReadOnly  bool
	Timeout   time.Duration
	Hardlinks bool
	Sub       T
}
```





