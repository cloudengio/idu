# Package [cloudeng.io/cmd/idu/idudb/defaultdb](https://pkg.go.dev/cloudeng.io/cmd/idu/idudb/defaultdb?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/idudb/defaultdb)](https://goreportcard.com/report/cloudeng.io/cmd/idu/idudb/defaultdb)

```go
import cloudeng.io/cmd/idu/idudb/defaultdb
```

Package defaultdb opens idu databases using the backend used by the idu
command, ie. badgerdb. It allows other commands to read the databases that
idu creates without having to choose and configure a backend.

For example, to read the summaries for a configured filesystem f:

    sr, err := summary.Open(ctx, f.Config(), defaultdb.OpenOptions()...)

and to read its current statistics, see snapshot.Computer.Current:

    sf, path, err := snapshot.Computer{}.Current(ctx, f, dir, n, defaultdb.OpenOptions()...)

## Functions
### Func BackendFactory
```go
func BackendFactory() idudb.BackendFactory
```
BackendFactory returns the factory for the default backend.

### Func OpenOptions
```go
func OpenOptions(opts ...idudb.OpenOption) []idudb.OpenOption
```
OpenOptions returns the options for opening a database with the default
backend, followed by opts, eg. idudb.WithOpenProgress to display a message
while waiting for a database that is locked by another process, eg.
an analysis, to be released.

### Func OpenReadOnly
```go
func OpenReadOnly(ctx context.Context, cfg *iduconfig.Settings, opts ...idudb.OpenOption) (idudb.Reader, error)
```
OpenReadOnly opens the database specified by cfg for reading only, see
OpenOptions for opts.




