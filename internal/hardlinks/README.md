# Package [cloudeng.io/cmd/idu/internal/hardlinks](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/hardlinks?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/hardlinks)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/hardlinks)

```go
import cloudeng.io/cmd/idu/internal/hardlinks
```


## Types
### Type Incremental
```go
type Incremental struct {
	// contains filtered or unexported fields
}
```
Incremental tracks devices and inodes to determine if a newly seen file or
directory is a duplicate, i.e. is a hard link to an existing filesystem
entries. It is incremental and hence cannot detect the first entry in a set
of hard links.

### Methods

```go
func (i *Incremental) Ref(dev uint64, ino uint64) bool
```







