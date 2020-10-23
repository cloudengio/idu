# Package [cloudeng.io/cmd/idu/internal/exclusions](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/exclusions?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/exclusions)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/exclusions)

```go
import cloudeng.io/cmd/idu/internal/exclusions
```


## Types
### Type T
```go
type T struct {
	// contains filtered or unexported fields
}
```
T represents a set of exclusions as regular expressions.

### Functions

```go
func New(exclusions []config.Exclusions) *T
```
New creates a new instance of exclusions.



### Methods

```go
func (e T) Exclude(path string) bool
```
Exclude returns true if the supplied path matches any of the exclusions.







