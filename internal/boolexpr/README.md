# Package [cloudeng.io/cmd/idu/internal/boolexpr](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/boolexpr?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/boolexpr)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/boolexpr)

```go
import cloudeng.io/cmd/idu/internal/boolexpr
```

Package boolexpr provides a wrapper for cloudeng.io/cmdutil/boolexpr for use
with idu.

## Functions
### Func NewHardlink
```go
func NewHardlink(ctx context.Context, n, v string, fs file.FS) boolexpr.Operand
```
NewHardlink returns an operand that determines if the supplied value is,
or is not, a hardlink to the specified file or directory.

### Func NewParser
```go
func NewParser(ctx context.Context, fs filewalk.FS) *boolexpr.Parser
```

### Func NewParserTests
```go
func NewParserTests(ctx context.Context, fs filewalk.FS) *boolexpr.Parser
```
NewParserTests registers user and group operands that will accept any
uid/gid rather than testing to ensure that they exist.



## Types
### Type Matcher
```go
type Matcher struct {
	// contains filtered or unexported fields
}
```

### Functions

```go
func AlwaysMatch(p *boolexpr.Parser) Matcher
```


```go
func CreateMatcher(parser *boolexpr.Parser, opts ...Option) (Matcher, error)
```



### Methods

```go
func (m Matcher) Entry(prefix string, pi *prefixinfo.T, fi file.Info) bool
```


```go
func (m Matcher) IsHardlink(xattr file.XAttr) bool
```


```go
func (m Matcher) Prefix(prefix string, pi *prefixinfo.T) bool
```


```go
func (m Matcher) String() string
```




### Type Option
```go
type Option func(o *options)
```

### Functions

```go
func WithEmptyEntryValue(v bool) Option
```


```go
func WithEntryExpression(expr ...string) Option
```


```go
func WithFilewalkFS(fs filewalk.FS) Option
```


```go
func WithHardlinkHandling(v bool) Option
```
WithHardlinkHandling enables incrmental detection of hardlinks so as to
avoid visiting the second and subsequent file system entries that represent
the same file. This is primarily useful for avoiding overcounting the
resources shared by hardlinks. With this option enabled, the matcher's Entry
method will return false for any file that has already been seen (based on
its device and inode numbers).







