# Package [cloudeng.io/cmd/idu/cli/tui](https://pkg.go.dev/cloudeng.io/cmd/idu/cli/tui?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/cli/tui)](https://goreportcard.com/report/cloudeng.io/cmd/idu/cli/tui)

```go
import cloudeng.io/cmd/idu/cli/tui
```

Package tui implements idu's terminal user interface, see tui-design.md.
It provides an interactive view of a filesystem's usage, as a tree of
its prefixes together with the summary of, and top-N rankings beneath,
the highlighted prefix, and the ability to run analyze.

## Types
### Type Commands
```go
type Commands struct {
	// contains filtered or unexported fields
}
```
Commands implements the tui command.

### Functions

```go
func New(env *cli.Env, opts ...Option) *Commands
```
New returns the Commands that use env.



### Methods

```go
func (c *Commands) TUI(ctx context.Context, values interface{}, args []string) error
```
TUI implements the tui command, values must be a *Flags.




### Type Flags
```go
type Flags struct {
	cli.FSFlag
	TopN int `subcmd:"n,20,number of top entries to display in each of the top-N tables"`
}
```
Flags are the flags for the tui command.


### Type Option
```go
type Option func(*Commands)
```
Option configures the Commands created by New.

### Functions

```go
func WithLogToFile(fn func() bool) Option
```
WithLogToFile sets a function that returns true if log output is written to
a file, rather than the terminal, and hence need not be discarded while the
tui is running. It is called when the tui starts.







