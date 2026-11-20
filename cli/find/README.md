# Package [cloudeng.io/cmd/idu/cli/find](https://pkg.go.dev/cloudeng.io/cmd/idu/cli/find?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/cli/find)](https://goreportcard.com/report/cloudeng.io/cmd/idu/cli/find)

```go
import cloudeng.io/cmd/idu/cli/find
```

Package find implements idu's commands that scan the database for prefixes
and files that match flexible criteria, ie. find.

## Types
### Type Commands
```go
type Commands struct {
	// contains filtered or unexported fields
}
```
Commands implements the commands that scan the database for prefixes and
files that match flexible criteria, ie. find.

### Functions

```go
func New(env *cli.Env) *Commands
```
New returns the Commands that use env.



### Methods

```go
func (fc *Commands) Find(ctx context.Context, values interface{}, args []string) error
```
Find implements the find command, values must be a *Flags.




### Type Flags
```go
type Flags struct {
	cli.FSFlag
	Long   bool            `subcmd:"l,false,'show long listing for each result'"`
	Prefix flags.Repeating `subcmd:"prefix,,'prefix match expression'"`
}
```
Flags are the flags for the find command.





### TODO
- cnicolaou: generalize this to other filesystems.




