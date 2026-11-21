# Package [cloudeng.io/cmd/idu/cli/mcp](https://pkg.go.dev/cloudeng.io/cmd/idu/cli/mcp?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/cli/mcp)](https://goreportcard.com/report/cloudeng.io/cmd/idu/cli/mcp)

```go
import cloudeng.io/cmd/idu/cli/mcp
```

Package mcp provides the mcp command, which runs the Model Context Protocol
server implemented by mcpserver on stdin/stdout so that AI agents can
query the databases of the configured filesystems. For example, to make it
available to Claude Code:

    claude mcp add idu -- idu mcp

or, for a specific configuration file:

    claude mcp add idu -- idu --config=<file> mcp

## Types
### Type Commands
```go
type Commands struct {
	// contains filtered or unexported fields
}
```
Commands implements the mcp command.

### Functions

```go
func New(env *cli.Env) *Commands
```
New returns the Commands for env.



### Methods

```go
func (c *Commands) MCP(ctx context.Context, values any, _ []string) error
```
MCP implements the mcp command, values must be a *Flags.




### Type Flags
```go
type Flags struct {
	OpenTimeout time.Duration `subcmd:"open-timeout,5s,how long a tool waits for a database that is locked by an analysis before reporting that it is"`
}
```
Flags are the flags for the mcp command.





