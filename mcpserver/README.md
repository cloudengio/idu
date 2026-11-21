# Package [cloudeng.io/cmd/idu/mcpserver](https://pkg.go.dev/cloudeng.io/cmd/idu/mcpserver?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/mcpserver)](https://goreportcard.com/report/cloudeng.io/cmd/idu/mcpserver)

```go
import cloudeng.io/cmd/idu/mcpserver
```

Package mcpserver provides a Model Context Protocol (MCP) server that allows
AI agents to query the databases of the filesystems analyzed by idu: the
filesystems that are configured, the totals for any prefix and its children,
the top-N prefixes, subtrees, users and groups by any metric, and the errors
recorded by analyze. All of its tools are read-only.

The server uses only idu's public packages and may be embedded in other
commands, it is run by idu mcp. For example, to serve the configured
filesystems on stdin/stdout:

    cfg, _, err := iduconfig.Load(ctx, &cmdconfig.FilesFlag{})
    srv := mcpserver.New(cfg, mcpserver.WithOpenOptions(defaultdb.OpenOptions()...))
    err = srv.Run(ctx, &mcp.StdioTransport{})

Each tool call opens the database it needs, for reading only, and closes
it before returning so that the server never prevents analyze from opening
the database for writing. Conversely, while an analysis is running,
its database is locked and the tools that need it fail, after a timeout,
see WithOpenTimeout, with an error that says so.

## Constants
### DefaultOpenTimeout
```go
const DefaultOpenTimeout = 5 * time.Second
```
DefaultOpenTimeout is the default time that a tool waits for a database that
is locked, eg. by an analysis, to be released.



## Variables
### ErrLocked
```go
ErrLocked = errors.New("the database is locked, probably because an analysis is running: try again later")

```
ErrLocked is returned, wrapped, by tools whose database could not be opened
within the open timeout, typically because an analysis is running.



## Types
### Type Child
```go
type Child struct {
	Path string `json:"path"`
	// Recursive is omitted if the recursive totals are not available.
	Recursive *Totals `json:"recursive,omitempty"`
	Direct    Totals  `json:"direct"`
}
```
Child is a child of a prefix.


### Type Children
```go
type Children struct {
	Path     string  `json:"path"`
	Metric   string  `json:"metric"`
	Total    int     `json:"total" jsonschema:"the number of children, which may be more than are returned"`
	Children []Child `json:"children"`
	Note     string  `json:"note,omitempty"`
}
```
Children is the result of children.


### Type ChildrenArgs
```go
type ChildrenArgs struct {
	FSArgs
	Metric string `json:"metric,omitempty" jsonschema:"the metric to order the children by, storage_bytes by default"`
	N      int    `json:"n,omitempty" jsonschema:"the maximum number of children to return, 20 by default"`
}
```
ChildrenArgs are the arguments to children.


### Type Entry
```go
type Entry struct {
	Path  string `json:"path,omitempty"`
	User  string `json:"user,omitempty"`
	Group string `json:"group,omitempty"`
	ID    *int64 `json:"id,omitempty" jsonschema:"the numeric id of the user or group"`
	Value int64  `json:"value"`
}
```
Entry is an entry in a ranking.


### Type ErrorEntry
```go
type ErrorEntry struct {
	Path    string    `json:"path"`
	When    time.Time `json:"when"`
	Message string    `json:"message"`
}
```
ErrorEntry is an error recorded by analyze.


### Type Errors
```go
type Errors struct {
	Path   string       `json:"path"`
	Errors []ErrorEntry `json:"errors"`
	More   bool         `json:"more" jsonschema:"true if there are more errors than were returned"`
}
```
Errors is the result of errors.


### Type ErrorsArgs
```go
type ErrorsArgs struct {
	FSArgs
	N int `json:"n,omitempty" jsonschema:"the maximum number of errors to return, 20 by default"`
}
```
ErrorsArgs are the arguments to errors.


### Type FSArgs
```go
type FSArgs struct {
	FS     string `json:"fs,omitempty" jsonschema:"the name, or root, of the filesystem, see list_filesystems; if omitted, an absolute prefix selects the filesystem that contains it and otherwise the default filesystem is used"`
	Prefix string `json:"prefix,omitempty" jsonschema:"a path, either absolute or relative to the filesystem's root; omit it for the entire filesystem"`
}
```
FSArgs identify a filesystem and a prefix within it.


### Type Filesystem
```go
type Filesystem struct {
	Name         string    `json:"name,omitempty"`
	Location     string    `json:"location" jsonschema:"the root of the filesystem, or the range of keys for a key oriented store"`
	Database     string    `json:"database"`
	LastAnalyzed time.Time `json:"last_analyzed,omitzero"`
	Status       string    `json:"status" jsonschema:"ok, not analyzed, or why the database could not be read, eg. because it is locked by an analysis"`
	Default      string    `json:"default,omitempty" jsonschema:"set, to why, for the filesystem used when a tool call specifies neither fs nor an absolute prefix"`
}
```
Filesystem describes a configured filesystem.


### Type Filesystems
```go
type Filesystems struct {
	Filesystems []Filesystem `json:"filesystems"`
}
```
Filesystems is the result of list_filesystems.


### Type Option
```go
type Option func(*options)
```
Option configures a Server.

### Functions

```go
func WithDefaultFilesystem(fs string) Option
```
WithDefaultFilesystem sets the filesystem, by name or location, used by
tool calls that specify neither a filesystem nor an absolute prefix.
By default it is the only filesystem configured or, if there are several,
the local one, ie. the one whose root is /.


```go
func WithOpenOptions(opts ...idudb.OpenOption) Option
```
WithOpenOptions sets the options used to open databases, which must
include idudb.WithBackendFactory, see defaultdb.OpenOptions. Any messages
written while waiting for a locked database are discarded unless
idudb.WithOpenProgress is included, which must not write to the transport,
eg. stdout for the stdio transport.


```go
func WithOpenTimeout(d time.Duration) Option
```
WithOpenTimeout sets the time that a tool waits for a locked database to be
released, DefaultOpenTimeout by default.


```go
func WithVersion(v string) Option
```
WithVersion sets the version reported by the server.




### Type QueryArgs
```go
type QueryArgs struct {
	FSArgs
	Table   string `json:"table,omitempty" jsonschema:"prefixes (the default), users, groups or hotspots"`
	Scope   string `json:"scope,omitempty" jsonschema:"self, children or subtree (the default), relative to the prefix"`
	OrderBy string `json:"order_by,omitempty" jsonschema:"the column to order the rows by, largest first, total_storage_bytes by default, or direct_storage_bytes for the users or groups of a prefix"`
	User    string `json:"user,omitempty" jsonschema:"rank prefixes by the usage of this user's files, a name or numeric id"`
	Group   string `json:"group,omitempty" jsonschema:"rank prefixes by the usage of this group's files, a name or numeric id"`
	Level   int    `json:"level,omitempty" jsonschema:"group by the prefixes at this depth below the root, only the depth of the prefix's children is supported"`
	Filter  string `json:"filter,omitempty" jsonschema:"a filter expression, not supported"`
	N       int    `json:"n,omitempty" jsonschema:"the maximum number of rows to return, 20 by default"`
}
```
QueryArgs are the arguments to query.


### Type QueryResult
```go
type QueryResult struct {
	Path    string     `json:"path"`
	Table   string     `json:"table"`
	Scope   string     `json:"scope"`
	OrderBy string     `json:"order_by"`
	Rows    []QueryRow `json:"rows"`
	Exact   bool       `json:"exact" jsonschema:"true if the rows are known to be exactly the top rows"`
	Source  string     `json:"source" jsonschema:"where the rows came from"`
}
```
QueryResult is the result of query.


### Type QueryRow
```go
type QueryRow struct {
	Path    string                   `json:"path,omitempty"`
	Parent  string                   `json:"parent,omitempty"`
	Name    string                   `json:"name,omitempty"`
	Depth   int                      `json:"depth"`
	ID      *int64                   `json:"id,omitempty" jsonschema:"the numeric id of the user or group"`
	User    string                   `json:"user,omitempty"`
	Group   string                   `json:"group,omitempty"`
	Direct  *Totals                  `json:"direct,omitempty" jsonschema:"the totals for the prefix's, user's or group's own files, if known"`
	Total   *Totals                  `json:"total,omitempty" jsonschema:"the recursive totals, if known"`
	Hotspot *report.DistributedStats `json:"hotspot,omitempty"`
	Value   int64                    `json:"value" jsonschema:"the value of the column that the rows are ordered by"`
}
```
QueryRow is a row returned by query.


### Type Server
```go
type Server struct {
	// contains filtered or unexported fields
}
```
Server is an MCP server for the filesystems in an idu configuration.

### Functions

```go
func New(cfg iduconfig.T, opts ...Option) *Server
```
New returns a Server for the filesystems in cfg.



### Methods

```go
func (s *Server) MCPServer() *mcp.Server
```
MCPServer returns the underlying mcp.Server, eg. to add further tools or to
connect it to a transport.


```go
func (s *Server) Run(ctx context.Context, transport mcp.Transport) error
```
Run runs the server on transport until the client disconnects or ctx is
canceled.




### Type TopN
```go
type TopN struct {
	Path    string  `json:"path"`
	Metric  string  `json:"metric"`
	By      string  `json:"by"`
	Entries []Entry `json:"entries"`
	Exact   bool    `json:"exact" jsonschema:"true if the ranking is known to be exact"`
	Source  string  `json:"source" jsonschema:"how the ranking was obtained"`
}
```
TopN is the result of top_n.


### Type TopNArgs
```go
type TopNArgs struct {
	FSArgs
	Metric string `json:"metric,omitempty" jsonschema:"the metric to rank by, storage_bytes by default"`
	By     string `json:"by,omitempty" jsonschema:"what to rank: subtree (the default), prefix, user, group or hotspot"`
	User   string `json:"user,omitempty" jsonschema:"for by=prefix, rank the prefixes by the usage of this user's files, a name or numeric id"`
	Group  string `json:"group,omitempty" jsonschema:"for by=prefix, rank the prefixes by the usage of this group's files, a name or numeric id"`
	N      int    `json:"n,omitempty" jsonschema:"the number of entries to return, 20 by default"`
}
```
TopNArgs are the arguments to top_n.


### Type Totals
```go
type Totals struct {
	Files        int64 `json:"files"`
	Prefixes     int64 `json:"prefixes" jsonschema:"the number of prefixes (directories) beneath: the immediate children for direct totals and all descendants for recursive totals"`
	Bytes        int64 `json:"bytes" jsonschema:"the total size of the files"`
	StorageBytes int64 `json:"storage_bytes" jsonschema:"the space used on disk by the files"`
	PrefixBytes  int64 `json:"prefix_bytes" jsonschema:"the total size of the prefixes (directories) themselves"`
	Hardlinks    int64 `json:"hardlinks" jsonschema:"the number of links to files that are counted elsewhere"`
	SparseFiles  int64 `json:"sparse_files"`
	SparseBytes  int64 `json:"sparse_bytes" jsonschema:"the size of sparse files that is not allocated on disk"`
}
```
Totals are the totals for a prefix, or for a user or group.


### Type Usage
```go
type Usage struct {
	Path string `json:"path"`
	// Recursive is omitted if the recursive totals are not available.
	Recursive *Totals `json:"recursive,omitempty" jsonschema:"the totals for the prefix and everything beneath it"`
	Direct    Totals  `json:"direct" jsonschema:"the totals for the prefix's own files"`
	Note      string  `json:"note,omitempty"`
}
```
Usage is the result of usage.




## Examples
### [Example](https://pkg.go.dev/cloudeng.io/cmd/idu/mcpserver?tab=doc#example-)




