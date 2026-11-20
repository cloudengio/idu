# Package [cloudeng.io/cmd/idu/cli/stats](https://pkg.go.dev/cloudeng.io/cmd/idu/cli/stats?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/cli/stats)](https://goreportcard.com/report/cloudeng.io/cmd/idu/cli/stats)

```go
import cloudeng.io/cmd/idu/cli/stats
```

Package stats implements idu's stats commands: compute, which computes
statistics from the database and saves them to a file, view, which displays
them, and suggest, which suggests configuration settings based on the state
of the database.

## Types
### Type Commands
```go
type Commands struct {
	// contains filtered or unexported fields
}
```
Commands implements the stats commands, ie. compute, view and suggest.

### Functions

```go
func New(env *cli.Env) *Commands
```
New returns the Commands that use env.



### Methods

```go
func (st *Commands) Compute(ctx context.Context, values interface{}, args []string) error
```
Compute implements the stats compute command, values must be a
*ComputeFlags.


```go
func (st *Commands) Suggest(ctx context.Context, values interface{}, args []string) error
```
Suggest implements the stats suggest command, values must be a
*SuggestFlags.


```go
func (st *Commands) View(ctx context.Context, values interface{}, args []string) error
```
View implements the stats view command, values must be a *ViewFlags.




### Type ComputeFlags
```go
type ComputeFlags struct {
	cli.FSFlag
	ComputeN  int             `subcmd:"n,2000,number of top entries to compute"`
	Scan      bool            `subcmd:"scan,false,'compute the stats from all of the summaries for the prefix, rather than from the rankings and recursive totals maintained by analyze, which are used by default for the root of a filesystem but do not include the distributed subtree statistics and limit the per user and group rankings to topn_per_id entries'"`
	Progress  bool            `subcmd:"progress,false,show progress"`
	StatsDir  string          `subcmd:"stats-dir,,'directory that stats files are written to, defaults to stats_dir from the configuration or stats'"`
	StatsFile string          `subcmd:"stats-file,,'write stats to the specified file, rather than a directory, use - for stdout'"`
	Prefix    flags.Repeating `subcmd:"prefix,,'prefix match expression'"`
}
```
ComputeFlags are the flags for the stats compute command.


### Type DisplayFlags
```go
type DisplayFlags struct {
	DisplayN int  `subcmd:"display,10,number of top entries to display"`
	Progress bool `subcmd:"progress,false,show progress"`
}
```
DisplayFlags are the flags that control how stats are displayed.


### Type SuggestFlags
```go
type SuggestFlags struct {
	cli.FSFlag
	N      int  `subcmd:"n,20,maximum number of prefixes with missing links to consider"`
	Locate bool `subcmd:"locate,true,'scan the metadata for the entire filesystem to locate the prefixes that contain the other links to the files with missing links, which are the prefixes to add to hardlinks_record_all'"`
}
```
SuggestFlags are the flags for the stats suggest command.


### Type ViewFlags
```go
type ViewFlags struct {
	DisplayFlags
	FS       string `subcmd:"fs,,'the name of the configured filesystem to operate on, overrides the global --fs flag'"`
	StatsDir string `subcmd:"stats-dir,,'directory that stats files are read from, defaults to stats_dir from the configuration of the filesystem specified by --fs; --fs is not required if this flag is set'"`
	User     string `subcmd:"user,,display stats for the specified user"`
	Group    string `subcmd:"group,,display stats for the specified group"`
	Info     bool   `subcmd:"info,false,display metadata for the stats file"`
}
```
ViewFlags are the flags for the stats view command.





### TODO
- cnicolaou: generalize this to other filesystems.




