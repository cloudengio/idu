# Package [cloudeng.io/cmd/idu/cli/analyze](https://pkg.go.dev/cloudeng.io/cmd/idu/cli/analyze?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/cli/analyze)](https://goreportcard.com/report/cloudeng.io/cmd/idu/cli/analyze)

```go
import cloudeng.io/cmd/idu/cli/analyze
```

Package analyze implements idu's commands that create and maintain
the database: analyze, logs, errors (both of which can erase entries)
and database locate. Progress reporting is supplied by the caller via
NewProgress, so that main controls how, and whether, it is displayed.

## Types
### Type Commands
```go
type Commands struct {
	// contains filtered or unexported fields
}
```
Commands implements the commands that create and maintain the database, ie.
analyze, logs, errors and database locate.

### Functions

```go
func New(env *cli.Env, opts ...Option) *Commands
```
New returns the Commands that use env.



### Methods

```go
func (alz *Commands) Analyze(ctx context.Context, values interface{}, args []string) error
```
Analyze implements the analyze command, values must be a *Flags.


```go
func (c *Commands) Errors(ctx context.Context, values interface{}, args []string) error
```
Errors implements the errors command, values must be an *ErrorFlags.


```go
func (c *Commands) Locate(_ context.Context, values interface{}, args []string) error
```
Locate implements the database locate command, values must be a
*LocateFlags.


```go
func (c *Commands) Logs(ctx context.Context, values interface{}, args []string) error
```
Logs implements the logs command, values must be a *LogFlags.




### Type ErrorFlags
```go
type ErrorFlags struct {
	cli.FSFlag
	Prefix bool `subcmd:"prefix,false,list errors by prefix"`
	Erase  bool `subcmd:"erase,false,erase the errors rather than displaying them"`
}
```
ErrorFlags are the flags for the errors command.


### Type Flags
```go
type Flags struct {
	cli.FSFlag
	Progress    bool          `subcmd:"progress,true,show progress"`
	Force       bool          `subcmd:"force,false,reanalyze even if the database is up to date"`
	SlowScans   time.Duration `subcmd:"slow-scan-duration,10s,duration at which scans are reported as slow"`
	Defaults    bool          `subcmd:"show-defaults,false,display default scanning options and exit"`
	Snapshot    bool          `subcmd:"snapshot,false,'write a snapshot of the statistics for the entire filesystem to its snapshot_dir once the analysis has completed successfully, it is an error if snapshot_dir is not set'"`
	RefreshTopN bool          `subcmd:"refresh-topn,false,recompute the recursive totals and top-N rankings from scratch once the analysis is complete; they are otherwise maintained incrementally and rebuilt automatically when needed"`
}
```
Flags are the flags for the analyze command.


### Type LocateFlags
```go
type LocateFlags struct {
	cli.FSFlag
	Verbose bool `subcmd:"verbose,false,enable verbose output"`
}
```
LocateFlags are the flags for the database locate command.


### Type LogFlags
```go
type LogFlags struct {
	cli.FSFlag
	Range flags.TimeInterval `subcmd:"range,,'display the log entries within this time range, written <from>..<to>, <from>.., ..<to> or as a single date/time; eg. -1h.. for the last hour, 2026-11-01..2026-11-05 to include all of the fifth, or 2026-11 for all of November. All entries are displayed if not set'"`
	Erase bool               `subcmd:"erase,false,erase the logs rather than displaying them"`
	JSON  bool               `subcmd:"json,true,display logs in json format"`
}
```
LogFlags are the flags for the logs command.


### Type NewProgress
```go
type NewProgress func(ctx context.Context, display bool) Progress
```
NewProgress creates the Progress for an analysis, display is true if the
progress should be displayed as the analysis proceeds. Any display must stop
when ctx is canceled or Stop is called.


### Type Option
```go
type Option func(*Commands)
```
Option configures the Commands created by New.

### Functions

```go
func WithProgress(newProgress NewProgress) Option
```
WithProgress sets the function used to create the Progress for each
analysis. Without it progress is neither displayed nor reported.


```go
func WithSnapshot(fn SnapshotFunc) Option
```
WithSnapshot sets the function used to write a snapshot once an analysis has
completed, as requested by --snapshot, which is an error without it.




### Type Progress
```go
type Progress interface {
	idudb.ScanProgress
	// Stop stops any display of the progress of the analysis and waits
	// for it to finish.
	Stop()
	// Summary returns the progress counters, the caller fills in the
	// Operation, Command and Duration fields.
	Summary() Summary
	// Report displays, and logs, a final summary of the analysis.
	Report(ctx context.Context)
}
```
Progress is notified of the progress of an analysis and reports on it.


### Type SnapshotFunc
```go
type SnapshotFunc func(ctx context.Context, f iduconfig.Filesystem) (string, error)
```
SnapshotFunc writes a snapshot of the stats for the entire filesystem f,
computed from its database, to its snapshot_dir and returns its path.


### Type Summary
```go
type Summary struct {
	Operation            string        `json:"operation"`
	Command              string        `json:"command"`
	Duration             time.Duration `json:"duration"`
	PrefixesStarted      int64         `json:"prefixes_started"`
	PrefixesFinished     int64         `json:"prefixes_finished"`
	SynchronousScans     int64         `json:"synchronous_scans"`
	SlowScans            int64         `json:"slow_scans"`
	FSStats              int64         `json:"fs_stats"`
	FSStatsTotal         int64         `json:"fs_stats_total"`
	FSStatMeanLatency    int64         `json:"fs_stat_mean_latency"`
	Files                int64         `json:"files"`
	ParentUnchanged      int64         `json:"parent_unchanged"`
	ChildrenUnchanged    int64         `json:"children_unchanged"`
	Errors               int64         `json:"errors"`
	PrefixesDeleted      int64         `json:"prefixes_deleted"`
	FilesRestated        int64         `json:"files_restated"`
	FilesRestatedChanged int64         `json:"files_restated_changed"`
}
```
Summary is the summary of an analysis that is recorded, as JSON, in the
database log and displayed by the logs command.





### TODO
- cnicolaou: generalize this to other filesystems.




