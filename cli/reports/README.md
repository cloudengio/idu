# Package [cloudeng.io/cmd/idu/cli/reports](https://pkg.go.dev/cloudeng.io/cmd/idu/cli/reports?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/cli/reports)](https://goreportcard.com/report/cloudeng.io/cmd/idu/cli/reports)

```go
import cloudeng.io/cmd/idu/cli/reports
```

Package reports implements idu's reports commands: generate, which writes
tsv, json and markdown reports from a stats file, and locate.

## Constants
### DefaultReportsDir
```go
const DefaultReportsDir = "reports"
```
DefaultReportsDir is the directory that reports are written to if neither
--report-dir nor reports_dir is set.



## Types
### Type Commands
```go
type Commands struct {
	// contains filtered or unexported fields
}
```
Commands implements the reports commands, ie. generate and locate.

### Functions

```go
func New(env *cli.Env) *Commands
```
New returns the Commands that use env.



### Methods

```go
func (rc *Commands) Generate(ctx context.Context, values interface{}, args []string) error
```
Generate implements the reports generate command, values must be a
*GenerateFlags.


```go
func (rc *Commands) Locate(ctx context.Context, values interface{}, args []string) error
```
Locate implements the reports locate command, values must be a *LocateFlags.




### Type GenerateFlags
```go
type GenerateFlags struct {
	cli.FSFlag
	ReportDir string `subcmd:"report-dir,,'directory to write reports to, defaults to reports_dir from the configuration of the filesystem specified by --fs, or reports'"`
	TSV       int    `subcmd:"tsv,100,'generate tsv reports with the requested number of entries, 0 for none'"`
	Markdown  int    `subcmd:"markdown,20,'generate markdown reports with the requested number of entries, 0 for none'"`
	JSON      int    `subcmd:"json,100,'generate json reports with the requested number of entries, 0 for none'"`
}
```
GenerateFlags are the flags for the reports generate command.


### Type LocateFlags
```go
type LocateFlags struct {
	cli.FSFlag
	N        int    `subcmd:"n,2,'locate the n most recent reports'"`
	Extesion string `subcmd:"extension,,file extension to match"`
}
```
LocateFlags are the flags for the reports locate command.





