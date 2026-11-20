# Package [cloudeng.io/cmd/idu/cli/progress](https://pkg.go.dev/cloudeng.io/cmd/idu/cli/progress?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/cli/progress)](https://goreportcard.com/report/cloudeng.io/cmd/idu/cli/progress)

```go
import cloudeng.io/cmd/idu/cli/progress
```

Package progress provides the text based display of the progress of an
analysis used by the analyze command, see NewAnalyze, and the memory
statistics, see ReadMemory, also displayed by the tui.

## Functions
### Func NewAnalyze
```go
func NewAnalyze(ctx context.Context, display bool) analyze.Progress
```
NewAnalyze implements analyze.NewProgress. If display is true the progress
is displayed on a single line, rewritten once a second, on an interactive
terminal and on a new line every 30 seconds otherwise. A summary is
displayed, and logged, by Report.



## Types
### Type Memory
```go
type Memory struct {
	// HeapBytes is the memory occupied by live, and not yet collected,
	// heap objects.
	HeapBytes uint64
	// TotalBytes is all of the memory mapped by the Go runtime.
	TotalBytes uint64
	// MaxRSSBytes is the peak resident set size of the process, it is
	// negative if unknown, eg. on Windows.
	MaxRSSBytes int64
	Goroutines  uint64
	GCCycles    uint64
}
```
Memory describes the memory used by the process.

### Functions

```go
func ReadMemory() Memory
```
ReadMemory returns the memory currently used by the process. Unlike
runtime.ReadMemStats it does not stop the world and hence may be called
frequently.







