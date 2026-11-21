# Package [cloudeng.io/cmd/idu/internal/cli](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/cli?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/internal/cli)](https://goreportcard.com/report/cloudeng.io/cmd/idu/internal/cli)

```go
import cloudeng.io/cmd/idu/internal/cli
```

Package cli provides the support shared by idu's command groups (see
cloudeng.io/cmd/idu/cli/...): the environment configured by main, locating
the filesystem and database to operate on, opening databases, per-filesystem
logging, formatting sizes and counts and the format of stats files.

## Constants
### ReportDirLayout
```go
const ReportDirLayout = StatsFileLayout
```
ReportDirLayout is the time layout used to name report directories, it is
the same as StatsFileLayout.

### StatsFileLayout
```go
const StatsFileLayout = "2006-01-02_15-04-05.000"
```
StatsFileLayout is the time layout used to name stats files, ie. snapshots.
Names are in local time, with millisecond resolution, contain neither spaces
nor ':', which is not permitted in Windows filenames, and are the same on
all systems.



## Variables
### LegacyReportDirLayouts
```go
LegacyReportDirLayouts = []string{time.RFC3339, "2006-01-02T15-04-05Z0700", "2006-01-02_15-04-05Z0700", "2006-01-02_15-04-05"}

```
LegacyReportDirLayouts are the layouts previously used to name report
directories, which are still recognized.

### LegacyStatsFileLayouts
```go
LegacyStatsFileLayouts = []string{time.DateTime, "2006-01-02 15-04-05", "2006-01-02_15-04-05"}

```
LegacyStatsFileLayouts are the layouts previously used to name stats files,
which are still recognized.

### Printer
```go
Printer = message.NewPrinter(language.English)

```
Printer formats numbers using English conventions, eg. 1,000.



## Functions
### Func Banner
```go
func Banner(out io.Writer, ul string, format string, args ...any)
```
Banner writes the formatted text to out underlined by ul.

### Func BinaryBytes
```go
func BinaryBytes(size int64) (float64, string)
```
BinaryBytes formats size using binary units, eg. KiB.

### Func ConfigLoadError
```go
func ConfigLoadError(ctx context.Context) error
```
ConfigLoadError returns the error recorded by WithConfigLoadError, if any.

### Func DecimalBytes
```go
func DecimalBytes(size int64) (float64, string)
```
DecimalBytes formats size using decimal units, eg. KB.

### Func FmtCount
```go
func FmtCount(count int64) string
```
FmtCount formats count right-aligned with thousands separators.

### Func FmtKiBytes
```go
func FmtKiBytes(size int64) string
```
FmtKiBytes formats size in KiB.

### Func LogsToFileFor
```go
func LogsToFileFor(ctx context.Context, global cmdutil.LoggingConfig, perFS *cmdutil.LoggingConfig) bool
```
LogsToFileFor reports whether the effective logger in ctx for a filesystem
with optional perFS logging configuration writes to a file rather than the
terminal.

### Func LookupFSFor
```go
func LookupFSFor(all iduconfig.T, fsName, prefix string) (iduconfig.Filesystem, string, error)
```
LookupFSFor locates the configuration for the filesystem named fsName,
which may be empty if only one is configured, and the location within it
specified by prefix.

### Func PrefixArg
```go
func PrefixArg(args []string) string
```
PrefixArg returns the optional <prefix> argument, or the empty string,
which refers to the root of the filesystem, if it was not supplied.

### Func SquashCanceled
```go
func SquashCanceled(err error) error
```
SquashCanceled returns err with all but one of the instances of
context.Canceled and context.DeadlineExceeded removed, so that a command
that is interrupted, or times out, while many operations are in flight
reports each once, together with the number of times that it occurred,
rather than once per operation. Any errors.M within err, at any depth,
are flattened into a single errors.M first since errors.M.Squash does not
descend into an errors.M that contains a single error.

### Func WithConfigLoadError
```go
func WithConfigLoadError(ctx context.Context, err error) context.Context
```
WithConfigLoadError returns a context that records err, the error, if any,
encountered when loading the configuration. It allows commands that can run
without a configuration to report it when they need one.

### Func WithLoggers
```go
func WithLoggers(ctx context.Context, ls *Loggers) context.Context
```
WithLoggers returns a context that carries ls so that LookupPrefix can
create the loggers for filesystems with their own logging configuration.



## Types
### Type Env
```go
type Env struct {
	// Config is the loaded configuration.
	Config iduconfig.T
	// FS is the name of the filesystem to operate on, ie. the value of
	// the global --fs flag.
	FS string
	// DatabaseFactory creates the database backend.
	DatabaseFactory idudb.BackendFactory
	// OpenProgress is where messages are written while waiting for a
	// database to open.
	OpenProgress io.Writer
	// BytesPrinter formats a size in bytes as a value and units, see
	// FmtSize.
	BytesPrinter func(size int64) (float64, string)
}
```
Env is the environment shared by all of the commands. It is created by main,
which sets Config and FS once the configuration has been loaded and the
global flags parsed, ie. before any command runs.

### Functions

```go
func NewEnv() *Env
```
NewEnv returns an Env that uses badger databases, writes database open
progress to os.Stdout and displays sizes in decimal units.



### Methods

```go
func (e *Env) FmtSize(size int64) string
```
FmtSize formats size using BytesPrinter.


```go
func (e *Env) ForFS(fs string) *Env
```
ForFS returns e if fs is empty, or otherwise a copy of e that operates on
the filesystem named fs, ie. one whose FS is fs.


```go
func (e *Env) LookupFS(prefix string) (iduconfig.Filesystem, string, error)
```
LookupFS is like LookupPrefix but does not configure logging.


```go
func (e *Env) LookupPrefix(ctx context.Context, prefix string) (context.Context, iduconfig.Filesystem, string, error)
```
LookupPrefix uses iduconfig.T.Lookup to locate the configuration for the
filesystem specified by FS and the location within it specified by prefix.
It also arranges for the returned context to use that filesystem's logging
configuration, if any, see WithLoggers.


```go
func (e *Env) OpenDatabase(ctx context.Context, path string, readonly bool) (idudb.Database, error)
```
OpenDatabase opens the database backend at path, waiting until it is opened
or ctx is canceled and displaying a message while waiting.


```go
func (e *Env) OpenOptions() []idudb.OpenOption
```
OpenOptions returns the options used to open all databases.


```go
func (e *Env) OpenPrefixReadOnly(ctx context.Context, prefix string) (context.Context, *iduconfig.Settings, string, idudb.Reader, error)
```
OpenPrefixReadOnly resolves prefix to its configuration and the target path
to operate on and opens the corresponding database for reading.


```go
func (e *Env) OpenReadOnlyDB(ctx context.Context, cfg *iduconfig.Settings) (idudb.Reader, error)
```
OpenReadOnlyDB opens the database specified by cfg for reading only.


```go
func (e *Env) OpenWritableDB(ctx context.Context, cfg *iduconfig.Settings) (*idudb.DB, error)
```
OpenWritableDB opens the database specified by cfg for use by analyze.




### Type FSFlag
```go
type FSFlag struct {
	FS string `subcmd:"fs,,'the name of the configured filesystem to operate on, overrides the global --fs flag; it may be omitted if only one filesystem is configured'"`
}
```
FSFlag is embedded in the flags of every command that operates on a
configured filesystem, it overrides the global --fs flag.


### Type Loggers
```go
type Loggers struct {
	// contains filtered or unexported fields
}
```
Loggers records the flags used to override logging configurations and
the loggers created for a command so that they can all be closed when the
command completes.

### Functions

```go
func NewLoggers(fs *flag.FlagSet, flags cmdutil.LoggingFlags) *Loggers
```
NewLoggers returns a Loggers that applies any of flags that are set in fs to
every logging configuration.



### Methods

```go
func (ls *Loggers) Close() error
```
Close closes all of the loggers created by NewLogger.


```go
func (ls *Loggers) LogsToFile(cfg cmdutil.LoggingConfig) bool
```
LogsToFile reports whether a logger configured with cfg and overridden by
ls's flags writes to a file rather than the terminal.


```go
func (ls *Loggers) NewLogger(cfg cmdutil.LoggingConfig) (*cmdutil.Logger, error)
```
NewLogger creates a logger for cfg, overridden by any of the logging flags
that are set.







