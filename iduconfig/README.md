# Package [cloudeng.io/cmd/idu/iduconfig](https://pkg.go.dev/cloudeng.io/cmd/idu/iduconfig?tab=doc)
[![CircleCI](https://circleci.com/gh/cloudengio/go.gotools.svg?style=svg)](https://circleci.com/gh/cloudengio/go.gotools) [![Go Report Card](https://goreportcard.com/badge/cloudeng.io/cmd/idu/iduconfig)](https://goreportcard.com/report/cloudeng.io/cmd/idu/iduconfig)

```go
import cloudeng.io/cmd/idu/iduconfig
```


## Constants
### AppName, DefaultConfigName, EnvConfig, EnvVars
```go
const (
	// AppName is the application directory name used when searching for
	// configuration files, e.g. $HOME/.config/idu.
	AppName = "idu"

	// DefaultConfigName is the base filename of the primary configuration file
	// searched for in cmdconfig.SearchPath("idu") when --config is not set.
	DefaultConfigName = "config"

	// EnvConfig names the environment variable that supplies a comma-separated
	// list of YAML configuration files when --config is not set.
	EnvConfig = "IDU_CONFIG"

	// EnvVars names the environment variable that supplies a comma-separated
	// list of .env files when --config is not set for env files.
	EnvVars = "IDU_ENV"
)
```

### DefaultTopN
```go
const DefaultTopN = 2000
```
DefaultTopN is the default value of Settings.TopN, it matches the default
number of entries computed by the stats command.

### DefaultTopNOversubscription
```go
const DefaultTopNOversubscription = 2.0
```
DefaultTopNOversubscription is the default value of
Settings.TopNOversubscription.

### DefaultTopNPerID
```go
const DefaultTopNPerID = 100
```
DefaultTopNPerID is the default value of Settings.TopNPerID.



## Variables
### DefaultConcurrentStats, DefaultConcurrentStatsThreshold, DefaultConcurrentScans, DefaultScanSize
```go
DefaultConcurrentStats = 0
DefaultConcurrentStatsThreshold = 0
DefaultConcurrentScans = 0
DefaultScanSize = 0

```

### ErrFilesystemNameRequired
```go
ErrFilesystemNameRequired = errors.New("a filesystem name is required when more than one filesystem is configured")

```
ErrFilesystemNameRequired is returned by Lookup when no filesystem name is
supplied and more than one filesystem is configured.



## Functions
### Func ConfigCommands
```go
func ConfigCommands() cmdconfig.ConfigCommands[T]
```
ConfigCommands returns the implementation of the config show and config
describe commands for the configuration, see cmdconfig.ConfigExtension.

### Func DescribeFilesystems
```go
func DescribeFilesystems(w io.Writer) error
```
DescribeFilesystems writes the documentation for the entries in the
filesystems list to w. It is needed since the entries are represented by the
Filesystem interface, which cannot be described via reflection.

### Func DescribeHardlinkDedup
```go
func DescribeHardlinkDedup(w io.Writer) error
```
DescribeHardlinkDedup writes documentation for the supported hardlink
deduplication strategies to w.

### Func DescribeLayouts
```go
func DescribeLayouts(w io.Writer) error
```
DescribeLayouts writes documentation for all supported disk usage layouts to
w.

### Func Label
```go
func Label(f Filesystem) string
```
Label returns a human readable label for f that includes its name, if it has
one, and its location.

### Func PrefixSuccessor
```go
func PrefixSuccessor(prefix string) string
```
PrefixSuccessor returns the first key, in lexicographic order, that does not
start with prefix, ie. such that [prefix, PrefixSuccessor(prefix)) contains
exactly those keys that start with prefix. It returns the empty string,
which is to be interpreted as being unbounded, if there is no such key,
which is the case for the empty prefix and for prefixes that consist solely
of 0xff bytes.

### Func PrintLocated
```go
func PrintLocated(w io.Writer, loc *cmdconfig.Located)
```
PrintLocated writes comments naming the YAML and environment variable files
in loc to w in the same format used by cmdconfig.ConfigCommands.Show.



## Types
### Type Block
```go
type Block struct {
	BlockSize cmdtypes.ByteSize `yaml:"size" doc:"block size used by this filesystem (e.g. 4KiB or 4096), it is only used if the space allocated to files cannot be determined automatically"`
}
```


### Type Files
```go
type Files []string
```
Files lists additional configuration files to search for and merge.
Unmarshaling appends across files so multiple specs can contribute entries.

### Methods

```go
func (cf *Files) UnmarshalYAML(value *yaml.Node) error
```
UnmarshalYAML implements yaml.Unmarshaler, appending decoded file names into
*cf.




### Type Filesystem
```go
type Filesystem interface {
	// Config returns the settings common to all filesystem configurations.
	Config() *Settings
	// Location returns a human readable description of the location of
	// the filesystem, ie. its root or its range of keys.
	Location() string
	// contains filtered or unexported methods
}
```
Filesystem represents a configured filesystem, it is implemented by *Root,
for hierarchical filesystems, and *KeyRange, for a range of keys in a key
oriented store such as an object store.


### Type Filesystems
```go
type Filesystems []Filesystem
```
Filesystems is a list of filesystem configurations, each of which is either
a *Root or a *KeyRange. Unmarshaling appends decoded blocks into *fs so
multiple config files merge cleanly.

### Methods

```go
func (fs *Filesystems) UnmarshalYAML(value *yaml.Node) error
```
UnmarshalYAML implements yaml.Unmarshaler, appending decoded filesystem
blocks while enforcing known field names.




### Type HardlinkDedup
```go
type HardlinkDedup string
```
HardlinkDedup determines how hardlinks are deduplicated when computing
statistics.

### Constants
### HardlinkDedupSummaries, HardlinkDedupSummariesAll, HardlinkDedupRescan
```go
const (
	// HardlinkDedupSummaries records potential hardlinks, ie. files with
	// more than one link, in the precomputed per-prefix summaries when
	// analyzing so that stats can deduplicate them without rescanning all
	// of the file metadata. The link counts are only refreshed when a
	// prefix is rescanned, see stats.RecordLinkedFiles. This is the default.
	HardlinkDedupSummaries HardlinkDedup = "summaries"
	// HardlinkDedupSummariesAll is like HardlinkDedupSummaries but records
	// every file rather than only those that may be hardlinks, ie. those
	// with more than one link. It requires more storage but does not
	// depend on the link counts being up to date.
	HardlinkDedupSummariesAll HardlinkDedup = "summaries-all"
	// HardlinkDedupRescan computes stats from all of the file metadata
	// stored for each prefix, which requires no additional storage but is
	// considerably slower.
	HardlinkDedupRescan HardlinkDedup = "rescan"
)
```




### Type KeyRange
```go
type KeyRange struct {
	StartPrefix string `yaml:"start_prefix" doc:"the first key (inclusive) of a range of keys in a key oriented store, such as an object store, to be analyzed"`
	StopPrefix  string `yaml:"stop_prefix" doc:"the first key, in lexicographic order, that is outside of the range of keys to be analyzed. If omitted it defaults to the first key that does not have start_prefix as a prefix so that the range contains exactly those keys that start with start_prefix; an empty start_prefix and stop_prefix refers to all keys"`
	Settings    `yaml:",inline"`
}
```
KeyRange represents the configuration for a range of keys in a key
oriented store such as an object store. The range starts at, and includes,
StartPrefix and extends up to, but does not include, StopPrefix. That is,
StopPrefix is the first key, in lexicographic order, that is outside of the
range. An empty StopPrefix means that the range has no upper bound.

### Methods

```go
func (kr *KeyRange) Contains(key string) bool
```
Contains reports whether key is within the range.


```go
func (kr *KeyRange) ContainsPrefix(prefix string) bool
```
ContainsPrefix reports whether all of the keys that start with prefix are
within the range.


```go
func (kr *KeyRange) Location() string
```
Location implements Filesystem.




### Type Layout
```go
type Layout struct {
	Calculator string    `yaml:"calculator" doc:"the type of disk usage calculator to use (allocated, bytes, block, raid0), defaults to allocated for a root and bytes for a key range"`
	Parameters yaml.Node `yaml:"parameters,omitempty" doc:"the layout parameters to use for this calculator"`
}
```


### Type RAID0
```go
type RAID0 struct {
	StripeSize cmdtypes.ByteSize `yaml:"stripe_size" doc:"the size of the raid0 stripes (e.g. 64KiB or 65536)"`
	NumStripes int               `yaml:"num_stripes" doc:"the number of stripes used"`
}
```


### Type Root
```go
type Root struct {
	Root     string `yaml:"root" doc:"the root of a hierarchical filesystem to be analyzed, prefix is accepted as a deprecated alias for root"`
	Settings `yaml:",inline"`
}
```
Root represents the configuration for a hierarchical filesystem rooted at
Root.

### Methods

```go
func (r *Root) Location() string
```
Location implements Filesystem.




### Type Settings
```go
type Settings struct {
	Name                     string             `yaml:"name" doc:"optional name used to refer to this configuration"`
	Database                 string             `yaml:"database" doc:"the location of the database to use for this prefix"`
	ReportsDir               string             `yaml:"reports_dir" doc:"the directory that reports generate writes reports to, and reports locate looks in, when --report-dir, or the report directory argument, is not specified; each set of reports is in a sub-directory named by the time of the statistics it was generated from and latest links to the most recent; it defaults to reports"`
	SnapshotDir              string             `yaml:"snapshot_dir" doc:"the directory in which snapshots of the statistics, written by analyze --snapshot, stats compute and, when they require a full pass over the database, stats view and reports generate, are stored; each is named by the time it was created and latest links to the most recent; there is no default and snapshots are not stored if it is not set"`
	Separator                string             `yaml:"separator" doc:"filename separator to use, defaults to /"`
	ConcurrentScans          int                `yaml:"concurrent_scans" doc:"maximum number of concurrent scan operations"`
	ConcurrentStats          int                `yaml:"concurrent_stats" doc:"maximum number of concurrent stat operations"`
	ConcurrentStatsThreshold int                `yaml:"concurrent_stats_threshold" doc:"minimum number of files before stats are performed concurrently"`
	SetMaxThreads            int                `yaml:"set_max_threads" doc:"if non-zero used for debug.SetMaxThreads"`
	ScanSize                 int                `yaml:"scan_size" doc:"maximum number of items to fetch from the filesystem in a single operation"`
	Exclusions               cmdyaml.RegexpList `yaml:"exclusions" doc:"prefixes and files matching these regular expressions will be ignored when building a database. The database directory itself and pseudo filesystems such as /dev (and /proc and /sys on linux) are always ignored."`
	Restat                   cmdyaml.RegexpList `yaml:"restat" doc:"files whose full paths match these regular expressions are re-stat'ed by analyze even when their directory is unchanged, so that changes to files that are modified in place, such as logs and databases, are detected; analyze otherwise reuses the stored metadata for all of the files in an unchanged directory"`
	CountHardlinkAsFiles     bool               `yaml:"count_hardlinks_as_files" doc:"if true, hardlinks will be counted as separate files"`
	HardlinkDedup            HardlinkDedup      `yaml:"hardlink_dedup" doc:"the strategy used to deduplicate hardlinks when count_hardlinks_as_files is false (summaries, summaries-all, rescan), defaults to summaries, see Hardlink deduplication strategies below"`
	HardlinksRecordAll       cmdyaml.RegexpList `yaml:"hardlinks_record_all" doc:"when hardlink_dedup is summaries, the summaries of prefixes (directories) whose paths match these regular expressions record every file, as for summaries-all, rather than only those that may be hardlinks; this ensures that links created to files in such prefixes are deduplicated even if the prefix has not been rescanned since, see idu stats suggest; changing this list causes the next analyze to rewrite all of the summaries"`
	CompactIntervalDays      int                `yaml:"compact_interval_days" doc:"number of days after the last database write before running compaction, added to compact_interval"`
	CompactInterval          time.Duration      `yaml:"compact_interval" doc:"duration after the last database write before running compaction, added to compact_interval_days"`
	TopN                     int                `yaml:"topn" doc:"the number of top entries, N, maintained incrementally by analyze for each top-N report, ie. by prefix, subtree, user and group, reports for more entries than this require a scan of the database, it defaults to 2000"`
	TopNPerID                int                `yaml:"topn_per_id" doc:"the number of top entries maintained incrementally by analyze for each user and each group, ie. the prefixes and subtrees using the most of a given user's or group's storage; since there is a top-N report per user and per group it should be much smaller than topn, it defaults to 100"`
	TopNOversubscription     float64            `yaml:"topn_oversubscription" doc:"the factor by which the incrementally maintained top-N heaps are oversubscribed, ie. each heap holds this multiple of N entries so that entries which shrink or are deleted can be replaced without rescanning the database, it must be at least 1 and defaults to 2"`

	Layout Layout `yaml:"layout" doc:"the filesystem layout to use for calculating raw bytes used"`

	Logging *cmdutil.LoggingConfig `yaml:"logging,omitempty" doc:"logging configuration for this filesystem, if set it replaces the top-level logging configuration for commands that operate on this filesystem, and may be overridden by the --log-* command line flags"`
	// contains filtered or unexported fields
}
```
Settings contains the configuration settings that are common to all
filesystem configurations, ie. Root and KeyRange.

### Methods

```go
func (p *Settings) Calculator() diskusage.Calculator
```


```go
func (p *Settings) CalculatorOrDefault() diskusage.Calculator
```
CalculatorOrDefault returns the configured disk usage calculator or the
identity calculator if none is configured.


```go
func (p *Settings) CompactionInterval() time.Duration
```
CompactionInterval returns the combined compaction interval computed
by converting CompactIntervalDays to a time.Duration and adding
CompactInterval.


```go
func (p *Settings) Config() *Settings
```
Config implements Filesystem.


```go
func (p *Settings) Exclude(path string) bool
```
Exclude returns true if path should be excluded/ignored.


```go
func (p *Settings) ExcludeFunc() func(path string) bool
```
ExcludeFunc returns a function that reports whether a path is to be ignored
when scanning: those that match one of the configured Exclusions and,
always, the database directory, whose size is reported separately, and the
pseudo filesystems such as /dev.


```go
func (p *Settings) RecordAllFiles() bool
```
RecordAllFiles returns true if analyze should record every file, rather than
only those that may be hardlinks, in the precomputed summaries.


```go
func (p *Settings) RecordAllFilesFor(prefix string) bool
```
RecordAllFilesFor returns true if the summary for prefix should
record every file, rather than only those that may be hardlinks,
either because hardlink_dedup is summaries-all or because prefix matches
hardlinks_record_all.


```go
func (p *Settings) RecordHardlinks() bool
```
RecordHardlinks returns true if analyze should record potential hardlinks in
the precomputed summaries.


```go
func (p *Settings) RestatFile(path string) bool
```
RestatFile returns true if the file path should be re-stat'ed by analyze
even if its directory is unchanged, ie. if it matches restat.


```go
func (p *Settings) SeparatorOrDefault() string
```
SeparatorOrDefault returns the configured separator or "/".


```go
func (p *Settings) TopNCapacity() int
```
TopNCapacity returns the number of entries to be held by each
incrementally maintained top-N heap, ie. TopNOrDefault scaled by
TopNOversubscriptionOrDefault and rounded up.


```go
func (p *Settings) TopNOrDefault() int
```
TopNOrDefault returns the configured number of top entries to be maintained
by analyze or DefaultTopN if none is configured.


```go
func (p *Settings) TopNOversubscriptionOrDefault() float64
```
TopNOversubscriptionOrDefault returns the configured top-N oversubscription
factor or DefaultTopNOversubscription if none is configured.


```go
func (p *Settings) TopNPerIDCapacity() int
```
TopNPerIDCapacity returns the number of entries to be held by each
incrementally maintained per-user and per-group top-N heap, ie.
TopNPerIDOrDefault scaled by TopNOversubscriptionOrDefault and rounded up.


```go
func (p *Settings) TopNPerIDOrDefault() int
```
TopNPerIDOrDefault returns the configured number of top entries to be
maintained by analyze for each user and each group or DefaultTopNPerID if
none is configured.




### Type T
```go
type T struct {
	ConfigFiles Files                 `yaml:"filesystem_config_files,omitempty" doc:"other configuration files to search for and merge"`
	Filesystems Filesystems           `yaml:"filesystems" doc:"the filesystem configurations to be analyzed, see Filesystem entries below"`
	Logging     cmdutil.LoggingConfig `yaml:"logging,omitempty" doc:"logging configuration, it may be overridden by the --log-* command line flags"`
}
```

### Functions

```go
func Load(ctx context.Context, ff *cmdconfig.FilesFlag) (T, *cmdconfig.Located, error)
```
Load locates configuration files and parses/merges them into T.

Resolution order when YAML files are not explicitly set via ff or
IDU_CONFIG:
 1. If config.yaml (or config.yml) exists in cmdconfig.SearchPath("idu"),
    read it and any files listed under its top-level
    filesystem_config_files: key.
 2. Otherwise, discover and merge all .yaml/.yml and .env files in
    cmdconfig.SearchPathUserDirs("idu") (e.g. $HOME/.config/idu).

Any filesystem_config_files: listed in explicitly supplied or discovered
YAML files are also searched for and merged recursively.


```go
func ParseConfig(buf []byte) (T, error)
```
ParseConfig will parse a yaml config from the supplied byte slice.


```go
func ParseConfigSpecs(specs ...[]byte) (T, error)
```
ParseConfigSpecs parses and merges one or more YAML byte slices into T.


```go
func ReadConfig(filenames ...string) (T, error)
```
ReadConfig reads and merges yaml configs from the specified files.



### Methods

```go
func (t T) ForName(name string) (Filesystem, bool)
```
ForName returns the filesystem configuration with the given Name.


```go
func (t T) ForPrefix(path string) (*Root, bool)
```
ForPrefix returns the Root configuration that corresponds to path.
The Root is the one with the longest matching root in the configuration,
KeyRange configurations are ignored. The boolean return value is true if a
match is found.


```go
func (t T) Lookup(fsname, prefix string) (Filesystem, string, error)
```
Lookup returns the configuration for the filesystem named fsname, which will
be either a *Root or a *KeyRange, and the location within that filesystem
that is specified by prefix.

If fsname is empty and exactly one filesystem is configured then that
filesystem is used, otherwise ErrFilesystemNameRequired is returned.

For a *Root, an empty prefix refers to the root itself, a relative prefix
is interpreted relative to the root and an absolute prefix, including one
that starts with a separator but has no volume name on Windows, is used
as is. In all cases the resulting path is cleaned (see filepath.Clean),
and hence has no trailing separator other than for a root such as / or C:\,
and must be contained within the root. On Windows, the returned path uses
forward slashes if the root or absolute prefix uses only forward slashes.

For a *KeyRange, an empty prefix refers to the entire range and is returned
as the empty string. Otherwise prefix is a key prefix that is used as is and
all of the keys that start with it must be within the range.


```go
func (t *T) SetDefaults()
```
SetDefaults populates default values for any unset fields in t.


```go
func (t *T) UnmarshalYAML(value *yaml.Node) error
```
UnmarshalYAML implements yaml.Unmarshaler so that YAML documents can be
either top-level mappings (with filesystem_config_files:, filesystems:,
vars:, etc.) or top-level sequences of filesystem entries (used for
per-filesystem secondary config files).


```go
func (t *T) Validate() error
```
Validate validates the configuration and initializes derived state such as
the disk usage calculator for each prefix.







