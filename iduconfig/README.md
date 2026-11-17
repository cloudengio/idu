# Package [cloudeng.io/cmd/idu/iduconfig](https://pkg.go.dev/cloudeng.io/cmd/idu/iduconfig?tab=doc)

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

### Func Documentation
```go
func Documentation() string
```
Documentation will return a description of the format of the yaml
configuration file.

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
### HardlinkDedupSummaries, HardlinkDedupRescan
```go
const (
	// HardlinkDedupSummaries records potential hardlinks in the precomputed
	// per-prefix summaries when analyzing so that stats can deduplicate them
	// without rescanning all of the file metadata. This is the default.
	HardlinkDedupSummaries HardlinkDedup = "summaries"
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
	StatsDir                 string             `yaml:"stats_dir" doc:"the directory to use for stats files, defaults to stats"`
	Separator                string             `yaml:"separator" doc:"filename separator to use, defaults to /"`
	ConcurrentScans          int                `yaml:"concurrent_scans" doc:"maximum number of concurrent scan operations"`
	ConcurrentStats          int                `yaml:"concurrent_stats" doc:"maximum number of concurrent stat operations"`
	ConcurrentStatsThreshold int                `yaml:"concurrent_stats_threshold" doc:"minimum number of files before stats are performed concurrently"`
	SetMaxThreads            int                `yaml:"set_max_threads" doc:"if non-zero used for debug.SetMaxThreads"`
	ScanSize                 int                `yaml:"scan_size" doc:"maximum number of items to fetch from the filesystem in a single operation"`
	Exclusions               cmdyaml.RegexpList `yaml:"exclusions" doc:"prefixes and files matching these regular expressions will be ignored when building a database"`
	CountHardlinkAsFiles     bool               `yaml:"count_hardlinks_as_files" doc:"if true, hardlinks will be counted as separate files"`
	HardlinkDedup            HardlinkDedup      `yaml:"hardlink_dedup" doc:"the strategy used to deduplicate hardlinks when count_hardlinks_as_files is false (summaries, rescan), defaults to summaries, see Hardlink deduplication strategies below"`
	CompactIntervalDays      int                `yaml:"compact_interval_days" doc:"number of days after the last database write before running compaction, added to compact_interval"`
	CompactInterval          time.Duration      `yaml:"compact_interval" doc:"duration after the last database write before running compaction, added to compact_interval_days"`

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
func (p *Settings) RecordHardlinks() bool
```
RecordHardlinks returns true if analyze should record potential hardlinks in
the precomputed summaries.


```go
func (p *Settings) SeparatorOrDefault() string
```
SeparatorOrDefault returns the configured separator or "/".




### Type T
```go
type T struct {
	ConfigFiles Files                 `yaml:"filesystem_config_files,omitempty" doc:"other configuration files to search for and merge"`
	Filesystems Filesystems           `yaml:"filesystems" doc:"the filesystem configurations to be analyzed"`
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
is interpreted relative to the root and an absolute prefix is used as is.
In all cases the resulting path must be contained within the root.
The returned path is cleaned and has no trailing separator (other than for a
root such as / or C:\).

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







