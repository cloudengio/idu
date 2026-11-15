# Package [cloudeng.io/cmd/idu/internal/config](https://pkg.go.dev/cloudeng.io/cmd/idu/internal/config?tab=doc)

```go
import cloudeng.io/cmd/idu/internal/config
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




### Type Filesystems
```go
type Filesystems []Prefix
```
Filesystems is a list of filesystem Prefix configurations. Unmarshaling
appends decoded blocks into *fs so multiple config files merge cleanly.

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




### Type Layout
```go
type Layout struct {
	Calculator string    `yaml:"calculator" doc:"the type of disk usage calculator to use (allocated, bytes, block, raid0), defaults to allocated"`
	Parameters yaml.Node `yaml:"parameters,omitempty" doc:"the layout parameters to use for this calculator"`
}
```


### Type Prefix
```go
type Prefix struct {
	Name                     string             `yaml:"name" doc:"optional name used to refer to this configuration"`
	Prefix                   string             `yaml:"prefix" doc:"the prefix to be analyzed"`
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
	// contains filtered or unexported fields
}
```

### Methods

```go
func (p *Prefix) Calculator() diskusage.Calculator
```


```go
func (p *Prefix) CalculatorOrDefault() diskusage.Calculator
```
CalculatorOrDefault returns the configured disk usage calculator or the
identity calculator if none is configured.


```go
func (p *Prefix) CompactionInterval() time.Duration
```
CompactionInterval returns the combined compaction interval computed
by converting CompactIntervalDays to a time.Duration and adding
CompactInterval.


```go
func (p *Prefix) Exclude(path string) bool
```
Exclude returns true if path should be excluded/ignored.


```go
func (p *Prefix) RecordHardlinks() bool
```
RecordHardlinks returns true if analyze should record potential hardlinks in
the precomputed summaries.


```go
func (p *Prefix) SeparatorOrDefault() string
```
SeparatorOrDefault returns the configured separator or "/".




### Type RAID0
```go
type RAID0 struct {
	StripeSize cmdtypes.ByteSize `yaml:"stripe_size" doc:"the size of the raid0 stripes (e.g. 64KiB or 65536)"`
	NumStripes int               `yaml:"num_stripes" doc:"the number of stripes used"`
}
```


### Type T
```go
type T struct {
	ConfigFiles Files       `yaml:"filesystem_config_files,omitempty" doc:"other configuration files to search for and merge"`
	Filesystems Filesystems `yaml:"filesystems" doc:"the filesystem configurations to be analyzed"`
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
func (t T) ForName(name string) (Prefix, bool)
```
ForName returns the prefix configuration with the given Name.


```go
func (t T) ForPrefix(path string) (Prefix, bool)
```
ForPrefix returns the prefix configuration that corresponds to path.
The prefix is the longest matching prefix in the configuration. The boolean
return value is true if a match is found.


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







