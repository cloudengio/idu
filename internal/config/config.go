// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"cloudeng.io/cmdutil/cmdconfig"
	"cloudeng.io/cmdutil/cmdtypes"
	"cloudeng.io/cmdutil/cmdyaml"
	"cloudeng.io/cmdutil/structdoc"
	"cloudeng.io/file/diskusage"
	"gopkg.in/yaml.v3"
)

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
	CompactIntervalDays      int                `yaml:"compact_interval_days" doc:"number of days after the last database write before running compaction, added to compact_interval"`
	CompactInterval          time.Duration      `yaml:"compact_interval" doc:"duration after the last database write before running compaction, added to compact_interval_days"`

	Layout Layout `yaml:"layout" doc:"the filesystem layout to use for calculating raw bytes used"`

	calculator diskusage.Calculator
}

// CompactionInterval returns the combined compaction interval computed by
// converting CompactIntervalDays to a time.Duration and adding CompactInterval.
func (p *Prefix) CompactionInterval() time.Duration {
	return time.Duration(p.CompactIntervalDays)*24*time.Hour + p.CompactInterval
}

type Layout struct {
	Calculator string    `yaml:"calculator" doc:"the type of disk usage calculator to use (bytes, block, raid0)"`
	Parameters yaml.Node `yaml:"parameters,omitempty" doc:"the layout parameters to use for this calculator"`
}

func (p *Prefix) Calculator() diskusage.Calculator {
	return p.calculator
}

// Files lists additional configuration files to search for and merge.
// Unmarshaling appends across files so multiple specs can contribute entries.
type Files []string

// UnmarshalYAML implements yaml.Unmarshaler, appending decoded file names into *cf.
func (cf *Files) UnmarshalYAML(value *yaml.Node) error {
	for value.Kind == yaml.AliasNode && value.Alias != nil {
		value = value.Alias
	}
	if value.Tag == "!!null" {
		return nil
	}
	var items []string
	if err := value.Decode(&items); err != nil {
		return err
	}
	*cf = append(*cf, items...)
	return nil
}

// Filesystems is a list of filesystem Prefix configurations. Unmarshaling
// appends decoded blocks into *fs so multiple config files merge cleanly.
type Filesystems []Prefix

var prefixYAMLFields = structYAMLFields(Prefix{})

func structYAMLFields(v any) map[string]bool {
	fields := map[string]bool{}
	t := reflect.TypeOf(v)
	for i := range t.NumField() {
		tag := t.Field(i).Tag.Get("yaml")
		if tag == "" || tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name != "" {
			fields[name] = true
		}
	}
	return fields
}

func validateMappingKeys(node *yaml.Node, allowed map[string]bool, typeName string) error {
	for node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	if node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		valNode := node.Content[i+1]
		var key string
		if err := keyNode.Decode(&key); err != nil {
			continue
		}
		if key == "<<" {
			switch valNode.Kind {
			case yaml.SequenceNode:
				for _, elem := range valNode.Content {
					if err := validateMappingKeys(elem, allowed, typeName); err != nil {
						return err
					}
				}
			default:
				if err := validateMappingKeys(valNode, allowed, typeName); err != nil {
					return err
				}
			}
			continue
		}
		if !allowed[key] {
			return fmt.Errorf("yaml: line %d: field %s not found in type %s", keyNode.Line, key, typeName)
		}
	}
	return nil
}

// UnmarshalYAML implements yaml.Unmarshaler, appending decoded filesystem blocks
// while enforcing known field names.
func (fs *Filesystems) UnmarshalYAML(value *yaml.Node) error {
	for value.Kind == yaml.AliasNode && value.Alias != nil {
		value = value.Alias
	}
	if value.Tag == "!!null" {
		return nil
	}
	if value.Kind != yaml.SequenceNode {
		return fmt.Errorf("yaml: line %d: cannot unmarshal %s into filesystems sequence", value.Line, value.Tag)
	}
	for _, elem := range value.Content {
		for elem.Kind == yaml.AliasNode && elem.Alias != nil {
			elem = elem.Alias
		}
		var p Prefix
		if elem.Tag != "!!null" {
			if err := validateMappingKeys(elem, prefixYAMLFields, "config.Prefix"); err != nil {
				return err
			}
			if err := elem.Decode(&p); err != nil {
				return err
			}
		}
		*fs = append(*fs, p)
	}
	return nil
}

var tYAMLFields = structYAMLFields(T{})

type T struct {
	ConfigFiles Files       `yaml:"filesystem_config_files,omitempty" doc:"other configuration files to search for and merge"`
	Filesystems Filesystems `yaml:"filesystems" doc:"the filesystem configurations to be analyzed"`
}

// UnmarshalYAML implements yaml.Unmarshaler so that YAML documents can be
// either top-level mappings (with filesystem_config_files:, filesystems:, vars:, etc.)
// or top-level sequences of filesystem entries (used for per-filesystem
// secondary config files).
func (t *T) UnmarshalYAML(value *yaml.Node) error {
	for value.Kind == yaml.AliasNode && value.Alias != nil {
		value = value.Alias
	}
	if value.Tag == "!!null" {
		return nil
	}
	switch value.Kind {
	case yaml.SequenceNode:
		return t.Filesystems.UnmarshalYAML(value)
	case yaml.MappingNode:
		type rawT T
		if err := value.Decode((*rawT)(t)); err != nil {
			return err
		}
		var errs []string
		for i := 0; i+1 < len(value.Content); i += 2 {
			var key string
			if err := value.Content[i].Decode(&key); err != nil {
				continue
			}
			if key == "<<" {
				continue
			}
			if !tYAMLFields[key] {
				errs = append(errs, fmt.Sprintf("line %d: field %s not found in type config.T", value.Content[i].Line, key))
			}
		}
		if len(errs) > 0 {
			return &yaml.TypeError{Errors: errs}
		}
		return nil
	default:
		return fmt.Errorf("yaml: line %d: cannot unmarshal %s into config.T", value.Line, value.Tag)
	}
}

// ForName returns the prefix configuration with the given Name.
func (t T) ForName(name string) (Prefix, bool) {
	if name == "" {
		return Prefix{}, false
	}
	for _, p := range t.Filesystems {
		if p.Name == name {
			return p, true
		}
	}
	return Prefix{}, false
}

// ForPrefix returns the prefix configuration that corresponds to path. The
// prefix is the longest matching prefix in the configuration. The boolean
// return value is true if a match is found.
func (t T) ForPrefix(path string) (Prefix, bool) {
	var longest *Prefix
	for i := range t.Filesystems {
		p := &t.Filesystems[i]
		if p.Prefix == "" {
			continue
		}
		if strings.HasPrefix(path, p.Prefix) && (longest == nil || len(p.Prefix) > len(longest.Prefix)) {
			longest = p
		}
	}
	if longest == nil {
		return Prefix{}, false
	}
	return *longest, true
}

// Exclude returns true if path should be excluded/ignored.
func (p *Prefix) Exclude(path string) bool {
	for _, re := range p.Exclusions {
		if re.Regexp != nil && re.MatchString(path) {
			return true
		}
	}
	return false
}

var (
	DefaultConcurrentStats          = 0
	DefaultConcurrentStatsThreshold = 0
	DefaultConcurrentScans          = 0
	DefaultScanSize                 = 0
)

// SetDefaults populates default values for any unset fields in t.
func (t *T) SetDefaults() {
	for i := range t.Filesystems {
		p := &t.Filesystems[i]
		p.Name = os.ExpandEnv(p.Name)
		p.Prefix = os.ExpandEnv(p.Prefix)
		p.Database = os.ExpandEnv(p.Database)
		p.StatsDir = os.ExpandEnv(p.StatsDir)
		if len(p.StatsDir) == 0 {
			p.StatsDir = "stats"
		}
		if len(p.Separator) == 0 {
			p.Separator = string(filepath.Separator)
		}
		if p.ConcurrentStatsThreshold == 0 {
			p.ConcurrentStatsThreshold = DefaultConcurrentStatsThreshold
		}
		if p.ConcurrentStats == 0 {
			p.ConcurrentStats = DefaultConcurrentStats
		}
		if p.ConcurrentScans == 0 {
			p.ConcurrentScans = DefaultConcurrentScans
		}
		if p.ScanSize == 0 {
			p.ScanSize = DefaultScanSize
		}
		if len(p.Layout.Calculator) == 0 {
			p.Layout.Calculator = "bytes"
		}
	}
}

// Validate validates the configuration and initializes derived state such as
// the disk usage calculator for each prefix.
func (t *T) Validate() error {
	seenNames := map[string]int{}
	for i := range t.Filesystems {
		p := &t.Filesystems[i]
		if p.Name != "" {
			if prev, ok := seenNames[p.Name]; ok {
				return fmt.Errorf("duplicate configuration name %q (filesystem entries %d and %d)", p.Name, prev, i)
			}
			seenNames[p.Name] = i
		}
		if p.Prefix == "" {
			return fmt.Errorf("filesystem entry %d (%q) has an empty prefix path", i, p.Name)
		}
		if err := validateCompactIntervals(p); err != nil {
			return fmt.Errorf("filesystem %q: %w", prefixLabel(p), err)
		}
		calc, err := parseLayout(&p.Layout)
		if err != nil {
			return fmt.Errorf("filesystem %q: %w", prefixLabel(p), err)
		}
		p.calculator = calc
	}
	return nil
}

func prefixLabel(p *Prefix) string {
	if p.Name != "" {
		return fmt.Sprintf("%s (%s)", p.Name, p.Prefix)
	}
	return p.Prefix
}

func validateCompactIntervals(p *Prefix) error {
	if p.CompactIntervalDays < 0 {
		return fmt.Errorf("compact_interval_days must not be negative (%d)", p.CompactIntervalDays)
	}
	if p.CompactInterval < 0 {
		return fmt.Errorf("compact_interval must not be negative (%v)", p.CompactInterval)
	}
	const maxCompactDays = math.MaxInt64 / int64(24*time.Hour)
	if int64(p.CompactIntervalDays) > maxCompactDays {
		return fmt.Errorf("compact_interval_days overflows duration (%d)", p.CompactIntervalDays)
	}
	daysDur := time.Duration(p.CompactIntervalDays) * 24 * time.Hour
	if int64(p.CompactInterval) > math.MaxInt64-int64(daysDur) {
		return fmt.Errorf("compact_interval_days + compact_interval overflows duration")
	}
	return nil
}

func parserOptions(mapping func(string) string) []cmdyaml.Option {
	if mapping == nil {
		mapping = os.Getenv
	}
	return []cmdyaml.Option{
		cmdyaml.WithStrictFields(true),
		cmdyaml.WithYAMLVariables("vars"),
		cmdyaml.WithSequenceMerge("<<"),
		cmdyaml.WithExpandMapping(mapping),
	}
}

// ParseConfigSpecs parses and merges one or more YAML byte slices into T.
func ParseConfigSpecs(specs ...[]byte) (T, error) {
	var cfg T
	if err := cmdyaml.NewParser(parserOptions(os.Getenv)...).Parse(&cfg, specs...); err != nil {
		return T{}, err
	}
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return T{}, err
	}
	return cfg, nil
}

// ParseConfig will parse a yaml config from the supplied byte slice.
func ParseConfig(buf []byte) (T, error) {
	return ParseConfigSpecs(buf)
}

// ReadConfig reads and merges yaml configs from the specified files.
func ReadConfig(filenames ...string) (T, error) {
	var cfg T
	if err := cmdyaml.NewParser(parserOptions(os.Getenv)...).ParseFiles(context.Background(), &cfg, filenames...); err != nil {
		return T{}, wrapFilesError(filenames, err)
	}
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return T{}, wrapFilesError(filenames, err)
	}
	return cfg, nil
}

// PrintLocated writes comments naming the YAML and environment variable files
// in loc to w in the same format used by cmdconfig.ConfigCommands.Show.
func PrintLocated(w io.Writer, loc *cmdconfig.Located) {
	if loc == nil {
		return
	}
	if len(loc.YAML) == 0 {
		fmt.Fprintln(w, "# Configuration file: none")
	}
	for _, f := range loc.YAML {
		fmt.Fprintf(w, "# Configuration file: %s\n", f)
	}
	for _, f := range loc.Env {
		fmt.Fprintf(w, "# Environment variables file: %s\n", f)
	}
}

func formatLocatedFiles(loc *cmdconfig.Located) string {
	if loc == nil {
		return ""
	}
	var parts []string
	if len(loc.YAML) > 0 {
		parts = append(parts, "yaml: "+strings.Join(loc.YAML, ", "))
	}
	if len(loc.Env) > 0 {
		parts = append(parts, "env: "+strings.Join(loc.Env, ", "))
	}
	if len(parts) == 0 && loc.SearchPath != "" {
		parts = append(parts, "search path: "+loc.SearchPath)
	}
	return strings.Join(parts, "; ")
}

func wrapLocatedError(loc *cmdconfig.Located, err error) error {
	if err == nil {
		return nil
	}
	files := formatLocatedFiles(loc)
	if files == "" {
		return err
	}
	return fmt.Errorf("failed to load configuration (%s): %w", files, err)
}

func wrapFilesError(filenames []string, err error) error {
	if err == nil || len(filenames) == 0 {
		return err
	}
	return fmt.Errorf("failed to load configuration (%s): %w", strings.Join(filenames, ", "), err)
}

// Load locates configuration files and parses/merges them into T.
//
// Resolution order when YAML files are not explicitly set via ff or IDU_CONFIG:
//  1. If config.yaml (or config.yml) exists in cmdconfig.SearchPath("idu"),
//     read it and any files listed under its top-level filesystem_config_files: key.
//  2. Otherwise, discover and merge all .yaml/.yml and .env files in
//     cmdconfig.SearchPathUserDirs("idu") (e.g. $HOME/.config/idu).
//
// Any filesystem_config_files: listed in explicitly supplied or discovered YAML files are
// also searched for and merged recursively.
func Load(ctx context.Context, ff *cmdconfig.FilesFlag) (T, *cmdconfig.Located, error) {
	if ff == nil {
		ff = &cmdconfig.FilesFlag{}
	}
	spec := cmdconfig.Spec{
		Path: cmdconfig.SearchPath(AppName),
		EnvVars: map[cmdconfig.FileType]string{
			cmdconfig.FileTypeYAML: EnvConfig,
			cmdconfig.FileTypeEnv:  EnvVars,
		},
	}
	loc, err := ff.Locate(spec)
	if err != nil {
		if loc == nil {
			loc = &cmdconfig.Located{SearchPath: spec.Path}
		}
		return T{}, loc, wrapLocatedError(loc, err)
	}
	if err := locateDefaultFiles(ff, loc, spec.Path); err != nil {
		return T{}, loc, wrapLocatedError(loc, err)
	}
	cfg, err := loadAndResolveIncludes(ctx, loc)
	if err != nil {
		return T{}, loc, err
	}
	return cfg, loc, nil
}

func locateDefaultFiles(ff *cmdconfig.FilesFlag, loc *cmdconfig.Located, searchPath string) error {
	yamlExplicit := ff.HasSet(cmdconfig.FileTypeYAML) || strings.TrimSpace(os.Getenv(EnvConfig)) != ""
	if yamlExplicit || len(loc.YAML) > 0 {
		return nil
	}
	envExplicit := ff.HasSet(cmdconfig.FileTypeEnv) || strings.TrimSpace(os.Getenv(EnvVars)) != ""
	userPath := cmdconfig.SearchPathUserDirs(AppName)

	if mainFile := cmdconfig.LocateFilePath(DefaultConfigName, searchPath, cmdyaml.Suffixes()...); mainFile != "" {
		loc.YAML = []string{mainFile}
		if !envExplicit && len(loc.Env) == 0 {
			_, envFiles, err := discoverConfigFiles(userPath)
			if err != nil {
				return err
			}
			loc.Env = envFiles
		}
		return nil
	}

	yamlFiles, envFiles, err := discoverConfigFiles(userPath)
	if err != nil {
		return err
	}
	loc.YAML = yamlFiles
	if !envExplicit && len(loc.Env) == 0 {
		loc.Env = envFiles
	}
	if userPath != "" {
		loc.SearchPath = userPath
	}
	return nil
}

func loadAndResolveIncludes(ctx context.Context, loc *cmdconfig.Located) (T, error) {
	if len(loc.YAML) == 0 {
		var cfg T
		if err := cmdconfig.LoadFiles(ctx, &cfg, loc, cmdyaml.WithYAMLVariables("vars"), cmdyaml.WithSequenceMerge("<<")); err != nil {
			return T{}, wrapLocatedError(loc, err)
		}
		return cfg, nil
	}

	seen := map[string]bool{}
	for _, f := range loc.YAML {
		seen[canonicalPath(f)] = true
	}

	var cfg T
	for {
		cfg = T{}
		if err := cmdconfig.LoadFiles(ctx, &cfg, loc, cmdyaml.WithYAMLVariables("vars"), cmdyaml.WithSequenceMerge("<<")); err != nil {
			return T{}, wrapLocatedError(loc, err)
		}
		added, err := appendConfigFiles(loc, seen, cfg.ConfigFiles)
		if err != nil {
			return T{}, wrapLocatedError(loc, err)
		}
		if !added {
			break
		}
	}

	if err := cfg.Validate(); err != nil {
		return T{}, wrapLocatedError(loc, err)
	}
	return cfg, nil
}

func appendConfigFiles(loc *cmdconfig.Located, seen map[string]bool, names []string) (bool, error) {
	if len(names) == 0 {
		return false, nil
	}
	searchPath := configFilesSearchPath(loc.YAML)
	found, missing := cmdconfig.LocateFilesPath(searchPath, cmdyaml.Suffixes(), names...)
	added := false
	for _, f := range found {
		cp := canonicalPath(f)
		if seen[cp] {
			continue
		}
		seen[cp] = true
		loc.YAML = append(loc.YAML, f)
		added = true
	}
	if len(missing) > 0 {
		return added, fmt.Errorf("missing filesystem_config_files %v (searched %s)", missing, searchPath)
	}
	return added, nil
}

func canonicalPath(p string) string {
	if abs, err := filepath.Abs(p); err == nil {
		return abs
	}
	return filepath.Clean(p)
}

func configFilesSearchPath(loaded []string) string {
	var dirs []string
	seen := map[string]bool{}
	for _, f := range loaded {
		d := filepath.Dir(f)
		if !seen[d] {
			seen[d] = true
			dirs = append(dirs, d)
		}
	}
	for _, d := range filepath.SplitList(cmdconfig.SearchPath(AppName)) {
		if d == "" || seen[d] {
			continue
		}
		seen[d] = true
		dirs = append(dirs, d)
	}
	return strings.Join(dirs, string(os.PathListSeparator))
}

var sharedConfigNames = []string{"idu.yaml", "idu.yml", "vars.yaml", "vars.yml"}

func discoverConfigFiles(searchPath string) (yamlFiles, envFiles []string, err error) {
	if searchPath == "" {
		return nil, nil, nil
	}
	seen := map[string]bool{}
	for _, dir := range filepath.SplitList(searchPath) {
		if dir == "" {
			continue
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, nil, err
		}
		var dirYAML []string
		var dirEnv []string
		for _, entry := range entries {
			name := entry.Name()
			if strings.HasPrefix(name, "._") {
				continue
			}
			fullPath := filepath.Join(dir, name)
			info, err := os.Stat(fullPath)
			if err != nil || info.IsDir() {
				continue
			}
			absPath := canonicalPath(fullPath)
			if seen[absPath] {
				continue
			}
			ext := filepath.Ext(name)
			switch {
			case slices.Contains(cmdyaml.Suffixes(), ext):
				seen[absPath] = true
				dirYAML = append(dirYAML, fullPath)
			case ext == ".env":
				seen[absPath] = true
				dirEnv = append(dirEnv, fullPath)
			}
		}
		sortConfigFiles(dirYAML)
		sort.Strings(dirEnv)
		yamlFiles = append(yamlFiles, dirYAML...)
		envFiles = append(envFiles, dirEnv...)
	}
	return yamlFiles, envFiles, nil
}

func sortConfigFiles(files []string) {
	sort.Slice(files, func(i, j int) bool {
		bi := filepath.Base(files[i])
		bj := filepath.Base(files[j])
		pi := slices.Index(sharedConfigNames, bi)
		pj := slices.Index(sharedConfigNames, bj)
		switch {
		case pi >= 0 && pj >= 0:
			return pi < pj
		case pi >= 0:
			return true
		case pj >= 0:
			return false
		default:
			return bi < bj
		}
	})
}

type RAID0 struct {
	StripeSize cmdtypes.ByteSize `yaml:"stripe_size" doc:"the size of the raid0 stripes (e.g. 64KiB or 65536)"`
	NumStripes int               `yaml:"num_stripes" doc:"the number of stripes used"`
}

type Block struct {
	BlockSize cmdtypes.ByteSize `yaml:"size" doc:"block size used by this filesystem (e.g. 4KiB or 4096)"`
}

type layoutConfig struct {
	newLayout      func(n yaml.Node) (diskusage.Calculator, error)
	describeLayout func() string
}

func decodeNodeStrict(n yaml.Node, dst any) error {
	if n.IsZero() {
		return nil
	}
	data, err := yaml.Marshal(&n)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(dst); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func bytesCalc(n yaml.Node) (diskusage.Calculator, error) {
	if !n.IsZero() {
		var empty struct{}
		if err := decodeNodeStrict(n, &empty); err != nil {
			return nil, fmt.Errorf("failed parsing bytes layout parameters: %v", err)
		}
	}
	return diskusage.NewIdentity(), nil
}

func bytesDesc() string {
	return "bytes: assumes that the size of each file is the number of bytes used\n"
}

func blockCalc(n yaml.Node) (diskusage.Calculator, error) {
	var b Block
	if err := decodeNodeStrict(n, &b); err != nil {
		return nil, fmt.Errorf("failed parsing block layout parameters: %v", err)
	}
	return diskusage.NewBlock(int64(b.BlockSize)), nil
}

func blockDesc() string {
	desc, _ := structdoc.Describe(&Block{}, "doc", "block calculator parameters\n")
	out := &strings.Builder{}
	out.WriteString("block: the size of each file is a multiple of the block size\n")
	out.WriteString(structdoc.FormatFields(2, 4, desc.Fields))
	return out.String()
}

func raid0Calc(n yaml.Node) (diskusage.Calculator, error) {
	var r RAID0
	if err := decodeNodeStrict(n, &r); err != nil {
		return nil, fmt.Errorf("failed parsing RAID0 layout parameters: %v", err)
	}
	return diskusage.NewRAID0(int64(r.StripeSize), r.NumStripes), nil
}

func raid0Desc() string {
	desc, _ := structdoc.Describe(&RAID0{}, "doc", "raid0 calculator parameters\n")
	out := &strings.Builder{}
	out.WriteString("raid0: the size of each file depends on the RAID0 parameters in use\n")
	out.WriteString(structdoc.FormatFields(2, 4, desc.Fields))
	return out.String()
}

var supportedLayouts = map[string]layoutConfig{
	"bytes": {bytesCalc, bytesDesc},
	"block": {blockCalc, blockDesc},
	"raid0": {raid0Calc, raid0Desc},
}

func parseLayout(l *Layout) (diskusage.Calculator, error) {
	if len(l.Calculator) == 0 {
		l.Calculator = "bytes"
	}
	supported, ok := supportedLayouts[strings.ToLower(l.Calculator)]
	if !ok {
		return nil, fmt.Errorf("unsupported disk usage calculator: %v", l.Calculator)
	}
	return supported.newLayout(l.Parameters)
}

// DescribeLayouts writes documentation for all supported disk usage layouts to w.
func DescribeLayouts(w io.Writer) error {
	keys := make([]string, 0, len(supportedLayouts))
	for k := range supportedLayouts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if _, err := fmt.Fprint(w, "\nSupported layouts:\n\n"); err != nil {
		return err
	}
	for _, k := range keys {
		if _, err := fmt.Fprintf(w, "%s\n", supportedLayouts[k].describeLayout()); err != nil {
			return err
		}
	}
	return nil
}

// Documentation will return a description of the format of the
// yaml configuration file.
func Documentation() string {
	out := &strings.Builder{}
	desc, err := structdoc.Describe(&T{}, "doc", "YAML configuration file options\n")
	if err != nil {
		panic(err)
	}
	out.WriteString(structdoc.FormatFields(0, 2, desc.Fields))
	_ = DescribeLayouts(out)
	return out.String()
}
