// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package config

import (
	"context"
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strings"

	"cloudeng.io/cmdutil/structdoc"
	"cloudeng.io/errors"
	"cloudeng.io/file/diskusage"
	"cloudeng.io/file/filewalk"
	"gopkg.in/yaml.v2"
)

// Layout represents a means of calculating the disk usage for files with
// the specified prefix.
type Layout struct {
	Prefix     string
	Calculator diskusage.Calculator
}

// DatabaseOpenFunc is called to open a filewalk.Database instance in
// write or read-only mode.
type DatabaseOpenFunc func(ctx context.Context, opts ...filewalk.DatabaseOption) (filewalk.Database, error)

// DatabaseDeleteFunc is called to delete an instance of filewalk.Database.
type DatabaseDeleteFunc func(ctx context.Context) error

// Database represents a means of creating instances of filewalk.Database
// configured as per the yaml configuration file that is to be used for
// storing data for entries within the specified prefix.
type Database struct {
	Prefix      string
	Type        string
	Open        DatabaseOpenFunc
	Delete      DatabaseDeleteFunc
	Description string
}

// Exclusions represents a set of exclusion regular expressions to
// apply to a prefix.
type Exclusions struct {
	Prefix  string
	Regexps []*regexp.Regexp
}

// Config represents a complete configuration.
type Config struct {
	Databases  []Database   // Per-prefix databases.
	Layouts    []Layout     // Per-prefix layouts.
	Exclusions []Exclusions // Per-prefix exclusions.
}

func (cfg *Config) DatabaseFor(prefix string) (Database, bool) {
	for _, d := range cfg.Databases {
		if strings.HasPrefix(prefix, d.Prefix) {
			return d, true
		}
	}
	return Database{}, false
}

var identityCalculator = diskusage.NewIdentity()

func (cfg *Config) CalculatorFor(prefix string) diskusage.Calculator {
	for _, l := range cfg.Layouts {
		if strings.HasPrefix(prefix, l.Prefix) {
			return l.Calculator
		}
	}
	return identityCalculator
}

func (cfg *Config) ExclusionsFor(prefix string) (Exclusions, bool) {
	for _, e := range cfg.Exclusions {
		if strings.HasPrefix(prefix, e.Prefix) {
			return e, true
		}
	}
	return Exclusions{}, false
}

type exclusions struct {
	Prefix  string   `yaml:"prefix", cmd:"prefix that these exclusions apply to"`
	Regexps []string `yaml:"regexps" cmd:"prefixes and files matching these regular expressions will be ignored when building a datagase"`
}

type yamlConfig struct {
	Databases  []database   `yaml:"databases" cmd:"per-prefix database configurations"`
	Layouts    []layout     `yaml:"layouts" cmd:"per-prefix filesystem layouts"`
	Exclusions []exclusions `yaml:"exclusions" cmd:"per-prefix exclusions"`
}

// ReadConfig will read a yaml config from the specified file.
func ReadConfig(filename string) (*Config, error) {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v: %v", filename, err)
	}
	cfg, err := ParseConfig(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse/process config file %v: %v", filename, err)
	}
	if len(cfg.Layouts) == 0 {
		cfg.Layouts = []Layout{
			{"", diskusage.NewSimple(4096)},
		}
		fmt.Printf("warning: config %v does not contain a layout. assuming a simple layout with 4K block size\n", filename)
	}
	return cfg, nil
}

// ParseConfig will parse a yaml config from the supplied byte slice.
func ParseConfig(buf []byte) (*Config, error) {
	ymlcfg := &yamlConfig{}
	if err := yaml.Unmarshal(buf, ymlcfg); err != nil {
		return nil, err
	}
	cfg := &Config{}
	cfg.Exclusions = make([]Exclusions, len(ymlcfg.Exclusions))
	for i, e := range ymlcfg.Exclusions {
		regexps := make([]*regexp.Regexp, len(e.Regexps))
		errs := errors.M{}
		for i, expr := range e.Regexps {
			re, err := regexp.Compile(expr)
			if err != nil {
				errs.Append(fmt.Errorf("failed to compile %v: %v", expr, err))
			}
			regexps[i] = re
		}
		if err := errs.Err(); err != nil {
			return nil, err
		}
		cfg.Exclusions[i] = Exclusions{e.Prefix, regexps}
	}
	cfg.Layouts = make([]Layout, len(ymlcfg.Layouts))
	for i, l := range ymlcfg.Layouts {
		cfg.Layouts[i] = Layout{l.Spec.Prefix, l.instance}
	}

	cfg.Databases = make([]Database, len(ymlcfg.Databases))
	for i, db := range ymlcfg.Databases {
		cfg.Databases[i] = Database{
			Prefix:      db.Spec.Prefix,
			Type:        db.Spec.Type,
			Open:        db.open,
			Description: db.description,
			Delete:      db.delete,
		}
	}
	if len(cfg.Databases) == 0 || cfg.Databases[0].Open == nil {
		return nil, fmt.Errorf("no database was configured")
	}
	// Sort by longest, ie most specific, prefix first.
	sort.Slice(cfg.Exclusions, func(i, j int) bool {
		return len(cfg.Exclusions[i].Prefix) > len(cfg.Exclusions[j].Prefix)
	})
	sort.Slice(cfg.Layouts, func(i, j int) bool {
		return len(cfg.Layouts[i].Prefix) > len(cfg.Layouts[j].Prefix)
	})
	sort.Slice(cfg.Databases, func(i, j int) bool {
		return len(cfg.Databases[i].Prefix) > len(cfg.Databases[j].Prefix)
	})
	return cfg, nil
}

func describe(name string, cfg interface{}) string {
	out := &strings.Builder{}
	desc, err := structdoc.Describe(cfg, "cmd", name+":\n")
	if err != nil {
		panic(err)
	}
	out.WriteString("  " + desc.Detail)
	out.WriteString(structdoc.FormatFields(4, 2, desc.Fields))
	return out.String()
}

// Documentation will return a description of the format of the
// yaml configuration file.
func Documentation() string {
	out := &strings.Builder{}
	desc, err := structdoc.Describe(&yamlConfig{}, "cmd", "YAML configuration file options\n")
	if err != nil {
		panic(err)
	}
	out.WriteString(structdoc.FormatFields(0, 2, desc.Fields))
	if len(supportedLayouts) == 0 {
		return out.String()
	}
	out.WriteString("\nSupported Databases:\n")
	for name, cfg := range supportedDatabases {
		out.WriteString(describe(name, cfg.config))
	}
	out.WriteString("\nSupported Layouts:\n")
	for name, cfg := range supportedLayouts {
		out.WriteString(describe(name, cfg.config))
	}
	return out.String()
}
