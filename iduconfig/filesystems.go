// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package iduconfig

import (
	"fmt"
	"os"

	"cloudeng.io/cmdutil"
	"gopkg.in/yaml.v3"
)

// Filesystem represents a configured filesystem, it is implemented by
// *Root, for hierarchical filesystems, and *KeyRange, for a range of keys
// in a key oriented store such as an object store.
type Filesystem interface {
	// Config returns the settings common to all filesystem configurations.
	Config() *Settings
	// Location returns a human readable description of the location of
	// the filesystem, ie. its root or its range of keys.
	Location() string

	setDefaults()
	validate() error
	layoutPath() string
	lookup(prefix string) (string, error)
}

// Config implements Filesystem.
func (p *Settings) Config() *Settings {
	return p
}

// Label returns a human readable label for f that includes its name, if
// it has one, and its location.
func Label(f Filesystem) string {
	if n := f.Config().Name; n != "" {
		return fmt.Sprintf("%s (%s)", n, f.Location())
	}
	return f.Location()
}

// Root represents the configuration for a hierarchical filesystem rooted
// at Root.
type Root struct {
	Root     string `yaml:"root" doc:"the root of a hierarchical filesystem to be analyzed, prefix is accepted as a deprecated alias for root"`
	Settings `yaml:",inline"`
}

// Location implements Filesystem.
func (r *Root) Location() string {
	return r.Root
}

func (r *Root) setDefaults() {
	r.Root = os.ExpandEnv(r.Root)
}

func (r *Root) validate() error {
	if r.Root == "" {
		return fmt.Errorf("has an empty root path")
	}
	return nil
}

func (r *Root) layoutPath() string {
	return r.Root
}

// KeyRange represents the configuration for a range of keys in a key
// oriented store such as an object store. The range starts at, and includes,
// StartPrefix and extends up to, but does not include, StopPrefix. That is,
// StopPrefix is the first key, in lexicographic order, that is outside of
// the range. An empty StopPrefix means that the range has no upper bound.
type KeyRange struct {
	StartPrefix string `yaml:"start_prefix" doc:"the first key (inclusive) of a range of keys in a key oriented store, such as an object store, to be analyzed"`
	StopPrefix  string `yaml:"stop_prefix" doc:"the first key, in lexicographic order, that is outside of the range of keys to be analyzed. If omitted it defaults to the first key that does not have start_prefix as a prefix so that the range contains exactly those keys that start with start_prefix; an empty start_prefix and stop_prefix refers to all keys"`
	Settings    `yaml:",inline"`
}

// Location implements Filesystem.
func (kr *KeyRange) Location() string {
	if kr.StopPrefix == "" {
		return fmt.Sprintf("[%q, <unbounded>)", kr.StartPrefix)
	}
	return fmt.Sprintf("[%q, %q)", kr.StartPrefix, kr.StopPrefix)
}

func (kr *KeyRange) setDefaults() {
	if kr.Separator == "" {
		// Keys in key oriented stores conventionally use / regardless of
		// the local operating system.
		kr.Separator = "/"
	}
	if kr.StopPrefix == "" {
		kr.StopPrefix = PrefixSuccessor(kr.StartPrefix)
	}
	if kr.Layout.Calculator == "" {
		// There are no blocks etc. in a key oriented store.
		kr.Layout.Calculator = "bytes"
	}
}

func (kr *KeyRange) validate() error {
	if kr.StopPrefix != "" && kr.StartPrefix >= kr.StopPrefix {
		return fmt.Errorf("start_prefix %q must be less than stop_prefix %q", kr.StartPrefix, kr.StopPrefix)
	}
	return nil
}

func (kr *KeyRange) layoutPath() string {
	return ""
}

// Contains reports whether key is within the range.
func (kr *KeyRange) Contains(key string) bool {
	return key >= kr.StartPrefix && (kr.StopPrefix == "" || key < kr.StopPrefix)
}

// ContainsPrefix reports whether all of the keys that start with prefix
// are within the range.
func (kr *KeyRange) ContainsPrefix(prefix string) bool {
	if !kr.Contains(prefix) {
		return false
	}
	if kr.StopPrefix == "" {
		return true
	}
	succ := PrefixSuccessor(prefix)
	return succ != "" && succ <= kr.StopPrefix
}

// PrefixSuccessor returns the first key, in lexicographic order, that does
// not start with prefix, ie. such that [prefix, PrefixSuccessor(prefix))
// contains exactly those keys that start with prefix. It returns the empty
// string, which is to be interpreted as being unbounded, if there is no such
// key, which is the case for the empty prefix and for prefixes that consist
// solely of 0xff bytes.
func PrefixSuccessor(prefix string) string {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

const deprecatedRootKey = "prefix"

// filesystemYAML is the union of the fields supported for each filesystem
// entry and is used for validation and documentation.
type filesystemYAML struct {
	Root     `yaml:",inline"`
	KeyRange `yaml:",inline"`
}

// docT mirrors T for the purposes of documentation since the Filesystem
// interface used in T cannot be described.
type docT struct {
	ConfigFiles Files                 `yaml:"filesystem_config_files" doc:"other configuration files to search for and merge"`
	Filesystems []filesystemYAML      `yaml:"filesystems" doc:"the filesystem configurations to be analyzed, each of which specifies either a root for a hierarchical filesystem or a start_prefix and/or stop_prefix for a range of keys in a key oriented store"`
	Logging     cmdutil.LoggingConfig `yaml:"logging" doc:"logging configuration, it may be overridden by the --log-* command line flags"`
}

// decodeFilesystem decodes a single filesystem entry as either a *Root or
// a *KeyRange depending on the fields it contains. The fields are
// determined by decoding the node, rather than inspecting its keys, so that
// fields supplied via YAML merge keys (<<) are taken into account.
func decodeFilesystem(node *yaml.Node) (Filesystem, error) {
	var probe struct {
		Root        *string `yaml:"root"`
		Prefix      *string `yaml:"prefix"`
		StartPrefix *string `yaml:"start_prefix"`
		StopPrefix  *string `yaml:"stop_prefix"`
	}
	if err := node.Decode(&probe); err != nil {
		return nil, err
	}
	if probe.Root != nil && probe.Prefix != nil {
		return nil, fmt.Errorf("yaml: line %d: only one of root and its deprecated alias prefix may be specified", node.Line)
	}
	hasRoot := probe.Root != nil || probe.Prefix != nil
	hasRange := probe.StartPrefix != nil || probe.StopPrefix != nil
	if hasRoot && hasRange {
		return nil, fmt.Errorf("yaml: line %d: a filesystem must specify either root or start_prefix/stop_prefix, but not both", node.Line)
	}
	if hasRange {
		kr := &KeyRange{}
		return kr, node.Decode(kr)
	}
	// Note that the deprecated prefix key is ignored when decoding into
	// Root since the decoding is not strict; the keys have already been
	// validated by validateMappingKeys.
	r := &Root{}
	if err := node.Decode(r); err != nil {
		return nil, err
	}
	if probe.Prefix != nil {
		r.Root = *probe.Prefix
	}
	return r, nil
}
