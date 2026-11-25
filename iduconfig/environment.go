// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package iduconfig

import (
	"cloudeng.io/cmdutil/cmdconfig"
)

// Environment provides the values of environment variables defined in the
// process environment or in the .env files that a configuration was loaded
// with. It is intended for secrets, such as API keys, that should not
// appear in the YAML configuration: the YAML refers to them by name, eg.
// LLM.APIKeyEnv, and the program looks them up when it needs them, so that
// they are not part of the configuration that config show displays.
//
// The process environment takes precedence over the .env files and, among
// the .env files, a later file over an earlier one. References to other
// variables in a value from a .env file are expanded.
type Environment struct {
	mapping func(string) string
	files   []string
}

// NewEnvironment returns the Environment for the .env files in loc, which
// may be nil, in which case it is the process environment alone.
func NewEnvironment(loc *cmdconfig.Located) (*Environment, error) {
	if loc == nil {
		loc = &cmdconfig.Located{}
	}
	mapping, err := loc.ExpandMapping()
	if err != nil {
		return nil, err
	}
	return &Environment{mapping: mapping, files: loc.Env}, nil
}

// Get returns the value of the named variable, or "" if it is not set.
func (e *Environment) Get(name string) string {
	if e == nil || e.mapping == nil || name == "" {
		return ""
	}
	return e.mapping(name)
}

// Files returns the .env files, if any, that the Environment was created
// from.
func (e *Environment) Files() []string {
	if e == nil {
		return nil
	}
	return e.files
}
