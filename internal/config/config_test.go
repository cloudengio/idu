// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package config_test

import (
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/internal/config"
)

const simple = `
databases:
  - prefix: /tmp
    type: local
    directory: ./db-tmp
  - prefix: /
    type: local
    directory: ./db-local
layouts:
  - type: simple
    prefix: "/"
    block_size: 4096
  - type: "simple"
    prefix: "/labs/asbhatt"
    block_size: 4096
  - type: "raid0"
    prefix: "/labs/bar"
    num_stripes: 3
    stripe_size: 1024
exclusions:
  - prefix: "/Users/cnicolaou"
    regexps:
      - ".DS_Store$"
  - prefix: "/tmp"
    regexps:
       - ".DS_Store$"
       - "something"
 `

func TestSimple(t *testing.T) {
	cfg, err := config.ParseConfig([]byte(simple))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(cfg.Databases), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cfg.Databases[0].Type, "local"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := cfg.Databases[1].Description, "local database in ./db-local"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := len(cfg.Layouts), 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cfg.Layouts[0].Calculator.String(), "simple: 4096"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cfg.Layouts[2].Prefix, "/"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := len(cfg.Exclusions), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(cfg.Exclusions[1].Regexps), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cfg.Exclusions[1].Regexps[1].String(), "something"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestDocumentation(t *testing.T) {
	got := config.Documentation()
	for _, expected := range []string{
		"raid0",
		"local",
		"Supported Databases:",
		"Supported Layouts:",
	} {
		if !strings.Contains(got, expected) {
			t.Errorf("documentation does not contain: %q", expected)
		}
	}
}
