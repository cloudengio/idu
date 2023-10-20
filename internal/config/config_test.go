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
- prefix: /tmp
  database: ./db-tmp
  exclusions:
    - something
  layout:
    calculator: block
    parameters:
      size: 4096
- prefix: /
  database: ./db-local
  concurrent_scans: 2
  concurrent_stats: 4
  exclusions:
    - /.DS_Store$"
    - ^/System/Volumes/
    - ^/System/Volumes$
`

func TestSimple(t *testing.T) {
	cfg, err := config.ParseConfig([]byte(simple))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(cfg.Prefixes), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cfg.Prefixes[0].Prefix, "/tmp"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := cfg.Prefixes[0].StorageBytes(3), int64(4096); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := cfg.Prefixes[1].Exclude("something"), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := cfg.Prefixes[1].Exclude("/System/Volumes/XX"), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := cfg.Prefixes[1].Exclude("/System/Volumes"), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := cfg.Prefixes[1].Exclude("/System/VolumesX"), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := cfg.Prefixes[1].ConcurrentScans == 2, true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := cfg.Prefixes[1].ConcurrentStats == 4, true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

}

func TestPrefixMatch(t *testing.T) {
	cfg, err := config.ParseConfig([]byte(simple))
	if err != nil {
		t.Fatal(err)
	}
	p, path, ok := cfg.ForPrefix("/tmp")
	if got, want := p, "/tmp"; !ok || got.Prefix != want || path != "" {
		t.Errorf("got %v, want %v", got, want)
	}
	p, path, ok = cfg.ForPrefix("/tmp/xyz")
	if got, want := p, "/tmp"; !ok || got.Prefix != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := path, "xyz"; !ok || got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestDocumentation(t *testing.T) {
	got := config.Documentation()
	for _, expected := range []string{
		"raid0",
		"prefix:",
		"when building",
		"raid0",
	} {
		if !strings.Contains(got, expected) {
			t.Errorf("documentation does not contain: %q", expected)
		}
	}
}
