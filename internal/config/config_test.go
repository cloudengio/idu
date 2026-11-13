// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package config_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmdutil/cmdconfig"
)

const simple = `
filesystems:
  - name: tmp
    prefix: /tmp
    database: ./db-tmp
    stats_dir: ${HOME}/stats-tmp
    exclusions:
      - something
    layout:
      calculator: block
      parameters:
        size: 4KiB
  - name: root
    prefix: /
    database: ./db-local
    concurrent_scans: 2
    concurrent_stats: 4
    exclusions:
      - '.DS_Store$'
      - ^/System/Volumes/
      - ^/System/Volumes$
  - name: raid
    prefix: /raid
    database: ./db-raid
    layout:
      calculator: raid0
      parameters:
        stripe_size: 64KiB
        num_stripes: 4
`

func TestSimple(t *testing.T) {
	cfg, err := config.ParseConfig([]byte(simple))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(cfg.Filesystems), 3; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := cfg.Filesystems[0].Prefix, "/tmp"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cfg.Filesystems[0].Calculator().Calculate(1, 1), int64(4096); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := cfg.Filesystems[2].Calculator().Calculate(1, 1), int64(65536*4); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	root := cfg.Filesystems[1]
	if got, want := root.Prefix, "/"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := root.Exclude("something"), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := root.Exclude("/System/Volumes/XX"), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := root.Exclude("/System/Volumes"), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := root.Exclude("/System/VolumesX"), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := root.ConcurrentScans, 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := root.ConcurrentStats, 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

const defaults = `
filesystems:
  - prefix: /tmp
    database: ./db-tmp
  - prefix: /var
    concurrent_scans: 300
    concurrent_stats: 100
    concurrent_stats_threshold: 200
    scan_size: 400
`

func init() {
	config.DefaultConcurrentStats = 1
	config.DefaultConcurrentStatsThreshold = 2
	config.DefaultConcurrentScans = 3
	config.DefaultScanSize = 4
}

func TestDefaults(t *testing.T) {
	cfg, err := config.ParseConfig([]byte(defaults))
	if err != nil {
		t.Fatal(err)
	}

	dp := cfg.Filesystems[0]
	if got, want := dp.ConcurrentStats, 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := dp.ConcurrentStatsThreshold, 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := dp.ConcurrentScans, 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := dp.ScanSize, 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := dp.StatsDir, "stats"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	dp = cfg.Filesystems[1]
	if got, want := dp.ConcurrentStats, 100; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := dp.ConcurrentStatsThreshold, 200; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := dp.ConcurrentScans, 300; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := dp.ScanSize, 400; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPrefixMatch(t *testing.T) {
	cfg, err := config.ParseConfig([]byte(simple))
	if err != nil {
		t.Fatal(err)
	}
	p, ok := cfg.ForPrefix("/tmp")
	if got, want := p.Prefix, "/tmp"; !ok || got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	p, ok = cfg.ForPrefix("/tmp/xyz")
	if got, want := p.Prefix, "/tmp"; !ok || got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	p, ok = cfg.ForName("tmp")
	if got, want := p.Prefix, "/tmp"; !ok || got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	p, ok = cfg.ForName("raid")
	if got, want := p.Prefix, "/raid"; !ok || got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestMergeConfigs(t *testing.T) {
	shared := `
vars:
  db_base: /var/db/idu
common_exclusions: &common_excl
  - '\.DS_Store$'
filesystem_config_files:
  - fs1.yaml
  - fs2.yaml
`
	fs1 := `
- name: home
  prefix: /home
  database: ${db_base}/home
  concurrent_scans: 10
  exclusions:
    - <<: *common_excl
    - '^/home/cache'
`
	fs2 := `
filesystems:
  - name: data
    prefix: /labs/data
    database: ${db_base}/data
    layout:
      calculator: block
      parameters:
        size: 8192
`
	cfg, err := config.ParseConfigSpecs([]byte(shared), []byte(fs1), []byte(fs2))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(cfg.Filesystems), 2; got != want {
		t.Fatalf("got %d filesystems, want %d", got, want)
	}
	if got, want := len(cfg.ConfigFiles), 2; got != want {
		t.Fatalf("got %d filesystem_config_files, want %d", got, want)
	}
	home, ok := cfg.ForName("home")
	if !ok {
		t.Fatal("missing home")
	}
	if got, want := home.Database, "/var/db/idu/home"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := home.ConcurrentScans, 10; got != want {
		t.Errorf("got %d, want %d", got, want)
	}
	if !home.Exclude("/home/user/.DS_Store") || !home.Exclude("/home/cache/tmp") {
		t.Errorf("expected inherited and local exclusions to match")
	}
	data, ok := cfg.ForName("data")
	if !ok {
		t.Fatal("missing data")
	}
	if got, want := data.Database, "/var/db/idu/data"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got, want := data.Calculator().Calculate(1, 1), int64(8192); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestStrictAndErrors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{
			name: "unknown-top-level-field",
			yaml: "unknown: 1\n",
		},
		{
			name: "unknown-filesystem-field",
			yaml: "filesystems:\n  - prefix: /tmp\n    bogus: 1\n",
		},
		{
			name: "unknown-sequence-filesystem-field",
			yaml: "- prefix: /tmp\n  bogus: 1\n",
		},
		{
			name: "invalid-top-level-scalar",
			yaml: "just-a-string\n",
		},
		{
			name: "unknown-layout-param",
			yaml: "filesystems:\n  - prefix: /tmp\n    layout:\n      calculator: block\n      parameters:\n        bad: 4096\n",
		},
		{
			name: "invalid-regexp",
			yaml: "filesystems:\n  - prefix: /tmp\n    exclusions:\n      - '[unclosed'\n",
		},
		{
			name: "invalid-bytesize",
			yaml: "filesystems:\n  - prefix: /tmp\n    layout:\n      calculator: block\n      parameters:\n        size: not-a-size\n",
		},
		{
			name: "duplicate-name",
			yaml: "filesystems:\n  - name: shared\n    prefix: /a\n  - name: shared\n    prefix: /b\n",
		},
		{
			name: "empty-prefix",
			yaml: "filesystems:\n  - name: noprefix\n    database: ./db\n",
		},
		{
			name: "invalid-filesystems-type",
			yaml: "filesystems:\n  foo: bar\n",
		},
		{
			name: "invalid-filesystem_config_files-type",
			yaml: "filesystem_config_files: 123\n",
		},
		{
			name: "negative-compact-interval-days",
			yaml: "filesystems:\n  - prefix: /tmp\n    compact_interval_days: -1\n",
		},
		{
			name: "negative-compact-interval",
			yaml: "filesystems:\n  - prefix: /tmp\n    compact_interval: -5m\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := config.ParseConfig([]byte(tc.yaml)); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestLoadConfigYAML(t *testing.T) {
	home := t.TempDir()
	workDir := t.TempDir()
	t.Chdir(workDir)
	t.Setenv("HOME", home)
	t.Setenv(config.EnvConfig, "")
	t.Setenv(config.EnvVars, "")

	confDir := filepath.Join(home, ".config", config.AppName)
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatal(err)
	}

	write := func(dir, name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	write(confDir, "extra.env", "ENV_DB_ROOT=/srv/idu-db\n")
	write(confDir, "config.yaml", `vars:
  db_root: ${ENV_DB_ROOT}
filesystem_config_files:
  - fs-one.yaml
  - fs-two
filesystems:
  - name: root-fs
    prefix: /
    database: ${db_root}/root
`)
	write(confDir, "fs-one.yaml", `- name: one
  prefix: /one
  database: ${db_root}/one
`)
	write(confDir, "fs-two.yml", `filesystem_config_files:
  - fs-three.yaml
filesystems:
  - name: two
    prefix: /two
    database: ${db_root}/two
`)
	write(confDir, "fs-three.yaml", `- name: three
  prefix: /three
  database: ${db_root}/three
`)
	write(confDir, "ignored.yaml", `filesystems:
  - name: ignored
    prefix: /ignored
    database: ${db_root}/ignored
`)

	ctx := context.Background()
	var ff cmdconfig.FilesFlag
	cfg, loc, err := config.Load(ctx, &ff)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(loc.YAML), 4; got != want {
		t.Fatalf("got %d YAML files (%v), want %d", got, loc.YAML, want)
	}
	for _, tc := range []struct {
		name, prefix, db string
	}{
		{"root-fs", "/", "/srv/idu-db/root"},
		{"one", "/one", "/srv/idu-db/one"},
		{"two", "/two", "/srv/idu-db/two"},
		{"three", "/three", "/srv/idu-db/three"},
	} {
		p, ok := cfg.ForName(tc.name)
		if !ok {
			t.Fatalf("missing filesystem %q", tc.name)
		}
		if p.Prefix != tc.prefix || p.Database != tc.db {
			t.Errorf("%s: got prefix=%q db=%q, want prefix=%q db=%q", tc.name, p.Prefix, p.Database, tc.prefix, tc.db)
		}
	}
	if _, ok := cfg.ForName("ignored"); ok {
		t.Errorf("did not expect ignored.yaml to be loaded when config.yaml specifies filesystem_config_files")
	}
	if _, ok := cfg.ForName(""); ok {
		t.Errorf("did not expect empty name lookup to match")
	}

	// Missing entry in filesystem_config_files returns an error mentioning
	// both the missing entry and the files read so far.
	cfgPath := write(confDir, "config.yaml", `filesystem_config_files:
  - fs-one.yaml
  - not-present
`)
	_, loc, err = config.Load(ctx, &ff)
	assertErrorContainsAll(t, err, cfgPath, filepath.Join(confDir, "fs-one.yaml"), "not-present")
	if loc == nil || len(loc.YAML) != 2 {
		t.Fatalf("expected 2 located YAML files before missing error, got %v", loc)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	home := t.TempDir()
	workDir := t.TempDir()
	t.Chdir(workDir)
	t.Setenv("HOME", home)
	t.Setenv(config.EnvConfig, "")
	t.Setenv(config.EnvVars, "")

	confDir := filepath.Join(home, ".config", config.AppName)
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(dir, name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(confDir, "config.yaml", "filesystems:\n  - name: home-only\n    prefix: /home\n    database: ./home-db\n")
	write(confDir, "fs-one.yaml", "filesystems:\n  - name: one\n    prefix: /one\n    database: ./one-db\n")
	write(workDir, "config.yaml", "filesystems:\n  - name: cwd-only\n    prefix: /cwd\n    database: ./cwd-db\n")

	ctx := context.Background()
	cfg, loc, err := config.Load(ctx, &cmdconfig.FilesFlag{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(loc.YAML), 1; got != want {
		t.Fatalf("got %d YAML files (%v), want %d", got, loc.YAML, want)
	}
	if _, ok := cfg.ForName("cwd-only"); !ok {
		t.Errorf("expected cwd config.yaml to take precedence over $HOME/.config/idu/config.yaml")
	}

	// IDU_CONFIG overrides config.yaml discovery.
	t.Setenv(config.EnvConfig, "fs-one.yaml")
	cfg, loc, err = config.Load(ctx, &cmdconfig.FilesFlag{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(loc.YAML), 1; got != want {
		t.Fatalf("got %d YAML files (%v), want %d", got, loc.YAML, want)
	}
	if _, ok := cfg.ForName("one"); !ok {
		t.Errorf("expected IDU_CONFIG=fs-one.yaml to load 'one'")
	}
}

func TestLoadSearchPathFallback(t *testing.T) {
	home := t.TempDir()
	workDir := t.TempDir()
	t.Chdir(workDir)
	t.Setenv("HOME", home)
	t.Setenv(config.EnvConfig, "")
	t.Setenv(config.EnvVars, "")

	confDir := filepath.Join(home, ".config", config.AppName)
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatal(err)
	}

	write := func(dir, name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	write(confDir, "vars.yaml", "vars:\n  db_root: ${ENV_DB_ROOT}\n")
	write(confDir, "extra.env", "ENV_DB_ROOT=/srv/idu-db\n")
	write(confDir, "fs-one.yaml", "- name: one\n  prefix: /one\n  database: ${db_root}/one\n")
	write(confDir, "fs-two.yml", "- name: two\n  prefix: /two\n  database: ${db_root}/two\n")

	ctx := context.Background()
	var ff cmdconfig.FilesFlag
	cfg, loc, err := config.Load(ctx, &ff)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(loc.YAML), 3; got != want {
		t.Fatalf("got %d YAML files (%v), want %d", got, loc.YAML, want)
	}
	if p, ok := cfg.ForName("one"); !ok || p.Database != "/srv/idu-db/one" {
		t.Errorf("got %+v, %v", p, ok)
	}
	if p, ok := cfg.ForName("two"); !ok || p.Database != "/srv/idu-db/two" {
		t.Errorf("got %+v, %v", p, ok)
	}

	// Explicit --config selects specific files in SearchPath (e.g. cwd + $HOME/.config/idu).
	write(workDir, "override.yaml", "filesystems:\n  - name: local\n    prefix: /local\n    database: ./local-db\n")
	var explicit cmdconfig.FilesFlag
	if err := explicit.Set("override.yaml,fs-two.yml"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ENV_DB_ROOT", "/env-override")
	cfg, loc, err = config.Load(ctx, &explicit)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(loc.YAML), 2; got != want {
		t.Fatalf("got %d YAML files (%v), want %d", got, loc.YAML, want)
	}
	if _, ok := cfg.ForName("local"); !ok {
		t.Errorf("expected local filesystem")
	}
	if _, ok := cfg.ForName("two"); !ok {
		t.Errorf("expected two filesystem")
	}
	if _, ok := cfg.ForName("one"); ok {
		t.Errorf("did not expect one when explicit --config was provided")
	}
}

func TestExampleConfig(t *testing.T) {
	cfg, err := config.ReadConfig(filepath.Join("..", "..", "example-config.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.ForPrefix("/Users/cnicolaou"); !ok {
		t.Errorf("expected /Users/cnicolaou in example-config.yml")
	}
	if p, ok := cfg.ForName("home"); !ok || p.StatsDir != "./stats/home" || p.CompactionInterval() != 7*24*time.Hour+12*time.Hour {
		t.Errorf("expected name home with stats_dir ./stats/home and 7d12h compact interval in example-config.yml, got %+v, %v", p, ok)
	}
	if _, ok := cfg.ForPrefix("/labs/asbhatt/data"); !ok {
		t.Errorf("expected /labs/asbhatt in example-config.yml")
	}
	if p, ok := cfg.ForName("asbhatt"); !ok || p.Prefix != "/labs/asbhatt" {
		t.Errorf("expected name asbhatt -> /labs/asbhatt in example-config.yml, got %+v, %v", p, ok)
	}
}

func TestDocumentation(t *testing.T) {
	got := config.Documentation()
	for _, expected := range []string{
		"raid0",
		"filesystems:",
		"filesystem_config_files:",
		"name:",
		"prefix:",
		"stats_dir:",
		"compact_interval_days:",
		"compact_interval:",
		"when building",
		"stripe_size:",
	} {
		if !strings.Contains(got, expected) {
			t.Errorf("documentation does not contain: %q\n%s", expected, got)
		}
	}
}

func assertErrorContainsAll(t *testing.T, err error, wants ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %v, got nil", wants)
	}
	for _, want := range wants {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
}

func TestLoadAndReadConfigErrorsShowFilesRead(t *testing.T) {
	home := t.TempDir()
	workDir := t.TempDir()
	t.Chdir(workDir)
	t.Setenv("HOME", home)
	t.Setenv(config.EnvConfig, "")
	t.Setenv(config.EnvVars, "")

	confDir := filepath.Join(home, ".config", config.AppName)
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatal(err)
	}

	write := func(name, content string) string {
		p := filepath.Join(confDir, name)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	mainFile := write("config.yaml", "filesystem_config_files:\n  - fs-a.yaml\n  - fs-bad.yaml\n")
	fsA := write("fs-a.yaml", "filesystems:\n  - name: a\n    prefix: /a\n    database: /db/a\n")
	fsBad := write("fs-bad.yaml", "filesystems:\n  - name: bad\n    unknown_field: 123\n")
	envFile := write("vars.env", "SOME_VAR=hello\n")

	ctx := context.Background()
	var ff cmdconfig.FilesFlag
	_, loc, err := config.Load(ctx, &ff)
	assertErrorContainsAll(t, err, mainFile, fsA, fsBad, envFile)

	var buf bytes.Buffer
	config.PrintLocated(&buf, loc)
	for _, want := range []string{
		"# Configuration file: " + mainFile,
		"# Configuration file: " + fsA,
		"# Configuration file: " + fsBad,
		"# Environment variables file: " + envFile,
	} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("PrintLocated output %q missing %q", buf.String(), want)
		}
	}

	// Duplicate name validation error across two files also lists all read files.
	if err := os.WriteFile(fsBad, []byte("filesystems:\n  - name: a\n    prefix: /b\n    database: /db/b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, loc, err = config.Load(ctx, &ff)
	if loc == nil {
		t.Fatalf("expected non-nil Located on validation error")
	}
	assertErrorContainsAll(t, err, mainFile, fsA, fsBad)

	// ReadConfig lists all supplied files on error.
	_, err = config.ReadConfig(fsA, fsBad)
	assertErrorContainsAll(t, err, fsA, fsBad)
}

func TestPrintLocatedNone(t *testing.T) {
	var buf bytes.Buffer
	config.PrintLocated(&buf, &cmdconfig.Located{})
	if !strings.Contains(buf.String(), "# Configuration file: none") {
		t.Errorf("expected '# Configuration file: none', got %q", buf.String())
	}
}

func TestCompactIntervals(t *testing.T) {
	cfg, err := config.ParseConfig([]byte(`
filesystems:
  - name: default
    prefix: /default
  - name: days-only
    prefix: /days
    compact_interval_days: 3
  - name: dur-only
    prefix: /dur
    compact_interval: 2h30m
  - name: combined
    prefix: /combined
    compact_interval_days: 2
    compact_interval: 6h
`))
	if err != nil {
		t.Fatal(err)
	}
	wants := map[string]time.Duration{
		"default":   0,
		"days-only": 3 * 24 * time.Hour,
		"dur-only":  2*time.Hour + 30*time.Minute,
		"combined":  2*24*time.Hour + 6*time.Hour,
	}
	for name, want := range wants {
		p, ok := cfg.ForName(name)
		if !ok {
			t.Fatalf("missing %q", name)
		}
		if got := p.CompactionInterval(); got != want {
			t.Errorf("%s: CompactionInterval() = %v, want %v", name, got, want)
		}
	}
}
