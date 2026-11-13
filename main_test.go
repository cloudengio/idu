// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"cloudeng.io/os/executil"
)

var iduCommand string

func TestMain(m *testing.M) {
	tmpDir, _ := os.MkdirTemp("", "idu")
	bin, err := executil.GoBuild(context.Background(), filepath.Join(tmpDir, "idu"), "cloudeng.io/cmd/idu")
	if err != nil {
		os.RemoveAll(tmpDir)
		os.Exit(1)
	}
	iduCommand = bin
	code := m.Run()
	os.RemoveAll(tmpDir)
	os.Exit(code)
}

func runIDUEnv(env []string, args ...string) (string, error) {
	cmd := exec.Command(iduCommand, args...)
	if len(env) > 0 {
		cmd.Env = env
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%v: %v\n%s", strings.Join(cmd.Args, " "), err, string(out))
	}
	return string(out), nil
}

func runIDU(args ...string) (string, error) {
	return runIDUEnv(nil, args...)
}

func containsAnyOf(got string, expected ...string) error {
	for _, want := range expected {
		if !strings.Contains(got, want) {
			_, file, line, _ := runtime.Caller(1)
			return fmt.Errorf("%v:%v: %q does not contain %v", filepath.Base(file), line, got, want)
		}
	}
	return nil
}

func TestHelp(t *testing.T) {
	out, _ := runIDU("help") // will return exit status 1 for help.
	base := []string{
		"analyze disk usage using a database for incremental updates",
		"errors - list the errors stored in the database",
		"config - show the configuration, and its documentation",
	}
	if err := containsAnyOf(out, base...); err != nil {
		t.Fatal(err)
	}
	out, _ = runIDU("--help") // will return exit status 1 for --help
	if err := containsAnyOf(out, base...); err != nil {
		t.Fatal(err)
	}
	err := containsAnyOf(out, "[--config= --gcpercent=50 --http= --log-dir= --profile= --stderr=false --units=decimal --v=0]")
	if err != nil {
		t.Fatal(err)
	}
	out, _ = runIDU("help", "analyze") // will return exit status 1 for help.

	err = containsAnyOf(out, "Usage of command \"analyze\": analyze the file system to build a database of directory and file metadata.",
		"analyze [--force=false --progress=true --show-defaults=false --slow-scan-duration=10s] <prefix>")
	if err != nil {
		t.Fatal(err)
	}
}

func TestConfigSubcommandsAndUnits(t *testing.T) {
	home := t.TempDir()
	confDir := filepath.Join(home, ".config", "idu")
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(confDir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("config.yaml", "vars:\n  db_root: /tmp/idu-dbs\nfilesystem_config_files:\n  - fs-a.yaml\n  - fs-b\n")
	write("fs-a.yaml", "- name: fs-alpha\n  prefix: /fs/a\n  database: ${db_root}/a\n")
	write("fs-b.yml", "- name: fs-beta\n  prefix: /fs/b\n  database: ${db_root}/b\n  layout:\n    calculator: block\n    parameters:\n      size: 4KiB\n")

	env := append(os.Environ(), "HOME="+home, "IDU_CONFIG=", "IDU_ENV=")

	out, err := runIDUEnv(env, "--units=binary", "config", "show")
	if err != nil {
		t.Fatal(err)
	}
	if err := containsAnyOf(out,
		"Configuration file:",
		"config.yaml",
		"fs-a.yaml",
		"fs-b.yml",
		"fs-alpha",
		"fs-beta",
		"/fs/a",
		"/fs/b",
		"/tmp/idu-dbs/a",
		"/tmp/idu-dbs/b",
	); err != nil {
		t.Fatal(err)
	}

	for name, wantDB := range map[string]string{
		"fs-alpha": "/tmp/idu-dbs/a",
		"fs-beta":  "/tmp/idu-dbs/b",
		"/fs/a":    "/tmp/idu-dbs/a",
		"/fs/b":    "/tmp/idu-dbs/b",
	} {
		out, err = runIDUEnv(env, "database", "locate", name)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(out) != wantDB {
			t.Fatalf("database locate %q: got %q, want %q", name, strings.TrimSpace(out), wantDB)
		}
	}

	out, err = runIDUEnv(env, "config", "describe")
	if err != nil {
		t.Fatal(err)
	}
	if err := containsAnyOf(out,
		"YAML configuration file options",
		"filesystem_config_files:",
		"filesystems:",
		"raid0:",
		"stripe_size:",
	); err != nil {
		t.Fatal(err)
	}

	out, err = runIDUEnv(env, "--units=invalid", "config", "show")
	if err == nil {
		t.Fatalf("expected error for --units=invalid, got output:\n%s", out)
	}
	if !strings.Contains(out, "decimal") || !strings.Contains(out, "binary") {
		t.Fatalf("expected allowed enum values in error, got:\n%s", out)
	}
}

func TestConfigFallbackAndCLIFlowByName(t *testing.T) {
	home := t.TempDir()
	confDir := filepath.Join(home, ".config", "idu")
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatal(err)
	}

	treeDir := filepath.Join(t.TempDir(), "tree")
	if err := os.MkdirAll(filepath.Join(treeDir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(treeDir, "sub", "hello.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(home, "db-tree")
	statsDir := filepath.Join(home, "tree-stats")
	overrideStatsDir := filepath.Join(home, "override-stats")

	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(confDir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// No config.yaml: fallback merges all .yaml/.yml files in $HOME/.config/idu.
	write("a.yaml", fmt.Sprintf("filesystems:\n  - name: mytree\n    prefix: %s\n    database: %s\n    stats_dir: %s\n", treeDir, dbPath, statsDir))
	write("b.yml", "filesystems:\n  - name: other\n    prefix: /other\n    database: /tmp/db-other\n")

	env := append(os.Environ(), "HOME="+home, "IDU_CONFIG=", "IDU_ENV=")

	out, err := runIDUEnv(env, "config", "show")
	if err != nil {
		t.Fatal(err)
	}
	if err := containsAnyOf(out, "a.yaml", "b.yml", "mytree", "other"); err != nil {
		t.Fatal(err)
	}

	if _, err := runIDUEnv(env, "analyze", "--progress=false", "mytree"); err != nil {
		t.Fatal(err)
	}

	out, err = runIDUEnv(env, "find", "mytree")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, filepath.Join(treeDir, "sub", "hello.txt")) {
		t.Fatalf("find mytree output %q does not contain expected file", out)
	}

	if _, err := runIDUEnv(env, "stats", "compute", "mytree"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(statsDir, "latest.idustats")); err != nil {
		t.Fatalf("expected latest.idustats in configured stats_dir %s: %v", statsDir, err)
	}

	if _, err := runIDUEnv(env, "stats", "compute", "--stats-dir="+overrideStatsDir, "mytree"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(overrideStatsDir, "latest.idustats")); err != nil {
		t.Fatalf("expected latest.idustats in --stats-dir override %s: %v", overrideStatsDir, err)
	}
}

func TestConfigShowAndCommandsOnLoadError(t *testing.T) {
	home := t.TempDir()
	confDir := filepath.Join(home, ".config", "idu")
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
	fsA := write("fs-a.yaml", "- name: a\n  prefix: /a\n  database: /db/a\n")
	fsBad := write("fs-bad.yaml", "- name: bad\n  unknown_field: true\n")
	envFile := write("vars.env", "TEST_VAR=1\n")

	env := append(os.Environ(), "HOME="+home, "IDU_CONFIG=", "IDU_ENV=")

	out, err := runIDUEnv(env, "config", "show")
	if err == nil {
		t.Fatalf("expected config show to fail on invalid config, got output:\n%s", out)
	}
	if err := containsAnyOf(out,
		"# Configuration file: "+mainFile,
		"# Configuration file: "+fsA,
		"# Configuration file: "+fsBad,
		"# Environment variables file: "+envFile,
		"failed to load configuration",
	); err != nil {
		t.Fatalf("config show output mismatch: %v", err)
	}

	out, err = runIDUEnv(env, "analyze", "a")
	if err == nil {
		t.Fatalf("expected analyze to fail on invalid config, got output:\n%s", out)
	}
	if err := containsAnyOf(out, mainFile, fsA, fsBad, envFile); err != nil {
		t.Fatalf("analyze error output did not show read files: %v", err)
	}

	// Commands that do not require the config still succeed.
	if _, err := runIDUEnv(env, "config", "describe"); err != nil {
		t.Fatalf("expected config describe to succeed even with broken config: %v", err)
	}

	// Explicit missing --config reports none and mentions the missing path.
	out, err = runIDUEnv(env, "--config=does-not-exist.yaml", "config", "show")
	if err == nil {
		t.Fatalf("expected error for missing --config, got output:\n%s", out)
	}
	if err := containsAnyOf(out, "# Configuration file: none", "does-not-exist.yaml"); err != nil {
		t.Fatalf("missing --config output mismatch: %v", err)
	}
}
