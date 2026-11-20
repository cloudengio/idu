// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/internal/cli/clitest"
	"cloudeng.io/os/executil"
)

var iduCommand string

func TestMain(m *testing.M) {
	tmpDir, _ := os.MkdirTemp("", "idu")
	name := "idu"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	bin, err := executil.GoBuild(context.Background(), filepath.Join(tmpDir, name), "cloudeng.io/cmd/idu")
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
	err := containsAnyOf(out, "[--config= --fs= --gcpercent=50 --http= --log-file= --log-format=json --log-level=0 --log-source-code=false --log-tempfile= --profile= --units=decimal]")
	if err != nil {
		t.Fatal(err)
	}
	out, _ = runIDU("help", "analyze") // will return exit status 1 for help.

	err = containsAnyOf(out, "Usage of command \"analyze\": analyze the file system to build a database of directory and file metadata.",
		"analyze [--force=false --fs= --progress=true --refresh-topn=false --show-defaults=false --slow-scan-duration=10s --snapshot=false] [<prefix>]")
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

	env := append(os.Environ(), "HOME="+home, "APPDATA="+filepath.Join(home, ".config"), "IDU_CONFIG=", "IDU_ENV=")

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

	for _, tc := range []struct {
		args   []string
		wantDB string
	}{
		{[]string{"--fs=fs-alpha", "database", "locate"}, "/tmp/idu-dbs/a"},
		{[]string{"--fs=fs-beta", "database", "locate"}, "/tmp/idu-dbs/b"},
		{[]string{"--fs=fs-alpha", "database", "locate", "/fs/a"}, "/tmp/idu-dbs/a"},
		{[]string{"--fs=fs-beta", "database", "locate", "/fs/b/sub"}, "/tmp/idu-dbs/b"},
		{[]string{"--fs=fs-beta", "database", "locate", "sub"}, "/tmp/idu-dbs/b"},
	} {
		out, err = runIDUEnv(env, tc.args...)
		if err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(out) != tc.wantDB {
			t.Fatalf("%v: got %q, want %q", tc.args, strings.TrimSpace(out), tc.wantDB)
		}
	}

	// --fs is required when more than one filesystem is configured and
	// prefixes must be contained within the filesystem's prefix.
	for _, args := range [][]string{
		{"database", "locate"},
		{"database", "locate", "/fs/a"},
		{"--fs=fs-alpha", "database", "locate", "/fs/b"},
		{"--fs=fs-alpha", "database", "locate", "../b"},
		{"--fs=nosuch", "database", "locate"},
	} {
		if out, err := runIDUEnv(env, args...); err == nil {
			t.Fatalf("%v: expected an error, got %q", args, out)
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

func assertStatsViewOutput(t *testing.T, env []string, wantPrefix string, globalArgs []string, extraArgs ...string) {
	t.Helper()
	args := append(slices.Clone(globalArgs), "stats", "view", "--info")
	args = append(args, extraArgs...)
	out, err := runIDUEnv(env, args...)
	if err != nil {
		t.Fatalf("stats view %v failed: %v\n%s", extraArgs, err, out)
	}
	if !strings.Contains(out, "Prefix     : "+wantPrefix) {
		t.Fatalf("stats view %v unexpected output:\n%s", extraArgs, out)
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
	write("a.yaml", fmt.Sprintf("filesystems:\n  - name: mytree\n    prefix: %s\n    database: %s\n    snapshot_dir: %s\n", treeDir, dbPath, statsDir))
	write("b.yml", "filesystems:\n  - name: other\n    prefix: /other\n    database: /tmp/db-other\n")

	env := append(os.Environ(), "HOME="+home, "APPDATA="+filepath.Join(home, ".config"), "IDU_CONFIG=", "IDU_ENV=")

	out, err := runIDUEnv(env, "config", "show")
	if err != nil {
		t.Fatal(err)
	}
	if err := containsAnyOf(out, "a.yaml", "b.yml", "mytree", "other"); err != nil {
		t.Fatal(err)
	}

	if _, err := runIDUEnv(env, "--fs=mytree", "analyze", "--progress=false"); err != nil {
		t.Fatal(err)
	}
	assertLogsRange(t, env)

	out, err = runIDUEnv(env, "--fs=mytree", "find", ".")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, filepath.Join(treeDir, "sub", "hello.txt")) {
		t.Fatalf("find mytree output %q does not contain expected file", out)
	}

	if _, err := runIDUEnv(env, "--fs=mytree", "stats", "compute", "."); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(statsDir, "latest")); err != nil {
		t.Fatalf("expected latest in configured snapshot_dir %s: %v", statsDir, err)
	}

	if _, err := runIDUEnv(env, "--fs=mytree", "stats", "compute", "--snapshot-dir="+overrideStatsDir, "."); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(overrideStatsDir, "latest")); err != nil {
		t.Fatalf("expected latest in --snapshot-dir override %s: %v", overrideStatsDir, err)
	}

	assertStatsViews(t, env, treeDir, statsDir, overrideStatsDir)
}

// assertStatsViews verifies the various ways that stats view can locate
// the stats file for the mytree filesystem.
func assertStatsViews(t *testing.T, env []string, treeDir, statsDir, overrideStatsDir string) {
	t.Helper()
	mytree := []string{"--fs=mytree"}
	assertStatsViewOutput(t, env, treeDir, mytree)
	assertStatsViewOutput(t, env, treeDir, nil, "--fs=mytree")
	assertStatsViewOutput(t, env, treeDir, []string{"--fs=other"}, "--fs=mytree")
	assertStatsViewOutput(t, env, treeDir, nil, filepath.Join(statsDir, "latest"))
	assertStatsViewOutput(t, env, treeDir, mytree, "--snapshot-dir="+overrideStatsDir)
	if targetFile, err := os.Readlink(filepath.Join(statsDir, "latest")); err == nil {
		assertStatsViewOutput(t, env, treeDir, mytree, filepath.Base(targetFile))
		assertStatsViewOutput(t, env, treeDir, nil, "--fs=mytree", filepath.Base(targetFile))
	}
	if out, err := runIDUEnv(env, "stats", "view"); err == nil {
		t.Fatalf("expected stats view without --fs to fail, got %q", out)
	}
	// --fs is not required when --snapshot-dir is specified.
	assertStatsViewOutput(t, env, treeDir, nil, "--snapshot-dir="+overrideStatsDir)
	assertStatsViewOutput(t, env, treeDir, nil, "--snapshot-dir="+statsDir, "latest")
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

	env := append(os.Environ(), "HOME="+home, "APPDATA="+filepath.Join(home, ".config"), "IDU_CONFIG=", "IDU_ENV=")

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

func createFanoutFixtureTree(t *testing.T, treeDir string) (fanoutRoot string) {
	t.Helper()
	fanoutRoot = filepath.Join(treeDir, "wrapper", "fanout")
	payload := make([]byte, 2048)
	for b := 0; b < 3; b++ {
		for c := 0; c < 3; c++ {
			leaf := filepath.Join(fanoutRoot, fmt.Sprintf("b%d", b), fmt.Sprintf("c%d", c))
			if err := os.MkdirAll(leaf, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(leaf, "data.bin"), payload, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	singleDir := filepath.Join(treeDir, "single")
	if err := os.MkdirAll(singleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(singleDir, "big.bin"), make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	return fanoutRoot
}

func verifyDistributedMode(t *testing.T, home, confDir, treeDir, fanoutRoot, name string, countHardlinks bool, hardlinkDedup string) {
	t.Helper()
	dbPath := filepath.Join(home, "db-"+name)
	statsDir := filepath.Join(home, "stats-"+name)
	reportsDir := filepath.Join(home, "reports-"+name)
	cfgPath := filepath.Join(confDir, name+".yaml")
	cfgYAML := fmt.Sprintf("filesystems:\n  - name: %s\n    prefix: %s\n    database: %s\n    snapshot_dir: %s\n    count_hardlinks_as_files: %v\n    hardlink_dedup: %s\n",
		name, treeDir, dbPath, statsDir, countHardlinks, hardlinkDedup)
	if err := os.WriteFile(cfgPath, []byte(cfgYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(), "HOME="+home, "APPDATA="+filepath.Join(home, ".config"), "IDU_CONFIG="+cfgPath, "IDU_ENV=")

	if out, err := runIDUEnv(env, "analyze"); err != nil {
		t.Fatalf("%s analyze failed: %v\n%s", name, err, out)
	}
	latestStats := filepath.Join(statsDir, "latest")
	// By default, the stats for the root of the filesystem are constructed
	// from the stored rankings, which do not include the distributed
	// subtree statistics.
	if out, err := runIDUEnv(env, "stats", "compute", "."); err != nil {
		t.Fatalf("%s stats compute failed: %v\n%s", name, err, out)
	}
	viewOut, err := runIDUEnv(env, "stats", "view", "--info", latestStats)
	if err != nil {
		t.Fatalf("%s stats view failed: %v\n%s", name, err, viewOut)
	}
	// The stored rankings cannot deduplicate hardlinks when the summaries
	// do not record them, so a rescan is always used for rescan.
	expected := []string{"Usage by top 10 Subtrees", "Distributed subtree hotspots were not computed", fanoutRoot, "maintained by analyze"}
	if hardlinkDedup == "rescan" {
		expected = []string{"Usage by top 10 Subtrees", "Distributed Subtree Hotspots", fanoutRoot, "Source     : the summaries"}
	}
	if err := containsAnyOf(viewOut, expected...); err != nil {
		t.Fatalf("%s stats view missing expected output: %v\n%s", name, err, viewOut)
	}

	if out, err := runIDUEnv(env, "stats", "compute", "--scan", "."); err != nil {
		t.Fatalf("%s stats compute failed: %v\n%s", name, err, out)
	}
	viewOut, err = runIDUEnv(env, "stats", "view", latestStats)
	if err != nil {
		t.Fatalf("%s stats view failed: %v\n%s", name, err, viewOut)
	}
	if err := containsAnyOf(viewOut, "Usage by top 10 Subtrees", "Distributed Subtree Hotspots", fanoutRoot); err != nil {
		t.Fatalf("%s stats view missing expected subtree/distributed output: %v\n%s", name, err, viewOut)
	}

	if out, err := runIDUEnv(env, "reports", "generate", "--report-dir="+reportsDir, "--tsv=10", "--json=10", "--markdown=10", latestStats); err != nil {
		t.Fatalf("%s reports generate failed: %v\n%s", name, err, out)
	}

	latestReport := filepath.Join(reportsDir, "latest")
	for _, rel := range []string{"subtrees.tsv", "subtrees.json", "distributed.tsv", "distributed.json", "usage-summary.md"} {
		data, err := os.ReadFile(filepath.Join(latestReport, rel))
		if err != nil {
			t.Fatalf("%s missing report %s: %v", name, rel, err)
		}
		if !strings.Contains(string(data), reportPath(rel, fanoutRoot)) {
			t.Errorf("%s report %s does not mention fanout root %s:\n%s", name, rel, fanoutRoot, string(data))
		}
	}
}

// reportPath returns path as it appears in the report file rel, i.e.
// escaped as a JSON string, which matters for Windows paths, for json
// reports.
func reportPath(rel, path string) string {
	if filepath.Ext(rel) != ".json" {
		return path
	}
	quoted, _ := json.Marshal(path)
	return strings.Trim(string(quoted), `"`)
}

func TestDistributedSubtreeDetection(t *testing.T) {
	home := t.TempDir()
	confDir := filepath.Join(home, ".config", "idu")
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatal(err)
	}

	treeDir := filepath.Join(home, "tree")
	fanoutRoot := createFanoutFixtureTree(t, treeDir)

	// Summaries without hardlink deduplication (count_hardlinks_as_files: true)
	verifyDistributedMode(t, home, confDir, treeDir, fanoutRoot, "fast", true, "summaries")
	// Summaries with hardlink deduplication (hardlink_dedup: summaries)
	verifyDistributedMode(t, home, confDir, treeDir, fanoutRoot, "dedup", false, "summaries")
	// Rescan of all file metadata to deduplicate hardlinks (hardlink_dedup: rescan)
	verifyDistributedMode(t, home, confDir, treeDir, fanoutRoot, "rescan", false, "rescan")
}

// assertLogsRange verifies that logs --range selects the logs written by the
// preceding analyze of the mytree filesystem.
func assertLogsRange(t *testing.T, env []string) {
	t.Helper()
	for rng, want := range map[string]bool{
		"-1h..":        true,
		"..+1h":        true,
		"2020-01":      false,
		"..2020-01-01": false,
	} {
		out, err := runIDUEnv(env, "--fs=mytree", "logs", "--range="+rng)
		if err != nil {
			t.Fatalf("logs --range=%v: %v", rng, err)
		}
		if got := strings.Contains(out, `"begin"`); got != want {
			t.Errorf("logs --range=%v: got log entries %v, want %v: %q", rng, got, want, out)
		}
	}
	if out, err := runIDUEnv(env, "--fs=mytree", "logs", "--range=2026-11-05..2026-11-01"); err == nil {
		t.Errorf("expected an error for an invalid range, got %q", out)
	}
}

// TestPerCommandFS verifies that every command that operates on a
// configured filesystem accepts --fs, which overrides the global --fs.
func TestPerCommandFS(t *testing.T) {
	cfgFile, trees := writePerCommandFSConfig(t)
	run := func(args ...string) string {
		t.Helper()
		out, err := runIDU(append([]string{"--config=" + cfgFile}, args...)...)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	runErr := func(args ...string) string {
		t.Helper()
		out, err := runIDU(append([]string{"--config=" + cfgFile}, args...)...)
		if err == nil {
			t.Fatalf("%v: expected an error: %s", args, out)
		}
		return out
	}
	expect := func(out string, want, notWant string) {
		t.Helper()
		if !strings.Contains(out, want) || (notWant != "" && strings.Contains(out, notWant)) {
			t.Errorf("want %q but not %q: %s", want, notWant, out)
		}
	}

	// Without --fs the filesystem is ambiguous.
	expect(runErr("database", "locate"), "--fs", "")
	expect(run("database", "locate", "--fs=fb"), "db-fb", "")
	// The command's --fs overrides the global one.
	expect(run("--fs=fa", "database", "locate", "--fs=fb"), "db-fb", "")
	expect(run("--fs=fa", "database", "locate"), "db-fa", "")

	run("analyze", "--fs=fb", "--progress=false")
	expect(run("find", "--fs=fb", trees["fb"]), "file-fb", "file-fa")
	// fa has not been analyzed.
	runErr("find", "--fs=fa", trees["fa"])
	expect(run("logs", "--fs=fb", "--json=false"), "analyze", "")
	run("errors", "--fs=fb")
	run("errors", "--fs=fb", "--erase")
	run("stats", "compute", "--fs=fb", trees["fb"])
	expect(run("stats", "view", "--fs=fb"), "sub-fb", "")
	expect(run("stats", "suggest", "--fs=fb"), "tree-fb", "")
	run("logs", "--fs=fb", "--erase")
}

// writePerCommandFSConfig writes a configuration for two filesystems, fa
// and fb, and returns its filename and the root of each.
func writePerCommandFSConfig(t *testing.T) (string, map[string]string) {
	t.Helper()
	tmp := t.TempDir()
	trees := map[string]string{}
	var cfg strings.Builder
	cfg.WriteString("filesystems:\n")
	for _, name := range []string{"fa", "fb"} {
		root := filepath.Join(tmp, "tree-"+name)
		if err := os.MkdirAll(filepath.Join(root, "sub-"+name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "sub-"+name, "file-"+name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
		trees[name] = root
		fmt.Fprintf(&cfg, "  - name: %s\n    root: %s\n    database: %s\n    snapshot_dir: %s\n",
			name, root, filepath.Join(tmp, "db-"+name), filepath.Join(tmp, "stats-"+name))
	}
	cfgFile := filepath.Join(tmp, "config.yaml")
	if err := os.WriteFile(cfgFile, []byte(cfg.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return cfgFile, trees
}

// TestSnapshots verifies the snapshots written by analyze --snapshot and
// their use by stats view and reports generate.
func TestSnapshots(t *testing.T) {
	tmp := t.TempDir()
	root := filepath.Join(tmp, "tree")
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sub", "f"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshots := filepath.Join(tmp, "snapshots")
	writeConfig := func(name string, withSnapshots bool) string {
		text := fmt.Sprintf("filesystems:\n  - root: %s\n    database: %s\n", root, filepath.Join(tmp, "db-"+name))
		if withSnapshots {
			text += "    snapshot_dir: " + snapshots + "\n"
		}
		p := filepath.Join(tmp, name+".yaml")
		if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		return "--config=" + p
	}
	noSnapshots, withSnapshots := writeConfig("none", false), writeConfig("snapshots", true)
	// expect runs idu and checks that its output contains all of want.
	expect := func(want []string, args ...string) {
		t.Helper()
		out, err := runIDU(args...)
		if err != nil {
			t.Fatal(err)
		}
		if err := containsAnyOf(out, want...); err != nil {
			t.Error(err)
		}
	}
	const fromDB = "Snapshot   : none, computed from the database"

	// --snapshot fails immediately without snapshot_dir.
	if out, err := runIDU(noSnapshots, "analyze", "--progress=false", "--snapshot"); err == nil || !strings.Contains(out, "snapshot_dir") {
		t.Fatalf("expected an error: %v: %s", err, out)
	}
	if _, err := os.Stat(filepath.Join(tmp, "db-none")); !os.IsNotExist(err) {
		t.Errorf("database created: %v", err)
	}
	// Without snapshots, view uses the database directly.
	expect(nil, noSnapshots, "analyze", "--progress=false")
	expect([]string{fromDB}, noSnapshots, "stats", "view", "--info")

	expect([]string{"snapshot: " + snapshots}, withSnapshots, "analyze", "--progress=false", "--snapshot")
	target, err := os.Readlink(filepath.Join(snapshots, "latest"))
	if err != nil {
		t.Fatal(err)
	}
	latest := filepath.Join(snapshots, target)
	// The snapshot is more recent than the database and is used.
	expect([]string{"Snapshot   : " + latest, "Prefix     : " + root}, withSnapshots, "stats", "view", "--info")
	reports := filepath.Join(tmp, "reports")
	expect(nil, withSnapshots, "reports", "generate", "--report-dir="+reports)
	if _, err := os.Stat(filepath.Join(reports, "latest", "usage-summary.md")); err != nil {
		t.Errorf("reports: %v", err)
	}
	// Once the database is updated the snapshot is stale.
	expect(nil, withSnapshots, "analyze", "--progress=false")
	expect([]string{fromDB}, withSnapshots, "stats", "view", "--info")
	// A named snapshot is used regardless.
	expect([]string{"Snapshot   : " + latest}, withSnapshots, "stats", "view", "--info", filepath.Base(latest))
}

// TestViewHotspotsAndHardlinks verifies that stats view --hardlinks and
// --hotspots compute, and save, a full pass when the current stats lack
// them and reuse it thereafter.
func TestViewHotspotsAndHardlinks(t *testing.T) {
	tmp, root := clitest.NewHardlinkTree(t)
	snapshots := filepath.Join(tmp, "snapshots")
	cfgFile := filepath.Join(tmp, "config.yaml")
	text := fmt.Sprintf("filesystems:\n  - root: %s\n    database: %s\n    snapshot_dir: %s\n", root, filepath.Join(tmp, "db"), snapshots)
	if err := os.WriteFile(cfgFile, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	config := "--config=" + cfgFile
	run := func(args ...string) string {
		t.Helper()
		out, err := runIDU(append([]string{config}, args...)...)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	run("analyze", "--progress=false", "--snapshot")
	out := run("stats", "view", "--info", "--hardlinks")
	for _, want := range []string{
		"computing stats from the summaries: the hardlink rankings require a full pass",
		"Top 10 prefixes by number of hardlinks",
		filepath.Join(root, "ab"),
		"No files with links that analyze has not seen were found",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("--hardlinks: missing %q:\n%s", want, out)
		}
	}
	// The full pass was saved and includes the hotspots.
	target, err := os.Readlink(filepath.Join(snapshots, "latest"))
	if err != nil {
		t.Fatal(err)
	}
	out = run("stats", "view", "--info", "--hotspots")
	if !strings.Contains(out, "Snapshot   : "+filepath.Join(snapshots, target)) || strings.Contains(out, "computing stats") ||
		!strings.Contains(out, "Distributed Subtree Hotspots") {
		t.Errorf("--hotspots:\n%s", out)
	}
}
