// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func readLogRecords(t *testing.T, filename string) []map[string]any {
	t.Helper()
	f, err := os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var recs []map[string]any
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("%v: %q: %v", filename, sc.Text(), err)
		}
		recs = append(recs, rec)
	}
	return recs
}

func TestLoggingConfigAndFlagOverrides(t *testing.T) {
	tmpDir := t.TempDir()
	tree := filepath.Join(tmpDir, "tree")
	if err := os.MkdirAll(filepath.Join(tree, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tree, "a", "f"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfgLog := filepath.Join(tmpDir, "config.log")
	cfgFile := filepath.Join(tmpDir, "config.yaml")
	cfg := fmt.Sprintf(`logging:
  level: 3
  file: %q
  format: json
filesystems:
  - name: tree
    prefix: %q
    database: %q
`, cfgLog, tree, filepath.Join(tmpDir, "db"))
	if err := os.WriteFile(cfgFile, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	// Logging as configured in the config file.
	if _, err := runIDU("--config="+cfgFile, "analyze", "--progress=false"); err != nil {
		t.Fatal(err)
	}
	var prefixDone, badger bool
	for _, rec := range readLogRecords(t, cfgLog) {
		if rec["msg"] == "prefix done" {
			prefixDone = true
		}
		if rec["component"] == "badger" {
			badger = true
		}
	}
	if !prefixDone || !badger {
		t.Errorf("expected debug and badger log records: prefix done: %v, badger: %v", prefixDone, badger)
	}

	// The flags override the config file.
	flagLog := filepath.Join(tmpDir, "flag.log")
	if err := os.Remove(cfgLog); err != nil {
		t.Fatal(err)
	}
	if _, err := runIDU("--config="+cfgFile, "--log-level=0", "--log-file="+flagLog, "analyze", "--progress=false"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cfgLog); !os.IsNotExist(err) {
		t.Errorf("%v should not have been created: %v", cfgLog, err)
	}
	for _, rec := range readLogRecords(t, flagLog) {
		if rec["level"] != "ERROR" {
			t.Errorf("unexpected log record at level %v: %v", rec["level"], rec)
		}
	}
}

func hasMsg(recs []map[string]any, msg string) bool {
	for _, rec := range recs {
		if rec["msg"] == msg {
			return true
		}
	}
	return false
}

func TestPerFilesystemLogging(t *testing.T) {
	tmpDir := t.TempDir()
	for _, d := range []string{"one", "two"} {
		if err := os.MkdirAll(filepath.Join(tmpDir, d, "a"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	globalLog := filepath.Join(tmpDir, "global.log")
	oneLog := filepath.Join(tmpDir, "one.log")
	cfgFile := filepath.Join(tmpDir, "config.yaml")
	cfg := fmt.Sprintf(`logging:
  level: 3
  file: %q
filesystems:
  - name: one
    prefix: %q
    database: %q
    logging:
      level: 3
      file: %q
  - name: two
    prefix: %q
    database: %q
`, globalLog,
		filepath.Join(tmpDir, "one"), filepath.Join(tmpDir, "db-one"), oneLog,
		filepath.Join(tmpDir, "two"), filepath.Join(tmpDir, "db-two"))
	if err := os.WriteFile(cfgFile, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	// The filesystem's own logging configuration is used for one.
	if _, err := runIDU("--config="+cfgFile, "--fs=one", "analyze", "--progress=false"); err != nil {
		t.Fatal(err)
	}
	if !hasMsg(readLogRecords(t, oneLog), "prefix done") {
		t.Errorf("%v: missing records for filesystem one", oneLog)
	}
	if hasMsg(readLogRecords(t, globalLog), "prefix done") {
		t.Errorf("%v: should not contain records for filesystem one", globalLog)
	}

	// The top-level configuration is used for two.
	if _, err := runIDU("--config="+cfgFile, "--fs=two", "analyze", "--progress=false"); err != nil {
		t.Fatal(err)
	}
	if !hasMsg(readLogRecords(t, globalLog), "prefix done") {
		t.Errorf("%v: missing records for filesystem two", globalLog)
	}

	// Flags override the filesystem's configuration.
	flagLog := filepath.Join(tmpDir, "flag.log")
	if err := os.Remove(oneLog); err != nil {
		t.Fatal(err)
	}
	if _, err := runIDU("--config="+cfgFile, "--fs=one", "--log-file="+flagLog, "analyze", "--progress=false"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oneLog); !os.IsNotExist(err) {
		t.Errorf("%v should not have been created: %v", oneLog, err)
	}
	if !hasMsg(readLogRecords(t, flagLog), "prefix done") {
		t.Errorf("%v: missing records for filesystem one", flagLog)
	}
}
