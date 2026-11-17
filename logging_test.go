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
	if _, err := runIDU("--config="+cfgFile, "analyze", "--progress=false", "tree"); err != nil {
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
	if _, err := runIDU("--config="+cfgFile, "--log-level=0", "--log-file="+flagLog, "analyze", "--progress=false", "tree"); err != nil {
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
