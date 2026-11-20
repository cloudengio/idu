// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package clitest

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"cloudeng.io/cmd/idu/iduconfig"
)

// CreateHardlinkTree creates a tree with hard links to files in sibling
// directories whose names share a string prefix (a, a-b, ab).
func CreateHardlinkTree(t *testing.T, root string) {
	t.Helper()
	for _, d := range []string{"a", "ab", "a-b", "b/c"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, size := range map[string]int{"a/f1": 1000, "a/f2": 500, "ab/g": 300, "b/h": 200} {
		if err := os.WriteFile(filepath.Join(root, name), make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for link, target := range map[string]string{"ab/l1": "a/f1", "a-b/l2": "a/f1", "b/c/l3": "a/f2"} {
		if err := os.Link(filepath.Join(root, target), filepath.Join(root, link)); err != nil {
			t.Fatal(err)
		}
	}
}

// WriteHardlinkConfig writes a configuration for root, with its database
// in dir, that uses the hardlink deduplication strategy dedup.
func WriteHardlinkConfig(t *testing.T, dir, root string, dedup iduconfig.HardlinkDedup) iduconfig.T {
	t.Helper()
	cfgFile := filepath.Join(dir, fmt.Sprintf("config-%v.yaml", dedup))
	yaml := fmt.Sprintf("filesystems:\n  - prefix: %s\n    database: %s\n    count_hardlinks_as_files: false\n    hardlink_dedup: %s\n",
		root, filepath.Join(dir, "db"), dedup)
	if err := os.WriteFile(cfgFile, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := iduconfig.ReadConfig(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// NewHardlinkTree creates the tree created by CreateHardlinkTree in a new
// temporary directory, returning the directory and the root of the tree.
func NewHardlinkTree(t *testing.T) (tmpDir, root string) {
	t.Helper()
	tmpDir = t.TempDir()
	root, err := filepath.EvalSymlinks(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	root = filepath.Join(root, "tree")
	CreateHardlinkTree(t, root)
	return tmpDir, root
}

// LinkUnchanged creates a hardlink in a, which is therefore rescanned, to
// b/h, a file in b, which is not rescanned since it is unchanged, and which
// had a link count of 1 when last scanned. a/hl precedes b/h and is
// counted, b/h is now a hardlink.
func LinkUnchanged(t *testing.T, root string) {
	t.Helper()
	if err := os.Link(filepath.Join(root, "b", "h"), filepath.Join(root, "a", "hl")); err != nil {
		t.Fatal(err)
	}
}
