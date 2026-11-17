// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb/backends/badgerdb"
	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/tui"
)

func TestBrowseSummarySource(t *testing.T) {
	home := t.TempDir()
	confDir := filepath.Join(home, ".config", "idu")
	treeDir := filepath.Join(t.TempDir(), "tree")
	files := map[string]int{
		"top.txt":       500,
		"a/f":           1000,
		"b/x/f":         2000,
		"b/y/z/f":       3000,
		"b/y/z/another": 4000,
	}
	for name, size := range files {
		path := filepath.Join(treeDir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfgYAML := fmt.Sprintf("filesystems:\n  - name: tree\n    prefix: %s\n    database: %s\n",
		treeDir, filepath.Join(home, "db"))
	if err := os.MkdirAll(confDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(confDir, "config.yaml"), []byte(cfgYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(), "HOME="+home, "APPDATA="+filepath.Join(home, ".config"), "IDU_CONFIG=", "IDU_ENV=")
	if out, err := runIDUEnv(env, "analyze", "--progress=false", "tree"); err != nil {
		t.Fatalf("analyze: %v: %s", err, out)
	}

	all, err := iduconfig.ParseConfig([]byte(cfgYAML))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cfg, target, err := internal.LookupPrefix(context.Background(), all, "tree")
	if err != nil {
		t.Fatal(err)
	}
	db, err := badgerdb.Open(cfg.Database, badgerdb.ReadOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	src := tui.NewSummarySource(db, &cfg)

	// Bytes include the size of each directory itself, which varies by
	// filesystem, so the expected values below are for files only and
	// directory sizes are added using Lstat.
	dirBytes := func(path string, recursive bool) int64 {
		t.Helper()
		var n int64
		err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				return nil
			}
			fi, err := d.Info()
			if err != nil {
				return err
			}
			n += fi.Size()
			if !recursive {
				return filepath.SkipAll
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	type want struct {
		name                        string
		treeFileBytes, ownFileBytes int64
		treeFiles, dirs             int64
	}
	check := func(e tui.Entry, w want) {
		t.Helper()
		treeBytes := w.treeFileBytes + dirBytes(e.Path, true)
		ownBytes := w.ownFileBytes + dirBytes(e.Path, false)
		if e.Name != w.name || e.Tree.Bytes != treeBytes || e.Own.Bytes != ownBytes ||
			e.Tree.Files != w.treeFiles || e.Dirs != w.dirs {
			t.Errorf("got %v: tree %v bytes, own %v bytes, %v files, %v dirs; want tree %v bytes, own %v bytes, %+v",
				e.Name, e.Tree.Bytes, e.Own.Bytes, e.Tree.Files, e.Dirs, treeBytes, ownBytes, w)
		}
	}

	root, err := src.List(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	check(root.Self, want{target, 10500, 500, 5, 5})
	if got, want := len(root.Children), 2; got != want {
		t.Fatalf("got %v children, want %v", got, want)
	}
	check(root.Children[0], want{"a", 1000, 1000, 1, 0})
	check(root.Children[1], want{"b", 9000, 0, 3, 3})

	b, err := src.List(ctx, root.Children[1].Path)
	if err != nil {
		t.Fatal(err)
	}
	check(b.Self, want{filepath.Join(target, "b"), 9000, 0, 3, 3})
	if got, want := len(b.Children), 2; got != want {
		t.Fatalf("got %v children, want %v", got, want)
	}
	check(b.Children[0], want{"x", 2000, 2000, 1, 0})
	check(b.Children[1], want{"y", 7000, 0, 2, 1})
}
