// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/file/localfs"
)

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	fnErr := fn()
	_ = w.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	_ = r.Close()
	return buf.String(), fnErr
}

func TestFindFS(t *testing.T) {
	ctx := context.Background()
	tmpDir, cfgFile, target, _ := setupAnalyze(t)
	defer os.RemoveAll(tmpDir)

	cfg, err := iduconfig.ReadConfig(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	origCfg := globalConfig
	globalConfig = cfg
	t.Cleanup(func() {
		globalConfig = origCfg
	})
	setTempLogDir(t)

	lfs := localfs.New()
	alz := &analyzeCmd{}
	if err := alz.analyzeFS(ctx, lfs, &analyzeFlags{}, []string{target}); err != nil {
		t.Fatalf("analyzeFS: %v", err)
	}

	fc := &findCmds{}
	for _, tc := range []struct {
		name  string
		flags findFlags
		args  []string
	}{
		{name: "match file by name", flags: findFlags{Long: false}, args: []string{target, "name=f00-00"}},
		{name: "long listing", flags: findFlags{Long: true}, args: []string{"testtree", "type=f && name=f00-00"}},
		{name: "empty expression matches all", flags: findFlags{Long: false}, args: []string{target}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := fc.findFS(ctx, lfs, &tc.flags, tc.args); err != nil {
				t.Fatalf("findFS: %v", err)
			}
		})
	}

	t.Run("invalid expression", func(t *testing.T) {
		if err := fc.findFS(ctx, lfs, &findFlags{}, []string{target, "bad_op=1"}); err == nil {
			t.Fatal("expected error for invalid expression")
		}
	})

	t.Run("unknown prefix", func(t *testing.T) {
		if err := fc.findFS(ctx, lfs, &findFlags{}, []string{"/unknown/prefix", "name=foo"}); err == nil {
			t.Fatal("expected error for unknown prefix")
		}
	})

	t.Run("corrupt prefix entry returns error", func(t *testing.T) {
		_, pcfg, _, db, err := openPrefixRaw(ctx, globalConfig, target)
		if err != nil {
			t.Fatal(err)
		}
		_ = pcfg
		if err := db.Set(ctx, filepath.Join(target, "corrupt"), []byte{0xff}, false); err != nil {
			_ = db.Close(ctx)
			t.Fatal(err)
		}
		_ = db.Close(ctx)

		if err := fc.findFS(ctx, lfs, &findFlags{}, []string{target}); err == nil {
			t.Fatal("expected error on corrupt prefix entry")
		}
	})
}

func TestFindNoSiblingBleedAndPrefixFlag(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	root := filepath.Join(tmpDir, "root")
	files := []string{
		filepath.Join(root, "a", "inside.txt"),
		filepath.Join(root, "a", "sub", "deep.txt"),
		filepath.Join(root, "a-b", "sibling_dash.txt"),
		filepath.Join(root, "ab", "sibling_ab.txt"),
		filepath.Join(root, "b", "other.txt"),
	}
	for _, f := range files {
		if err := os.MkdirAll(filepath.Dir(f), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	dbDir := filepath.Join(tmpDir, "db")
	cfgText := "filesystems:\n  - name: tree\n    prefix: " + root + "\n    database: " + dbDir + "\n"
	cfg, err := iduconfig.ParseConfig([]byte(cfgText))
	if err != nil {
		t.Fatal(err)
	}
	origCfg := globalConfig
	globalConfig = cfg
	t.Cleanup(func() { globalConfig = origCfg })
	setTempLogDir(t)

	lfs := localfs.New()
	if err := (&analyzeCmd{}).analyzeFS(ctx, lfs, &analyzeFlags{}, []string{root}); err != nil {
		t.Fatalf("analyzeFS: %v", err)
	}

	targetA := filepath.Join(root, "a")
	fc := &findCmds{}

	out, err := captureStdout(t, func() error {
		return fc.findFS(ctx, lfs, &findFlags{}, []string{targetA})
	})
	if err != nil {
		t.Fatalf("findFS: %v", err)
	}
	for _, want := range []string{"inside.txt", "deep.txt"} {
		if !strings.Contains(out, want) {
			t.Errorf("findFS output missing %q:\n%s", want, out)
		}
	}
	for _, forbid := range []string{"sibling_dash.txt", "sibling_ab.txt", "other.txt"} {
		if strings.Contains(out, forbid) {
			t.Errorf("findFS leaked sibling %q:\n%s", forbid, out)
		}
	}

	prefOut, err := captureStdout(t, func() error {
		return fc.findFS(ctx, lfs, &findFlags{Prefix: flags.Repeating{Values: []string{"name=sub"}}}, []string{targetA})
	})
	if err != nil {
		t.Fatalf("findFS --prefix: %v", err)
	}
	if !strings.Contains(prefOut, "deep.txt") || strings.Contains(prefOut, "inside.txt") {
		t.Errorf("findFS --prefix name=sub output unexpected:\n%s", prefOut)
	}

	if err := fc.findFS(ctx, lfs, &findFlags{Prefix: flags.Repeating{Values: []string{"bad_op=1"}}}, []string{targetA}); err == nil {
		t.Fatal("expected error for invalid --prefix expression")
	}
}
