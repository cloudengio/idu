// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/file/localfs"
)

func TestFindFS(t *testing.T) {
	ctx := context.Background()
	tmpDir, cfgFile, target, _ := setupAnalyze(t)
	defer os.RemoveAll(tmpDir)

	cfg, err := config.ReadConfig(cfgFile)
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
		_, pcfg, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, target, false)
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
