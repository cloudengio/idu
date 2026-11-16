// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/file/localfs"
)

func TestListerLogsAndErrors(t *testing.T) {
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

	ls := &lister{}
	t.Run("logs json and plain", func(t *testing.T) {
		if err := ls.logs(ctx, &logFlags{JSON: true}, []string{target}); err != nil {
			t.Fatalf("logs json: %v", err)
		}
		if err := ls.logs(ctx, &logFlags{JSON: false}, []string{"testtree"}); err != nil {
			t.Fatalf("logs plain: %v", err)
		}
		lf := &logFlags{
			TimeRangeFlags: internal.TimeRangeFlags{Since: time.Hour},
			JSON:           true,
		}
		if err := ls.logs(ctx, lf, []string{target}); err != nil {
			t.Fatalf("logs since: %v", err)
		}
		if err := ls.logs(ctx, &logFlags{}, []string{"/unknown"}); err == nil {
			t.Fatal("expected error for unknown prefix")
		}
	})

	t.Run("errors list and erase", func(t *testing.T) {
		if err := ls.errors(ctx, &errorFlags{}, []string{target}); err != nil {
			t.Fatalf("errors list: %v", err)
		}
		if err := ls.errors(ctx, &errorFlags{Erase: true}, []string{target}); err != nil {
			t.Fatalf("errors erase: %v", err)
		}
		_, _, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, target, true)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close(ctx)
		var count int
		if err := db.VisitErrors(ctx, "", func(context.Context, string, time.Time, []byte) bool {
			count++
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Errorf("errors remaining after erase = %d, want 0", count)
		}
		if err := ls.errors(ctx, &errorFlags{}, []string{"/unknown"}); err == nil {
			t.Fatal("expected error for unknown prefix")
		}
	})
}
