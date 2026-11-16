// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/file/localfs"
)

func countLogsAndErrors(ctx context.Context, t *testing.T, target string) (int, int) {
	t.Helper()
	_, _, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, target, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	var logCount, errCount int
	if err := db.VisitLogs(ctx, time.Time{}, time.Now().Add(time.Hour), func(context.Context, time.Time, time.Time, []byte) bool {
		logCount++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.VisitErrors(ctx, "", func(context.Context, string, time.Time, []byte) bool {
		errCount++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return logCount, errCount
}

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

	t.Run("logs erase clears logs and preserves errors", func(t *testing.T) {
		if err := ls.logs(ctx, &logFlags{Erase: true}, []string{target}); err != nil {
			t.Fatalf("logs erase: %v", err)
		}
		logCount, errCount := countLogsAndErrors(ctx, t, target)
		if logCount != 0 {
			t.Errorf("logs remaining after erase = %d, want 0", logCount)
		}
		if errCount == 0 {
			t.Errorf("expected errors to be preserved after logs --erase, got 0")
		}
	})

	t.Run("errors list and erase", func(t *testing.T) {
		if err := ls.errors(ctx, &errorFlags{}, []string{target}); err != nil {
			t.Fatalf("errors list: %v", err)
		}
		if err := ls.errors(ctx, &errorFlags{Erase: true}, []string{target}); err != nil {
			t.Fatalf("errors erase: %v", err)
		}
		if _, count := countLogsAndErrors(ctx, t, target); count != 0 {
			t.Errorf("errors remaining after erase = %d, want 0", count)
		}
		if err := ls.errors(ctx, &errorFlags{}, []string{"/unknown"}); err == nil {
			t.Fatal("expected error for unknown prefix")
		}
	})
}

func setupSiblingErrorsDB(ctx context.Context, t *testing.T) (targetA, subDir string) {
	t.Helper()
	tmpDir := t.TempDir()
	root := filepath.Join(tmpDir, "root")
	dbDir := filepath.Join(tmpDir, "db")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	cfgText := "filesystems:\n  - name: tree\n    prefix: " + root + "\n    database: " + dbDir + "\n"
	cfg, err := config.ParseConfig([]byte(cfgText))
	if err != nil {
		t.Fatal(err)
	}
	origCfg := globalConfig
	globalConfig = cfg
	t.Cleanup(func() { globalConfig = origCfg })
	setTempLogDir(t)

	_, _, _, db, err := internal.OpenPrefixAndDatabase(ctx, cfg, root, false)
	if err != nil {
		t.Fatal(err)
	}
	targetA = filepath.Join(root, "a")
	subDir = filepath.Join(targetA, "sub")
	now := time.Now().UTC()
	for _, e := range []struct {
		key string
		msg string
	}{
		{targetA, "err root a"},
		{filepath.Join(subDir, "f1"), "err sub f1"},
		{filepath.Join(subDir, "f2"), "err sub f2"},
		{filepath.Join(root, "a-b", "f3"), "err sibling dash"},
		{filepath.Join(root, "ab", "f4"), "err sibling ab"},
		{filepath.Join(root, "z", "f5"), "err tail z"},
	} {
		if err := db.LogError(ctx, e.key, now, []byte(e.msg)); err != nil {
			_ = db.Close(ctx)
			t.Fatal(err)
		}
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return targetA, subDir
}

func TestErrorsNoSiblingBleedAndPrefixFlag(t *testing.T) {
	ctx := context.Background()
	targetA, subDir := setupSiblingErrorsDB(ctx, t)

	ls := &lister{}
	out, err := captureStdout(t, func() error {
		return ls.errors(ctx, &errorFlags{}, []string{targetA})
	})
	if err != nil {
		t.Fatalf("errors: %v", err)
	}
	for _, want := range []string{"err root a", "err sub f1", "err sub f2"} {
		if !strings.Contains(out, want) {
			t.Errorf("errors output missing %q:\n%s", want, out)
		}
	}
	for _, forbid := range []string{"err sibling dash", "err sibling ab", "err tail z"} {
		if strings.Contains(out, forbid) {
			t.Errorf("errors output leaked %q:\n%s", forbid, out)
		}
	}

	prefOut, err := captureStdout(t, func() error {
		return ls.errors(ctx, &errorFlags{Prefix: true}, []string{targetA})
	})
	if err != nil {
		t.Fatalf("errors --prefix: %v", err)
	}
	if count := strings.Count(prefOut, subDir+":\n"); count != 1 {
		t.Errorf("expected prefix header %q once in --prefix output, got %d:\n%s", subDir+":", count, prefOut)
	}
	if strings.Contains(prefOut, "err sibling dash") || strings.Contains(prefOut, "err sibling ab") {
		t.Errorf("errors --prefix leaked sibling:\n%s", prefOut)
	}
}
