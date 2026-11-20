// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package analyze

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/file/localfs"
)

func countLogsAndErrors(ctx context.Context, t *testing.T, target string) (int, int) {
	t.Helper()
	_, _, _, db, err := openPrefixReadOnly(ctx, testEnv.Config, target)
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
	tmpDir, cfgFile, target, _ := clitest.SetupAnalyze(t)
	defer os.RemoveAll(tmpDir)

	cfg, err := iduconfig.ReadConfig(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	origCfg := testEnv.Config
	testEnv.Config = cfg
	t.Cleanup(func() {
		testEnv.Config = origCfg
	})

	lfs := localfs.New()
	alz := newTestCommands()
	if err := alz.analyzeFS(ctx, lfs, &Flags{}, []string{target}); err != nil {
		t.Fatalf("analyzeFS: %v", err)
	}

	ls := newTestCommands()
	t.Run("logs json and plain", func(t *testing.T) {
		if err := ls.Logs(ctx, &LogFlags{JSON: true}, []string{target}); err != nil {
			t.Fatalf("logs json: %v", err)
		}
		if err := ls.Logs(ctx, &LogFlags{JSON: false}, []string{"testtree"}); err != nil {
			t.Fatalf("logs plain: %v", err)
		}
		runLogsWithRanges(ctx, t, ls, target, "-1h..", "..+1h", "-1d..+1d", "2020-01-01..")
		if err := ls.Logs(ctx, &LogFlags{}, []string{"/unknown"}); err == nil {
			t.Fatal("expected error for unknown prefix")
		}
	})

	t.Run("logs erase clears logs and preserves errors", func(t *testing.T) {
		if err := ls.Logs(ctx, &LogFlags{Erase: true}, []string{target}); err != nil {
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
		if err := ls.Errors(ctx, &ErrorFlags{}, []string{target}); err != nil {
			t.Fatalf("errors list: %v", err)
		}
		if err := ls.Errors(ctx, &ErrorFlags{Erase: true}, []string{target}); err != nil {
			t.Fatalf("errors erase: %v", err)
		}
		if _, count := countLogsAndErrors(ctx, t, target); count != 0 {
			t.Errorf("errors remaining after erase = %d, want 0", count)
		}
		if err := ls.Errors(ctx, &ErrorFlags{}, []string{"/unknown"}); err == nil {
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
	cfg, err := iduconfig.ParseConfig([]byte(cfgText))
	if err != nil {
		t.Fatal(err)
	}
	origCfg := testEnv.Config
	testEnv.Config = cfg
	t.Cleanup(func() { testEnv.Config = origCfg })

	_, _, _, db, err := clitest.OpenPrefixRaw(ctx, testEnv, root)
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

	ls := newTestCommands()
	out, err := clitest.CaptureStdout(t, func() error {
		return ls.Errors(ctx, &ErrorFlags{}, []string{targetA})
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

	prefOut, err := clitest.CaptureStdout(t, func() error {
		return ls.Errors(ctx, &ErrorFlags{Prefix: true}, []string{targetA})
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

func TestLogTimeRange(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.Local)
	parse := func(s string) *flags.TimeInterval {
		t.Helper()
		var iv flags.TimeInterval
		if s == "" {
			return &iv
		}
		if err := iv.Set(s); err != nil {
			t.Fatalf("Set(%q): %v", s, err)
		}
		return &iv
	}
	day := func(y int, m time.Month, d int) time.Time {
		return time.Date(y, m, d, 0, 0, 0, 0, time.Local)
	}
	for _, tc := range []struct {
		in       string
		from, to time.Time
	}{
		{"", time.Time{}, now},
		{"2026-11-01..", day(2026, 11, 1), now},
		{"..2026-11-05", time.Time{}, day(2026, 11, 6).Add(-time.Nanosecond)},
		{"2026-11-01..2026-11-05", day(2026, 11, 1), day(2026, 11, 6).Add(-time.Nanosecond)},
		{"2026-11", day(2026, 11, 1), day(2026, 12, 1).Add(-time.Nanosecond)},
	} {
		from, to := logTimeRange(parse(tc.in), now)
		if !from.Equal(tc.from) || !to.Equal(tc.to) {
			t.Errorf("%q: got [%v, %v], want [%v, %v]", tc.in, from, to, tc.from, tc.to)
		}
	}

	// Relative times are relative to the current time.
	before := time.Now()
	from, to := logTimeRange(parse("-1h.."), now)
	if d := before.Add(-time.Hour).Sub(from); d < -time.Minute || d > time.Minute {
		t.Errorf("-1h..: from %v is not about an hour ago", from)
	}
	if !to.Equal(now) {
		t.Errorf("-1h..: got to %v, want %v", to, now)
	}

	var iv flags.TimeInterval
	if err := iv.Set("2026-11-05..2026-11-01"); err == nil {
		t.Errorf("expected an error for an interval that ends before it starts")
	}
}

func runLogsWithRanges(ctx context.Context, t *testing.T, ls *Commands, target string, ranges ...string) {
	t.Helper()
	for _, rng := range ranges {
		lf := &LogFlags{JSON: true}
		if err := lf.Range.Set(rng); err != nil {
			t.Fatalf("--range=%v: %v", rng, err)
		}
		if err := ls.Logs(ctx, lf, []string{target}); err != nil {
			t.Fatalf("logs --range=%v: %v", rng, err)
		}
	}
}
