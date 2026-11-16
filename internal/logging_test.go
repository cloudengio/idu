// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestLogNameAndCreateNamedLogfile(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	name, link := logName("idu-test", ts)
	if !strings.HasPrefix(name, "idu-test.log.20260304-050607.") {
		t.Errorf("logName = %q, unexpected format", name)
	}
	if link != "idu-test.log" {
		t.Errorf("link = %q, want idu-test.log", link)
	}

	dir := t.TempDir()
	f, fname, err := createNamedLogfile("idu-test", dir, ts)
	if err != nil {
		t.Fatalf("createNamedLogfile: %v", err)
	}
	defer f.Close()

	if filepath.Dir(fname) != dir {
		t.Errorf("fname = %q, want in %q", fname, dir)
	}
	target, err := os.Readlink(filepath.Join(dir, link))
	if err != nil {
		if runtime.GOOS != "windows" {
			t.Fatalf("Readlink: %v", err)
		}
	} else if target != name {
		t.Errorf("symlink target = %q, want %q", target, name)
	}

	// Failure when directory does not exist.
	if _, _, err := createNamedLogfile("idu-test", filepath.Join(dir, "missing"), ts); err == nil {
		t.Fatal("expected error for non-existent log directory")
	}
}

func TestLogAndBadgerLogger(t *testing.T) {
	// globalLogger retains the open log file handle for the process lifetime,
	// so avoid t.TempDir() here which fails cleanup on Windows.
	dir, err := os.MkdirTemp("", "idu-logtest")
	if err != nil {
		t.Fatal(err)
	}
	origDir := LogDir
	origVerbosity := Verbosity
	globalLogger.Lock()
	origLoggers := globalLogger.loggers
	globalLogger.loggers = map[string]*slog.Logger{}
	globalLogger.Unlock()
	t.Cleanup(func() {
		LogDir = origDir
		Verbosity = origVerbosity
		globalLogger.Lock()
		globalLogger.loggers = origLoggers
		globalLogger.Unlock()
		_ = os.RemoveAll(dir)
	})

	LogDir = dir
	Verbosity = slog.LevelDebug // lowest level so all messages are logged
	ctx := context.Background()

	Log(ctx, LogPrefix, "prefix-msg", "k", "v")
	Log(ctx, LogProgress, "progress-msg", "n", 1)
	Log(ctx, LogError, "error-msg", "err", "boom")

	bl := &badgerLogger{}
	bl.Errorf("err %d", 1)
	bl.Warningf("warn %d", 2)
	bl.Infof("info %d", 3)
	bl.Debugf("debug %d", 4)

	// Messages below Verbosity should be suppressed.
	Verbosity = LogError
	Log(ctx, LogProgress, "suppressed-progress")
	bl.Warningf("suppressed-warn")
	bl.Infof("suppressed-info")
	bl.Debugf("suppressed-debug")

	matches, err := filepath.Glob(filepath.Join(dir, defaultLogName+".log.*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("Glob log files = (%v, %v), want 1 file", matches, err)
	}
	data, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", matches[0], err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d default log lines, want 3:\n%s", len(lines), string(data))
	}

	bmatches, err := filepath.Glob(filepath.Join(dir, badgerLogName+".log.*"))
	if err != nil || len(bmatches) != 1 {
		t.Fatalf("Glob badger log files = (%v, %v), want 1 file", bmatches, err)
	}
	bdata, err := os.ReadFile(bmatches[0])
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", bmatches[0], err)
	}
	blines := strings.Split(strings.TrimSpace(string(bdata)), "\n")
	if len(blines) != 4 {
		t.Fatalf("got %d badger log lines, want 4:\n%s", len(blines), string(bdata))
	}

	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("unmarshal log line: %v", err)
	}
	if rec[slog.LevelKey] != "PREFIX" || rec[slog.MessageKey] != "prefix-msg" {
		t.Errorf("unexpected first record: %v", rec)
	}

	// newLogger(nil) must fall back to stderr cleanly instead of panicking on nil *os.File.
	nilLogger := newLogger(nil)
	if err := nilLogger.Handler().Handle(ctx, slog.NewRecord(time.Now(), LogError, "stderr-fallback", 0)); err != nil {
		t.Errorf("newLogger(nil) Handle error = %v", err)
	}
}
