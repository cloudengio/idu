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
		t.Fatalf("Readlink: %v", err)
	}
	if target != name {
		t.Errorf("symlink target = %q, want %q", target, name)
	}

	// Failure when directory does not exist.
	if _, _, err := createNamedLogfile("idu-test", filepath.Join(dir, "missing"), ts); err == nil {
		t.Fatal("expected error for non-existent log directory")
	}
}

func TestLogAndBadgerLogger(t *testing.T) {
	dir := t.TempDir()
	origDir := LogDir
	origVerbosity := Verbosity
	globalLogger.Lock()
	origLogger := globalLogger.Logger
	globalLogger.Logger = nil
	globalLogger.Unlock()
	t.Cleanup(func() {
		LogDir = origDir
		Verbosity = origVerbosity
		globalLogger.Lock()
		globalLogger.Logger = origLogger
		globalLogger.Unlock()
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

	linkPath := filepath.Join(dir, defaultLogName+".log")
	data, err := os.ReadFile(linkPath)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", linkPath, err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 7 {
		t.Fatalf("got %d log lines, want 7:\n%s", len(lines), string(data))
	}

	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("unmarshal log line: %v", err)
	}
	if rec[slog.LevelKey] != "PREFIX" || rec[slog.MessageKey] != "prefix-msg" {
		t.Errorf("unexpected first record: %v", rec)
	}
}
