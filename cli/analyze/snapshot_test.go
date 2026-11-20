// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package analyze

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
)

// setupSnapshotTest creates an empty root and returns the path of its
// database and a function that configures testEnv for it, with
// snapshot_dir set to snapshotDir if it is not empty.
func setupSnapshotTest(t *testing.T) (string, func(snapshotDir string)) {
	t.Helper()
	tmpDir := t.TempDir()
	root := filepath.Join(tmpDir, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(tmpDir, "db")
	return db, func(snapshotDir string) {
		t.Helper()
		text := fmt.Sprintf("filesystems:\n  - root: %s\n    database: %s\n", root, db)
		if snapshotDir != "" {
			text += "    snapshot_dir: " + snapshotDir + "\n"
		}
		cfg, err := iduconfig.ParseConfig([]byte(text))
		if err != nil {
			t.Fatal(err)
		}
		clitest.WithConfig(t, testEnv, cfg)
	}
}

func TestAnalyzeSnapshotPreconditions(t *testing.T) {
	ctx := context.Background()
	db, setConfig := setupSnapshotTest(t)
	snapshot := func(context.Context, iduconfig.Filesystem) (string, error) {
		return "", errors.New("unexpected snapshot")
	}
	// Without snapshot_dir --snapshot fails before the analysis starts.
	setConfig("")
	err := New(testEnv, WithSnapshot(snapshot)).Analyze(ctx, &Flags{Snapshot: true}, nil)
	if err == nil || !strings.Contains(err.Error(), "snapshot_dir") {
		t.Fatalf("got %v", err)
	}
	if _, err := os.Stat(db); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("database was created: %v", err)
	}
	// Without a SnapshotFunc --snapshot is not supported.
	setConfig(filepath.Join(t.TempDir(), "snapshots"))
	if err := New(testEnv).Analyze(ctx, &Flags{Snapshot: true}, nil); err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("got %v", err)
	}
	if _, err := os.Stat(db); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("database was created: %v", err)
	}
}

func TestAnalyzeSnapshot(t *testing.T) {
	ctx := context.Background()
	db, setConfig := setupSnapshotTest(t)
	setConfig(filepath.Join(t.TempDir(), "snapshots"))
	var snapshots []string
	snapshot := func(_ context.Context, f iduconfig.Filesystem) (string, error) {
		if _, err := os.Stat(db); err != nil {
			return "", fmt.Errorf("snapshot taken before the database was written: %w", err)
		}
		snapshots = append(snapshots, f.Location())
		return filepath.Join(f.Config().SnapshotDir, "snap"), nil
	}
	alz := New(testEnv, WithProgress(newCountingProgress), WithSnapshot(snapshot))

	// No snapshot is taken unless requested.
	if err := alz.Analyze(ctx, &Flags{}, nil); err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 0 {
		t.Errorf("unexpected snapshots: %v", snapshots)
	}
	if err := alz.Analyze(ctx, &Flags{Snapshot: true}, nil); err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0] != testEnv.Config.Filesystems[0].Location() {
		t.Errorf("got %v", snapshots)
	}

	// A failure to write the snapshot is reported.
	failing := New(testEnv, WithSnapshot(func(context.Context, iduconfig.Filesystem) (string, error) {
		return "", errors.New("oops")
	}))
	if err := failing.Analyze(ctx, &Flags{Snapshot: true}, nil); err == nil || !strings.Contains(err.Error(), "oops") {
		t.Errorf("got %v", err)
	}
}
