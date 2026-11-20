// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package snapshot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
)

func writeSnapshotConfig(t *testing.T, dir, root, snapshotDir string) iduconfig.T {
	t.Helper()
	text := fmt.Sprintf("filesystems:\n  - root: %s\n    database: %s\n    snapshot_dir: %s\n    topn: 50\n",
		root, filepath.Join(dir, "db"), snapshotDir)
	cfg, err := iduconfig.ParseConfig([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func numSnapshots(t *testing.T, dir string) int {
	t.Helper()
	entries, err := NewStore(dir).dir.Entries()
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

// checkCurrent checks the snapshot, sf, and its path, returned by Current.
// wantPath is empty if no snapshot is expected, new if a new one is
// expected and otherwise the path of the expected snapshot.
func checkCurrent(t *testing.T, sf File, path, root, wantSource, wantPath string) {
	t.Helper()
	if sf.Source != wantSource || !sf.Whole || sf.Prefix != root || sf.Stats.Prefix.TotalFiles != 4 {
		t.Errorf("got source %q whole %v prefix %v files %v, want %q", sf.Source, sf.Whole, sf.Prefix, sf.Stats.Prefix.TotalFiles, wantSource)
	}
	switch wantPath {
	case "":
		if path != "" {
			t.Errorf("got path %q, want none", path)
		}
	case "new":
		if path == "" {
			t.Errorf("expected a new snapshot")
		}
	default:
		if path != wantPath {
			t.Errorf("got path %q, want %q", path, wantPath)
		}
	}
}

func TestCurrent(t *testing.T) {
	ctx := context.Background()
	tmpDir, root := clitest.NewHardlinkTree(t)
	dir := filepath.Join(tmpDir, "snapshots")
	cfg := writeSnapshotConfig(t, tmpDir, root, dir)
	f := cfg.Filesystems[0]
	analyzeWithConfig(ctx, t, cfg, root)
	c := Computer{}

	current := func(dir string, n int, wantSource, wantPath string) string {
		t.Helper()
		sf, path, err := c.Current(ctx, testEnv, f, dir, n)
		if err != nil {
			t.Fatal(err)
		}
		checkCurrent(t, sf, path, root, wantSource, wantPath)
		return path
	}

	// The stored state is used, without writing a snapshot.
	current(dir, 10, FromStoredSource, "")
	if n := numSnapshots(t, dir); n != 0 {
		t.Errorf("got %v snapshots, want 0", n)
	}
	// More entries than topn require a full pass, which is saved.
	saved := current(dir, 100, FromSummariesSource, "new")
	if n := numSnapshots(t, dir); n != 1 {
		t.Errorf("got %v snapshots, want 1", n)
	}
	if _, err := os.Stat(NewStore(dir).LatestPath()); err != nil {
		t.Errorf("latest: %v", err)
	}
	// The snapshot is more recent than the database and hence is used.
	current(dir, 10, FromSummariesSource, saved)

	// Once the database has been updated the snapshot is stale.
	time.Sleep(25 * time.Millisecond)
	analyzeWithConfig(ctx, t, cfg, root)
	current(dir, 10, FromStoredSource, "")
	// Without a snapshot directory a full pass is not saved.
	current("", 100, FromSummariesSource, "")
	if n := numSnapshots(t, dir); n != 1 {
		t.Errorf("got %v snapshots, want 1", n)
	}

	// Take always writes a snapshot, from the stored state if usable.
	taken, err := c.Take(ctx, testEnv, f, dir)
	if err != nil {
		t.Fatal(err)
	}
	if n := numSnapshots(t, dir); n != 2 {
		t.Errorf("got %v snapshots, want 2", n)
	}
	current(dir, 10, FromStoredSource, taken)
}

func TestFreshFor(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	since := time.Now()
	write := func(prefix string, whole bool, when time.Time) string {
		t.Helper()
		path, err := s.Save(File{Prefix: prefix, Whole: whole, Date: when})
		if err != nil {
			t.Fatal(err)
		}
		return path
	}
	write("/root", true, since.Add(-time.Hour))
	if _, _, ok, err := s.FreshFor("/", "/root", since, nil); ok || err != nil {
		t.Errorf("stale snapshot used: %v %v", ok, err)
	}
	want := write("/root", true, since.Add(time.Hour))
	// Snapshots that are not of the entire filesystem, or are for another
	// root, are ignored.
	write("/root", false, since.Add(2*time.Hour))
	write("/other", true, since.Add(3*time.Hour))
	sf, path, ok, err := s.FreshFor("/", "/root/", since, nil)
	if !ok || err != nil || path != want || !sf.Whole || sf.Prefix != "/root" {
		t.Errorf("got %v %v %v %+v, want %v", ok, err, path, sf, want)
	}
}

// checkStoredSnapshot takes a snapshot from the stored state and checks
// that it has neither hotspots nor hardlink rankings, returning its path.
func checkStoredSnapshot(ctx context.Context, t *testing.T, f iduconfig.Filesystem, dir string) string {
	t.Helper()
	taken, err := Computer{}.Take(ctx, testEnv, f, dir)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := Load(taken)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Source != FromStoredSource || stored.Stats.Distributed != nil || stored.Stats.Prefix.Hardlinks != nil {
		t.Fatalf("unexpected stored snapshot: %+v", stored)
	}
	for _, c := range []Computer{{Hotspots: true}, {Hardlinks: true}} {
		if c.Satisfies(stored) {
			t.Errorf("%+v: stored snapshot satisfies requirements", c)
		}
	}
	if !(Computer{}).Satisfies(stored) {
		t.Errorf("stored snapshot does not satisfy no requirements")
	}
	return taken
}

func TestCurrentRequirements(t *testing.T) {
	ctx := context.Background()
	tmpDir, root := clitest.NewHardlinkTree(t)
	dir := filepath.Join(tmpDir, "snapshots")
	cfg := writeSnapshotConfig(t, tmpDir, root, dir)
	f := cfg.Filesystems[0]
	analyzeWithConfig(ctx, t, cfg, root)

	taken := checkStoredSnapshot(ctx, t, f, dir)

	// Both require a full pass, which is saved and then reused.
	c := Computer{Hotspots: true, Hardlinks: true}
	sf, path, err := c.Current(ctx, testEnv, f, dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	if path == "" || path == taken || sf.Source != FromSummariesSource || !c.Satisfies(sf) {
		t.Fatalf("got %v %v %v", path, sf.Source, c.Satisfies(sf))
	}
	// The tree has 3 hardlinks, see clitest.CreateHardlinkTree.
	if sf.Stats.Prefix.TotalHardlinks != 3 || sf.Stats.Prefix.Hardlinks.Len() == 0 {
		t.Errorf("got %v hardlinks, ranking of %v", sf.Stats.Prefix.TotalHardlinks, sf.Stats.Prefix.Hardlinks.Len())
	}
	for _, c := range []Computer{{Hotspots: true}, {Hardlinks: true}, {}} {
		if _, reused, err := c.Current(ctx, testEnv, f, dir, 10); err != nil || reused != path {
			t.Errorf("%+v: got %v, %v, want %v", c, reused, err, path)
		}
	}
}
