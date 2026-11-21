// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package snapshot_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb/defaultdb"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/snapshot"
	"cloudeng.io/cmd/idu/summary"
	"cloudeng.io/cmd/idu/usernames"
)

// TestPublicAPI checks that the statistics for a filesystem analyzed by idu
// can be read using only idu's public packages, as a command other than
// idu would. Only the analysis itself uses idu's internal packages.
func TestPublicAPI(t *testing.T) {
	ctx := context.Background()
	f, root, dir := analyzed(ctx, t)
	opts := defaultdb.OpenOptions()

	// The stats are constructed from the rankings maintained by analyze,
	// there is no snapshot yet.
	sf, path, err := snapshot.Computer{}.Current(ctx, f, dir, 10, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if path != "" || sf.Source != snapshot.FromStoredSource || !sf.Whole || sf.Stats.Prefix.TotalFiles != 4 {
		t.Errorf("unexpected stats: %q %v %v %v", path, sf.Source, sf.Whole, sf.Stats.Prefix.TotalFiles)
	}
	keys, prefixes := report.PopN(sf.Stats.Subtree.Bytes, 1)
	if len(keys) != 1 || prefixes[0] != root {
		t.Errorf("unexpected top subtree: %v %v", keys, prefixes)
	}

	// Snapshots can be taken, located and read, and are then used.
	taken, err := snapshot.Computer{}.Take(ctx, f, dir, opts...)
	if err != nil {
		t.Fatal(err)
	}
	latest, err := snapshot.Load(snapshot.NewStore(dir).LatestPath())
	if err != nil || latest.Prefix != root || !latest.Whole {
		t.Fatalf("unexpected snapshot: %+v: %v", latest, err)
	}
	if _, path, err := (snapshot.Computer{}).Current(ctx, f, dir, 10, opts...); err != nil || path != taken {
		t.Errorf("got %q, %v, want %q", path, err, taken)
	}
	checkSummaries(ctx, t, f, root)
}

// analyzed analyzes a new tree, using idu's internal packages, and returns
// its filesystem, root and snapshot directory.
func analyzed(ctx context.Context, t *testing.T) (iduconfig.Filesystem, string, string) {
	t.Helper()
	tmpDir, root := clitest.NewHardlinkTree(t)
	dir := filepath.Join(tmpDir, "snapshots")
	cfg, err := iduconfig.ParseConfig(fmt.Appendf(nil, "filesystems:\n  - root: %s\n    database: %s\n    snapshot_dir: %s\n",
		root, filepath.Join(tmpDir, "db"), dir))
	if err != nil {
		t.Fatal(err)
	}
	env := cli.NewEnv()
	env.Config = cfg
	if err := analyze.New(env, analyze.WithOutput(io.Discard)).Analyze(ctx, &analyze.Flags{}, []string{root}); err != nil {
		t.Fatal(err)
	}
	return cfg.Filesystems[0], root, dir
}

// checkSummaries checks that the summaries, and user names, can be read
// directly.
func checkSummaries(ctx context.Context, t *testing.T, f iduconfig.Filesystem, root string) {
	t.Helper()
	sr, err := summary.Open(ctx, f.Config(), defaultdb.OpenOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close(ctx)
	if tot, ok, err := sr.Subtree(ctx, root); err != nil || !ok || tot.Files != 4 {
		t.Errorf("got %+v, %v, %v", tot, ok, err)
	}
	uid := int64(os.Getuid())
	if name := usernames.Manager.NameForUID(uid); name == "" {
		t.Errorf("no name for uid %v", uid)
	}
}
