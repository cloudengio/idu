// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package query_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/idudb/defaultdb"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
	"cloudeng.io/cmd/idu/query"
	"cloudeng.io/cmd/idu/snapshot"
	"cloudeng.io/cmd/idu/stats"
)

// setup analyzes the hardlink tree, see clitest.CreateHardlinkTree, and
// returns its filesystem, root and snapshot directory.
func setup(t *testing.T) (iduconfig.Filesystem, string, string) {
	t.Helper()
	ctx := context.Background()
	tmpDir, root := clitest.NewHardlinkTree(t)
	dir := filepath.Join(tmpDir, "snapshots")
	cfg, err := iduconfig.ParseConfig(fmt.Appendf(nil, "filesystems:\n  - root: %s\n    database: %s\n    snapshot_dir: %s\n",
		root, filepath.Join(tmpDir, "db"), dir))
	if err != nil {
		t.Fatal(err)
	}
	env := cli.NewEnv()
	env.Config = cfg
	env.OpenProgress = io.Discard
	if err := analyze.New(env, analyze.WithOutput(io.Discard)).Analyze(ctx, &analyze.Flags{}, []string{root}); err != nil {
		t.Fatal(err)
	}
	return cfg.Filesystems[0], root, dir
}

func open(t *testing.T, f iduconfig.Filesystem) idudb.Reader {
	t.Helper()
	ctx := context.Background()
	db, err := defaultdb.OpenReadOnly(ctx, f.Config())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(ctx) })
	return db
}

func run(t *testing.T, e *query.Engine, q query.Query) query.Result {
	t.Helper()
	res, err := e.Run(context.Background(), q)
	if err != nil {
		t.Fatalf("%+v: %v", q, err)
	}
	return res
}

func paths(rows []query.Row) []string {
	p := make([]string, len(rows))
	for i, r := range rows {
		p[i] = r.Path
	}
	return p
}

func TestPrefixes(t *testing.T) {
	f, root, _ := setup(t)
	e := query.New(f, open(t, f))
	a := filepath.Join(root, "a")

	// Files with more than one link are counted once.
	res := run(t, e, query.Query{Scope: query.Self})
	if r := res.Rows[0]; len(res.Rows) != 1 || r.Path != root || r.Depth != 0 || r.Total == nil || r.Total.Files != 4 || r.Direct == nil {
		t.Errorf("unexpected root: %+v", res)
	}
	res = run(t, e, query.Query{Scope: query.Self, Within: "a"})
	if r := res.Rows[0]; r.Path != a || r.Parent != root || r.Name != "a" || r.Depth != 1 || r.Direct.Files != 2 {
		t.Errorf("unexpected a: %+v", r)
	}

	checkChildren(t, e, a, filepath.Join(root, "b"))
}

// checkChildren checks the children of the root, which are ordered by any
// column; a or b (which includes b/c when directories report 4 KiB+ sizes)
// is the largest by bytes.
func checkChildren(t *testing.T, e *query.Engine, a, b string) {
	t.Helper()
	// Children, and level 1, ordered by any column.
	for _, q := range []query.Query{
		{Scope: query.Children, OrderBy: new(query.Total(stats.MetricBytes))},
		{Level: 1, OrderBy: new(query.Total(stats.MetricBytes))},
	} {
		res := run(t, e, q)
		if len(res.Rows) != 4 || (res.Rows[0].Path != a && res.Rows[0].Path != b) || res.Source != query.SourceChildren || res.OrderBy != "total_bytes" {
			t.Errorf("unexpected children: %+v", res)
		}
		for i := 1; i < len(res.Rows); i++ {
			if res.Rows[i].Value > res.Rows[i-1].Value {
				t.Errorf("not in descending order: %v", res.Rows)
			}
		}
	}

}

func TestPrefixRankings(t *testing.T) {
	f, root, _ := setup(t)
	e := query.New(f, open(t, f))
	a, b := filepath.Join(root, "a"), filepath.Join(root, "b")

	// Rankings by recursive totals beneath any prefix.
	res := run(t, e, query.Query{Limit: 2})
	if len(res.Rows) != 2 || res.Rows[0].Path != root || !res.Exact || res.Source != query.SourceBestFirst {
		t.Errorf("unexpected ranking: %+v", res)
	}
	res = run(t, e, query.Query{Within: "b", OrderBy: new(query.Total(stats.MetricFiles))})
	if got := paths(res.Rows); len(got) != 1 || got[0] != b {
		t.Errorf("unexpected ranking: %v", got)
	}

	// Rankings by direct totals for the entire filesystem, optionally for
	// a single user.
	res = run(t, e, query.Query{OrderBy: new(query.Direct(stats.MetricBytes)), Limit: 1})
	if len(res.Rows) != 1 || res.Rows[0].Path != a || res.Source != query.SourceRankings || res.Rows[0].Value != res.Rows[0].Direct.Bytes {
		t.Errorf("unexpected ranking: %+v", res)
	}
	uid := int64(os.Getuid())
	res = run(t, e, query.Query{OrderBy: new(query.Direct(stats.MetricBytes)), UID: &uid, Limit: 1})
	if len(res.Rows) != 1 || res.Rows[0].Path != a {
		t.Errorf("unexpected ranking: %+v", res)
	}
}

func TestUsersAndGroups(t *testing.T) {
	f, root, _ := setup(t)
	e := query.New(f, open(t, f))
	uid := int64(os.Getuid())

	res := run(t, e, query.Query{Table: query.Users})
	if len(res.Rows) != 1 || *res.Rows[0].ID != uid || res.Rows[0].Total == nil || res.Rows[0].Total.Files != 4 || res.Source != query.SourceIDTotals {
		t.Errorf("unexpected users: %+v", res)
	}
	res = run(t, e, query.Query{Table: query.Groups, Within: filepath.Join(root, "a"), Scope: query.Self})
	if len(res.Rows) != 1 || res.Rows[0].Direct.Files != 2 || res.OrderBy != "direct_storage_bytes" {
		t.Errorf("unexpected groups: %+v", res)
	}
}

func checkUnsupported(t *testing.T, e *query.Engine, q query.Query, want string) {
	t.Helper()
	_, err := e.Run(context.Background(), q)
	var ue *query.UnsupportedError
	if !errors.Is(err, query.ErrUnsupported) || !errors.As(err, &ue) || !strings.Contains(err.Error(), want) {
		t.Errorf("%+v: got %v, want an unsupported query error containing %q", q, err, want)
	}
}

func TestUnsupported(t *testing.T) {
	f, _, _ := setup(t)
	e := query.New(f, open(t, f))
	uid := int64(1)
	for _, tc := range []struct {
		q    query.Query
		want string
	}{
		{query.Query{Filter: "user=me", Scope: query.Self}, "filter expressions are only supported for rankings"},
		{query.Query{UID: &uid}, "a user or group may only be specified"},
		{query.Query{UID: &uid, Scope: query.Children}, "a user or group may only be specified"},
		{query.Query{Table: query.Users, OrderBy: new(query.Direct(stats.MetricBytes))}, "order by a total_ column"},
		{query.Query{Table: query.Groups, Scope: query.Children}, "scope children is not supported"},
		{query.Query{Table: query.Users, Level: 2}, "only prefixes"},
		{query.Query{Scope: query.Children, Level: 2}, "only prefixes"},
		{query.Query{Table: query.Hotspots, Scope: query.Self}, "use scope subtree"},
	} {
		checkUnsupported(t, e, tc.q, tc.want)
	}
}

// TestLookupsOnly checks that the queries that require a pass are not
// supported by an Engine restricted to lookups.
func TestLookupsOnly(t *testing.T) {
	f, _, _ := setup(t)
	db := open(t, f)
	e := query.New(f, db, query.WithLookupsOnly())
	uid := int64(os.Getuid())
	for _, tc := range []struct {
		q    query.Query
		want string
	}{
		{query.Query{Filter: "file-larger=600"}, "filter expressions require a pass"},
		{query.Query{Level: 2}, "grouping by level 2 requires a pass"},
		{query.Query{Within: "b", OrderBy: new(query.Direct(stats.MetricBytes))}, "only maintained for the entire filesystem"},
		{query.Query{Within: "b", UID: &uid, OrderBy: new(query.Direct(stats.MetricBytes))}, "for a user or group are only maintained for the entire filesystem"},
		{query.Query{Table: query.Users, Within: "a"}, "only maintained for the entire filesystem"},
		{query.Query{Table: query.Users, Scope: query.Self, OrderBy: new(query.Total(stats.MetricBytes))}, "only maintained for the entire filesystem"},
		{query.Query{Table: query.Hotspots}, "idu stats compute --hotspots"},
		{query.Query{Table: query.Hotspots, Within: "a"}, "hotspots require a pass"},
	} {
		checkUnsupported(t, e, tc.q, tc.want)
	}
	checkUnsupported(t, query.New(f, db, query.WithLookupsOnly(), query.WithSnapshotDir("")), query.Query{Table: query.Hotspots}, "hotspots require a pass")
}

// TestPasses checks the queries that are answered by a pass over the
// summaries, or file metadata, beneath a prefix.
func TestPasses(t *testing.T) {
	f, root, _ := setup(t)
	e := query.New(f, open(t, f))
	a, b := filepath.Join(root, "a"), filepath.Join(root, "b")
	uid := int64(os.Getuid())
	isPass := func(res query.Result) bool { return strings.HasPrefix(res.Source, query.SourcePassOf) }

	res := run(t, e, query.Query{Within: "b", OrderBy: new(query.Direct(stats.MetricBytes))})
	if !isPass(res) || len(res.Rows) != 2 || !strings.HasPrefix(res.Rows[0].Path, b) || res.Rows[0].Value != res.Rows[0].Direct.Bytes {
		t.Errorf("unexpected ranking: %+v", res)
	}
	res = run(t, e, query.Query{Within: "a", UID: &uid, OrderBy: new(query.Direct(stats.MetricFiles))})
	if got := paths(res.Rows); !isPass(res) || len(got) != 1 || got[0] != a || res.Rows[0].Value != 2 {
		t.Errorf("unexpected ranking: %+v", res)
	}
	checkLevelAndFilter(t, e, root)
}

// checkLevelAndFilter checks grouping by level and filters, which are
// answered by a pass.
func checkLevelAndFilter(t *testing.T, e *query.Engine, root string) {
	t.Helper()
	a, b := filepath.Join(root, "a"), filepath.Join(root, "b")
	isPass := func(res query.Result) bool { return strings.HasPrefix(res.Source, query.SourcePassOf) }
	// b/c is the only prefix at level 2.
	res := run(t, e, query.Query{Level: 2})
	if got := paths(res.Rows); !isPass(res) || len(got) != 1 || got[0] != filepath.Join(b, "c") || res.Rows[0].Depth != 2 {
		t.Errorf("unexpected level: %+v", res)
	}
	// Only a/f1 is larger than 600 bytes, the prefixes themselves must
	// match for their files to be counted.
	res = run(t, e, query.Query{Filter: "type=d || file-larger=600", OrderBy: new(query.Direct(stats.MetricFiles))})
	if got := paths(res.Rows); !isPass(res) || !strings.Contains(res.Source, "type=d || file-larger=600") || len(got) == 0 || got[0] != a || res.Rows[0].Value != 1 || res.Rows[0].Direct != nil {
		t.Errorf("unexpected filtered ranking: %+v", res)
	}
}

// TestPassIDsAndHotspots checks the rankings of users, groups and
// hotspots that are answered by a pass.
func TestPassIDsAndHotspots(t *testing.T) {
	f, _, _ := setup(t)
	e := query.New(f, open(t, f))
	uid := int64(os.Getuid())
	isPass := func(res query.Result) bool { return strings.HasPrefix(res.Source, query.SourcePassOf) }
	res := run(t, e, query.Query{Table: query.Users, Within: "a", OrderBy: new(query.Total(stats.MetricFiles))})
	if !isPass(res) || len(res.Rows) != 1 || *res.Rows[0].ID != uid || res.Rows[0].Value != 2 {
		t.Errorf("unexpected users: %+v", res)
	}
	// b/c contains a link to a file in a, which is counted when b is
	// considered in isolation.
	res = run(t, e, query.Query{Table: query.Groups, Scope: query.Self, Within: "b", OrderBy: new(query.Total(stats.MetricFiles))})
	if !isPass(res) || len(res.Rows) != 1 || res.Rows[0].Value != 2 {
		t.Errorf("unexpected groups: %+v", res)
	}
	// Hotspots beneath any prefix.
	res = run(t, e, query.Query{Table: query.Hotspots, Within: "a"})
	if !isPass(res) || res.OrderBy != "distributed_storage_bytes" {
		t.Errorf("unexpected hotspots: %+v", res)
	}
}

func TestHotspots(t *testing.T) {
	ctx := context.Background()
	f, root, dir := setup(t)
	// A full pass, which includes the hotspots, is saved as a snapshot.
	if _, _, err := (snapshot.Computer{Hotspots: true}).Current(ctx, f, dir, 10, defaultdb.OpenOptions()...); err != nil {
		t.Fatal(err)
	}
	res := run(t, query.New(f, open(t, f)), query.Query{Table: query.Hotspots, OrderBy: new(query.Total(stats.MetricBytes))})
	if !strings.HasPrefix(res.Source, query.SourceSnapshotOf+dir) || res.OrderBy != "distributed_bytes" {
		t.Errorf("unexpected hotspots: %+v", res)
	}
	for _, r := range res.Rows {
		if r.Hotspot == nil || !strings.HasPrefix(r.Path, root) || r.Value != r.Hotspot.DistributedBytes {
			t.Errorf("unexpected hotspot: %+v", r)
		}
	}
}

func TestColumns(t *testing.T) {
	for _, name := range query.Columns() {
		c, err := query.ParseColumn(name)
		if err != nil || c.String() != name {
			t.Errorf("%v: got %v, %v", name, c, err)
		}
	}
	if c, err := query.ParseColumn("total_storage_bytes"); err != nil || c != query.Total(stats.MetricStorageBytes) {
		t.Errorf("got %v, %v", c, err)
	}
	for _, name := range []string{"storage_bytes", "total_size", ""} {
		if _, err := query.ParseColumn(name); err == nil || !strings.Contains(err.Error(), "direct_files") {
			t.Errorf("%q: got %v", name, err)
		}
	}
}
