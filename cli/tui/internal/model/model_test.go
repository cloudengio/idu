// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package model_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/cmd/idu/cli/tui/internal/model"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
)

func setup(t *testing.T) (context.Context, *cli.Env, iduconfig.Filesystem, string) {
	t.Helper()
	ctx := context.Background()
	tmpDir, root := clitest.NewHardlinkTree(t)
	cfg := clitest.WriteHardlinkConfig(t, tmpDir, root, iduconfig.HardlinkDedupSummaries)
	env := cli.NewEnv()
	env.OpenProgress = io.Discard
	env.Config = cfg
	return ctx, env, cfg.Filesystems[0], root
}

func analyzeFS(ctx context.Context, t *testing.T, env *cli.Env, root string) {
	t.Helper()
	if err := analyze.New(env, analyze.WithOutput(io.Discard)).Analyze(ctx, &analyze.Flags{}, []string{root}); err != nil {
		t.Fatal(err)
	}
}

func TestNotAnalyzed(t *testing.T) {
	ctx, env, f, root := setup(t)
	// Create an empty database by opening and closing it for writing.
	db, err := env.OpenWritableDB(ctx, f.Config())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := model.Open(ctx, env, f, "", io.Discard); !errors.Is(err, model.ErrNotAnalyzed) {
		t.Fatalf("got %v, want %v", err, model.ErrNotAnalyzed)
	}
	analyzeFS(ctx, t, env, root)
	src, err := model.Open(ctx, env, f, "", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func openSource(t *testing.T) (context.Context, *model.DB, string) {
	t.Helper()
	ctx, env, f, root := setup(t)
	analyzeFS(ctx, t, env, root)
	src, err := model.Open(ctx, env, f, "", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close(ctx) })
	if src.Root() != root {
		t.Errorf("got %v, want %v", src.Root(), root)
	}
	return ctx, src, root
}

func TestChildren(t *testing.T) {
	ctx, src, root := openSource(t)
	// The children are sorted by subtree size, see
	// clitest.CreateHardlinkTree: a contains f1 and f2, ab contains g and
	// a link to f1, which a owns, and b contains h and c.
	children, err := src.Children(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range children {
		names = append(names, c.Name)
		if !c.HasSubtree {
			t.Errorf("%v: missing subtree totals", c.Path)
		}
	}
	if want := []string{"a", "ab", "b", "a-b"}; !equal(names, want) {
		t.Errorf("got %v, want %v", names, want)
	}
	if !children[2].HasChildren() || children[0].HasChildren() {
		t.Errorf("unexpected children: %+v", children)
	}
}

func TestSummary(t *testing.T) {
	ctx, src, root := openSource(t)
	sum, err := src.Summary(ctx, filepath.Join(root, "a"))
	if err != nil {
		t.Fatal(err)
	}
	// Bytes include the size of the directory itself.
	if sum.Direct.Files != 2 || sum.Direct.Bytes < 1500 || !sum.HasSubtree || !sum.HasInfo || len(sum.Users) != 1 {
		t.Errorf("unexpected summary: %+v", sum)
	}
	if _, err := src.Summary(ctx, filepath.Join(root, "missing")); err == nil {
		t.Errorf("expected an error")
	}
}

func TestTopN(t *testing.T) {
	ctx, src, root := openSource(t)
	top, err := src.TopN(ctx, root, 10)
	if err != nil {
		t.Fatal(err)
	}
	// The root's rankings come from the stored state, which has no
	// hotspots, those beneath any other prefix from a pass over its
	// summaries.
	if len(top.Bytes) == 0 || top.Bytes[0].Item != filepath.Join(root, "a") || len(top.Users) != 1 || top.HasHotspots {
		t.Errorf("unexpected root top-N: %+v", top)
	}
	sub, err := src.TopN(ctx, filepath.Join(root, "b"), 10)
	if err != nil {
		t.Fatal(err)
	}
	// Within b, the link in b/c to a/f2 is counted since its owner, a, is
	// not beneath b.
	if !sub.HasHotspots || len(sub.Bytes) != 2 || sub.Bytes[0].Item != filepath.Join(root, "b", "c") {
		t.Errorf("unexpected top-N for b: %+v", sub)
	}
	// The rankings can be read repeatedly, ie. they are cached copies.
	again, err := src.TopN(ctx, filepath.Join(root, "b"), 5)
	if err != nil || !equalRanked(again.Bytes, sub.Bytes) {
		t.Errorf("got %+v, %v, want %+v", again.Bytes, err, sub.Bytes)
	}
}

func TestOpenMissing(t *testing.T) {
	ctx, env, f, _ := setup(t)
	if _, err := os.Stat(f.Config().Database); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("database exists: %v", err)
	}
	if _, err := model.Open(ctx, env, f, "", io.Discard); err == nil {
		t.Errorf("expected an error")
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalRanked(a, b []model.Ranked[string]) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestLenient(t *testing.T) {
	ctx, env, f, root := setup(t)
	// Summaries computed for rescan do not record hardlinks, but can be
	// used, with a warning, when deduplicating with the summaries is
	// configured.
	rescan := *f.Config()
	rescan.HardlinkDedup = iduconfig.HardlinkDedupRescan
	rescanCfg := env.Config
	rescanCfg.Filesystems = iduconfig.Filesystems{&iduconfig.Root{Root: root, Settings: rescan}}
	renv := *env
	renv.Config = rescanCfg
	analyzeFS(ctx, t, &renv, root)

	src, err := model.Open(ctx, env, f, "", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close(ctx)
	if w, fixable := src.Warning(); !fixable || !strings.Contains(w, "counted more than once") || !strings.Contains(w, "recording no hardlink information") {
		t.Errorf("unexpected warning: %q, %v", w, fixable)
	}
	if children, err := src.Children(ctx, root); err != nil || len(children) != 4 {
		t.Errorf("got %v, %v", children, err)
	}
	if top, err := src.TopN(ctx, root, 10); err != nil || len(top.Bytes) == 0 {
		t.Errorf("got %+v, %v", top, err)
	}
	if top, err := src.TopN(ctx, filepath.Join(root, "b"), 10); err != nil || len(top.Bytes) == 0 {
		t.Errorf("got %+v, %v", top, err)
	}

	// A database analyzed as configured has no warning.
	ctx, env, f, root = setup(t)
	analyzeFS(ctx, t, env, root)
	src2, err := model.Open(ctx, env, f, "", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer src2.Close(ctx)
	if w, _ := src2.Warning(); w != "" {
		t.Errorf("unexpected warning: %q", w)
	}
}
