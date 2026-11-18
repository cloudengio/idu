// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package summary_test

import (
	"context"
	"errors"
	"iter"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/idudb/backends/badgerdb"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmd/idu/summary"
)

var withBadger = idudb.WithBackendFactory(idudb.NewBackendFactory(badgerdb.NewBackend))

func writeFile(t *testing.T, name string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
}

// createTree creates:
//
//	root/f0      (1 byte)
//	root/a/f1    (10 bytes)
//	root/a/b/f2  (100 bytes)
//	root/c/f3    (1000 bytes)
//	root/c/l1    (a hardlink to root/a/f1, if links is true)
func createTree(t *testing.T, links bool) (root string, cfg *iduconfig.Settings) {
	t.Helper()
	tmp := t.TempDir()
	root = filepath.Join(tmp, "root")
	writeFile(t, filepath.Join(root, "f0"), 1)
	writeFile(t, filepath.Join(root, "a", "f1"), 10)
	writeFile(t, filepath.Join(root, "a", "b", "f2"), 100)
	writeFile(t, filepath.Join(root, "c", "f3"), 1000)
	if links {
		if err := os.Link(filepath.Join(root, "a", "f1"), filepath.Join(root, "c", "l1")); err != nil {
			t.Fatal(err)
		}
	}
	cfg = &iduconfig.Settings{
		Database:  filepath.Join(tmp, "db"),
		Separator: string(filepath.Separator),
	}
	return root, cfg
}

func analyze(ctx context.Context, t *testing.T, root string, cfg *iduconfig.Settings) {
	t.Helper()
	db, err := idudb.OpenWritable(ctx, cfg, []idudb.OpenOption{withBadger})
	if err != nil {
		t.Fatal(err)
	}
	if err := idudb.NewAnalyzer(db, root).Analyze(ctx, root); err != nil {
		t.Fatal(err)
	}
	if err := db.LogAndClose(ctx, time.Now(), time.Now(), []byte("{}")); err != nil {
		t.Fatal(err)
	}
}

func openReader(ctx context.Context, t *testing.T, cfg *iduconfig.Settings) *summary.Reader {
	t.Helper()
	sr, err := summary.Open(ctx, cfg, withBadger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sr.Close(ctx) })
	if err := sr.Check(ctx); err != nil {
		t.Fatal(err)
	}
	return sr
}

// fileBytes returns the bytes used by files, excluding the prefix itself.
func fileBytes(t stats.Totals) int64 {
	return t.Bytes - t.PrefixBytes
}

type pathBytes struct {
	path  string
	depth int
	bytes int64
}

func walk(ctx context.Context, t *testing.T, sr *summary.Reader, path string, opts ...summary.WalkOption) []pathBytes {
	t.Helper()
	var got []pathBytes
	for e, err := range sr.Walk(ctx, path, opts...) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, pathBytes{e.Path, e.Depth, fileBytes(e.Summary.Direct)})
	}
	return got
}

func TestWalkAndGet(t *testing.T) {
	ctx := context.Background()
	root, cfg := createTree(t, false)
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)

	j := filepath.Join
	all := []pathBytes{
		{root, 0, 1},
		{j(root, "a"), 1, 10},
		{j(root, "a", "b"), 2, 100},
		{j(root, "c"), 1, 1000},
	}
	if got := walk(ctx, t, sr, root); !slices.Equal(got, all) {
		t.Errorf("got %v, want %v", got, all)
	}
	if got, want := walk(ctx, t, sr, root, summary.WithMaxDepth(1)), []pathBytes{all[0], all[1], all[3]}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := walk(ctx, t, sr, j(root, "a")), []pathBytes{{j(root, "a"), 0, 10}, {j(root, "a", "b"), 1, 100}}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// Stopping early must not report an error.
	n := 0
	for _, err := range sr.Walk(ctx, root) {
		if err != nil {
			t.Fatal(err)
		}
		n++
		break
	}
	if n != 1 {
		t.Errorf("got %v, want 1", n)
	}

	ps, ok, err := sr.Get(ctx, j(root, "c"))
	if err != nil || !ok || fileBytes(ps.Direct) != 1000 || ps.Direct.Files != 1 {
		t.Errorf("Get: %v %v %+v", ok, err, ps.Direct)
	}
	if _, ok, err := sr.Get(ctx, j(root, "nonexistent")); err != nil || ok {
		t.Errorf("Get: %v %v", ok, err)
	}

}

func lastErr(it iter.Seq2[summary.Entry, error]) (err error) {
	for _, e := range it {
		err = e
	}
	return
}

func TestChildren(t *testing.T) {
	ctx := context.Background()
	root, cfg := createTree(t, false)
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)
	j := filepath.Join

	children, missing, err := sr.Children(ctx, root)
	if err != nil || missing != 0 {
		t.Fatalf("Children: %v %v", missing, err)
	}
	var names []string
	for _, c := range children {
		names = append(names, c.Path)
		if c.Depth != 1 || c.Key == "" {
			t.Errorf("%v: depth %v, key %q", c.Path, c.Depth, c.Key)
		}
	}
	slices.Sort(names)
	if want := []string{j(root, "a"), j(root, "c")}; !slices.Equal(names, want) {
		t.Errorf("got %v, want %v", names, want)
	}

	// Trailing separator on a non-root path is normalized consistently with Get/Walk.
	childrenTrail, missing, err := sr.Children(ctx, root+string(filepath.Separator))
	if err != nil || missing != 0 || len(childrenTrail) != 2 {
		t.Fatalf("Children with trailing sep: len=%d missing=%v err=%v", len(childrenTrail), missing, err)
	}
}

func TestNilGuards(t *testing.T) {
	ctx := context.Background()
	if _, err := summary.Open(ctx, nil, withBadger); err == nil {
		t.Error("expected an error from summary.Open with nil settings")
	}
	for _, sr := range []*summary.Reader{
		summary.New(nil, &iduconfig.Settings{}),
		summary.New(&mockNilCheckReader{}, nil),
	} {
		if err := sr.Check(ctx); err == nil {
			t.Error("expected Check error")
		}
		if _, _, err := sr.Get(ctx, "/"); err == nil {
			t.Error("expected Get error")
		}
		if _, _, err := sr.Children(ctx, "/"); err == nil {
			t.Error("expected Children error")
		}
		if err := lastErr(sr.Walk(ctx, "/")); err == nil {
			t.Error("expected Walk error")
		}
	}
}

type mockNilCheckReader struct{ idudb.Reader }

func TestHardlinkDedup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("hardlink metadata is not available on windows")
	}
	ctx := context.Background()
	root, cfg := createTree(t, true)
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)

	c := filepath.Join(root, "c")
	// Walk counts the link in root/a, which is visited first, so the one
	// in root/c is not counted again.
	for e, err := range sr.Walk(ctx, root) {
		if err != nil {
			t.Fatal(err)
		}
		if e.Path == c && (fileBytes(e.Summary.Direct) != 1000 || e.Summary.Direct.Hardlinks != 1) {
			t.Errorf("Walk: %v: %+v", e.Path, e.Summary.Direct)
		}
	}
	// Get does not deduplicate.
	ps, _, err := sr.Get(ctx, c)
	if err != nil || fileBytes(ps.Direct) != 1010 {
		t.Errorf("Get: %v: %+v", err, ps.Direct)
	}

	// A configuration that counts hardlinks as files sees both.
	files := *cfg
	files.CountHardlinkAsFiles = true
	sr2 := summary.New(sr.DB(), &files)
	for e, err := range sr2.Walk(ctx, c) {
		if err != nil {
			t.Fatal(err)
		}
		if fileBytes(e.Summary.Direct) != 1010 {
			t.Errorf("Walk: %v: %+v", e.Path, e.Summary.Direct)
		}
	}
}

func TestStale(t *testing.T) {
	ctx := context.Background()
	root, cfg := createTree(t, false)
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)

	// Summaries were computed with the identity calculator.
	all, err := iduconfig.ParseConfig([]byte("filesystems:\n  - root: " + root +
		"\n    database: " + cfg.Database +
		"\n    layout:\n      calculator: block\n      parameters:\n        size: 4KiB\n"))
	if err != nil {
		t.Fatal(err)
	}
	stale := summary.New(sr.DB(), all.Filesystems[0].Config())
	if err := stale.Check(ctx); !errors.Is(err, idudb.ErrStaleSummaries) {
		t.Errorf("Check: got %v, want ErrStaleSummaries", err)
	}
	err = lastErr(stale.Walk(ctx, root))
	if !errors.Is(err, idudb.ErrStaleSummaries) {
		t.Errorf("Walk: got %v, want ErrStaleSummaries", err)
	}
	if _, _, err := stale.Get(ctx, root); !errors.Is(err, idudb.ErrStaleSummaries) {
		t.Errorf("Get: got %v, want ErrStaleSummaries", err)
	}
}
