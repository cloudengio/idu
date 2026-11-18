// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package summary_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmd/idu/summary"
	"cloudeng.io/file/diskusage"
	"cloudeng.io/file/localfs"
)

func entryNames(p *summary.Prefix) (files, dirs []string) {
	for _, e := range p.Entries() {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		} else {
			files = append(files, e.Name())
		}
	}
	slices.Sort(files)
	slices.Sort(dirs)
	return
}

func prefixPaths(ctx context.Context, t *testing.T, sr *summary.Reader, path string) []string {
	t.Helper()
	var paths []string
	for p, err := range sr.Prefixes(ctx, path) {
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p.Path())
	}
	return paths
}

func checkXAttrs(t *testing.T, p *summary.Prefix) {
	t.Helper()
	if runtime.GOOS == "windows" {
		return
	}
	uid := int64(os.Getuid())
	if got := p.Info().XAttr.UID; got != uid {
		t.Errorf("prefix uid: got %v, want %v", got, uid)
	}
	for _, e := range p.Entries() {
		if e.XAttr.UID != uid || e.XAttr.FileID == 0 {
			t.Errorf("%v: %+v", e.Name(), e.XAttr)
		}
	}
}

func TestPrefix(t *testing.T) {
	ctx := context.Background()
	root, cfg := createTree(t, false)
	// a-b sorts between a and a/b.
	writeFile(t, filepath.Join(root, "a-b", "f"), 1)
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)
	a := filepath.Join(root, "a")

	p, ok, err := sr.Prefix(ctx, a+string(filepath.Separator))
	if err != nil || !ok {
		t.Fatalf("Prefix: %v %v", ok, err)
	}
	if got, want := p.Path(), a; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	info := p.Info()
	if info.Name() != a || !info.IsDir() {
		t.Errorf("Info: %v %v", info.Name(), info.IsDir())
	}
	files, dirs := entryNames(p)
	if !slices.Equal(files, []string{"f1"}) || !slices.Equal(dirs, []string{"b"}) {
		t.Errorf("got %v %v", files, dirs)
	}
	checkXAttrs(t, p)

	if _, ok, err := sr.Prefix(ctx, filepath.Join(root, "nonexistent")); err != nil || ok {
		t.Errorf("Prefix: %v %v", ok, err)
	}

	j := filepath.Join
	if got, want := prefixPaths(ctx, t, sr, root), []string{root, j(root, "a"), j(root, "a-b"), j(root, "a", "b"), j(root, "c")}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := prefixPaths(ctx, t, sr, a), []string{a, j(a, "b")}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPrefixMatchAndTotals(t *testing.T) {
	ctx := context.Background()
	root, cfg := createTree(t, false)
	analyze(ctx, t, root, cfg)
	sr := openReader(ctx, t, cfg)

	fs := localfs.New()
	m, err := stats.NewMatcher(stats.NewParser(ctx, fs),
		stats.WithFilewalkFS(fs), stats.WithExpression("name=f2 || name=b"))
	if err != nil {
		t.Fatal(err)
	}
	var matched []string
	for p, err := range sr.Prefixes(ctx, root) {
		if err != nil {
			t.Fatal(err)
		}
		if p.Match(m) {
			matched = append(matched, p.Path())
		}
		for _, e := range p.Entries() {
			if !e.IsDir() && p.MatchEntry(m, e) {
				matched = append(matched, filepath.Join(p.Path(), e.Name()))
			}
		}
	}
	b := filepath.Join(root, "a", "b")
	if want := []string{b, filepath.Join(b, "f2")}; !slices.Equal(matched, want) {
		t.Errorf("got %v, want %v", matched, want)
	}

	p, _, err := sr.Prefix(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	totals, _, _ := p.Totals(diskusage.NewIdentity(), stats.AlwaysMatch())
	if totals.Files != 1 || totals.SubPrefixes != 2 || fileBytes(totals) != 1 {
		t.Errorf("Totals: %+v", totals)
	}
}
