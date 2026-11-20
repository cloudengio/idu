// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package statedir_test

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/cli/internal/statedir"
)

const layout = "2006-01-02_15-04-05.000"

func readLatest(t *testing.T, d statedir.Dir) string {
	t.Helper()
	buf, err := os.ReadFile(d.LatestPath())
	if err != nil {
		t.Fatal(err)
	}
	return string(buf)
}

func TestFiles(t *testing.T) {
	d := statedir.New(filepath.Join(t.TempDir(), "snapshots"), layout, ".snap")
	if entries, err := d.Entries(); err != nil || len(entries) != 0 {
		t.Fatalf("missing directory: got %v, %v", entries, err)
	}
	base := time.Date(2026, 11, 19, 10, 0, 0, 0, time.Local)
	for i, when := range []time.Time{base, base.Add(2 * time.Hour), base.Add(time.Hour)} {
		path, err := d.WriteFile(when, []byte(when.String()))
		if err != nil {
			t.Fatal(err)
		}
		if got, want := path, filepath.Join(d.Root(), when.Format(layout)+".snap"); got != want {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}
		// latest is the most recently written entry.
		if got, want := readLatest(t, d), when.String(); got != want {
			t.Errorf("%v: latest: got %v, want %v", i, got, want)
		}
	}
	checkLink(t, d, base.Add(time.Hour))
	// Names that do not match are ignored.
	for _, name := range []string{"other.snap", base.Format(layout) + ".other", ".tmp-x"} {
		if err := os.WriteFile(filepath.Join(d.Root(), name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	checkFileEntries(t, d, base.Add(2*time.Hour), base.Add(time.Hour), base)
}

func checkLink(t *testing.T, d statedir.Dir, when time.Time) {
	t.Helper()
	if runtime.GOOS == "windows" {
		return
	}
	if target, err := os.Readlink(d.LatestPath()); err != nil || target != d.Name(when) {
		t.Errorf("latest: got %v, %v", target, err)
	}
}

func checkFileEntries(t *testing.T, d statedir.Dir, want ...time.Time) {
	t.Helper()
	entries, err := d.Entries()
	if err != nil {
		t.Fatal(err)
	}
	var got []time.Time
	for _, e := range entries {
		got = append(got, e.When)
		if e.IsDir || e.Path != d.Path(e.When) || e.Name != d.Name(e.When) {
			t.Errorf("unexpected entry: %+v", e)
		}
	}
	if !slices.EqualFunc(got, want, time.Time.Equal) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestDirectories(t *testing.T) {
	d := statedir.New(filepath.Join(t.TempDir(), "reports"), time.RFC3339, "")
	base := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	for _, when := range []time.Time{base, base.Add(time.Hour)} {
		path, err := d.MkdirAll(when, "users", "groups")
		if err != nil {
			t.Fatal(err)
		}
		for _, sd := range []string{"users", "groups"} {
			if fi, err := os.Stat(filepath.Join(path, sd)); err != nil || !fi.IsDir() {
				t.Errorf("%v: %v", sd, err)
			}
		}
		if err := os.WriteFile(filepath.Join(path, "summary"), []byte(when.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := d.SetLatest(when); err != nil {
			if runtime.GOOS == "windows" {
				t.Skipf("symbolic links not supported: %v", err)
			}
			t.Fatal(err)
		}
		buf, err := os.ReadFile(filepath.Join(d.LatestPath(), "summary"))
		if err != nil || string(buf) != when.String() {
			t.Errorf("latest: got %q, %v", buf, err)
		}
	}
	entries, err := d.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || !entries[0].IsDir || !entries[0].When.Equal(base.Add(time.Hour)) {
		t.Errorf("unexpected entries: %+v", entries)
	}
}

func TestNoOverwrite(t *testing.T) {
	d := statedir.New(t.TempDir(), layout, ".snap")
	when := time.Date(2026, 11, 19, 10, 0, 0, 0, time.Local)
	first, err := d.WriteFile(when, []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	// Within the same millisecond.
	second, err := d.WriteFile(when.Add(100*time.Microsecond), []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if first == second || second != d.Path(when.Add(time.Millisecond)) {
		t.Errorf("got %v and %v", first, second)
	}
	if buf, err := os.ReadFile(first); err != nil || string(buf) != "first" {
		t.Errorf("first: got %q, %v", buf, err)
	}
	if got := readLatest(t, d); got != "second" {
		t.Errorf("latest: got %q", got)
	}
}

func TestLegacyLayouts(t *testing.T) {
	root := t.TempDir()
	d := statedir.New(root, layout, ".snap", time.DateTime)
	old := time.Date(2026, 11, 19, 10, 0, 0, 0, time.Local)
	if err := os.WriteFile(filepath.Join(root, old.Format(time.DateTime)+".snap"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	path, err := d.WriteFile(old.Add(time.Hour), nil)
	if err != nil {
		t.Fatal(err)
	}
	// New entries use the layout, which has no spaces.
	if got, want := filepath.Base(path), "2026-11-19_11-00-00.000.snap"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	checkFileEntries(t, statedir.New(root, layout, ".snap"), old.Add(time.Hour))
	entries, err := d.Entries()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || !entries[1].When.Equal(old) {
		t.Errorf("got %+v", entries)
	}
}

func TestNoOverwriteSeconds(t *testing.T) {
	// A layout with a resolution of a second steps to the next second.
	d := statedir.New(t.TempDir(), "2006-01-02_15-04-05", ".snap")
	when := time.Date(2026, 11, 19, 10, 0, 0, 0, time.Local)
	if _, err := d.WriteFile(when, nil); err != nil {
		t.Fatal(err)
	}
	second, err := d.WriteFile(when.Add(time.Millisecond), nil)
	if err != nil || second != d.Path(when.Add(time.Second)) {
		t.Errorf("got %v, %v", second, err)
	}
}
