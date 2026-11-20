// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package iduconfig

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"cloudeng.io/cmdutil/cmdyaml"
)

func TestExcludeFuncPseudoFilesystems(t *testing.T) {
	var s Settings
	for _, tc := range []struct {
		goos string
		path string
		want bool
	}{
		{"darwin", "/dev", true},
		{"darwin", "/dev/fd/7/000596.sst", true},
		{"darwin", "/devices", false},
		{"darwin", "/proc/1", false},
		{"darwin", "/Users/me/dev/x", false},
		{"linux", "/dev/fd", true},
		{"linux", "/proc/1/maps", true},
		{"linux", "/sys/kernel", true},
		{"linux", "/system", false},
		{"windows", "/dev/fd", false},
		{"linux", "/", false},
	} {
		if got := s.excludeFunc(tc.goos)(tc.path); got != tc.want {
			t.Errorf("%v: %v: got %v, want %v", tc.goos, tc.path, got, tc.want)
		}
	}
}

func TestExcludeFuncDatabase(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "db")
	if err := os.Mkdir(db, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(db, link); err != nil {
		t.Skip("symlinks are not supported")
	}
	other := filepath.Join(dir, "database")

	for _, configured := range []string{db} {
		s := Settings{Database: configured}
		exclude := s.ExcludeFunc()
		for _, tc := range []struct {
			path string
			want bool
		}{
			{db, true},
			{filepath.Join(db, "000596.sst"), true},
			{filepath.Join(db, "sub", "x"), true},
			{other, false}, // shares a name prefix only
			{dir, false},
			{filepath.Join(dir, "elsewhere"), false},
		} {
			if got := exclude(tc.path); got != tc.want {
				t.Errorf("database %v: %v: got %v, want %v", configured, tc.path, got, tc.want)
			}
		}
	}
	// A database configured via a symbolic link is excluded by both names,
	// since scans do not follow links but may reach the target by its resolved path.
	resolved, err := filepath.EvalSymlinks(db)
	if err != nil {
		t.Fatal(err)
	}
	s := Settings{Database: link}
	exclude := s.ExcludeFunc()
	for _, p := range []string{link, filepath.Join(link, "MANIFEST"), resolved, filepath.Join(resolved, "000596.sst")} {
		if !exclude(p) {
			t.Errorf("%v: not excluded", p)
		}
	}
	if (&Settings{}).ExcludeFunc()(db) {
		t.Error("no database is configured")
	}
}

func TestExcludeFuncRegexps(t *testing.T) {
	s := Settings{Database: t.TempDir()}
	s.Exclusions = cmdyaml.RegexpList{{Regexp: regexp.MustCompile(`^/Volumes`)}}
	exclude := s.ExcludeFunc()
	if !exclude("/Volumes/x") {
		t.Error("configured exclusion ignored")
	}
	if exclude("/Users/x") {
		t.Error("unexpected exclusion")
	}
}
