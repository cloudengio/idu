// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package iduconfig_test

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/iduconfig"
)

func TestLookup(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "tree")
	cfg, err := iduconfig.ParseConfig([]byte(
		"filesystems:\n" +
			"  - name: mytmp\n    root: /tmp\n" +
			"  - name: root\n    prefix: /\n" +
			"  - name: mytree\n    prefix: " + root + string(filepath.Separator) + "\n" +
			"  - name: messy\n    root: /tmp//messy/./\n",
	))
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		fs, prefix     string
		wantFS, wantTo string
	}{
		{"mytmp", "", "/tmp", "/tmp"},
		{"mytmp", ".", "/tmp", "/tmp"},
		{"mytmp", "/tmp", "/tmp", "/tmp"},
		{"mytmp", "/tmp/", "/tmp", "/tmp"},
		{"mytmp", "/tmp/xx", "/tmp", "/tmp/xx"},
		{"mytmp", "xx", "/tmp", "/tmp/xx"},
		{"mytmp", "xx/yy/", "/tmp", "/tmp/xx/yy"},
		{"mytmp", "xx/../yy", "/tmp", "/tmp/yy"},
		// Absolute prefixes are cleaned.
		{"mytmp", "/tmp/xx/../yy", "/tmp", "/tmp/yy"},
		{"mytmp", "/tmp/.", "/tmp", "/tmp"},
		{"mytmp", "/tmp//xx", "/tmp", "/tmp/xx"},
		{"mytmp", "/tmp/xx/", "/tmp", "/tmp/xx"},
		// As is the configured root.
		{"messy", "", "/tmp//messy/./", "/tmp/messy"},
		{"messy", "a", "/tmp//messy/./", "/tmp/messy/a"},
		{"messy", "/tmp/messy/a/", "/tmp//messy/./", "/tmp/messy/a"},
		{"root", "", "/", "/"},
		{"root", "/tmp/xx", "/", "/tmp/xx"},
		{"root", "tmp", "/", "/tmp"},
		{"mytree", "", root + string(filepath.Separator), root},
		{"mytree", "sub" + string(filepath.Separator), root + string(filepath.Separator), filepath.Join(root, "sub")},
	} {
		p, target, err := cfg.Lookup(tc.fs, tc.prefix)
		if err != nil {
			t.Errorf("Lookup(%q, %q): %v", tc.fs, tc.prefix, err)
			continue
		}
		if got, want := rootOf(t, p).Root, tc.wantFS; got != want {
			t.Errorf("Lookup(%q, %q): prefix: got %v, want %v", tc.fs, tc.prefix, got, want)
		}
		if got, want := target, tc.wantTo; got != want {
			t.Errorf("Lookup(%q, %q): target: got %v, want %v", tc.fs, tc.prefix, got, want)
		}
	}

	for _, tc := range []struct {
		fs, prefix, errMsg string
	}{
		{"mytmp", "/tmpx", "not contained"},
		{"mytmp", "/var", "not contained"},
		{"mytmp", "..", "not contained"},
		{"mytmp", "../var", "not contained"},
		{"mytmp", "/tmp/../var", "not contained"},
		{"nosuch", "", `no filesystem named "nosuch"`},
	} {
		if _, _, err := cfg.Lookup(tc.fs, tc.prefix); err == nil || !strings.Contains(err.Error(), tc.errMsg) {
			t.Errorf("Lookup(%q, %q): got %v, want error containing %q", tc.fs, tc.prefix, err, tc.errMsg)
		}
	}

	if _, _, err := cfg.Lookup("", "/tmp"); !errors.Is(err, iduconfig.ErrFilesystemNameRequired) {
		t.Errorf("got %v, want %v", err, iduconfig.ErrFilesystemNameRequired)
	}
}

func TestLookupSingleFilesystem(t *testing.T) {
	t.Parallel()
	cfg, err := iduconfig.ParseConfig([]byte("filesystems:\n  - root: /fs/a\n"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ prefix, want string }{
		{"", "/fs/a"},
		{"b", "/fs/a/b"},
		{"/fs/a/b/", "/fs/a/b"},
	} {
		p, target, err := cfg.Lookup("", tc.prefix)
		if err != nil {
			t.Errorf("Lookup(%q): %v", tc.prefix, err)
			continue
		}
		if r := rootOf(t, p); r.Root != "/fs/a" || target != tc.want {
			t.Errorf("Lookup(%q): got (%v, %v), want (/fs/a, %v)", tc.prefix, r.Root, target, tc.want)
		}
	}

	empty, err := iduconfig.ParseConfig([]byte("filesystems: []\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := empty.Lookup("", ""); err == nil {
		t.Error("expected an error when no filesystems are configured")
	}
}

func TestLookupKeyRange(t *testing.T) {
	t.Parallel()
	cfg, err := iduconfig.ParseConfig([]byte(`filesystems:
  - name: alpha
    start_prefix: projects/alpha/
  - name: span
    start_prefix: a/
    stop_prefix: c/
  - name: all
    start_prefix: ""
    database: ./db-all
`))
	if err != nil {
		t.Fatal(err)
	}
	keyRange := func(name string) *iduconfig.KeyRange {
		f, ok := cfg.ForName(name)
		if !ok {
			t.Fatalf("missing %v", name)
		}
		kr, ok := f.(*iduconfig.KeyRange)
		if !ok {
			t.Fatalf("%v is not a key range", name)
		}
		return kr
	}
	for _, tc := range []struct{ name, start, stop string }{
		{"alpha", "projects/alpha/", "projects/alpha0"},
		{"span", "a/", "c/"},
		{"all", "", ""},
	} {
		kr := keyRange(tc.name)
		if kr.StartPrefix != tc.start || kr.StopPrefix != tc.stop {
			t.Errorf("%v: got [%q, %q), want [%q, %q)", tc.name, kr.StartPrefix, kr.StopPrefix, tc.start, tc.stop)
		}
		if got, want := kr.Layout.Calculator, "bytes"; got != want {
			t.Errorf("%v: got %v, want %v", tc.name, got, want)
		}
		// Key ranges default to / as a separator on all systems.
		if got, want := kr.SeparatorOrDefault(), "/"; got != want {
			t.Errorf("%v: got %q, want %q", tc.name, got, want)
		}
	}

	for _, tc := range []struct{ fs, prefix, want string }{
		{"alpha", "", ""},
		{"alpha", "projects/alpha/", "projects/alpha/"},
		{"alpha", "projects/alpha/x", "projects/alpha/x"},
		{"span", "a/", "a/"},
		{"span", "b", "b"},
		{"span", "a/zz", "a/zz"},
		{"all", "anything", "anything"},
		{"all", "\xff\xff", "\xff\xff"},
	} {
		f, target, err := cfg.Lookup(tc.fs, tc.prefix)
		if err != nil {
			t.Errorf("Lookup(%q, %q): %v", tc.fs, tc.prefix, err)
			continue
		}
		if _, ok := f.(*iduconfig.KeyRange); !ok || target != tc.want {
			t.Errorf("Lookup(%q, %q): got %T, %q, want *KeyRange, %q", tc.fs, tc.prefix, f, target, tc.want)
		}
	}

	for _, tc := range []struct{ fs, prefix string }{
		{"alpha", "projects/"},      // starts before the range
		{"alpha", "projects/alpha"}, // includes projects/alpha-x etc.
		{"alpha", "projects/beta/"}, // after the range
		{"span", "a"},               // includes a-, a. etc.
		{"span", "c"},               // after the range
		{"span", "c/"},              // stop is exclusive
		{"span", "\x00"},            // before the range
	} {
		if _, _, err := cfg.Lookup(tc.fs, tc.prefix); err == nil {
			t.Errorf("Lookup(%q, %q): expected an error", tc.fs, tc.prefix)
		}
	}
}

func TestKeyRangeConfigErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ yaml, errMsg string }{
		{"filesystems:\n  - root: /a\n    start_prefix: a\n", "not both"},
		{"filesystems:\n  - prefix: /a\n    stop_prefix: a\n", "not both"},
		{"filesystems:\n  - root: /a\n    prefix: /a\n", "deprecated alias"},
		{"filesystems:\n  - start_prefix: b\n    stop_prefix: a\n", "must be less than"},
		{"filesystems:\n  - start_prefix: a\n    stop_prefix: a\n", "must be less than"},
		{"filesystems:\n  - name: x\n", "empty root"},
	} {
		_, err := iduconfig.ParseConfig([]byte(tc.yaml))
		if err == nil || !strings.Contains(err.Error(), tc.errMsg) {
			t.Errorf("%q: got %v, want error containing %q", tc.yaml, err, tc.errMsg)
		}
	}
}

func TestPrefixSuccessor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ in, want string }{
		{"", ""},
		{"a", "b"},
		{"a/", "a0"},
		{"ab", "ac"},
		{"a\xff", "b"},
		{"a\xff\xff", "b"},
		{"\xff", ""},
		{"\xff\xff", ""},
		{"\x00", "\x01"},
	} {
		if got := iduconfig.PrefixSuccessor(tc.in); got != tc.want {
			t.Errorf("PrefixSuccessor(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestKeyRangeContains(t *testing.T) {
	t.Parallel()
	kr := iduconfig.KeyRange{StartPrefix: "b", StopPrefix: "d"}
	for _, tc := range []struct {
		key        string
		contains   bool
		containsPf bool
	}{
		{"a", false, false},
		{"b", true, true},
		{"bzzz", true, true},
		{"c", true, true},
		{"czz", true, true},
		{"d", false, false},
		{"", false, false},
	} {
		if got := kr.Contains(tc.key); got != tc.contains {
			t.Errorf("Contains(%q) = %v, want %v", tc.key, got, tc.contains)
		}
		if got := kr.ContainsPrefix(tc.key); got != tc.containsPf {
			t.Errorf("ContainsPrefix(%q) = %v, want %v", tc.key, got, tc.containsPf)
		}
	}
	unbounded := iduconfig.KeyRange{StartPrefix: "b"}
	if !unbounded.ContainsPrefix("zzz") || !unbounded.ContainsPrefix("\xff") || unbounded.Contains("a") {
		t.Errorf("unexpected results for an unbounded range")
	}
}

func TestForPrefixCleansPaths(t *testing.T) {
	t.Parallel()
	cfg, err := iduconfig.ParseConfig([]byte(`filesystems:
  - name: tmp
    root: /tmp
  - name: sub
    root: /tmp/sub
  - name: messy
    root: /data//./x/
  - name: range
    start_prefix: /tmp/sub/
`))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, want string }{
		{"/tmp/sub/..", "tmp"},
		{"/tmp/sub/../other", "tmp"},
		{"/tmp/sub/./a", "sub"},
		{"/tmp//sub", "sub"},
		{"/data/x/y", "messy"},
		{"/data/./x", "messy"},
	} {
		r, ok := cfg.ForPrefix(tc.path)
		if !ok {
			t.Errorf("ForPrefix(%q): no match", tc.path)
			continue
		}
		if got := r.Name; got != tc.want {
			t.Errorf("ForPrefix(%q): got %v, want %v", tc.path, got, tc.want)
		}
	}
	if r, ok := cfg.ForPrefix("/data/xy"); ok {
		t.Errorf("ForPrefix(/data/xy): unexpected match %v", r.Name)
	}
}

func TestFilesystemMergeKeys(t *testing.T) {
	t.Parallel()
	cfg, err := iduconfig.ParseConfig([]byte(`filesystems:
  - &keys
    name: keys-a
    start_prefix: a/
    database: ./db-a
  - <<: *keys
    name: keys-b
    database: ./db-b
  - &old
    name: old-a
    prefix: /old
  - <<: *old
    name: old-b
  - <<: [*old]
    name: old-c
`))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"keys-a", "keys-b"} {
		f, ok := cfg.ForName(name)
		if !ok {
			t.Fatalf("missing %v", name)
		}
		kr, ok := f.(*iduconfig.KeyRange)
		if !ok {
			t.Fatalf("%v: got %T, want *iduconfig.KeyRange", name, f)
		}
		if kr.StartPrefix != "a/" || kr.StopPrefix != "a0" {
			t.Errorf("%v: got %v", name, kr.Location())
		}
	}
	for _, name := range []string{"old-a", "old-b", "old-c"} {
		f, ok := cfg.ForName(name)
		if !ok {
			t.Fatalf("missing %v", name)
		}
		if got, want := rootOf(t, f).Root, "/old"; got != want {
			t.Errorf("%v: got %v, want %v", name, got, want)
		}
	}

	// Conflicts introduced via merge keys are detected.
	for _, tc := range []struct{ yaml, errMsg string }{
		{"filesystems:\n  - &r\n    name: r\n    root: /r\n  - <<: *r\n    name: x\n    start_prefix: a\n", "not both"},
		{"filesystems:\n  - &r\n    name: r\n    prefix: /r\n  - <<: *r\n    name: x\n    root: /r\n", "deprecated alias"},
	} {
		if _, err := iduconfig.ParseConfig([]byte(tc.yaml)); err == nil || !strings.Contains(err.Error(), tc.errMsg) {
			t.Errorf("%q: got %v, want error containing %q", tc.yaml, err, tc.errMsg)
		}
	}
}
