// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boolexpr_test

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk/localfs"
)

func createMatcher(t *testing.T, fs fs.FS, expr string) boolexpr.Matcher {
	parser := boolexpr.NewParser(fs)
	matcher, err := boolexpr.CreateMatcher(parser,
		boolexpr.WithExpression(expr))
	if err != nil {
		t.Fatalf("failed to create matcher: %v", err)
	}
	return matcher
}

func TestIDs(t *testing.T) {

	fi := file.NewInfo("foo", 0, 0, time.Now(), prefixinfo.NewSysInfo(1, 2, 3, 4))
	pi, err := prefixinfo.New("foo", fi)
	if err != nil {
		t.Fatal(err)
	}
	pi.AppendInfo(file.NewInfo("bar", 0, 0, time.Now(), prefixinfo.NewSysInfo(10, 20, 30, 40)))
	pi.AppendInfo(file.NewInfo("dir", 0, fs.ModeDir, time.Now(), prefixinfo.NewSysInfo(10, 20, 30, 40)))

	for _, fi := range pi.InfoList() {
		matcher := createMatcher(t, nil, "user=10")
		if !matcher.Entry("foo", &pi, fi) {
			t.Errorf("failed to match")
		}

		matcher = createMatcher(t, nil, "user=20")
		if matcher.Entry("foo", &pi, fi) {
			t.Errorf("incorrect match")
		}

		matcher = createMatcher(t, nil, "group=20")
		if !matcher.Entry("foo", &pi, fi) {
			t.Errorf("failed to match")
		}

		matcher = createMatcher(t, nil, "group=30")
		if matcher.Entry("foo", &pi, fi) {
			t.Errorf("incorrect match")
		}
	}

}

func TestHardlinks(t *testing.T) {
	tmpdir := t.TempDir()
	ta := filepath.Join(tmpdir, "a")
	f, err := os.Create(ta)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	fi := file.NewInfoFromFileInfo(info)
	_, _, dev, ino, err := prefixinfo.GetSysInfo(ta, fi)
	if err != nil {
		t.Fatal(err)
	}
	fi.SetSys(prefixinfo.NewSysInfo(10, 20, 1000, 1000))
	pi, err := prefixinfo.New("foo", fi)
	if err != nil {
		t.Fatal(err)
	}

	a := file.NewInfo("a", 0, 0, time.Now(), prefixinfo.NewSysInfo(10, 20, 30, ino))
	b := file.NewInfo("b", 0, 0, time.Now(), prefixinfo.NewSysInfo(10, 20, dev, ino))
	c := file.NewInfo("c", 0, 0, time.Now(), prefixinfo.NewSysInfo(10, 20, dev, 40))
	pi.AppendInfoList(file.InfoList{a, b, c})

	lfs := localfs.New()
	matcher := createMatcher(t, lfs, fmt.Sprintf("hardlink='%v'", ta))
	for i, fi := range pi.InfoList() {
		want := false
		if i == 1 {
			want = true
		}
		if got := matcher.Entry("foo", &pi, fi); got != want {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}
	}
}
