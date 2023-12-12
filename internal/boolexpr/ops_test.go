// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boolexpr_test

import (
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file"
)

func createMatcher(t *testing.T, hl bool, expr string) boolexpr.Matcher {
	parser := boolexpr.NewParser()
	matcher, err := boolexpr.CreateMatcher(parser,
		boolexpr.WithExpression(expr),
		boolexpr.WithHardlinkHandling(hl))
	if err != nil {
		t.Fatalf("failed to create matcher: %v", err)
	}
	return matcher
}

func TestIDs(t *testing.T) {
	fi := file.NewInfo("foo", 0, 0, time.Now(), prefixinfo.NewSysInfo(1, 2, 3, 4))
	pi := prefixinfo.New(fi)
	pi.AppendInfo(file.NewInfo("bar", 0, 0, time.Now(), prefixinfo.NewSysInfo(10, 20, 30, 40)))

	matcher := createMatcher(t, false, "user=1")
	if !matcher.Prefix("foo", &pi) {
		t.Errorf("failed to match")
	}

	matcher = createMatcher(t, false, "user=2")
	if matcher.Prefix("foo", &pi) {
		t.Errorf("incorrect match")
	}

	matcher = createMatcher(t, false, "group=2")
	if !matcher.Prefix("foo", &pi) {
		t.Errorf("failed to match")
	}

	matcher = createMatcher(t, false, "group=3")
	if matcher.Prefix("foo", &pi) {
		t.Errorf("incorrect match")
	}

	fi = pi.InfoList()[0]
	matcher = createMatcher(t, false, "user=10")
	if !matcher.Entry("foo", &pi, fi) {
		t.Errorf("failed to match")
	}

	matcher = createMatcher(t, false, "user=20")
	if matcher.Entry("foo", &pi, fi) {
		t.Errorf("incorrect match")
	}

	matcher = createMatcher(t, false, "group=20")
	if !matcher.Entry("foo", &pi, fi) {
		t.Errorf("failed to match")
	}

	matcher = createMatcher(t, false, "group=30")
	if matcher.Entry("foo", &pi, fi) {
		t.Errorf("incorrect match")
	}

}

func TestHardlinks(t *testing.T) {
	fi := file.NewInfo("foo", 0, 0, time.Now(), prefixinfo.NewSysInfo(1, 2, 3, 4))
	pi := prefixinfo.New(fi)
	a := file.NewInfo("a", 0, 0, time.Now(), prefixinfo.NewSysInfo(10, 20, 30, 40))
	b := file.NewInfo("b", 0, 0, time.Now(), prefixinfo.NewSysInfo(10, 20, 30, 41))
	c := file.NewInfo("c", 0, 0, time.Now(), prefixinfo.NewSysInfo(10, 20, 30, 40))
	pi.AppendInfoList(file.InfoList{a, b, c})

	matcher := createMatcher(t, true, "")

	if got, want := matcher.Prefix("foo", &pi), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for i, fi := range pi.InfoList() {
		want := true
		if i == 2 {
			want = false
		}
		if got := matcher.Entry("foo", &pi, fi); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
