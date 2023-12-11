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

func createMatcher(t *testing.T, expr string) boolexpr.Matcher {
	parser := boolexpr.NewParser()
	matcher, err := boolexpr.CreateMatcher(parser, []string{expr})
	if err != nil {
		t.Fatalf("failed to create matcher: %v", err)
	}
	return matcher
}

func TestIDs(t *testing.T) {
	fi := file.NewInfo("foo", 0, 0, time.Now(), prefixinfo.NewSysInfo(1, 2, 3, 4))
	pi := prefixinfo.New(fi)
	pi.AppendInfo(file.NewInfo("bar", 0, 0, time.Now(), prefixinfo.NewSysInfo(10, 20, 30, 40)))

	matcher := createMatcher(t, "user=1")
	if !matcher.Prefix("foo", &pi) {
		t.Errorf("failed to match")
	}

	matcher = createMatcher(t, "user=2")
	if matcher.Prefix("foo", &pi) {
		t.Errorf("incorrect match")
	}

	matcher = createMatcher(t, "group=2")
	if !matcher.Prefix("foo", &pi) {
		t.Errorf("failed to match")
	}

	matcher = createMatcher(t, "group=3")
	if matcher.Prefix("foo", &pi) {
		t.Errorf("incorrect match")
	}

	fi = pi.InfoList()[0]
	matcher = createMatcher(t, "user=10")
	if !matcher.Entry("foo", &pi, fi) {
		t.Errorf("failed to match")
	}

	matcher = createMatcher(t, "user=20")
	if matcher.Entry("foo", &pi, fi) {
		t.Errorf("incorrect match")
	}

	matcher = createMatcher(t, "group=20")
	if !matcher.Entry("foo", &pi, fi) {
		t.Errorf("failed to match")
	}

	matcher = createMatcher(t, "group=30")
	if matcher.Entry("foo", &pi, fi) {
		t.Errorf("incorrect match")
	}

}

func TestHardlinks(t *testing.T) {

}
