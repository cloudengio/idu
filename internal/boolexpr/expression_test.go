// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boolexpr_test

import (
	"context"
	"io/fs"
	"os/user"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file"
	"cloudeng.io/file/localfs"
)

func testMatcherFixture() (prefixinfo.T, file.Info, file.Info) {
	now := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	dirInfo := file.NewInfo("sub", 0, fs.ModeDir|0o755, now, prefixinfo.NewSysInfo(10, 20, 1, 100, 8))
	pi := prefixinfo.New("/root/sub", dirInfo)
	f1 := file.NewInfo("alpha.txt", 123, 0o644, now, prefixinfo.NewSysInfo(10, 20, 1, 101, 8))
	f2 := file.NewInfo("beta.log", 456, 0o600, now, prefixinfo.NewSysInfo(10, 20, 1, 101, 8))
	pi.AppendInfoList(file.InfoList{f1, f2})
	return pi, f1, f2
}

func TestAlwaysMatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lfs := localfs.New()
	pi, f1, _ := testMatcherFixture()

	p := boolexpr.NewParserTests(ctx, lfs)
	m := boolexpr.AlwaysMatch(p)
	if m.HasExpression() {
		t.Error("expected HasExpression() == false")
	}
	if !m.Prefix("/root/sub", &pi) || !m.Entry("/root/sub", &pi, f1) {
		t.Error("AlwaysMatch should return true for Prefix and Entry")
	}
	if s := m.String(); !strings.Contains(s, "hardlink handling disabled") {
		t.Errorf("unexpected String(): %q", s)
	}

	mFalse, err := boolexpr.CreateMatcher(p, boolexpr.WithEmptyEntryValue(false))
	if err != nil {
		t.Fatal(err)
	}
	if mFalse.Prefix("/root/sub", &pi) || mFalse.Entry("/root/sub", &pi, f1) {
		t.Error("expected false when WithEmptyEntryValue(false)")
	}
}

func TestMatcherEntryOperands(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lfs := localfs.New()
	pi, f1, f2 := testMatcherFixture()
	p := boolexpr.NewParserTests(ctx, lfs)

	tests := []struct {
		name  string
		expr  string
		entry file.Info
		want  bool
	}{
		{name: "name match", expr: "name=alpha.txt", entry: f1, want: true},
		{name: "name mismatch", expr: "name=alpha.txt", entry: f2, want: false},
		{name: "full path match with fs", expr: "name=/root/sub/alpha.txt", entry: f1, want: true},
		{name: "regexp path match", expr: "re=sub/beta", entry: f2, want: true},
		{name: "type regular match", expr: "type=f", entry: f1, want: true},
		{name: "type dir mismatch", expr: "type=d", entry: f1, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, err := boolexpr.CreateMatcher(p,
				boolexpr.WithFilewalkFS(lfs),
				boolexpr.WithEntryExpression(tc.expr))
			if err != nil {
				t.Fatalf("CreateMatcher(%q): %v", tc.expr, err)
			}
			if !m.HasExpression() {
				t.Error("expected HasExpression() == true")
			}
			if got := m.Entry("/root/sub", &pi, tc.entry); got != tc.want {
				t.Errorf("Entry(%q) = %v, want %v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestMatcherPrefixOperands(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lfs := localfs.New()
	pi, _, _ := testMatcherFixture()
	p := boolexpr.NewParserTests(ctx, lfs)

	for _, tc := range []struct {
		name string
		expr string
		want bool
	}{
		{name: "prefix name", expr: "name=sub", want: true},
		{name: "prefix full path", expr: "name=/root/sub", want: true},
		{name: "prefix regexp", expr: "re=^/root/sub$", want: true},
		{name: "prefix regexp mismatch", expr: "re=^/other", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, err := boolexpr.CreateMatcher(p,
				boolexpr.WithFilewalkFS(lfs),
				boolexpr.WithEntryExpression(tc.expr))
			if err != nil {
				t.Fatalf("CreateMatcher(%q): %v", tc.expr, err)
			}
			if got := m.Prefix("/root/sub", &pi); got != tc.want {
				t.Errorf("Prefix(%q) = %v, want %v", tc.expr, got, tc.want)
			}
		})
	}
}

func TestMatcherHardlinkAndNewParser(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lfs := localfs.New()
	p := boolexpr.NewParserTests(ctx, lfs)

	m, err := boolexpr.CreateMatcher(p, boolexpr.WithHardlinkHandling(true))
	if err != nil {
		t.Fatal(err)
	}
	if s := m.String(); !strings.Contains(s, "hardlink handling enabled") {
		t.Errorf("unexpected String(): %q", s)
	}
	x := file.XAttr{Device: 1, FileID: 101}
	if m.IsHardlink(x) {
		t.Error("first reference should not be hardlink")
	}
	if !m.IsHardlink(x) {
		t.Error("second reference should be hardlink")
	}
	if _, err := boolexpr.CreateMatcher(p, boolexpr.WithEntryExpression("invalid_op=1")); err == nil {
		t.Error("expected parse error for unknown operand")
	}

	cur, err := user.Current()
	if err != nil {
		t.Skipf("user.Current: %v", err)
	}
	realParser := boolexpr.NewParser(ctx, lfs)
	if _, err := boolexpr.CreateMatcher(realParser, boolexpr.WithEntryExpression("user="+cur.Username)); err != nil {
		t.Errorf("CreateMatcher(user=%s): %v", cur.Username, err)
	}
}
