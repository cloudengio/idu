// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats_test

import (
	"context"
	"fmt"
	"io/fs"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/testutil"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmdutil/boolexpr"
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

	p := testutil.NewParser(ctx, lfs)
	m := stats.AlwaysMatch()
	if m.HasExpression() {
		t.Error("expected HasExpression() == false")
	}
	if !m.Prefix("/root/sub", &pi) || !m.Entry("/root/sub", &pi, f1) {
		t.Error("AlwaysMatch should return true for Prefix and Entry")
	}
	if s := m.String(); !strings.Contains(s, "hardlink handling disabled") {
		t.Errorf("unexpected String(): %q", s)
	}

	mFalse, err := stats.NewMatcher(p, stats.WithEmptyValue(false))
	if err != nil {
		t.Fatal(err)
	}
	if mFalse.Prefix("/root/sub", &pi) || mFalse.Entry("/root/sub", &pi, f1) {
		t.Error("expected false when WithEmptyValue(false)")
	}
}

func TestMatcherEntryOperands(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lfs := localfs.New()
	pi, f1, f2 := testMatcherFixture()
	p := testutil.NewParser(ctx, lfs)
	prefix := filepath.Join(string(filepath.Separator)+"root", "sub")
	fullPath := lfs.Join(prefix, "alpha.txt")

	tests := []struct {
		name  string
		expr  string
		entry file.Info
		want  bool
	}{
		{name: "name match", expr: "name=alpha.txt", entry: f1, want: true},
		{name: "name mismatch", expr: "name=alpha.txt", entry: f2, want: false},
		{name: "full path match with fs", expr: fmt.Sprintf("name='%s'", fullPath), entry: f1, want: true},
		{name: "regexp path match", expr: `re='sub[/\\]beta'`, entry: f2, want: true},
		{name: "type regular match", expr: "type=f", entry: f1, want: true},
		{name: "type dir mismatch", expr: "type=d", entry: f1, want: false},
		{name: "file larger match", expr: "file-larger=200", entry: f2, want: true},
		{name: "file larger mismatch", expr: "file-larger=200", entry: f1, want: false},
		{name: "file smaller match", expr: "file-smaller=200", entry: f1, want: true},
		{name: "newer match", expr: "newer=2026-01-01", entry: f1, want: true},
		{name: "newer mismatch", expr: "newer=2026-12-01", entry: f1, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, err := stats.NewMatcher(p,
				stats.WithFilewalkFS(lfs),
				stats.WithExpression(tc.expr))
			if err != nil {
				t.Fatalf("NewMatcher(%q): %v", tc.expr, err)
			}
			if !m.HasExpression() {
				t.Error("expected HasExpression() == true")
			}
			if got := m.Entry(prefix, &pi, tc.entry); got != tc.want {
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
	p := testutil.NewParser(ctx, lfs)

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
			m, err := stats.NewMatcher(p,
				stats.WithFilewalkFS(lfs),
				stats.WithExpression(tc.expr))
			if err != nil {
				t.Fatalf("NewMatcher(%q): %v", tc.expr, err)
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
	p := testutil.NewParser(ctx, lfs)

	m, err := stats.NewMatcher(p, stats.WithHardlinkHandling(true))
	if err != nil {
		t.Fatal(err)
	}
	if s := m.String(); !strings.Contains(s, "hardlink handling enabled") || strings.Contains(s, "::") {
		t.Errorf("unexpected String(): %q", s)
	}
	x := file.XAttr{Device: 1, FileID: 101}
	if m.IsHardlink(x) {
		t.Error("first reference should not be hardlink")
	}
	if !m.IsHardlink(x) {
		t.Error("second reference should be hardlink")
	}
	if _, err := stats.NewMatcher(p, stats.WithExpression("invalid_op=1")); err == nil {
		t.Error("expected parse error for unknown operand")
	}
	if _, err := stats.NewMatcher(nil, stats.WithExpression("name=alpha")); err == nil {
		t.Error("expected parse error for nil parser with non-empty expression")
	}
	if _, err := stats.NewMatcher(stats.NewParser(ctx, nil), stats.WithExpression("hardlink=missing")); err == nil {
		t.Error("expected parse error for hardlink operand with nil filesystem")
	}

	cur, err := user.Current()
	if err != nil {
		t.Skipf("user.Current: %v", err)
	}
	realParser := stats.NewParser(ctx, lfs)
	if _, err := stats.NewMatcher(realParser, stats.WithExpression(fmt.Sprintf("user='%s'", cur.Username))); err != nil {
		t.Errorf("NewMatcher(user=%s): %v", cur.Username, err)
	}
}

// sizeOperand is an example of a user-defined operand that matches
// entries whose size is exactly the operand's value.
type sizeOperand struct {
	name, value string
	size        int64
}

func (op sizeOperand) Prepare() (boolexpr.Operand, error) {
	n, err := strconv.ParseInt(op.value, 10, 64)
	op.size = n
	return op, err
}

func (op sizeOperand) Eval(v any) bool {
	e, ok := v.(stats.MatchEntry)
	return ok && e.Info().Size() == op.size
}

func (op sizeOperand) Needs(t reflect.Type) bool {
	return t == reflect.TypeFor[stats.MatchEntry]()
}

func (op sizeOperand) Document() string { return op.name + "=<bytes>" }

func (op sizeOperand) String() string { return op.name + "=" + op.value }

func TestMatcherCustomOperand(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pi, f1, f2 := testMatcherFixture()
	p := stats.NewParser(ctx, nil,
		stats.WithOperand("exact-size", func(n, v string) boolexpr.Operand {
			return sizeOperand{name: n, value: v}
		}))
	m, err := stats.NewMatcher(p, stats.WithExpression("exact-size=123 && name=alpha.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !m.Entry("/root/sub", &pi, f1) {
		t.Errorf("expected %v to match", f1.Name())
	}
	if m.Entry("/root/sub", &pi, f2) {
		t.Errorf("expected %v to not match", f2.Name())
	}
	if m.Prefix("/root/sub", &pi) {
		t.Error("custom entry operand should not match a prefix")
	}
	if !p.HasOperand("user") || !p.HasOperand("hardlink") {
		t.Error("default operands should still be registered")
	}
}
