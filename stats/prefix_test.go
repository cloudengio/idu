// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats_test

import (
	"context"
	"fmt"
	"io/fs"
	"reflect"
	"strconv"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmdutil/boolexpr"
	"cloudeng.io/file"
)

// minFiles is a custom operand that matches prefixes with at least n files
// and the entries within such prefixes, it uses stats.PrefixInfo methods
// that are not used by any of the builtin operands.
type minFiles struct {
	text string
	n    int
	uid  int64
}

func (o minFiles) Prepare() (boolexpr.Operand, error) {
	n, err := strconv.Atoi(o.text)
	o.n = n
	return o, err
}

func (o minFiles) matches(pi stats.PrefixInfo) bool {
	uid, _ := pi.UserGroup()
	return len(pi.FilesOnly()) >= o.n && uid == o.uid && len(pi.PrefixesOnly()) > 0
}

func (o minFiles) Eval(v any) bool {
	switch v := v.(type) {
	case stats.MatchPrefix:
		return o.matches(v)
	case stats.MatchEntry:
		return o.matches(v.Prefix())
	}
	return false
}

func (o minFiles) Needs(reflect.Type) bool { return false }
func (o minFiles) Document() string        { return "min-files=<n>" }
func (o minFiles) String() string          { return fmt.Sprintf("min-files=%v", o.text) }

func TestCustomOperand(t *testing.T) {
	now := time.Now()
	pi := prefixinfo.New("/p", file.NewInfo("p", 0, fs.ModeDir, now, prefixinfo.NewSysInfo(1, 2, 3, 4, 5)))
	pi.AppendInfo(file.NewInfo("f1", 0, 0, now, prefixinfo.NewSysInfo(1, 2, 3, 5, 1)))
	pi.AppendInfo(file.NewInfo("f2", 0, 0, now, prefixinfo.NewSysInfo(1, 2, 3, 6, 1)))
	pi.AppendInfo(file.NewInfo("d", 0, fs.ModeDir, now, prefixinfo.NewSysInfo(1, 2, 3, 7, 1)))

	parser := stats.NewParser(context.Background(), nil,
		stats.WithOperand("min-files", func(_, v string) boolexpr.Operand {
			return minFiles{text: v, uid: 1}
		}))
	for _, tc := range []struct {
		expr string
		want bool
	}{
		{"min-files=2", true},
		{"min-files=3", false},
	} {
		m, err := stats.NewMatcher(parser, stats.WithExpression(tc.expr))
		if err != nil {
			t.Fatal(err)
		}
		if got := m.Prefix("/p", &pi); got != tc.want {
			t.Errorf("%v: prefix: got %v, want %v", tc.expr, got, tc.want)
		}
		for _, fi := range pi.InfoList() {
			if got := m.Entry("/p", &pi, fi); got != tc.want {
				t.Errorf("%v: %v: got %v, want %v", tc.expr, fi.Name(), got, tc.want)
			}
		}
	}

	if totals, _, _ := stats.ComputeTotals("/p", nil, nil, stats.AlwaysMatch()); totals != (stats.Totals{}) {
		t.Errorf("ComputeTotals(nil): got %+v, want zero", totals)
	}
	if totals, _, _ := stats.ComputeTotals("/p", &pi, nil, stats.AlwaysMatch()); totals.Files != 2 || totals.SubPrefixes != 1 {
		t.Errorf("ComputeTotals(nil calc): got %+v", totals)
	}
}
