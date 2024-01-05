// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats_test

import (
	"context"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/testutil"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file"
)

type sumSizeAndBlocks struct{}

func (sumSizeAndBlocks) Calculate(n, b int64) int64 { return n + b }

func (sumSizeAndBlocks) String() string {
	return "sumSizeAndBlocks"
}

func TestTotals(t *testing.T) {
	modTime := time.Now().Truncate(0)
	var uid, gid int64 = 100, 2

	ug00, ug10, ug01, ug11, ugOther := testutil.TestdataIDCombinationsFiles(modTime, uid, gid, 100)
	ug00d, ug10d, ug01d, ug11d, ugOtherd := testutil.TestdataIDCombinationsDirs(modTime, uid, gid, 200)

	perUserStats := []stats.PerIDTotals{
		{{uid, 2, 1, 1, 4, 8, 1, 0, 0}},
		{{uid, 1, 1, 1, 2, 4, 1, 0, 0}, {uid + 1, 1, 1, 0, 2, 4, 0, 0, 0}},
		{{uid, 2, 1, 1, 4, 8, 1, 0, 0}},
		{{uid, 1, 1, 1, 2, 4, 1, 0, 0}, {uid + 1, 1, 1, 0, 2, 4, 0, 0, 0}},
		{{uid, 0, 1, 2, 1, 2, 1, 0, 0}, {uid + 1, 2, 0, 0, 3, 6, 0, 0, 0}},
	}
	perGroupStats := []stats.PerIDTotals{
		{{gid, 2, 1, 1, 4, 8, 1, 0, 0}},
		{{gid, 2, 1, 1, 4, 8, 1, 0, 0}},
		{{gid, 1, 1, 1, 2, 4, 1, 0, 0}, {gid + 1, 1, 1, 0, 2, 4, 0, 0, 0}},
		{{gid, 1, 1, 1, 2, 4, 1, 0, 0}, {gid + 1, 1, 1, 0, 2, 4, 0, 0, 0}},
		{{gid, 0, 1, 2, 1, 2, 1, 0, 0}, {gid + 1, 2, 0, 0, 3, 6, 0, 0, 0}},
	}

	parser := boolexpr.NewParserTests(context.Background(), nil)

	for i, tc := range []struct {
		fi         []file.Info
		fd         []file.Info
		uids, gids []int64
		perIDStats int
	}{
		{ug00, ug00d, []int64{uid}, []int64{gid}, 0},
		{ug10, ug10d, []int64{uid, uid + 1}, []int64{gid}, 1},
		{ug01, ug01d, []int64{uid}, []int64{gid, gid + 1}, 2},
		{ug11, ug11d, []int64{uid, uid + 1}, []int64{gid, gid + 1}, 3},
		{ugOther, ugOtherd, []int64{uid, uid + 1}, []int64{gid, gid + 1}, 4},
	} {
		if i != 4 {
			continue
		}
		pi := testutil.TestdataNewPrefixInfo("dir", 1, 1, 0700, modTime, uid, gid, 33, 100)
		pi.AppendInfoList(tc.fi)
		pi.AppendInfoList(tc.fd)

		totals, us, gs := stats.ComputeTotals("", &pi, sumSizeAndBlocks{}, boolexpr.AlwaysMatch(parser))

		sort.Slice(us, func(i, j int) bool { return us[i].ID < us[j].ID })
		sort.Slice(gs, func(i, j int) bool { return gs[i].ID < gs[j].ID })

		if got, want := totals, (stats.Totals{Files: 2, Prefix: 1, SubPrefixes: 2, Bytes: 4, StorageBytes: 4 * 2, PrefixBytes: 1}); !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}

		buf, err := totals.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		var nstats stats.Totals
		if err := nstats.UnmarshalBinary(buf); err != nil {
			t.Fatal(err)
		}
		if got, want := nstats, totals; !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}

		for _, ugs := range []stats.PerIDTotals{us, gs} {
			var sum stats.Totals
			for _, s := range ugs {
				sum.Bytes += s.Bytes
				sum.PrefixBytes += s.PrefixBytes
				sum.SubPrefixes += 1
				sum.StorageBytes += s.StorageBytes
				sum.Files += s.Files
			}
			sum.Prefix = 1
			if got, want := sum, totals; !reflect.DeepEqual(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		}

		if got, want := us, perUserStats[tc.perIDStats]; !reflect.DeepEqual(got, want) {
			t.Errorf("%v: got %#v, want %#v", i, got, want)
		}

		if got, want := gs, perGroupStats[tc.perIDStats]; !reflect.DeepEqual(got, want) {
			t.Errorf("%v: got %#v, want %#v", i, got, want)
		}

		if got, want := IDsFromStats(us), tc.uids; !slices.Equal(got, want) {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}

		if got, want := IDsFromStats(gs), tc.gids; !slices.Equal(got, want) {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}
	}
}

func IDsFromStats(s stats.PerIDTotals) []int64 {
	r := []int64{}
	for _, st := range s {
		r = append(r, st.ID)
	}
	return r
}

func computeWithExpression(t *testing.T, prefix string, pi *prefixinfo.T, expr string) (totals stats.Totals, perUser, perGroup stats.PerIDTotals) {
	t.Helper()
	parser := boolexpr.NewParserTests(context.Background(), nil)
	matcher, err := boolexpr.CreateMatcher(parser,
		boolexpr.WithEntryExpression(expr))
	if err != nil {
		t.Fatal(err)
	}
	totals, perUser, perGroup = stats.ComputeTotals(prefix, pi, sumSizeAndBlocks{}, matcher)

	sort.Slice(perUser,
		func(i, j int) bool { return perUser[i].ID < perUser[j].ID })
	sort.Slice(perGroup,
		func(i, j int) bool { return perGroup[i].ID < perGroup[j].ID })
	return totals, perUser, perGroup
}

func testLens(t *testing.T, perUser, perGroup stats.PerIDTotals, nUsers, nGroups int) {
	t.Helper()
	if got, want := len(perUser), nUsers; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := len(perGroup), nGroups; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestTotalsMatch(t *testing.T) {
	modTime := time.Now().Truncate(0)
	var uid, gid int64 = 100, 2

	_, ug01, ug10, _, _ := testutil.TestdataIDCombinationsFiles(modTime, uid, gid, 100)
	_, ug01d, ug10d, _, _ := testutil.TestdataIDCombinationsDirs(modTime, uid, gid, 200)

	pi := testutil.TestdataNewPrefixInfo("my-prefix", 3, 4, 0700, modTime, uid, gid, 33, 100)
	pi.AppendInfoList(ug01)
	pi.AppendInfoList(ug01d)
	pi.AppendInfoList(ug10)
	pi.AppendInfoList(ug10d)

	totals, us, gs := computeWithExpression(t, "my-prefix", &pi, "(user=100||user=101) && (group=2||group=3)")

	if got, want := totals, (stats.Totals{Files: 4, Prefix: 1, SubPrefixes: 4, Bytes: 3 + 6, StorageBytes: 7 + 2 + 4 + 2 + 4, PrefixBytes: 3}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}

	testLens(t, us, gs, 2, 2)

	totalsA := stats.Totals{Files: 3, Prefix: 1, SubPrefixes: 4, Bytes: 3 + 4, StorageBytes: 7 + 2 + 2 + 4, PrefixBytes: 3}
	totalsB := stats.Totals{Files: 1, SubPrefixes: 0, Bytes: 2, StorageBytes: 4, PrefixBytes: 0}

	uid100 := totalsA
	uid100.ID = 100
	uid101 := totalsB
	uid101.ID = 101
	gid2 := totalsA
	gid2.ID = 2
	gid3 := totalsB
	gid3.ID = 3

	if got, want := us[0], uid100; !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}

	if got, want := us[1], uid101; !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}

	if got, want := gs[0], gid2; !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}

	if got, want := gs[1], gid3; !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}

	_, us, gs = computeWithExpression(t, "my-prefix", &pi, "user=100")

	testLens(t, us, gs, 1, 2)

	uid100.SubPrefixes = 3

	if got, want := us[0], uid100; !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}

	gid2User100 := stats.Totals{ID: 2, Files: 2, Prefix: 1, SubPrefixes: 3, Bytes: 3 + 2, StorageBytes: 7 + 2 + 2, PrefixBytes: 3}
	if got, want := gs[0], gid2User100; !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}

	if got, want := gs[1], gid3; !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}

	totals, us, gs = computeWithExpression(t, "my-prefix", &pi, "name=not-there")

	if got, want := totals, (stats.Totals{}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
	testLens(t, us, gs, 0, 0)
}
