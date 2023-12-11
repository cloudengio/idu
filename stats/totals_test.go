// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats_test

import (
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

type times2 struct{}

func (times2) Calculate(n int64) int64 { return n * 2 }

func (times2) String() string {
	return "plus10"
}

type alwaystrue struct{}

func (alwaystrue) Eval(pi *prefixinfo.T, fi *file.Info) bool { return true }

func TestTotals(t *testing.T) {
	modTime := time.Now().Truncate(0)
	var uid, gid uint32 = 100, 2

	ug00, ug10, ug01, ug11, ugOther := testutil.TestdataIDCombinationsFiles(modTime, uid, gid, 100)
	ug00d, ug10d, ug01d, ug11d, ugOtherd := testutil.TestdataIDCombinationsDirs(modTime, uid, gid, 200)

	perUserStats := []stats.PerIDTotals{
		{{uid, 2, 2, 3, 6, 3}},
		{{uid, 1, 1, 1, 2, 1}, {uid + 1, 1, 1, 2, 4, 2}},
		{{uid, 2, 2, 3, 6, 3}},
		{{uid, 1, 1, 1, 2, 1}, {uid + 1, 1, 1, 2, 4, 2}},
		{{uid + 1, 2, 2, 3, 6, 3}},
	}
	perGroupStats := []stats.PerIDTotals{
		{{gid, 2, 2, 3, 6, 3}},
		{{gid, 2, 2, 3, 6, 3}},
		{{gid, 1, 1, 1, 2, 1}, {gid + 1, 1, 1, 2, 4, 2}},
		{{gid, 1, 1, 1, 2, 1}, {gid + 1, 1, 1, 2, 4, 2}},
		{{gid + 1, 2, 2, 3, 6, 3}},
	}

	for _, tc := range []struct {
		fi         []file.Info
		fd         []file.Info
		uids, gids []uint32
		perIDStats int
	}{
		{ug00, ug00d, []uint32{uid}, []uint32{gid}, 0},
		{ug10, ug10d, []uint32{uid, uid + 1}, []uint32{gid}, 1},
		{ug01, ug01d, []uint32{uid}, []uint32{gid, gid + 1}, 2},
		{ug11, ug11d, []uint32{uid, uid + 1}, []uint32{gid, gid + 1}, 3},
		{ugOther, ugOtherd, []uint32{uid + 1}, []uint32{gid + 1}, 4},
	} {
		pi := testutil.TestdataNewPrefixInfo("dir", 1, 0700, modTime, uid, gid, 33, 100)
		pi.AppendInfoList(tc.fi)
		pi.AppendInfoList(tc.fd)

		totals, us, gs := stats.ComputeTotals("", &pi, times2{}, boolexpr.AlwaysTrue{})

		sort.Slice(us, func(i, j int) bool { return us[i].ID < us[j].ID })
		sort.Slice(gs, func(i, j int) bool { return gs[i].ID < gs[j].ID })

		if got, want := totals, (stats.Totals{Files: 2, Prefixes: 2, Bytes: 3, StorageBytes: 3 * 2, PrefixBytes: 3}); !reflect.DeepEqual(got, want) {
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
			t.Errorf("got %v, want %v", got, want)
		}

		for _, ugs := range []stats.PerIDTotals{us, gs} {
			var sum stats.Totals
			for _, s := range ugs {
				sum.Bytes += s.Bytes
				sum.PrefixBytes += s.PrefixBytes
				sum.Prefixes += s.Prefixes
				sum.StorageBytes += s.StorageBytes
				sum.Files += s.Files
			}
			if got, want := sum, totals; !reflect.DeepEqual(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		}

		if got, want := us, perUserStats[tc.perIDStats]; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		if got, want := gs, perGroupStats[tc.perIDStats]; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		if got, want := IDsFromStats(us), tc.uids; !slices.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		if got, want := IDsFromStats(gs), tc.gids; !slices.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func IDsFromStats(s stats.PerIDTotals) []uint32 {
	r := []uint32{}
	for _, st := range s {
		r = append(r, st.ID)
	}
	return r
}
