// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo_test

import (
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file"
)

func scanFilesByID(ids prefixinfo.IDSanner) []string {
	n := []string{}
	for ids.Next() {
		n = append(n, ids.Info().Name())
	}
	return n
}

func scanUsers(t *testing.T, pi *prefixinfo.T, uid uint32) []string {
	sc, err := pi.UserIDScan(uid)
	if err != nil {
		t.Fatal(err)
	}
	return scanFilesByID(sc)
}

func scanAndMatchUsers(t *testing.T, pi *prefixinfo.T, uid uint32, want []string) {
	_, _, l, _ := runtime.Caller(1)
	if want == nil {
		_, err := pi.UserIDScan(uid)
		if err == nil || err.Error() != fmt.Sprintf("no such user id: %v", uid) {
			t.Errorf("line %v: missing or unexpected error: %v", l, err)
		}
		return
	}
	names := scanUsers(t, pi, uid)
	if got := names; !reflect.DeepEqual(got, want) {
		t.Errorf("line %v: got %v, want %v", l, got, want)
	}
}

func scanGroups(t *testing.T, pi *prefixinfo.T, gid uint32) []string {
	sc, err := pi.GroupIDScan(gid)
	if err != nil {
		t.Fatal(err)
	}
	return scanFilesByID(sc)
}

func scanAndMatchGroups(t *testing.T, pi *prefixinfo.T, gid uint32, want []string) {
	_, _, l, _ := runtime.Caller(1)
	if want == nil {
		_, err := pi.GroupIDScan(gid)
		if err == nil || err.Error() != fmt.Sprintf("no such group id: %v", gid) {
			t.Errorf("line %v: missing or unexpected error: %v", l, err)
		}
		return
	}
	names := scanGroups(t, pi, gid)
	if got := names; !reflect.DeepEqual(got, want) {
		t.Errorf("line %v: got %v, want %v", l, got, want)
	}
}

func cmpInfoList(t *testing.T, npi prefixinfo.T, got, want file.InfoList) {
	// Can't use reflect.DeepEqual because the SysInfo field is not
	// encoded/decoded.
	if got, want := len(got), len(want); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for i, g := range got {
		if got, want := g.Name(), want[i].Name(); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := g.ModTime(), want[i].ModTime(); !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}

		if got, want := g.Size(), want[i].Size(); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := g.Mode(), want[i].Mode(); got != want {
			t.Errorf("got %v, want %v", got, want)
		}

	}
}

func TestBinaryEncoding(t *testing.T) {

	modTime := time.Now().Truncate(0)
	var uid, gid uint32 = 100, 2

	ug00, ug10, ug01, ug11, ugOther := prefixinfo.TestdataIDCombinationsFiles(modTime, uid, gid)

	for _, tc := range []struct {
		fi           []file.Info
		uids, gids   []string
		uid1s, gid1s []string
	}{
		{ug00, []string{"0", "1"}, []string{"0", "1"}, nil, nil},
		{ug10, []string{"0"}, []string{"0", "1"}, []string{"1"}, nil},
		{ug01, []string{"0", "1"}, []string{"0"}, nil, []string{"1"}},
		{ug11, []string{"0"}, []string{"0"}, []string{"1"}, []string{"1"}},
		{ugOther, []string{}, []string{}, []string{"0", "1"}, []string{"0", "1"}},
	} {
		pi := prefixinfo.TestdataNewPrefixInfo(t, "dir", 1, 0700, modTime, uid, gid)
		pi.AppendInfoList(tc.fi)
		if err := pi.Finalize(); err != nil {
			t.Fatal(err)
		}
		for _, fn := range []prefixinfo.RoundTripper{
			prefixinfo.GobRoundTrip, prefixinfo.BinaryRoundTrip,
		} {
			npi := fn(t, &pi)

			if !pi.Unchanged(npi) {
				t.Errorf("prefix info is changed")
			}

			nuid, ngid := npi.UserGroup()
			if got, want := uid, nuid; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := gid, ngid; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := npi.Mode(), pi.Mode(); got != want {
				t.Errorf("got %v, want %v", got, want)
			}

			if got, want := npi.ModTime(), pi.ModTime(); !got.Equal(want) {
				t.Errorf("got %v, want %v", got, want)
			}

			cmpInfoList(t, npi, npi.InfoList(), pi.InfoList())

			scanAndMatchUsers(t, &pi, uid, tc.uids)

			scanAndMatchUsers(t, &pi, uid+1, tc.uid1s)

			scanAndMatchGroups(t, &pi, gid, tc.gids)

			scanAndMatchGroups(t, &pi, gid+1, tc.gid1s)

		}
	}
}

type times2 struct{}

func (times2) Calculate(n int64) int64 { return n * 2 }

func (times2) String() string {
	return "plus10"
}

func TestStats(t *testing.T) {
	modTime := time.Now().Truncate(0)
	var uid, gid uint32 = 100, 2

	ug00, ug10, ug01, ug11, ugOther := prefixinfo.TestdataIDCombinationsFiles(modTime, uid, gid)
	ug00d, ug10d, ug01d, ug11d, ugOtherd := prefixinfo.TestdataIDCombinationsDirs(modTime, uid, gid)

	perUserStats := []prefixinfo.StatsList{
		{{uid, 2, 2, 3, 6, 3}},
		{{uid, 1, 1, 1, 2, 1}, {uid + 1, 1, 1, 2, 4, 2}},
		{{uid, 2, 2, 3, 6, 3}},
		{{uid, 1, 1, 1, 2, 1}, {uid + 1, 1, 1, 2, 4, 2}},
		{{uid + 1, 2, 2, 3, 6, 3}},
	}
	perGroupStats := []prefixinfo.StatsList{
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
		pi := prefixinfo.TestdataNewPrefixInfo(t, "dir", 1, 0700, modTime, uid, gid)
		pi.AppendInfoList(tc.fi)
		pi.AppendInfoList(tc.fd)
		if err := pi.Finalize(); err != nil {
			t.Fatal(err)
		}

		totals, us, gs, err := pi.ComputeStats(times2{})
		if err != nil {
			t.Fatal(err)
		}

		if got, want := totals, (prefixinfo.Stats{Files: 2, Prefixes: 2, Bytes: 3, StorageBytes: 3 * 2, PrefixBytes: 3}); !reflect.DeepEqual(got, want) {
			t.Errorf("got %#v, want %#v", got, want)
		}

		buf, err := totals.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		var nstats prefixinfo.Stats
		if err := nstats.UnmarshalBinary(buf); err != nil {
			t.Fatal(err)
		}
		if got, want := nstats, totals; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		for _, ugs := range []prefixinfo.StatsList{us, gs} {
			var sum prefixinfo.Stats
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

		if got, want := prefixinfo.IDsFromStats(us), tc.uids; !slices.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := prefixinfo.IDsFromStats(gs), tc.gids; !slices.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}

}
