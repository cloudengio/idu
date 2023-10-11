// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo_test

import (
	"os"
	"reflect"
	"syscall"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
)

func scanFilesByID(ids prefixinfo.IDSanner) []string {
	n := []string{}
	for ids.Next() {
		n = append(n, ids.Info().Name())
	}
	return n
}

func createPrefixInfo(t *testing.T, now time.Time) prefixinfo.T {
	info := file.NewInfo("0", 3, 0700, now.Truncate(0), &syscall.Stat_t{Uid: 1, Gid: 2})
	pi, err := prefixinfo.New(info)
	if err != nil {
		t.Fatal(err)
	}

	var fl file.InfoList
	fl = append(fl,
		file.NewInfo("0", 1, 0700, now.Truncate(0), &syscall.Stat_t{Uid: 1, Gid: 3}),
		file.NewInfo("1", 1, 0700, now.Add(time.Minute).Truncate(0), &syscall.Stat_t{Uid: 1, Gid: 2}),
	)
	var el filewalk.EntryList
	el = append(el,
		filewalk.Entry{Name: "0", Type: os.ModeDir},
		filewalk.Entry{Name: "1", Type: os.ModeDir},
	)

	pi.AppendInfoList(fl)
	pi.AppendEntries(el)
	return pi
}

func scanUsers(t *testing.T, pi *prefixinfo.T, uid uint32) []string {
	sc, err := pi.UserIDScan(uid)
	if err != nil {
		t.Fatal(err)
	}
	return scanFilesByID(sc)
}

func scanGroups(t *testing.T, pi *prefixinfo.T, gid uint32) []string {
	sc, err := pi.GroupIDScan(gid)
	if err != nil {
		t.Fatal(err)
	}
	return scanFilesByID(sc)
}

func cmpFileInfoList(t *testing.T, got, want file.InfoList) {
	// Can't use reflect.DeepEqual because the SysInfo field is not
	// encoded/decoded.
	if got, want := len(got), len(want); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for i := range got {
		if got, want := got[i].Name(), want[i].Name(); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := got[i].ModTime(), want[i].ModTime(); !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestBinaryEncoding(t *testing.T) {

	pi := createPrefixInfo(t, time.Now())
	if err := pi.Finalize(); err != nil {
		t.Fatal(err)
	}
	uid, gid := pi.UserGroup()

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

		if got, want := npi.Entries(), pi.Entries(); !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		cmpFileInfoList(t, npi.FileInfo(), pi.FileInfo())

		names := scanUsers(t, &pi, 1)
		if got, want := names, []string{"0", "1"}; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		names = scanGroups(t, &pi, 2)
		if got, want := names, []string{"1"}; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		names = scanGroups(t, &pi, 3)
		if got, want := names, []string{"0"}; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

type diskCalc struct{}

func (diskCalc) Calculate(n int64) int64 { return n + 10 }

func (diskCalc) String() string {
	return "plus10"
}

func createPrefixInfoForStats(t *testing.T, extra ...file.Info) prefixinfo.T {
	now := time.Now()
	pi := createPrefixInfo(t, now)
	var fl file.InfoList
	uid, gid := pi.UserGroup()
	fl = append(fl,
		file.NewInfo("2", 3, 0700, now, &syscall.Stat_t{Uid: uid, Gid: gid}),
		file.NewInfo("3", 7, 0700, now, &syscall.Stat_t{Uid: uid, Gid: gid}),
		file.NewInfo("4", 12, 0700|os.ModeDir, now, &syscall.Stat_t{Uid: uid, Gid: gid}),
	)
	pi.AppendInfoList(fl)
	pi.AppendInfoList(extra)
	if err := pi.Finalize(); err != nil {
		t.Fatal(err)
	}
	return pi
}

func TestStatsTotals(t *testing.T) {
	now := time.Now()
	opi := createPrefixInfo(t, now)
	uid, gid := opi.UserGroup()
	pi0 := createPrefixInfoForStats(t,
		file.NewInfo("5", 3, 0700, now, &syscall.Stat_t{Uid: uid, Gid: gid}),
		file.NewInfo("6", 7, 0700, now, &syscall.Stat_t{Uid: uid, Gid: gid}),
		file.NewInfo("7", 12, 0700|os.ModeDir, now, &syscall.Stat_t{Uid: uid, Gid: gid}))

	pi1 := createPrefixInfoForStats(t,
		file.NewInfo("5", 3, 0700, now, &syscall.Stat_t{Uid: uid * 100, Gid: gid * 100}),
		file.NewInfo("6", 7, 0700, now, &syscall.Stat_t{Uid: uid * 101, Gid: gid * 101}),
		file.NewInfo("7", 12, 0700|os.ModeDir, now, &syscall.Stat_t{Uid: uid * 102, Gid: gid * 102}))
	for _, pi := range []prefixinfo.T{pi0, pi1} {
		totals, _, _, err := pi.ComputeStats(diskCalc{})
		if err != nil {
			t.Fatal(err)
		}

		//uid, _ := pi.UserGroup()

		/*
			if got, want := us, (prefixinfo.StatsList{{ID: uid, Files: 4, Prefixes: 1, Bytes: 12, StorageBytes: 52, PrefixBytes: 12}}); !reflect.DeepEqual(got, want) {
				t.Errorf("got %#v, want %#v", got, want)
			}

			if got, want := gs, (prefixinfo.StatsList{
				{ID: 2, Files: 3, Prefixes: 1, Bytes: 11, StorageBytes: 41, PrefixBytes: 12},
				{ID: 3, Files: 1, Prefixes: 0, Bytes: 1, StorageBytes: 11}}); !reflect.DeepEqual(got, want) {
				t.Errorf("got %#v, want %#v", got, want)
			}*/

		if got, want := totals, (prefixinfo.Stats{Files: 6, Prefixes: 2, Bytes: 22, StorageBytes: 22 + 60, PrefixBytes: 24}); !reflect.DeepEqual(got, want) {
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
	}

}

func TestStatsUsers(t *testing.T) {
	now := time.Now()
	opi := createPrefixInfo(t, now)
	uid, gid := opi.UserGroup()
	pi := createPrefixInfoForStats(t,
		file.NewInfo("5", 3, 0700, now, &syscall.Stat_t{Uid: uid, Gid: gid}),
		file.NewInfo("6", 7, 0700, now, &syscall.Stat_t{Uid: uid, Gid: gid}),
		file.NewInfo("7", 12, 0700|os.ModeDir, now, &syscall.Stat_t{Uid: uid, Gid: gid}))
	_, us, gs, err := pi.ComputeStats(diskCalc{})
	if err != nil {
		t.Fatal(err)
	}

	if got, want := us, (prefixinfo.StatsList{{ID: uid, Files: 6, Prefixes: 2, Bytes: 22, StorageBytes: 22 + 60, PrefixBytes: 24}}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}

	if got, want := gs, (prefixinfo.StatsList{
		{ID: 2, Files: 5, Prefixes: 2, Bytes: 21, StorageBytes: 21 + 50, PrefixBytes: 24},
		{ID: 3, Files: 1, Prefixes: 0, Bytes: 1, StorageBytes: 1 + 10}}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}

	buf, err := gs.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var ngs prefixinfo.StatsList
	if err := ngs.UnmarshalBinary(buf); err != nil {
		t.Fatal(err)
	}
	if got, want := ngs, gs; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
