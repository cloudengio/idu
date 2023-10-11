// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal_test

import (
	"os"
	"reflect"
	"syscall"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
)

func scanFilesByID(ids internal.IDSanner) []string {
	n := []string{}
	for ids.Next() {
		n = append(n, ids.Info().Name())
	}
	return n
}

func createPrefixInfo(t *testing.T, now time.Time) internal.PrefixInfo {
	info := file.NewInfo("0", 0, 0700, now.Truncate(0), &syscall.Stat_t{Uid: 1, Gid: 2})
	pi, err := internal.NewPrefixInfo(info)
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

	pi.AppendFiles(fl)
	pi.AppendEntries(el)
	return pi
}

func scanUsers(t *testing.T, pi *internal.PrefixInfo, uid uint32) []string {
	sc, err := pi.UserIDScan(uid)
	if err != nil {
		t.Fatal(err)
	}
	return scanFilesByID(sc)
}

func scanGroups(t *testing.T, pi *internal.PrefixInfo, gid uint32) []string {
	sc, err := pi.GroupIDScan(gid)
	if err != nil {
		t.Fatal(err)
	}
	return scanFilesByID(sc)
}

func TestBinaryEncoding(t *testing.T) {

	pi := createPrefixInfo(t, time.Now())
	if err := pi.Finalize(); err != nil {
		t.Fatal(err)
	}
	uid, gid := pi.UserGroup()

	for _, fn := range []internal.RoundTripper{
		internal.GobRoundTrip, internal.BinaryRoundTrip,
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

		// Can't use reflect.DeepEqual because the SysInfo field is not
		// encoded/decoded.
		if got, want := len(npi.Files()), len(pi.Files()); got != want {
			t.Errorf("got %v, want %v", got, want)
		}

		for i := range npi.Files() {
			if got, want := npi.Files()[i].Name(), pi.Files()[i].Name(); got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := npi.Files()[i].ModTime(), pi.Files()[i].ModTime(); got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		}

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

func TestStats(t *testing.T) {
	now := time.Now()
	pi := createPrefixInfo(t, now)
	var fl file.InfoList
	uid, gid := pi.UserGroup()
	fl = append(fl,
		file.NewInfo("0", 3, 0700, now, &syscall.Stat_t{Uid: uid, Gid: gid}),
		file.NewInfo("1", 7, 0700, now, &syscall.Stat_t{Uid: uid, Gid: gid}),
		file.NewInfo("2", 12, 0700|os.ModeDir, now, &syscall.Stat_t{Uid: uid, Gid: gid}),
	)
	pi.AppendFiles(fl)
	if err := pi.Finalize(); err != nil {
		t.Fatal(err)
	}
	totals, us, gs, err := pi.ComputeStats(diskCalc{})
	if err != nil {
		t.Fatal(err)
	}

	if got, want := us, (internal.StatsList{{ID: uid, Files: 4, Prefixes: 1, Bytes: 12 + 12, StorageBytes: 52 + 12}}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := gs, (internal.StatsList{
		{ID: 2, Files: 3, Prefixes: 1, Bytes: 11 + 12, StorageBytes: 41 + 12},
		{ID: 3, Files: 1, Prefixes: 0, Bytes: 1, StorageBytes: 11}}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := totals, (internal.Stats{Files: 4, Prefixes: 1, Bytes: 12 + 12, StorageBytes: 52 + 12}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
