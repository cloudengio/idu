// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal_test

import (
	"fmt"
	"io/fs"
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

func createPrefixInfo(now time.Time) internal.PrefixInfo {
	var fl file.InfoList
	fl = append(fl,
		file.NewInfo("0", 0, 0700, now.Truncate(0), &syscall.Stat_t{Uid: 1, Gid: 3}),
		file.NewInfo("1", 1, 0700, now.Add(time.Minute).Truncate(0), &syscall.Stat_t{Uid: 1, Gid: 2}),
	)
	var el filewalk.EntryList
	el = append(el,
		filewalk.Entry{Name: "0", Type: os.ModeDir},
		filewalk.Entry{Name: "1", Type: os.ModeDir},
	)

	pi := internal.PrefixInfo{
		// NOTE, UserStats and GroupStats are not serialized.
		UserID:   1,
		GroupID:  2,
		Mode:     fs.FileMode(0700),
		ModTime:  time.Now(),
		Children: el,
		Files:    fl,
	}
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

	pi := createPrefixInfo(time.Now())

	for _, fn := range []internal.RoundTripper{
		internal.GobRoundTrip, internal.BinaryRoundTrip,
	} {
		npi := fn(t, &pi)

		if got, want := npi.UserID, pi.UserID; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := npi.GroupID, pi.GroupID; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := npi.Mode, pi.Mode; got != want {
			t.Errorf("got %v, want %v", got, want)
		}

		if got, want := npi.ModTime, pi.ModTime; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}

		if got, want := npi.Children, pi.Children; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		// Can't use reflect.DeepEqual because the SysInfo field is not
		// encoded/decoded.
		if got, want := len(npi.Files), len(pi.Files); got != want {
			t.Errorf("got %v, want %v", got, want)
		}

		for i := range npi.Files {
			if got, want := npi.Files[i].Name(), pi.Files[i].Name(); got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := npi.Files[i].ModTime(), pi.Files[i].ModTime(); got != want {
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
	pi := createPrefixInfo(now)
	var fl file.InfoList
	fl = append(fl,
		file.NewInfo("0", 3, 0700, now, &syscall.Stat_t{Uid: pi.UserID, Gid: pi.GroupID}),
		file.NewInfo("1", 7, 0700, now, &syscall.Stat_t{Uid: pi.UserID, Gid: pi.GroupID}),
	)
	pi.Files = fl

	us, gs := pi.ComputeStats(diskCalc{})
	fmt.Println(us, gs)
	t.Fail()

}

/*
	usl := internal.StatsList{
		{ID: 1, Files: 3, Bytes: 50, StorageBytes: 60},
		{ID: 7, Files: 9, Bytes: 70, StorageBytes: 80},
	}

	gsl := internal.StatsList{
		{ID: 1, Files: 3, Bytes: 5, StorageBytes: 15},
		{ID: 7, Files: 9, Bytes: 11, StorageBytes: 21},
	}*/

//if got, want := nfi.UserStats, pi.UserStats; !reflect.DeepEqual(got, want) {
//	t.Errorf("got %v, want %v", got, want)
//}

//if got, want := nfi.GroupStats, pi.GroupStats; !reflect.DeepEqual(got, want) {
//	t.Errorf("got %v, want %v", got, want)
//}
//UserStats:  usl,
//GroupStats: gsl,
