// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo_test

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/testutil"
	"cloudeng.io/file"
)

func scanFilesByID(ids prefixinfo.IDSanner) []string {
	n := []string{}
	for ids.Next() {
		n = append(n, ids.Info().Name())
	}
	return n
}

func scanUsers(t *testing.T, pi *prefixinfo.T, uid int64) []string {
	sc, err := pi.UserIDScan(uid)
	if err != nil {
		t.Fatal(err)
	}
	return scanFilesByID(sc)
}

func scanAndMatchUsers(t *testing.T, pi *prefixinfo.T, uid int64, want []string) {
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

func scanGroups(t *testing.T, pi *prefixinfo.T, gid int64) []string {
	sc, err := pi.GroupIDScan(gid)
	if err != nil {
		t.Fatal(err)
	}
	return scanFilesByID(sc)
}

func scanAndMatchGroups(t *testing.T, pi *prefixinfo.T, gid int64, want []string) {
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
	var uid, gid int64 = 100, 2

	ug00, ug10, ug01, ug11, ugOther := testutil.TestdataIDCombinationsFiles(modTime, uid, gid, 100)

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
		pi := testutil.TestdataNewPrefixInfo("dir", 1, 2, 0700, modTime, uid, gid, 33, 200)
		pi.AppendInfoList(tc.fi)

		expectedDevice, _ := pi.DevIno()
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

			if got, want := npi.Blocks(), pi.Blocks(); got != want {
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

			dev, ino := npi.DevIno()
			if got, want := dev, expectedDevice; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := ino, uint64(200); got != want {
				t.Errorf("got %v, want %v", got, want)
			}

			for j, fi := range npi.InfoList() {
				xattr := pi.XAttrInfo(fi)
				if got, want := xattr.Device, expectedDevice; got != want {
					t.Errorf("got %v, want %v", got, want)
				}
				if got, want := xattr.FileID, uint64(100); got != want {
					t.Errorf("got %v, want %v", got, want)
				}
				if got, want := xattr.Blocks, int64(j+1); got != want {
					t.Errorf("got %v, want %v", got, want)
				}

			}
		}
	}
}
