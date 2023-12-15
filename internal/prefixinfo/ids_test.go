// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo_test

import (
	"fmt"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/testutil"
	"cloudeng.io/file"
)

func TestCreateIDMaps(t *testing.T) {
	modTime := time.Now().Truncate(0)

	var uid, gid uint32 = 100, 1
	ug00, ug10, ug01, ug11, ugOther := testutil.TestdataIDCombinationsFiles(modTime, uid, gid, 100)

	for i, tc := range []struct {
		fi                   []file.Info
		uidMapLen, gidMapLen int
		uidPos, gidPos       []int
		uidMap, gidMap       []uint32
		uidFile, gidFile     []uint32
	}{
		{ug00, 0, 0, []int{-1, -1}, []int{-1, -1},
			[]uint32{uid}, []uint32{gid},
			[]uint32{uid, uid}, []uint32{gid, gid}},
		{ug10, 2, 0, []int{0, 1}, []int{-1, -1},
			[]uint32{uid, uid + 1}, []uint32{gid},
			[]uint32{uid, uid + 1}, []uint32{gid, gid},
		},
		{ug01, 0, 2, []int{-1, -1}, []int{0, 1},
			[]uint32{uid}, []uint32{gid, gid + 1},
			[]uint32{uid, uid}, []uint32{gid, gid + 1}},
		{ug11, 2, 2, []int{0, 1}, []int{0, 1},
			[]uint32{uid, uid + 1}, []uint32{gid, gid + 1},
			[]uint32{uid, uid + 1}, []uint32{gid, gid + 1}},
		{ugOther, 2, 2, []int{1, 1}, []int{1, 1},
			[]uint32{uid + 1, uid + 1}, []uint32{gid + 1, gid + 1},
			[]uint32{uid + 1, uid + 1}, []uint32{gid + 1, gid + 1}},
	} {
		info := testutil.TestdataNewInfo("dir", 1, 2, 0700, time.Now().Truncate(0), uid, gid, 37, 200)
		pi, err := prefixinfo.New("dir", info)
		if err != nil {
			t.Fatal(err)
		}
		pi.AppendInfoList(tc.fi)

		npi := prefixinfo.BinaryRoundTrip(t, &pi)

		if got, want := prefixinfo.NumUserIDs(npi), tc.uidMapLen; got != want {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}

		if got, want := prefixinfo.NumGroupIDs(npi), tc.gidMapLen; got != want {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}

		prefixinfo.CompareUserIDMap(t, npi, tc.uidMap, tc.uidPos)
		prefixinfo.CompareGroupIDMap(t, npi, tc.gidMap, tc.gidPos)

		for j, fi := range npi.InfoList() {
			u, g, dev, ino, blocks := npi.SysInfo(fi)
			if got, want := u, tc.uidFile[j]; got != want {
				t.Errorf("%v: %v: got %v, want %v", i, j, got, want)
			}
			if got, want := g, tc.gidFile[j]; got != want {
				t.Errorf("%v: %v: got %v, want %v", i, j, got, want)
			}
			if got, want := dev, uint64(37); got != want {
				t.Errorf("%v: %v: got %v, want %v", i, j, got, want)
			}
			if got, want := ino, uint64(100); got != want {
				t.Errorf("%v: %v: got %v, want %v", i, j, got, want)
			}
			if got, want := blocks, int64(j+1); got != want {
				t.Errorf("%v: %v: got %v, want %v", i, j, got, want)
			}
		}
	}
}

func TestSysTypes(t *testing.T) {
	var uid, gid uint32 = 100, 1
	modTime := time.Now().Truncate(0)

	info := testutil.TestdataNewInfo("dir", 1, 2, 0700, modTime, uid, gid, 37, 200)
	pi, err := prefixinfo.New("dir", info)
	if err != nil {
		t.Fatal(err)
	}
	ug00, ug10, ug01, ug11, ugOther := testutil.TestdataIDCombinationsFiles(modTime, uid, gid, 100)

	pi.AppendInfoList(ug00)
	_, _, _, _ = ug01, ug10, ug11, ugOther

	npi := prefixinfo.BinaryRoundTrip(t, &pi)
	for _, fi := range npi.InfoList() {
		if got, want := fmt.Sprintf("%T", fi.Sys()), "prefixinfo.fsOnly"; got != want {
			t.Errorf("expected fsOnly, got %T", fi.Sys())
		}
	}

	for _, tc := range [][]file.Info{ug10, ug01, ug11, ugOther} {
		pi, err := prefixinfo.New("dir", info)
		if err != nil {
			t.Fatal(err)
		}
		pi.AppendInfoList(tc)
		npi := prefixinfo.BinaryRoundTrip(t, &pi)
		for _, fi := range npi.InfoList() {
			if got, want := fmt.Sprintf("%T", fi.Sys()), "prefixinfo.idAndFS"; got != want {
				t.Errorf("expected idAndFS, got %T", fi.Sys())
			}
		}
	}
}
