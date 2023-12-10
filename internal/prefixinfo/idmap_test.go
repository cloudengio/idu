// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"cloudeng.io/file"
)

func testIDMapScanner(t *testing.T, positions ...int) {
	idm := newIDMap(3, 257)
	for _, p := range positions {
		idm.set(p)
	}
	sc := newIdMapScanner(idm)
	var idx []int
	for sc.next() {
		idx = append(idx, sc.pos())
	}
	if got, want := idx, positions; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestIDMapScan(t *testing.T) {
	idm := newIDMap(5, 64*2+1)

	hasVals := func(vals ...uint64) {
		if got, want := idm.Pos, vals; !reflect.DeepEqual(got, want) {
			t.Errorf("got %b, want %b", got, want)
		}
	}

	set := func(val int) {
		idm.set(val)
		if got, want := idm.isSet(val), true; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	set(0)
	hasVals(1, 0, 0)
	set(63)
	hasVals(1<<63|1, 0, 0)
	set(64)
	hasVals(1<<63|1, 1, 0)
	set(127)
	hasVals(1<<63|1, 1<<63|1, 0)
	set(130)
	hasVals(1<<63|1, 1<<63|1, 0x4)

	if idm.isSet(33) {
		t.Errorf("expected 33 to not be set")
	}

	testIDMapScanner(t)
	testIDMapScanner(t, 0)
	testIDMapScanner(t, 63)
	testIDMapScanner(t, 64)
	testIDMapScanner(t, 127)
	testIDMapScanner(t, 0, 5, 63, 64, 99, 256)

}

func TestIDMaps(t *testing.T) {
	var idms idMaps
	idms = append(idms, newIDMap(1, 64), newIDMap(2, 64))

	if got, want := idms.idMapFor(1), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := idms.idMapFor(2), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := idms.idMapFor(4), -1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	var buf bytes.Buffer
	idms.appendBinary(&buf)
	var idms2 idMaps
	if _, err := idms2.decodeBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	if got, want := idms2, idms; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

}

func TestCreateIDMaps(t *testing.T) {
	modTime := time.Now().Truncate(0)

	var uid, gid uint32 = 100, 1
	ug00, ug10, ug01, ug11, ugOther := TestdataIDCombinationsFiles(modTime, uid, gid, 100)

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
		info := TestdataNewInfo("dir", 1, 0700, time.Now().Truncate(0), uid, gid, 37, 200)
		pi := New(info)
		pi.AppendInfoList(tc.fi)

		npi := BinaryRoundTrip(t, &pi)

		if got, want := len(npi.userIDMap), tc.uidMapLen; got != want {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}

		if got, want := len(npi.groupIDMap), tc.gidMapLen; got != want {
			t.Errorf("%v: got %v, want %v", i, got, want)
		}

		for j, u := range tc.uidMap {
			if got, want := npi.userIDMap.idMapFor(u), tc.uidPos[j]; got != want {
				t.Errorf("%v: id %v: got %v, want %v", i, u, got, want)
			}
		}

		for j, g := range tc.gidMap {
			if got, want := npi.groupIDMap.idMapFor(g), tc.gidPos[j]; got != want {
				t.Errorf("%v: id %v: got %v, want %v", i, g, got, want)
			}
		}

		for j, fi := range npi.InfoList() {
			u, g, dev, ino := npi.SysInfo(fi)
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
		}
	}
}

func TestSysTypes(t *testing.T) {
	var uid, gid uint32 = 100, 1
	modTime := time.Now().Truncate(0)

	info := TestdataNewInfo("dir", 1, 0700, modTime, uid, gid, 37, 200)
	pi := New(info)
	ug00, ug10, ug01, ug11, ugOther := TestdataIDCombinationsFiles(modTime, uid, gid, 100)

	pi.AppendInfoList(ug00)
	_, _, _, _ = ug01, ug10, ug11, ugOther

	npi := BinaryRoundTrip(t, &pi)
	for _, fi := range npi.InfoList() {
		if _, ok := fi.Sys().(inoOnly); !ok {
			t.Errorf("expected inoOnly, got %T", fi.Sys())
		}
	}

	for _, tc := range [][]file.Info{ug10, ug01, ug11, ugOther} {
		pi := New(info)
		pi.AppendInfoList(tc)
		npi := BinaryRoundTrip(t, &pi)
		for _, fi := range npi.InfoList() {
			if _, ok := fi.Sys().(idAndIno); !ok {
				t.Errorf("expected idAndIno, got %T", fi.Sys())
			}
		}
	}
}
