// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"reflect"
	"syscall"
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

	buf := make([]byte, 0, 100)
	buf, err := idms.appendBinary(buf)
	if err != nil {
		t.Fatal(err)
	}
	var idms2 idMaps
	if _, err := idms2.decodeBinary(buf); err != nil {
		t.Fatal(err)
	}
	if got, want := idms2, idms; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

}

func TestCreateIDMaps(t *testing.T) {
	modtime := time.Now().Truncate(0)
	var fl file.InfoList
	fl = append(fl,
		file.NewInfo("0", 1, 0700, modtime, &syscall.Stat_t{Uid: 1, Gid: 2}),
		file.NewInfo("1", 2, 0700, modtime, &syscall.Stat_t{Uid: 1, Gid: 2}),
		file.NewInfo("2", 4, 0700, modtime, &syscall.Stat_t{Uid: 1, Gid: 2}))

	info := file.NewInfo("dir", 1, 0700, modtime, &syscall.Stat_t{Uid: 1, Gid: 2})
	pi, err := NewPrefixInfo(info)
	if err != nil {
		t.Fatal(err)
	}
	pi.AppendFiles(fl)
	if err := pi.Finalize(); err != nil {
		t.Fatal(err)
	}
	npi := BinaryRoundTrip(t, &pi)

	if npi.userIDMap != nil || npi.groupIDMap != nil {
		t.Errorf("expected idms to be nil: %v, %v", npi.userIDMap, npi.groupIDMap)
	}

	fl = append(fl,
		file.NewInfo("1", 2, 0700, modtime, &syscall.Stat_t{Uid: 4, Gid: 2}),
		file.NewInfo("2", 3, 0700, modtime, &syscall.Stat_t{Uid: 1, Gid: 2}),
		file.NewInfo("3", 4, 0700, modtime, &syscall.Stat_t{Uid: 10, Gid: 11}))

	pi, err = NewPrefixInfo(info)
	if err != nil {
		t.Fatal(err)
	}
	pi.AppendFiles(fl)
	if err := pi.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := pi.Finalize(); err != nil {
		t.Fatal(err)
	}

	npi = BinaryRoundTrip(t, &pi)

	if got, want := len(npi.userIDMap), 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := len(npi.groupIDMap), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := npi.userIDMap.idMapFor(1), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := npi.userIDMap.idMapFor(4), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := npi.userIDMap.idMapFor(10), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	if got, want := npi.groupIDMap.idMapFor(2), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := npi.groupIDMap.idMapFor(11), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
