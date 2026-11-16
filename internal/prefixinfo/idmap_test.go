// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
)

func testIDMapScanner(t *testing.T, positions ...int) {
	idm := newIDMap(3, 257)
	for _, p := range positions {
		idm.set(p)
	}
	sc := newIDMapScanner(idm)
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

	if s := idms.String(); !strings.Contains(s, "  1:") || !strings.Contains(s, "  2:") {
		t.Errorf("unexpected idMaps.String(): %q", s)
	}
	if id, ok := idms.idForPos(5); ok || id != 0 {
		t.Errorf("idForPos(5) = (%d, %v), want (0, false)", id, ok)
	}
}

func TestIDMapScannerExhaustionAndCorruptDecode(t *testing.T) {
	t.Parallel()

	idm := newIDMap(7, 64)
	idm.set(2)
	idm.set(10)
	sc := newIDMapScanner(idm)
	for sc.next() {
	}
	if sc.next() {
		t.Fatal("idMapScanner.next() restarted iteration after exhaustion")
	}

	// idMap.isSet must not panic on out-of-range position or empty Pos.
	var empty idMap
	if empty.isSet(0) || empty.isSet(-1) {
		t.Error("empty idMap.isSet returned true")
	}

	// Empty / truncated / overflowed / forged-length inputs must return error without panicking.
	cases := [][]byte{
		nil,
		{0x80},                             // truncated varint
		bytes.Repeat([]byte{0xff}, 12),     // overflowed varint
		binary.AppendUvarint(nil, 100_000), // forged length with no remaining bytes
		{0x01, 0x00, 0x05},                 // idms count=1, uid=0, Pos len=5, truncated Pos words
	}
	for i, c := range cases {
		var dec idMaps
		if _, err := dec.decodeBinary(c); err == nil {
			t.Errorf("case %d (%x): expected decodeBinary error, got nil", i, c)
		}
	}

	// l == 0 must reset an existing non-empty idMaps slice.
	existing := idMaps{newIDMap(1, 64)}
	if _, err := existing.decodeBinary([]byte{0x00}); err != nil {
		t.Fatal(err)
	}
	if len(existing) != 0 {
		t.Errorf("expected empty idMaps after decoding l=0, got %d", len(existing))
	}
}
