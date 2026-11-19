// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo_test

import (
	"fmt"
	"io/fs"
	"reflect"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/testutil"
	"cloudeng.io/file"
)

func newInfoWithLinks(name string, mode fs.FileMode, uid int64, ino, links uint64) file.Info {
	return file.NewInfo(name, 10, mode, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		&file.XAttr{UID: uid, GID: 20, Device: 1, FileID: ino, Blocks: 1, Hardlinks: links})
}

func newPrefixWithLinks(entries ...file.Info) prefixinfo.T {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	pi := testutil.TestdataNewPrefixInfo("dir", 1, 2, fs.ModeDir|0o700, now, 10, 20, 1, 100)
	pi.AppendInfoList(entries)
	return pi
}

func linksOf(pi prefixinfo.T) []uint64 {
	var links []uint64
	for _, fi := range pi.InfoList() {
		links = append(links, pi.XAttrInfo(fi).Hardlinks)
	}
	return links
}

func TestLinkCounts(t *testing.T) {
	t.Parallel()
	for i, tc := range []struct {
		links []uint64 // for f0, d1 (a directory), f2, f3, f4
		want  []uint64
	}{
		// Directories are never recorded.
		{[]uint64{1, 2, 1, 1, 1}, []uint64{1, 0, 1, 1, 1}},
		{[]uint64{3, 5, 1, 2, 1}, []uint64{3, 0, 1, 2, 1}},
		{[]uint64{2, 2, 3, 4, 2}, []uint64{2, 0, 3, 4, 2}},
		// Unknown, ie. zero, link counts.
		{[]uint64{0, 2, 0, 0, 1}, []uint64{0, 0, 0, 0, 1}},
		{[]uint64{0, 0, 1, 0, 1}, []uint64{0, 0, 1, 0, 1}},
	} {
		var uid int64 = 10
		if i%2 == 1 {
			uid = 11 // exercise the idAndFS encoding
		}
		pi := newPrefixWithLinks(
			newInfoWithLinks("f0", 0o600, uid, 200, tc.links[0]),
			newInfoWithLinks("d1", fs.ModeDir|0o700, 10, 201, tc.links[1]),
			newInfoWithLinks("f2", 0o600, 10, 202, tc.links[2]),
			newInfoWithLinks("f3", 0o600, uid, 203, tc.links[3]),
			newInfoWithLinks("f4", 0o600, 10, 204, tc.links[4]),
		)
		for _, rt := range []prefixinfo.RoundTripper{prefixinfo.BinaryRoundTrip, prefixinfo.GobRoundTrip} {
			npi := rt(t, &pi)
			if got := linksOf(npi); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("%v: got %v, want %v", i, got, tc.want)
			}
			// The link counts survive re-encoding a decoded prefix, as is
			// done for unchanged prefixes.
			reused := newPrefixWithLinks()
			reused.SetInfoList(npi.FilesOnly())
			rpi := rt(t, &reused)
			var wantFiles []uint64
			for j, l := range tc.want {
				if j != 1 {
					wantFiles = append(wantFiles, l)
				}
			}
			if got := linksOf(rpi); !reflect.DeepEqual(got, wantFiles) {
				t.Errorf("%v: reused: got %v, want %v", i, got, wantFiles)
			}
		}
	}
}

func TestLinkCountsSize(t *testing.T) {
	t.Parallel()
	var entries []file.Info
	for i := range 100 {
		entries = append(entries, newInfoWithLinks(fmt.Sprintf("f%v", i), 0o600, 10, uint64(200+i), 1))
	}
	single := newPrefixWithLinks(entries...)
	entries[50] = newInfoWithLinks("f50", 0o600, 10, 250, 2)
	linked := newPrefixWithLinks(entries...)
	for _, tc := range []struct {
		pi       prefixinfo.T
		overhead int
	}{
		{single, 2},     // the default and the number of exceptions
		{linked, 2 + 2}, // plus the position and count of one file
	} {
		v3, err := tc.pi.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		v2 := prefixinfo.MarshalV2(t, &tc.pi)
		if got, want := len(v3)-len(v2), tc.overhead; got != want {
			t.Errorf("got %v bytes for the link counts, want %v", got, want)
		}
	}
}

func TestLinkCountsVersion2(t *testing.T) {
	t.Parallel()
	pi := newPrefixWithLinks(
		newInfoWithLinks("f0", 0o600, 10, 200, 1),
		newInfoWithLinks("f1", 0o600, 11, 201, 3),
	)
	var npi prefixinfo.T
	if err := npi.UnmarshalBinary(prefixinfo.MarshalV2(t, &pi)); err != nil {
		t.Fatal(err)
	}
	if got, want := linksOf(npi), []uint64{0, 0}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := npi.XAttrInfo(npi.InfoList()[1]).FileID, uint64(201); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCorruptLinkCounts(t *testing.T) {
	t.Parallel()
	pi := newPrefixWithLinks(
		newInfoWithLinks("f0", 0o600, 10, 200, 2),
		newInfoWithLinks("d1", fs.ModeDir|0o700, 10, 201, 2),
		newInfoWithLinks("f2", 0o600, 10, 202, 1),
	)
	v2 := prefixinfo.MarshalV2(t, &pi)
	v3 := append([]byte{}, v2...)
	v3[0] = 0x3
	for name, trailer := range map[string][]byte{
		"missing":      nil,
		"too-many":     {1, 4},
		"truncated":    {1, 1, 0},
		"out-of-range": {1, 1, 3, 2},
		"directory":    {1, 1, 1, 2},
		"repeated":     {1, 2, 0, 2, 0, 3},
	} {
		assertUnmarshalFailsNoPanic(t, name, append(append([]byte{}, v3...), trailer...))
	}
}
