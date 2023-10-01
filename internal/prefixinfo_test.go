// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal_test

import (
	"bytes"
	"encoding/gob"
	"io/fs"
	"os"
	"reflect"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
)

func gobRoundTrip(t *testing.T, pi *internal.PrefixInfo) internal.PrefixInfo {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(pi); err != nil {
		t.Fatal(err)
	}
	var nfi internal.PrefixInfo
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&nfi); err != nil {
		t.Fatal(err)
	}
	return nfi
}

func binaryRoundTrip(t *testing.T, pi *internal.PrefixInfo) internal.PrefixInfo {
	buf, err := pi.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var npi internal.PrefixInfo
	if err := npi.UnmarshalBinary(buf); err != nil {
		t.Fatal(err)
	}
	return npi
}

func TestBinaryEncoding(t *testing.T) {
	now := time.Now()

	var fl file.InfoList
	fl = append(fl,
		file.NewInfo("0", 0, 0700, now.Truncate(0), nil),
		file.NewInfo("1", 1, 0700, now.Add(time.Minute).Truncate(0), nil),
	)
	var el filewalk.EntryList
	el = append(el,
		filewalk.Entry{Name: "0", Type: os.ModeDir},
		filewalk.Entry{Name: "1", Type: os.ModeDir},
	)

	usl := internal.StatsList{
		{ID: 1, Files: 3, Dirs: 4, Bytes: 5, StorageBytes: 6},
		{ID: 7, Files: 9, Dirs: 10, Bytes: 11, StorageBytes: 12},
	}

	gsl := internal.StatsList{
		{ID: 100, Files: 3, Dirs: 4, Bytes: 5, StorageBytes: 6},
		{ID: 700, Files: 9, Dirs: 10, Bytes: 11, StorageBytes: 12},
	}

	pi := internal.PrefixInfo{
		UserID:     1,
		GroupID:    2,
		UserStats:  usl,
		GroupStats: gsl,
		Mode:       fs.FileMode(0700),
		ModTime:    time.Now(),
		Children:   el,
		Files:      fl,
	}

	type roundTripper func(*testing.T, *internal.PrefixInfo) internal.PrefixInfo

	for _, fn := range []roundTripper{
		gobRoundTrip, binaryRoundTrip,
	} {
		nfi := fn(t, &pi)
		if got, want := nfi.UserID, pi.UserID; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := nfi.GroupID, pi.GroupID; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := nfi.Mode, pi.Mode; got != want {
			t.Errorf("got %v, want %v", got, want)
		}

		if got, want := nfi.ModTime, pi.ModTime; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}

		if got, want := nfi.UserStats, pi.UserStats; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		if got, want := nfi.GroupStats, pi.GroupStats; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}

		if got, want := nfi.Children, pi.Children; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := nfi.Files, pi.Files; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
