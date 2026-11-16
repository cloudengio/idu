// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo_test

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
)

func TestEncoding(t *testing.T) {
	sl := prefixinfo.StatsList{
		{ID: 1, Files: 3, Bytes: 5, StorageBytes: 6},
		{ID: 7, Files: 9, Bytes: 11, StorageBytes: 12},
	}

	buf, err := sl.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	sl2 := prefixinfo.StatsList{}
	if err := sl2.UnmarshalBinary(buf); err != nil {
		t.Fatal(err)
	}
	if got, want := sl, sl2; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestStatsBinaryRoundTrip(t *testing.T) {
	t.Parallel()
	in := prefixinfo.Stats{
		ID:           42,
		Files:        10,
		Prefixes:     2,
		Bytes:        1024,
		StorageBytes: 4096,
		PrefixBytes:  256,
	}
	data, err := in.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	var out prefixinfo.Stats
	if err := out.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if out != in {
		t.Errorf("got %+v, want %+v", out, in)
	}
}

func TestStatsDecodeErrors(t *testing.T) {
	t.Parallel()
	overflow := bytes.Repeat([]byte{0xff}, 12)
	hugeList := binary.AppendUvarint(nil, 100_000)

	for _, bad := range [][]byte{nil, {0x01, 0x02}, overflow} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Stats.UnmarshalBinary(%x) panicked: %v", bad, r)
				}
			}()
			var s prefixinfo.Stats
			if err := s.UnmarshalBinary(bad); err == nil {
				t.Errorf("Stats.UnmarshalBinary(%x) = nil, want error", bad)
			}
			_ = s.DecodeBinary(bad)
		}()
	}

	for _, bad := range [][]byte{nil, {0x02, 0x01}, overflow, hugeList} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("StatsList.UnmarshalBinary(%x) panicked: %v", bad, r)
				}
			}()
			var sl prefixinfo.StatsList
			if err := sl.UnmarshalBinary(bad); err == nil {
				t.Errorf("StatsList.UnmarshalBinary(%x) = nil, want error", bad)
			}
			_ = sl.DecodeBinary(bad)
		}()
	}
}
