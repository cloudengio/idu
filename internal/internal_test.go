// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"bytes"
	"encoding/gob"
	"testing"
)

type RoundTripper func(*testing.T, *PrefixInfo) PrefixInfo

func GobRoundTrip(t *testing.T, pi *PrefixInfo) PrefixInfo {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(pi); err != nil {
		t.Fatal(err)
	}
	var nfi PrefixInfo
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&nfi); err != nil {
		t.Fatal(err)
	}
	return nfi
}

func BinaryRoundTrip(t *testing.T, pi *PrefixInfo) PrefixInfo {
	buf, err := pi.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var npi PrefixInfo
	if err := npi.UnmarshalBinary(buf); err != nil {
		t.Fatal(err)
	}
	return npi
}
