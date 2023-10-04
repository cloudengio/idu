// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
)

type RoundTripper func(*testing.T, *PrefixInfo) PrefixInfo

func GobRoundTrip(t *testing.T, pi *PrefixInfo) PrefixInfo {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(pi); err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
	var nfi PrefixInfo
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&nfi); err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
	return nfi
}

func caller() string {
	_, file, line, _ := runtime.Caller(2)
	return fmt.Sprintf("%s:%v", filepath.Base(file), line)
}

func BinaryRoundTrip(t *testing.T, pi *PrefixInfo) PrefixInfo {
	buf, err := pi.MarshalBinary()
	if err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
	var npi PrefixInfo
	if err := npi.UnmarshalBinary(buf); err != nil {

		t.Fatalf("%s: %v", caller(), err)
	}
	return npi
}
