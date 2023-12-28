// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
)

type RoundTripper func(*testing.T, *T) T

func GobRoundTrip(t *testing.T, pi *T) T {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(pi); err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
	var nfi T
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

func BinaryRoundTrip(t *testing.T, pi *T) T {
	buf, err := pi.MarshalBinary()
	if err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
	var npi T
	if err := npi.UnmarshalBinary(buf); err != nil {

		t.Fatalf("%s: %v", caller(), err)
	}
	return npi
}

func NumUserIDs(pi T) int {
	return len(pi.userIDMap)
}

func NumGroupIDs(pi T) int {
	return len(pi.groupIDMap)
}

func CompareUserIDMap(t *testing.T, pi T, ids []int64, pos []int) {
	t.Helper()
	for j, u := range ids {
		if got, want := pi.userIDMap.idMapFor(u), pos[j]; got != want {
			t.Errorf("id %v: got %v, want %v", u, got, want)
		}
	}
}

func CompareGroupIDMap(t *testing.T, pi T, ids []int64, pos []int) {
	t.Helper()
	for j, u := range ids {
		if got, want := pi.groupIDMap.idMapFor(u), pos[j]; got != want {
			t.Errorf("id %v: got %v, want %v", u, got, want)
		}
	}
}
