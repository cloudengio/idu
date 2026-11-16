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
	"time"

	"cloudeng.io/file"
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

func TestValidateIDMapsAndXAttrPanic(t *testing.T) {
	t.Parallel()

	t.Run("duplicate ids rejected", func(t *testing.T) {
		t.Parallel()
		pi := T{
			userIDMap: idMaps{newIDMap(1, 1), newIDMap(1, 1)},
		}
		if err := pi.validateIDMaps(); err == nil {
			t.Error("expected duplicate user id error")
		}
		pi = T{
			groupIDMap: idMaps{newIDMap(2, 1), newIDMap(2, 1)},
		}
		if err := pi.validateIDMaps(); err == nil {
			t.Error("expected duplicate group id error")
		}
	})

	t.Run("missing pos in id map rejected", func(t *testing.T) {
		t.Parallel()
		f := file.NewInfo("f", 0, 0, time.Time{}, nil)
		pi := T{
			entries:   file.InfoList{f},
			userIDMap: idMaps{newIDMap(1, 2)},
		}
		if err := pi.validateIDMaps(); err == nil {
			t.Error("expected missing user id error")
		}
		pi = T{
			entries:    file.InfoList{f},
			groupIDMap: idMaps{newIDMap(2, 2)},
		}
		if err := pi.validateIDMaps(); err == nil {
			t.Error("expected missing group id error")
		}
	})

	t.Run("xAttrFromSys panics on unknown type", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if recover() == nil {
				t.Error("expected panic for unrecognized sys info type")
			}
		}()
		var pi T
		pi.xAttrFromSys("invalid")
	})
}
