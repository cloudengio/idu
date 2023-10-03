// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal_test

import (
	"reflect"
	"testing"

	"cloudeng.io/cmd/idu/internal"
)

func TestEncoding(t *testing.T) {
	sl := internal.StatsList{
		{ID: 1, Files: 3, Bytes: 5, StorageBytes: 6},
		{ID: 7, Files: 9, Bytes: 11, StorageBytes: 12},
	}

	buf, err := sl.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	sl2 := internal.StatsList{}
	if err := sl2.UnmarshalBinary(buf); err != nil {
		t.Fatal(err)
	}
	if got, want := sl, sl2; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
