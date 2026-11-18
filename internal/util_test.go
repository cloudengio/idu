// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal_test

import (
	"testing"

	"cloudeng.io/cmd/idu/internal"
)

func TestSubtreeMatch(t *testing.T) {
	for _, tc := range []struct {
		key, target string
		match, stop bool
	}{
		{"/a", "/a", true, false},
		{"/a/b", "/a", true, false},
		{"/a/b/c", "/a", true, false},
		{"/a-b", "/a", false, false},
		{"/ab", "/a", false, true},
		{"/b", "/a", false, true},
		{"/a", "/", true, false},
		{"/", "/", true, false},
		{"/x/y", "/", true, false},
	} {
		match, stop := internal.SubtreeMatch(tc.key, tc.target, "/")
		if match != tc.match || stop != tc.stop {
			t.Errorf("%q %q: got %v %v, want %v %v", tc.key, tc.target, match, stop, tc.match, tc.stop)
		}
	}
}
