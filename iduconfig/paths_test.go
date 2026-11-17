// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package iduconfig

import "testing"

func TestContainsPath(t *testing.T) {
	for _, tc := range []struct {
		root, path      string
		caseInsensitive bool
		want            bool
	}{
		{"/a", "/a/b", false, true},
		{"/a", "/a", false, true},
		{"/a", "/ab", false, false},
		{"/a", "/A/b", false, false},
		{"/a/b", "/a", false, false},
		{"/", "/a", false, true},
		{`c:/data`, `C:/Data/x`, true, true},
		{`C:\Data`, `c:\data`, true, true},
		{`c:/data`, `C:/Data/x`, false, false},
		{`c:\data`, `c:`, true, false},
		{`c:/`, `c:/data`, false, true},
	} {
		if got := containsPath(tc.root, tc.path, tc.caseInsensitive); got != tc.want {
			t.Errorf("containsPath(%q, %q, %v) = %v, want %v", tc.root, tc.path, tc.caseInsensitive, got, tc.want)
		}
	}
}
