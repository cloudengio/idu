// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package iduconfig

import "testing"

func TestHasPathPrefix(t *testing.T) {
	for _, tc := range []struct {
		path, prefix    string
		caseInsensitive bool
		want            bool
	}{
		{"/a/b", "/a", false, true},
		{"/A/b", "/a", false, false},
		{"/a", "/a/b", false, false},
		{`C:\Data\x`, `c:\data`, true, true},
		{`c:\data`, `C:\Data`, true, true},
		{`C:\Data\x`, `c:\data`, false, false},
		{`c:`, `c:\data`, true, false},
	} {
		if got := hasPathPrefix(tc.path, tc.prefix, tc.caseInsensitive); got != tc.want {
			t.Errorf("hasPathPrefix(%q, %q, %v) = %v, want %v", tc.path, tc.prefix, tc.caseInsensitive, got, tc.want)
		}
	}
}
