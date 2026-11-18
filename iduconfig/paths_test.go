// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package iduconfig

import (
	"path/filepath"
	"testing"
)

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

func TestCleanPath(t *testing.T) {
	cases := []struct{ in, want string }{
		{"/a/b/", "/a/b"},
		{"/a//b/./c/..", "/a/b"},
		{"/", "/"},
	}
	if filepath.Separator == '\\' {
		cases = append(cases, []struct{ in, want string }{
			// Forward slashes are preserved if used exclusively.
			{"C:/data//x/", "C:/data/x"},
			{`C:\data\\x\`, `C:\data\x`},
			// Mixed separators use the platform separator.
			{`C:/data\x/`, `C:\data\x`},
			{"C:/", "C:/"},
		}...)
	}
	for _, tc := range cases {
		if got := cleanPath(tc.in); got != tc.want {
			t.Errorf("cleanPath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestIsRooted(t *testing.T) {
	for _, tc := range []struct {
		path string
		want bool
	}{
		{"/tmp", true},
		{"/", true},
		{"tmp", false},
		{"./tmp", false},
		{"../tmp", false},
		{"", false},
	} {
		if got := isRooted(tc.path); got != tc.want {
			t.Errorf("isRooted(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
	if filepath.Separator == '\\' {
		for _, p := range []string{`\tmp`, `C:\tmp`, "C:/tmp"} {
			if !isRooted(p) {
				t.Errorf("isRooted(%q) = false, want true", p)
			}
		}
	}
}
