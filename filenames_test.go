// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"strings"
	"testing"
	"time"
)

func TestTimestampLayouts(t *testing.T) {
	when := time.Date(2026, 11, 16, 0, 25, 28, 0, time.FixedZone("PST", -8*3600))
	for _, goos := range []string{"windows", "linux", "darwin"} {
		statsLayout, reportsLayout := timestampLayouts(goos)
		statsName, reportsName := when.Format(statsLayout), when.Format(reportsLayout)
		if goos == "windows" {
			for _, name := range []string{statsName, reportsName} {
				if strings.ContainsAny(name, `<>:"/\|?*`) {
					t.Errorf("%v: %q is not a valid windows filename", goos, name)
				}
			}
		}
		// reports locate parses the names of report directories.
		parsed, err := time.Parse(reportsLayout, reportsName)
		if err != nil || !parsed.Equal(when) {
			t.Errorf("%v: time.Parse(%q, %q) = %v, %v, want %v", goos, reportsLayout, reportsName, parsed, err, when)
		}
	}
	// The layouts used on other systems are unchanged.
	if s, r := timestampLayouts("linux"); s != time.DateTime || r != time.RFC3339 {
		t.Errorf("got %q, %q, want %q, %q", s, r, time.DateTime, time.RFC3339)
	}
}
