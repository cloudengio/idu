// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cli

import (
	"strings"
	"testing"
	"time"
)

func TestTimestampLayouts(t *testing.T) {
	when := time.Date(2026, 11, 16, 0, 25, 28, 123456789, time.FixedZone("PST", -8*3600))
	for _, layout := range []string{StatsFileLayout, ReportDirLayout} {
		name := when.Format(layout)
		// Names must be valid on Windows and contain no spaces.
		if strings.ContainsAny(name, ` <>:"/\|?*`) {
			t.Errorf("%q is not a valid filename", name)
		}
	}
	if got, want := when.Format(StatsFileLayout), "2026-11-16_00-25-28.123"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	// reports locate parses the names of report directories.
	name := when.Format(ReportDirLayout)
	if parsed, err := time.ParseInLocation(ReportDirLayout, name, when.Location()); err != nil || !parsed.Equal(when.Truncate(time.Millisecond)) {
		t.Errorf("time.Parse(%q) = %v, %v, want %v", name, parsed, err, when)
	}
	if got, want := name, "2026-11-16_00-25-28.123"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
