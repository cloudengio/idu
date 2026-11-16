// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"runtime"
	"time"
)

// The layouts used for timestamped stats files and report directories on
// Windows avoid ':', which is not permitted in its filenames.
const (
	windowsStatsFileLayout = "2006-01-02 15-04-05"
	windowsReportDirLayout = "2006-01-02T15-04-05Z0700"
)

// statsFileLayout and reportDirLayout are the time layouts used to name
// stats files and report directories.
var statsFileLayout, reportDirLayout = timestampLayouts(runtime.GOOS)

func timestampLayouts(goos string) (stats, reports string) {
	if goos == "windows" {
		return windowsStatsFileLayout, windowsReportDirLayout
	}
	return time.DateTime, time.RFC3339
}
