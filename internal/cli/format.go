// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cli

import (
	"fmt"
	"io"
	"strings"
	"time"

	"cloudeng.io/file/diskusage"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// Printer formats numbers using English conventions, eg. 1,000.
var Printer = message.NewPrinter(language.English)

// FmtSize formats size using BytesPrinter.
func (e *Env) FmtSize(size int64) string {
	f, u := e.BytesPrinter(size)
	return Printer.Sprintf("% 8.3f %s", f, u)
}

// FmtCount formats count right-aligned with thousands separators.
func FmtCount(count int64) string {
	return Printer.Sprintf("% 11v", count)
}

// FmtKiBytes formats size in KiB.
func FmtKiBytes(size int64) string {
	size /= int64(diskusage.KiB)
	return Printer.Sprintf("%v KiB", size)
}

// Banner writes the formatted text to out underlined by ul.
func Banner(out io.Writer, ul string, format string, args ...any) {
	buf := strings.Builder{}
	o := fmt.Sprintf(format, args...)
	buf.WriteString(o)
	buf.WriteString(strings.Repeat(ul, len(o)))
	buf.WriteRune('\n')
	out.Write([]byte(buf.String())) //nolint:errcheck
}

// StatsFileLayout is the time layout used to name stats files, ie.
// snapshots. Names are in local time, with millisecond resolution, contain
// neither spaces nor ':', which is not permitted in Windows filenames, and
// are the same on all systems.
const StatsFileLayout = "2006-01-02_15-04-05.000"

// ReportDirLayout is the time layout used to name report directories, it
// is the same as StatsFileLayout.
const ReportDirLayout = StatsFileLayout

// LegacyStatsFileLayouts are the layouts previously used to name stats
// files, which are still recognized.
var LegacyStatsFileLayouts = []string{time.DateTime, "2006-01-02 15-04-05", "2006-01-02_15-04-05"}

// LegacyReportDirLayouts are the layouts previously used to name report
// directories, which are still recognized.
var LegacyReportDirLayouts = []string{time.RFC3339, "2006-01-02T15-04-05Z0700", "2006-01-02_15-04-05Z0700", "2006-01-02_15-04-05"}
