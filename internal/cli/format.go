// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cli

import (
	"fmt"
	"io"
	"runtime"
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

// The layouts used for timestamped stats files and report directories on
// Windows avoid ':', which is not permitted in its filenames.
const (
	windowsStatsFileLayout = "2006-01-02 15-04-05"
	windowsReportDirLayout = "2006-01-02T15-04-05Z0700"
)

// StatsFileLayout is the time layout used to name stats files.
var StatsFileLayout = statsFileLayout()

// ReportDirLayout is the time layout used to name report directories.
var ReportDirLayout = reportDirLayout()

func statsFileLayout() string {
	l, _ := TimestampLayouts(runtime.GOOS)
	return l
}

func reportDirLayout() string {
	_, l := TimestampLayouts(runtime.GOOS)
	return l
}

// TimestampLayouts returns the time layouts used to name stats files and
// report directories on goos.
func TimestampLayouts(goos string) (stats, reports string) {
	if goos == "windows" {
		return windowsStatsFileLayout, windowsReportDirLayout
	}
	return time.DateTime, time.RFC3339
}
