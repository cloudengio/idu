// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"encoding/csv"
	"strconv"

	"cloudeng.io/cmd/idu/internal/reports"
)

type tsvReports struct {
}

func (tr *tsvReports) generateReports(rf *generateReportsFlags, filenames *reportFilenames, stats statsFileFormat) error {
	return writeReportFiles(stats.Stats, filenames, tr.formatMerged, tr.formatUserGroupMerged, rf.TSV)
}

func (tr *tsvReports) formatMerged(merged map[string]reports.MergedStats) []byte {
	out := &bytes.Buffer{}
	wr := csv.NewWriter(out)
	wr.Comma = '\t'
	wr.Write([]string{"prefix", "bytes", "storage bytes", "files", "directories", "directory bytes"}) //nolint:errcheck
	for k, v := range merged {
		wr.Write([]string{k, //nolint:errcheck
			strconv.FormatInt(v.Bytes, 10),
			strconv.FormatInt(v.Storage, 10),
			strconv.FormatInt(v.Files, 10),
			strconv.FormatInt(v.Prefixes, 10),
			strconv.FormatInt(v.PrefixBytes, 10)})
	}
	wr.Flush()
	return out.Bytes()
}

func (tr *tsvReports) formatUserGroupMerged(merged map[int64]reports.MergedStats, nameForID func(int64) string) []byte {
	out := &bytes.Buffer{}
	wr := csv.NewWriter(out)
	wr.Comma = '\t'
	wr.Write([]string{"id", "idname", "bytes", "storage bytes", "files", "directories", "directory bytes"}) //nolint:errcheck
	for k, v := range merged {
		wr.Write([]string{ //nolint:errcheck
			strconv.FormatInt(k, 10),
			nameForID(k),
			strconv.FormatInt(v.Bytes, 10),
			strconv.FormatInt(v.Storage, 10),
			strconv.FormatInt(v.Files, 10),
			strconv.FormatInt(v.Prefixes, 10),
			strconv.FormatInt(v.PrefixBytes, 10),
		})
	}
	wr.Flush()
	return out.Bytes()
}
