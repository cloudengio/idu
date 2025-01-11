// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"encoding/json"

	"cloudeng.io/cmd/idu/internal/reports"
)

type jsonReports struct{}

func (jr *jsonReports) generateReports(rf *generateReportsFlags, filenames *reportFilenames, stats statsFileFormat) error {
	return writeReportFiles(stats.Stats, filenames, jr.formatMerged, jr.formatUserGroupMerged, rf.JSON)
}

func (jr *jsonReports) formatMerged(merged map[string]reports.MergedStats) []byte {
	out := &bytes.Buffer{}
	wr := json.NewEncoder(out)
	for k, v := range merged {
		v.Prefix = k
		_ = wr.Encode(v)
	}
	return out.Bytes()
}

func (jr *jsonReports) formatUserGroupMerged(merged map[int64]reports.MergedStats, nameForID func(int64) string) []byte {
	out := &bytes.Buffer{}
	wr := json.NewEncoder(out)
	for k, v := range merged {
		v.ID = k
		v.IDName = nameForID(k)
		_ = wr.Encode(v)
	}
	return out.Bytes()
}
