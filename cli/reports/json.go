// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package reports

import (
	"bytes"
	"encoding/json"

	"cloudeng.io/cmd/idu/cli/internal/snapshot"
	"cloudeng.io/cmd/idu/report"
)

type jsonReports struct{}

func (jr *jsonReports) generateReports(rf *GenerateFlags, filenames *reportFilenames, stats snapshot.File) error {
	return writeReportFiles(stats.Stats, filenames, jr.formatMerged, jr.formatUserGroupMerged, jr.formatDistributed, rf.JSON)
}

func (jr *jsonReports) formatDistributed(list []report.DistributedStats) []byte {
	out := &bytes.Buffer{}
	wr := json.NewEncoder(out)
	for _, v := range list {
		_ = wr.Encode(v)
	}
	return out.Bytes()
}

func (jr *jsonReports) formatMerged(merged map[string]report.MergedStats) []byte {
	out := &bytes.Buffer{}
	wr := json.NewEncoder(out)
	for k, v := range merged {
		v.Prefix = k
		_ = wr.Encode(v)
	}
	return out.Bytes()
}

func (jr *jsonReports) formatUserGroupMerged(merged map[int64]report.MergedStats, nameForID func(int64) string) []byte {
	out := &bytes.Buffer{}
	wr := json.NewEncoder(out)
	for k, v := range merged {
		v.ID = k
		v.IDName = nameForID(k)
		_ = wr.Encode(v)
	}
	return out.Bytes()
}
