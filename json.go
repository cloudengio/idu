// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"time"

	"cloudeng.io/cmd/idu/internal/reports"
)

type jsonReports struct{}

func (jr *jsonReports) generateReports(ctx context.Context, rf *reportsFlags, when time.Time, data []byte) error {
	var sdb reports.AllStats
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&sdb); err != nil {
		return fmt.Errorf("failed to decode stats: %v", err)
	}
	filenames, err := newReportFilenames(rf.ReportDir, when, ".json")
	if err != nil {
		return err
	}
	return writeReportFiles(&sdb, filenames, jr.formatMerged, jr.formatUserGroupMerged, rf.TSV)
}

func (jr *jsonReports) formatMerged(merged map[string]reports.MergedStats) []byte {
	out := &bytes.Buffer{}
	wr := json.NewEncoder(out)
	for k, v := range merged {
		v.Prefix = k
		wr.Encode(v)
	}
	return out.Bytes()
}

func (jr *jsonReports) formatUserGroupMerged(merged map[uint32]reports.MergedStats, nameForID func(uint32) string) []byte {
	out := &bytes.Buffer{}
	wr := json.NewEncoder(out)
	for k, v := range merged {
		v.ID = k
		v.IDName = nameForID(k)
		wr.Encode(v)
	}
	return out.Bytes()
}
