// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"encoding/csv"
	"strconv"

	"cloudeng.io/cmd/idu/report"
)

type tsvReports struct {
}

func (tr *tsvReports) generateReports(rf *generateReportsFlags, filenames *reportFilenames, stats statsFileFormat) error {
	return writeReportFiles(stats.Stats, filenames, tr.formatMerged, tr.formatUserGroupMerged, tr.formatDistributed, rf.TSV)
}

func (tr *tsvReports) formatDistributed(list []report.DistributedStats) []byte {
	out := &bytes.Buffer{}
	wr := csv.NewWriter(out)
	wr.Comma = '\t'
	wr.Write([]string{ //nolint:errcheck
		"prefix", "distributed bytes", "distributed storage", "distributed files",
		"subtree bytes", "subtree storage", "subtree files", "subtree dirs",
		"max leaf bytes", "max leaf storage", "max leaf files",
		"bytes amplification", "storage amplification", "files amplification", "max child share",
	})
	for _, v := range list {
		wr.Write([]string{ //nolint:errcheck
			v.Prefix,
			strconv.FormatInt(v.DistributedBytes, 10),
			strconv.FormatInt(v.DistributedStorage, 10),
			strconv.FormatInt(v.DistributedFiles, 10),
			strconv.FormatInt(v.SubtreeBytes, 10),
			strconv.FormatInt(v.SubtreeStorage, 10),
			strconv.FormatInt(v.SubtreeFiles, 10),
			strconv.FormatInt(v.SubtreeDirs, 10),
			strconv.FormatInt(v.MaxLeafBytes, 10),
			strconv.FormatInt(v.MaxLeafStorage, 10),
			strconv.FormatInt(v.MaxLeafFiles, 10),
			strconv.FormatFloat(v.BytesAmplification, 'f', 2, 64),
			strconv.FormatFloat(v.StorageAmplification, 'f', 2, 64),
			strconv.FormatFloat(v.FilesAmplification, 'f', 2, 64),
			strconv.FormatFloat(v.MaxChildShare, 'f', 4, 64),
		})
	}
	wr.Flush()
	return out.Bytes()
}

func (tr *tsvReports) formatMerged(merged map[string]report.MergedStats) []byte {
	out := &bytes.Buffer{}
	wr := csv.NewWriter(out)
	wr.Comma = '\t'
	wr.Write([]string{"prefix", "bytes", "storage bytes", "files", "directories", "directory bytes", "sparse bytes"}) //nolint:errcheck
	for k, v := range merged {
		wr.Write([]string{k, //nolint:errcheck
			strconv.FormatInt(v.Bytes, 10),
			strconv.FormatInt(v.Storage, 10),
			strconv.FormatInt(v.Files, 10),
			strconv.FormatInt(v.Prefixes, 10),
			strconv.FormatInt(v.PrefixBytes, 10),
			strconv.FormatInt(v.SparseBytes, 10)})
	}
	wr.Flush()
	return out.Bytes()
}

func (tr *tsvReports) formatUserGroupMerged(merged map[int64]report.MergedStats, nameForID func(int64) string) []byte {
	out := &bytes.Buffer{}
	wr := csv.NewWriter(out)
	wr.Comma = '\t'
	wr.Write([]string{"id", "idname", "bytes", "storage bytes", "files", "directories", "directory bytes", "sparse bytes"}) //nolint:errcheck
	for k, v := range merged {
		wr.Write([]string{ //nolint:errcheck
			strconv.FormatInt(k, 10),
			nameForID(k),
			strconv.FormatInt(v.Bytes, 10),
			strconv.FormatInt(v.Storage, 10),
			strconv.FormatInt(v.Files, 10),
			strconv.FormatInt(v.Prefixes, 10),
			strconv.FormatInt(v.PrefixBytes, 10),
			strconv.FormatInt(v.SparseBytes, 10),
		})
	}
	wr.Flush()
	return out.Bytes()
}
