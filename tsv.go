// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"os"
	"strconv"
	"time"

	"golang.org/x/exp/maps"
)

type tsvReports struct {
}

func (tr *tsvReports) generateReports(ctx context.Context, rf *reportsFlags, when time.Time, data []byte) error {
	var sdb stats
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&sdb); err != nil {
		return fmt.Errorf("failed to decode stats: %v", err)
	}

	filenames, err := newReportFilenames(rf.ReportDir, when, ".tsv")
	if err != nil {
		return err
	}

	topN := rf.TSV

	merged := sdb.Prefix.merge(topN)
	if err := os.WriteFile(filenames.summary("prefixes"), tr.formatMerged(merged), 0600); err != nil {
		return err
	}
	maps.Clear(merged)
	merged[sdb.Prefix.Prefix] = mergedStats{
		Prefix:   sdb.Prefix.Prefix,
		Bytes:    sdb.Prefix.TotalBytes,
		Storage:  sdb.Prefix.TotalStorageBytes,
		Files:    sdb.Prefix.TotalFiles,
		Prefixes: sdb.Prefix.TotalPrefixes,
	}

	if err := os.WriteFile(filenames.summary("totals"), tr.formatMerged(merged), 0600); err != nil {
		return err
	}

	for uid, us := range sdb.PerUser.ByPrefix {
		merged := us.merge(topN)
		if err := os.WriteFile(filenames.user(uid), tr.formatMerged(merged), 0600); err != nil {
			return err
		}
	}

	for gid, us := range sdb.PerGroup.ByPrefix {
		merged := us.merge(topN)
		if err := os.WriteFile(filenames.group(gid), tr.formatMerged(merged), 0600); err != nil {
			return err
		}
	}

	userMerged := sdb.ByUser.merge(topN)
	userdata := tr.formatUserGroupMerged(userMerged, globalUserManager.nameForUID)
	if err := os.WriteFile(filenames.summary("user"), userdata, 0600); err != nil {
		return err
	}

	groupMerged := sdb.ByGroup.merge(topN)
	groupdata := tr.formatUserGroupMerged(groupMerged, globalUserManager.nameForGID)
	if err := os.WriteFile(filenames.summary("group"), groupdata, 0600); err != nil {
		return err
	}

	return nil
}

func (tr *tsvReports) formatMerged(merged map[string]mergedStats) []byte {
	out := &bytes.Buffer{}
	wr := csv.NewWriter(out)
	wr.Comma = '\t'
	wr.Write([]string{"prefix", "bytes", "storage bytes", "files", "directories"})
	for k, v := range merged {
		wr.Write([]string{k,
			strconv.FormatInt(v.Bytes, 10),
			strconv.FormatInt(v.Storage, 10),
			strconv.FormatInt(v.Files, 10),
			strconv.FormatInt(v.Prefixes, 10)})
	}
	wr.Flush()
	return out.Bytes()
}

func (tr *tsvReports) formatUserGroupMerged(merged map[uint32]mergedStats, nameForID func(uint32) string) []byte {
	out := &bytes.Buffer{}
	wr := csv.NewWriter(out)
	wr.Comma = '\t'
	wr.Write([]string{"id", "idname", "bytes", "storage bytes", "files", "directories"})
	for k, v := range merged {
		wr.Write([]string{
			strconv.FormatUint(uint64(k), 10),
			nameForID(k),
			strconv.FormatInt(v.Bytes, 10),
			strconv.FormatInt(v.Storage, 10),
			strconv.FormatInt(v.Files, 10),
			strconv.FormatInt(v.Prefixes, 10)})
	}
	wr.Flush()
	return out.Bytes()
}
