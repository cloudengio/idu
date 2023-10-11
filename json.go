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
	"os"
	"time"

	"golang.org/x/exp/maps"
)

type jsonReports struct{}

func (jr *jsonReports) generateReports(ctx context.Context, rf *reportsFlags, when time.Time, data []byte) error {
	var sdb stats
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&sdb); err != nil {
		return fmt.Errorf("failed to decode stats: %v", err)
	}

	filenames, err := newReportFilenames(rf.ReportDir, when, ".json")
	if err != nil {
		return err
	}

	topN := rf.JSON

	merged := sdb.Prefix.merge(topN)
	if err := os.WriteFile(filenames.summary("prefixes"), jr.formatMerged(merged), 0600); err != nil {
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
	if err := os.WriteFile(filenames.summary("totals"), jr.formatMerged(merged), 0600); err != nil {
		return err
	}

	for uid, us := range sdb.PerUser.ByPrefix {
		merged := us.merge(topN)
		if err := os.WriteFile(filenames.user(uid), jr.formatMerged(merged), 0600); err != nil {
			return err
		}
	}

	for gid, us := range sdb.PerGroup.ByPrefix {
		merged := us.merge(topN)
		if err := os.WriteFile(filenames.group(gid), jr.formatMerged(merged), 0600); err != nil {
			return err
		}
	}

	userMerged := sdb.ByUser.merge(topN)
	userdata := jr.formatUserGroupMerged(userMerged, globalUserManager.nameForUID)
	if err := os.WriteFile(filenames.summary("user"), userdata, 0600); err != nil {
		return err
	}

	groupMerged := sdb.ByGroup.merge(topN)
	groupdata := jr.formatUserGroupMerged(groupMerged, globalUserManager.nameForGID)
	if err := os.WriteFile(filenames.summary("group"), groupdata, 0600); err != nil {
		return err
	}

	return nil
}

func (jr *jsonReports) formatMerged(merged map[string]mergedStats) []byte {
	out := &bytes.Buffer{}
	wr := json.NewEncoder(out)
	for k, v := range merged {
		v.Prefix = k
		wr.Encode(v)
	}
	return out.Bytes()
}

func (jr *jsonReports) formatUserGroupMerged(merged map[uint32]mergedStats, nameForID func(uint32) string) []byte {
	out := &bytes.Buffer{}
	wr := json.NewEncoder(out)
	for k, v := range merged {
		v.ID = k
		v.IDName = nameForID(k)
		wr.Encode(v)
	}
	return out.Bytes()
}
