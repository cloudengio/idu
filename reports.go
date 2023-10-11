// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/database/boltdb"
	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/errors"
	"golang.org/x/exp/maps"
)

type reportsFlags struct {
	internal.TimeRangeFlags
	ReportDir string `subcmd:"report-dir,reports,directory to write reports to"`
	TSV       int    `subcmd:"tsv,0,'generate tsv reports with the requested number of entries, 0 for none'"`
	Markdown  int    `subcmd:"markdown,0,'generate markdown reports with the requested number of entries, 0 for none'"`
	JSON      int    `subcmd:"json,0,'generate json reports with the requested number of entries, 0 for none'"`
}

func (st *statsCmds) reports(ctx context.Context, values interface{}, args []string) error {
	rf := values.(*reportsFlags)
	ctx, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], boltdb.ReadOnly())
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	from, to, err := rf.TimeRangeFlags.FromTo()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(rf.ReportDir, 0700); err != nil {
		return err
	}
	var errs errors.M
	err = db.VisitStats(ctx, from, to,
		func(_ context.Context, when time.Time, data []byte) bool {
			if rf.TSV > 0 {
				tr := &tsvReports{}
				if err := tr.generateReports(ctx, rf, when, data); err != nil {
					errs.Append(err)
					return false
				}
			}
			if rf.JSON > 0 {
				jr := &jsonReports{}
				if err := jr.generateReports(ctx, rf, when, data); err != nil {
					errs.Append(err)
					return false
				}
			}
			if rf.Markdown > 0 {
				mdr := &markdownReports{}
				if err := mdr.generateReports(ctx, rf, when, data); err != nil {
					errs.Append(err)
					return false
				}
			}
			return true
		})
	errs.Append(err)
	return errs.Err()
}

func reportFilename(reportDir string, when time.Time, tag, ext string) string {
	dir := filepath.Join(reportDir, when.Format(time.RFC3339))
	return filepath.Join(dir, "total", ext)
}

type reportFilenames struct {
	root string
	when time.Time
	ext  string
}

func newReportFilenames(root string, when time.Time, ext string) (*reportFilenames, error) {
	rf := &reportFilenames{root: root, when: when, ext: ext}
	for _, sd := range []string{
		rf.rootDir(),
		rf.usersDir(),
		rf.groupsDir(),
	} {
		if err := os.MkdirAll(sd, 0700); err != nil {
			return nil, err
		}
	}
	return rf, nil
}

func (rf *reportFilenames) rootDir() string {
	return filepath.Join(rf.root, rf.when.Format(time.RFC3339))
}

func (rf *reportFilenames) usersDir() string {
	return filepath.Join(rf.root, rf.when.Format(time.RFC3339), "users")
}

func (rf *reportFilenames) groupsDir() string {
	return filepath.Join(rf.root, rf.when.Format(time.RFC3339), "groups")
}

func (rf *reportFilenames) summary(file string) string {
	return filepath.Join(rf.rootDir(), file+rf.ext)
}

func (rf *reportFilenames) user(uid uint32) string {
	un := globalUserManager.nameForUID(uid)
	return filepath.Join(rf.usersDir(), un+rf.ext)
}

func (rf *reportFilenames) group(gid uint32) string {
	un := globalUserManager.nameForGID(gid)
	return filepath.Join(rf.groupsDir(), un+rf.ext)
}

func writeReportFiles(sdb *reports.AllStats,
	filenames *reportFilenames,
	prefixFormatter func(m map[string]reports.MergedStats) []byte,
	idFormatter func(m map[uint32]reports.MergedStats, nameForID func(uint32) string) []byte,
	topN int,
) error {

	merged := sdb.Prefix.Merge(topN)
	if err := os.WriteFile(filenames.summary("prefixes"), prefixFormatter(merged), 0600); err != nil {
		return err
	}
	maps.Clear(merged)
	merged[sdb.Prefix.Prefix] = reports.MergedStats{
		Prefix:   sdb.Prefix.Prefix,
		Bytes:    sdb.Prefix.TotalBytes,
		Storage:  sdb.Prefix.TotalStorageBytes,
		Files:    sdb.Prefix.TotalFiles,
		Prefixes: sdb.Prefix.TotalPrefixes,
	}

	if err := os.WriteFile(filenames.summary("totals"), prefixFormatter(merged), 0600); err != nil {
		return err
	}

	for uid, us := range sdb.PerUser.ByPrefix {
		merged := us.Merge(topN)
		if err := os.WriteFile(filenames.user(uid), prefixFormatter(merged), 0600); err != nil {
			return err
		}
	}

	for gid, gs := range sdb.PerGroup.ByPrefix {
		merged := gs.Merge(topN)
		if err := os.WriteFile(filenames.group(gid), prefixFormatter(merged), 0600); err != nil {
			return err
		}
	}

	userMerged := sdb.ByUser.Merge(topN)
	userdata := idFormatter(userMerged, globalUserManager.nameForUID)
	if err := os.WriteFile(filenames.summary("user"), userdata, 0600); err != nil {
		return err
	}

	groupMerged := sdb.ByGroup.Merge(topN)
	groupdata := idFormatter(groupMerged, globalUserManager.nameForGID)
	if err := os.WriteFile(filenames.summary("group"), groupdata, 0600); err != nil {
		return err
	}

	return nil

}
