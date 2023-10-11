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
	"cloudeng.io/errors"
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

type mergedStats struct {
	Prefix   string `json:"prefix,omitempty"`
	ID       uint32 `json:"id,omitempty"`
	IDName   string `json:"name,omitempty"`
	Bytes    int64  `json:"bytes"`
	Storage  int64  `json:"storage,omitempty"`
	Files    int64  `json:"files"`
	Prefixes int64  `json:"prefixes"`
}

func merger[T comparable](n int, merged map[T]mergedStats, keys []int64, prefixes []T, setter func(mergedStats, int64) mergedStats) {
	for i, prefix := range prefixes {
		if n > 0 && i >= n {
			break
		}
		merged[prefix] = setter(merged[prefix], keys[i])
	}
}

func (h *heaps[T]) merge(n int) map[T]mergedStats {
	merged := make(map[T]mergedStats)

	b, bp := h.popAll(h.Bytes, n)
	merger(n, merged, b, bp, func(m mergedStats, v int64) mergedStats {
		m.Bytes = v
		return m
	})
	if h.StorageBytes != nil {
		sb, sbp := h.popAll(h.StorageBytes, n)
		merger(n, merged, sb, sbp, func(m mergedStats, v int64) mergedStats {
			m.Storage = v
			return m
		})
	}
	fb, fbp := h.popAll(h.Files, n)
	merger(n, merged, fb, fbp, func(m mergedStats, v int64) mergedStats {
		m.Files = v
		return m
	})

	db, dbp := h.popAll(h.Prefixes, n)
	merger(n, merged, db, dbp, func(m mergedStats, v int64) mergedStats {
		m.Prefixes = v
		return m
	})

	return merged
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
	return filepath.Join(rf.root, rf.when.Format(time.RFC3339), "users"+rf.ext)
}

func (rf *reportFilenames) groupsDir() string {
	return filepath.Join(rf.root, rf.when.Format(time.RFC3339), "groups"+rf.ext)
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
