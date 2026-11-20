// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package reports implements idu's reports commands: generate, which
// writes tsv, json and markdown reports from a stats file, and locate.
package reports

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"cloudeng.io/cmd/idu/internal/cli"

	"cloudeng.io/cmd/idu/internal/usernames"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/errors"
)

// GenerateFlags are the flags for the reports generate command.
type GenerateFlags struct {
	ReportDir string `subcmd:"report-dir,reports,directory to write reports to"`
	TSV       int    `subcmd:"tsv,100,'generate tsv reports with the requested number of entries, 0 for none'"`
	Markdown  int    `subcmd:"markdown,20,'generate markdown reports with the requested number of entries, 0 for none'"`
	JSON      int    `subcmd:"json,100,'generate json reports with the requested number of entries, 0 for none'"`
}

// Commands implements the reports commands, ie. generate and locate.
type Commands struct {
	env       *cli.Env
	statsData []byte
}

// New returns the Commands that use env.
func New(env *cli.Env) *Commands {
	return &Commands{env: env}
}

// Generate implements the reports generate command, values must be a
// *GenerateFlags.
func (rc *Commands) Generate(ctx context.Context, values interface{}, args []string) error {
	rf := values.(*GenerateFlags)
	if err := os.MkdirAll(rf.ReportDir, 0770); err != nil {
		return err
	}
	errs := &errors.M{}
	for _, filename := range args {
		data, err := os.ReadFile(filename)
		if err != nil {
			errs.Append(err)
			continue
		}
		rc.statsData = data
		if err := rc.generateReports(ctx, rf); err != nil {
			errs.Append(err)
			continue
		}
	}
	return errs.Err()
}

// Need to recreate stats for every report as the topN values are
// removed from the heaps as they are used.
func (rc *Commands) getStats() (cli.StatsFile, error) {
	return cli.DecodeStats(bytes.NewReader(rc.statsData))
}

func (rc *Commands) statsFor(reportDir, suffix string) (cli.StatsFile, *reportFilenames, error) {
	stats, err := rc.getStats()
	if err != nil {
		return cli.StatsFile{}, nil, err
	}
	filenames, err := newReportFilenames(reportDir, stats.Date, suffix)
	if err != nil {
		return cli.StatsFile{}, nil, err
	}
	return stats, filenames, nil
}

func (rc *Commands) reportsFor(rf *GenerateFlags, suffix string) (*reportFilenames, error) {
	stats, filenames, err := rc.statsFor(rf.ReportDir, suffix)
	if err != nil {
		return nil, err
	}
	switch suffix {
	case ".tsv":
		tr := &tsvReports{}
		return filenames, tr.generateReports(rf, filenames, stats)
	case ".json":
		jr := &jsonReports{}
		return filenames, jr.generateReports(rf, filenames, stats)
	case ".md":
		md := &markdownReports{fmtSize: rc.env.FmtSize}
		return filenames, md.generateReports(rf, filenames, stats)
	}
	return nil, fmt.Errorf("unsupported report format: %v", suffix)
}

func (rc *Commands) generateReports(_ context.Context, rf *GenerateFlags) error {
	if rf.TSV == 0 && rf.JSON == 0 && rf.Markdown == 0 {
		return fmt.Errorf("no report requested, please specify one of --tsv, --json or --markdown")
	}

	var err error
	var filenames *reportFilenames
	if rf.TSV > 0 {
		filenames, err = rc.reportsFor(rf, ".tsv")
	}
	if rf.JSON > 0 {
		filenames, err = rc.reportsFor(rf, ".json")
	}
	if rf.Markdown > 0 {
		filenames, err = rc.reportsFor(rf, ".md")
	}
	if err != nil {
		return err
	}

	src, dst := filenames.latest()
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Symlink(src, dst); err != nil {
		if runtime.GOOS != "windows" {
			return err
		}
		// Creating symlinks on Windows requires developer mode or
		// administrator privileges, the reports can still be found
		// using reports locate.
		fmt.Fprintf(os.Stderr, "failed to link %v to %v: %v\n", dst, src, err)
	}
	return nil
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
		if err := os.MkdirAll(sd, 0770); err != nil {
			return nil, err
		}
	}
	return rf, nil
}

func (rf *reportFilenames) rootDir() string {
	return filepath.Join(rf.root, rf.when.Format(cli.ReportDirLayout))
}

func (rf *reportFilenames) usersDir() string {
	return filepath.Join(rf.root, rf.when.Format(cli.ReportDirLayout), "users")
}

func (rf *reportFilenames) groupsDir() string {
	return filepath.Join(rf.root, rf.when.Format(cli.ReportDirLayout), "groups")
}

func (rf *reportFilenames) summary(file string) string {
	return filepath.Join(rf.rootDir(), file+rf.ext)
}

func (rf *reportFilenames) user(uid int64) string {
	un := usernames.Manager.NameForUID(uid)
	return filepath.Join(rf.usersDir(), un+rf.ext)
}

func (rf *reportFilenames) group(gid int64) string {
	un := usernames.Manager.NameForGID(gid)
	return filepath.Join(rf.groupsDir(), un+rf.ext)
}

func (rf *reportFilenames) latest() (src, dst string) {
	return rf.when.Format(cli.ReportDirLayout), filepath.Join(rf.root, "latest")
}

func writeReportFiles(sdb *report.AllStats,
	filenames *reportFilenames,
	prefixFormatter func(m map[string]report.MergedStats) []byte,
	idFormatter func(m map[int64]report.MergedStats, nameForID func(int64) string) []byte,
	distributedFormatter func(list []report.DistributedStats) []byte,
	topN int,
) error {

	merged := sdb.Prefix.Merge(topN)
	if err := os.WriteFile(filenames.summary("prefixes"), prefixFormatter(merged), 0600); err != nil {
		return err
	}
	if sdb.Subtree != nil {
		subMerged := sdb.Subtree.Merge(topN)
		if err := os.WriteFile(filenames.summary("subtrees"), prefixFormatter(subMerged), 0600); err != nil {
			return err
		}
	}
	if dist := report.MergeDistributed(sdb.Distributed, topN); len(dist) > 0 && distributedFormatter != nil {
		if err := os.WriteFile(filenames.summary("distributed"), distributedFormatter(dist), 0600); err != nil {
			return err
		}
	}
	clear(merged)
	merged[sdb.Prefix.Prefix] = report.MergedStats{
		Prefix:      sdb.Prefix.Prefix,
		Bytes:       sdb.Prefix.TotalBytes,
		Storage:     sdb.Prefix.TotalStorageBytes,
		Files:       sdb.Prefix.TotalFiles,
		Prefixes:    sdb.Prefix.TotalPrefixes,
		PrefixBytes: sdb.Prefix.TotalPrefixBytes,
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
	userdata := idFormatter(userMerged, usernames.Manager.NameForUID)
	if err := os.WriteFile(filenames.summary("user"), userdata, 0600); err != nil {
		return err
	}

	groupMerged := sdb.ByGroup.Merge(topN)
	groupdata := idFormatter(groupMerged, usernames.Manager.NameForGID)
	if err := os.WriteFile(filenames.summary("group"), groupdata, 0600); err != nil {
		return err
	}
	return nil
}

// LocateFlags are the flags for the reports locate command.
type LocateFlags struct {
	N        int    `subcmd:"n,2,'locate the n most recent reports'"`
	Extesion string `subcmd:"extension,,file extension to match"`
}

func stripPrefix(prefix, path string) string {
	if prefix == path {
		return ""
	}
	return filepath.Join(stripPrefix(prefix, filepath.Dir(path)), filepath.Base(path))
}

func (rc *Commands) listfiles(dir, ext string) ([]string, error) {
	dir = filepath.Clean(dir)
	var files []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if len(ext) == 0 || filepath.Ext(path) == ext {
				files = append(files, stripPrefix(dir, path))
			}
		}
		return nil
	})
	return files, err
}

// Locate implements the reports locate command, values must be a
// *LocateFlags.
func (rc *Commands) Locate(_ context.Context, values interface{}, args []string) error {
	lf := values.(*LocateFlags)
	dir := args[0]
	dirs, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	type candidate struct {
		when time.Time
		dir  string
	}
	candidates := []candidate{}
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		when, err := time.Parse(cli.ReportDirLayout, d.Name())
		if err != nil {
			continue
		}
		candidates = append(candidates, candidate{when, d.Name()})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].dir > candidates[j].dir })

	type reportDir struct {
		ReportTime time.Time `json:"report_time"`
		ReportDir  string    `json:"report_dir"`
		Files      []string  `json:"files"`
	}
	var reports []reportDir
	for i := 0; i < lf.N; i++ {
		if i >= len(candidates) {
			break
		}
		files, err := rc.listfiles(filepath.Join(dir, candidates[i].dir), lf.Extesion)
		if err != nil {
			return err
		}
		reports = append(reports, reportDir{candidates[i].when, candidates[i].dir, files})
	}
	out, err := json.Marshal(reports)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(out)
	return err

}
