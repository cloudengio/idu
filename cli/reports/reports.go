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
	"time"

	"cloudeng.io/cmd/idu/cli/internal/snapshot"
	"cloudeng.io/cmd/idu/cli/internal/statedir"
	"cloudeng.io/cmd/idu/internal/cli"

	"cloudeng.io/cmd/idu/internal/usernames"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/errors"
)

// GenerateFlags are the flags for the reports generate command.
type GenerateFlags struct {
	cli.FSFlag
	ReportDir string `subcmd:"report-dir,,'directory to write reports to, defaults to reports_dir from the configuration of the filesystem specified by --fs, or reports'"`
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
	rf := *values.(*GenerateFlags)
	rf.ReportDir = rc.reportsDirFor(rf.FS, rf.ReportDir)
	if err := os.MkdirAll(rf.ReportDir, 0770); err != nil {
		return err
	}
	if len(args) == 0 {
		return rc.generateCurrent(ctx, &rf)
	}
	errs := &errors.M{}
	for _, filename := range args {
		data, err := os.ReadFile(filename)
		if err != nil {
			errs.Append(err)
			continue
		}
		rc.statsData = data
		if err := rc.generateReports(ctx, &rf); err != nil {
			errs.Append(err)
			continue
		}
	}
	return errs.Err()
}

// generateCurrent generates reports from the most recent snapshot of the
// entire filesystem specified by --fs, or from its database, see
// snapshot.Computer.Current.
func (rc *Commands) generateCurrent(ctx context.Context, rf *GenerateFlags) error {
	if err := cli.ConfigLoadError(ctx); err != nil {
		return err
	}
	env := rc.env.ForFS(rf.FS)
	ctx, f, _, err := env.LookupPrefix(ctx, "")
	if err != nil {
		return err
	}
	n := max(rf.TSV, rf.JSON, rf.Markdown)
	sf, path, err := snapshot.Computer{}.Current(ctx, env, f, f.Config().SnapshotDir, n)
	if err != nil {
		return err
	}
	if path != "" {
		fmt.Printf("snapshot: %v\n", path)
	}
	var buf bytes.Buffer
	if err := sf.Encode(&buf); err != nil {
		return err
	}
	rc.statsData = buf.Bytes()
	return rc.generateReports(ctx, rf)
}

// Need to recreate stats for every report as the topN values are
// removed from the heaps as they are used.
func (rc *Commands) getStats() (snapshot.File, error) {
	return snapshot.Decode(bytes.NewReader(rc.statsData))
}

func (rc *Commands) statsFor(reportDir, suffix string) (snapshot.File, *reportFilenames, error) {
	stats, err := rc.getStats()
	if err != nil {
		return snapshot.File{}, nil, err
	}
	filenames, err := newReportFilenames(reportDir, stats.Date, suffix)
	if err != nil {
		return snapshot.File{}, nil, err
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

	var errs errors.M
	var filenames *reportFilenames
	for _, spec := range []struct {
		n   int
		ext string
	}{
		{rf.TSV, ".tsv"},
		{rf.JSON, ".json"},
		{rf.Markdown, ".md"},
	} {
		if spec.n > 0 {
			fn, err := rc.reportsFor(rf, spec.ext)
			if err != nil {
				errs.Append(err)
			} else {
				filenames = fn
			}
		}
	}
	if err := errs.Err(); err != nil {
		return err
	}

	if err := filenames.dir.SetLatest(filenames.when); err != nil {
		if !errors.Is(err, statedir.ErrNoSymlinks) {
			return err
		}
		// The reports can still be found using reports locate.
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
	return nil
}

// DefaultReportsDir is the directory that reports are written to if
// neither --report-dir nor reports_dir is set.
const DefaultReportsDir = "reports"

// reportsDirFor returns the directory that reports are to be written to:
// flag, if set, otherwise the reports_dir of the filesystem named fs, or
// the only one configured, if set, and DefaultReportsDir otherwise.
func (rc *Commands) reportsDirFor(fs, flag string) string {
	if flag != "" {
		return flag
	}
	if f, _, err := rc.env.ForFS(fs).LookupFS(""); err == nil && f.Config().ReportsDir != "" {
		return f.Config().ReportsDir
	}
	return DefaultReportsDir
}

// reportsDir returns the statedir.Dir for reports written to root.
func reportsDir(root string) statedir.Dir {
	return statedir.New(root, cli.ReportDirLayout, "", cli.LegacyReportDirLayouts...)
}

type reportFilenames struct {
	dir  statedir.Dir
	when time.Time
	ext  string
	path string
}

func newReportFilenames(root string, when time.Time, ext string) (*reportFilenames, error) {
	rf := &reportFilenames{dir: reportsDir(root), when: when, ext: ext}
	path, err := rf.dir.MkdirAll(when, "users", "groups")
	if err != nil {
		return nil, err
	}
	rf.path = path
	return rf, nil
}

func (rf *reportFilenames) rootDir() string {
	return rf.path
}

func (rf *reportFilenames) usersDir() string {
	return filepath.Join(rf.path, "users")
}

func (rf *reportFilenames) groupsDir() string {
	return filepath.Join(rf.path, "groups")
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
	cli.FSFlag
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
func (rc *Commands) Locate(ctx context.Context, values interface{}, args []string) error {
	lf := values.(*LocateFlags)
	var dir string
	if len(args) == 1 {
		dir = args[0]
	} else {
		f, _, err := rc.env.ForFS(lf.FS).LookupFS("")
		if err != nil {
			if loadErr := cli.ConfigLoadError(ctx); loadErr != nil {
				return loadErr
			}
			return err
		}
		if dir = f.Config().ReportsDir; dir == "" {
			dir = DefaultReportsDir
		}
	}
	if _, err := os.Stat(dir); err != nil {
		return err
	}
	entries, err := reportsDir(dir).Entries()
	if err != nil {
		return err
	}
	type candidate struct {
		when time.Time
		dir  string
	}
	candidates := []candidate{}
	for _, e := range entries {
		if e.IsDir {
			candidates = append(candidates, candidate{e.When, e.Name})
		}
	}

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
