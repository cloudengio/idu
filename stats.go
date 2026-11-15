// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/hardlinks"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/cmd/idu/internal/usernames"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/localfs"
)

type statsFileFormat struct {
	Prefix     string
	Date       time.Time
	Expression string
	Stats      *reports.AllStats
}

func loadStats(filename string) (statsFileFormat, error) {
	var stats statsFileFormat
	if filename == "-" {
		if err := gob.NewDecoder(os.Stdin).Decode(&stats); err != nil {
			return statsFileFormat{}, err
		}
		return stats, nil
	}
	buf, err := os.ReadFile(filename)
	if err != nil {
		return statsFileFormat{}, err
	}
	if err := gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&stats); err != nil {
		return statsFileFormat{}, err
	}
	return stats, nil
}

func saveStats(dir, file string, stats statsFileFormat) error {
	if len(file) > 0 {
		out := os.Stdout
		if file != "-" {
			var err error
			out, err = os.Create(file)
			if err != nil {
				return err
			}
			defer out.Close()
		}
		return gob.NewEncoder(out).Encode(stats)
	}
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(stats); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	basename := stats.Date.Format(time.DateTime)
	basename = strings.TrimSuffix(basename, ".idustats") + ".idustats"
	filename := filepath.Join(dir, basename)
	if err := os.WriteFile(filename, buf.Bytes(), 0660); err != nil { //nolint:gosec
		return err
	}
	sl := filepath.Join(dir, "latest.idustats")
	os.Remove(sl)
	return os.Symlink(basename, sl)
}

type statsCmds struct {
}

type StatsFlags struct {
	DisplayN int  `subcmd:"display,10,number of top entries to display"`
	Progress bool `subcmd:"progress,false,show progress"`
}

type computeFlags struct {
	ComputeN  int             `subcmd:"n,2000,number of top entries to compute"`
	Progress  bool            `subcmd:"progress,false,show progress"`
	StatsDir  string          `subcmd:"stats-dir,,'directory that stats files are written to, defaults to stats_dir from the configuration or stats'"`
	StatsFile string          `subcmd:"stats-file,,'write stats to the specified file, rather than a directory, use - for stdout'"`
	Prefix    flags.Repeating `subcmd:"prefix,,'prefix match expression'"`
}

type viewFlags struct {
	StatsFlags
	User  string `subcmd:"user,,display stats for the specified user"`
	Group string `subcmd:"group,,display stats for the specified group"`
	Info  bool   `subcmd:"info,false,display metadata for the stats file"`
}

func (st *statsCmds) compute(ctx context.Context, values interface{}, args []string) error {
	// TODO(cnicolaou): generalize this to other filesystems.
	fs := localfs.New()
	return st.computeFS(ctx, fs, values.(*computeFlags), args)
}

func (st *statsCmds) computeFS(ctx context.Context, fwfs filewalk.FS, cf *computeFlags, args []string) error {

	parser := boolexpr.NewParser(ctx, fwfs)

	ctx, cfg, target, rdb, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], true)
	if err != nil {
		return err
	}

	match, err := boolexpr.CreateMatcher(parser,
		boolexpr.WithEntryExpression(args[1:]...),
		boolexpr.WithEmptyEntryValue(true),
		boolexpr.WithFilewalkFS(fwfs),
		boolexpr.WithHardlinkHandling(!cfg.CountHardlinkAsFiles))
	if err != nil {
		return err
	}

	sdb, err := st.computeStats(ctx, rdb, cfg, match, target, cf.ComputeN, cf.Progress)
	if err != nil {
		rdb.Close(ctx)
		return err
	}
	rdb.Close(ctx)

	// Save stats.
	stats := statsFileFormat{
		Prefix:     target,
		Date:       time.Now(),
		Expression: match.String(),
		Stats:      sdb,
	}
	statsDir := cf.StatsDir
	if len(statsDir) == 0 {
		statsDir = cfg.StatsDir
	}
	if len(statsDir) == 0 {
		statsDir = "stats"
	}
	return saveStats(statsDir, cf.StatsFile, stats)
}

// computeStats computes stats for prefix using the precomputed summaries
// unless they cannot be used, in which case all of the file metadata is
// rescanned: summaries are computed without any filter expression and only
// record what's needed to deduplicate hardlinks if configured to do so.
func (st *statsCmds) computeStats(ctx context.Context, db database.DB, cfg config.Prefix, match boolexpr.Matcher, prefix string, topN int, progress bool) (*reports.AllStats, error) {
	if match.HasExpression() || !cfg.CountHardlinkAsFiles && !cfg.RecordHardlinks() {
		return st.computeFromPrefixes(ctx, db, &cfg, match, prefix, topN, progress)
	}
	return st.computeFromSummaries(ctx, db, &cfg, prefix, topN, progress)
}

func (st *statsCmds) computeFromSummaries(ctx context.Context, db database.DB, cfg *config.Prefix, prefix string, topN int, progress bool) (*reports.AllStats, error) {
	calcID, sep := cfg.CalculatorOrDefault().String(), cfg.SeparatorOrDefault()
	dedup := !cfg.CountHardlinkAsFiles
	if err := internal.CheckSummaries(ctx, db, calcID, dedup); err != nil {
		return nil, err
	}
	var hl *hardlinks.Incremental
	if dedup {
		hl = &hardlinks.Incremental{}
	}
	sdb := reports.NewAllStats(prefix, topN)
	agg := reports.NewSubtreeAggregator(prefix, topN)
	exactKey, childPrefix := stats.SummaryKeys(sep, prefix)
	n := 0
	var summaryErr error
	err := db.ScanSummaries(ctx, exactKey, childPrefix, func(_ context.Context, k string, v []byte) bool {
		if progress && (n != 0 && n%1000 == 0) {
			fmt.Printf("processed % 10v summary entries\n", fmtCount(int64(n)))
		}
		n++
		path := stats.DecodeSummaryKey(sep, k)
		var ps stats.PrefixSummary
		if err := ps.UnmarshalBinary(v); err != nil {
			summaryErr = fmt.Errorf("%v: %w", path, err)
			return false
		}
		if ps.CalcID != calcID || dedup && !ps.HardlinkInfo {
			summaryErr = fmt.Errorf("%v: %w", path, internal.ErrStaleSummaries)
			return false
		}
		if dedup {
			// Summaries are visited in DFS pre-order, hence the
			// first of a set of hardlinks encountered is the one counted.
			ps.ApplyHardlinks(hl)
		}
		sdb.RecordDirect(path, ps.Direct, ps.PerUser, ps.PerGroup)
		agg.Step(k, path, ps.Direct)
		return true
	})
	if err == nil {
		err = summaryErr
	}
	if err != nil {
		return nil, err
	}
	if progress {
		fmt.Printf("processed % 10v summary entries\n", fmtCount(int64(n)))
	}
	agg.Finish()
	sdb.Subtree = agg.Subtree
	sdb.Distributed = agg.Distributed
	sdb.Finalize()
	return sdb, nil
}

// computeFromPrefixes computes stats from all of the file metadata stored for
// each prefix. The prefixes are visited in DFS pre-order using the summary
// keys, which exist for every prefix, so that memory usage is proportional to
// the depth of the tree and hardlinks are attributed to the same prefixes as
// when using the summaries.
func (st *statsCmds) computeFromPrefixes(ctx context.Context, db database.DB, cfg *config.Prefix, match boolexpr.Matcher, prefix string, topN int, progress bool) (*reports.AllStats, error) {
	if err := internal.CheckSummaryKeys(ctx, db); err != nil {
		return nil, err
	}
	calc, sep := cfg.CalculatorOrDefault(), cfg.SeparatorOrDefault()
	sdb := reports.NewAllStats(prefix, topN)
	agg := reports.NewSubtreeAggregator(prefix, topN)
	exactKey, childPrefix := stats.SummaryKeys(sep, prefix)
	n := 0
	var buf bytes.Buffer
	var getErr error
	err := db.ScanSummaries(ctx, exactKey, childPrefix, func(ctx context.Context, sk string, _ []byte) bool {
		if progress && (n != 0 && n%1000 == 0) {
			fmt.Printf("processed % 10v entries\n", fmtCount(int64(n)))
		}
		n++
		k := stats.DecodeSummaryKey(sep, sk)
		buf.Reset()
		if getErr = db.Get(ctx, k, &buf); getErr != nil {
			return false
		}
		if buf.Len() == 0 {
			fmt.Fprintf(os.Stderr, "missing metadata for %v\n", k)
			return true
		}
		var pi prefixinfo.T
		if err := pi.UnmarshalBinary(buf.Bytes()); err != nil {
			fmt.Fprintf(os.Stderr, "failed to unmarshal value for %v: %v\n", k, err)
			return true
		}
		if !match.Prefix(k, &pi) {
			return true
		}
		totals, users, groups := stats.ComputeTotals(k, &pi, calc, match)
		sdb.RecordDirect(k, totals, users, groups)
		agg.Step(sk, k, totals)
		return true
	})
	if err == nil {
		err = getErr
	}
	if err != nil {
		return nil, err
	}
	if progress {
		fmt.Printf("processed % 10v entries\n", fmtCount(int64(n)))
	}
	agg.Finish()
	sdb.Subtree = agg.Subtree
	sdb.Distributed = agg.Distributed
	sdb.Finalize()
	return sdb, nil
}

func (st *statsCmds) view(_ context.Context, values interface{}, args []string) error {
	af := values.(*viewFlags)
	if len(af.User) != 0 && len(af.Group) != 0 {
		return fmt.Errorf("only one of --user or --group may be specified")
	}

	stats, err := loadStats(args[0])
	if err != nil {
		return err
	}

	if af.Info {
		fmt.Printf("Date       : %v\n", stats.Date)
		fmt.Printf("Prefix     : %v\n", stats.Prefix)
		fmt.Printf("Expression : %v\n", stats.Expression)
		fmt.Println()
	}

	sdb := stats.Stats
	when := stats.Date

	if len(af.User) != 0 {
		return st.userOrGroup(af, stats, af.User, usernames.Manager.UIDForName)
	}

	if len(af.Group) != 0 {
		return st.userOrGroup(af, stats, af.Group, usernames.Manager.GIDForName)
	}

	heapFormatter[string]{}.formatTotals(sdb.Prefix, os.Stdout)

	banner(os.Stdout, "=", "Usage by top %v Prefixes as of: %v\n", af.DisplayN, when)
	heapFormatter[string]{}.formatHeaps(sdb.Prefix, os.Stdout, func(v string) string { return v }, af.DisplayN)

	if sdb.Subtree != nil {
		banner(os.Stdout, "=", "\nUsage by top %v Subtrees as of: %v\n", af.DisplayN, when)
		heapFormatter[string]{}.formatHeaps(sdb.Subtree, os.Stdout, func(v string) string { return v }, af.DisplayN)
	}

	if sdb.Distributed != nil {
		banner(os.Stdout, "=", "\nDistributed Subtree Hotspots (top %v, modest fanout) as of: %v\n", af.DisplayN, when)
		formatDistributedHeaps(sdb.Distributed, os.Stdout, af.DisplayN)
	}

	banner(os.Stdout, "=", "\nUsage by top %v users as of: %v\n", af.DisplayN, when)
	heapFormatter[int64]{}.formatHeaps(sdb.ByUser, os.Stdout,
		usernames.Manager.NameForUID, af.DisplayN)

	banner(os.Stdout, "=", "\nUsage by top %v groups as of: %v\n", af.DisplayN, when)
	heapFormatter[int64]{}.formatHeaps(sdb.ByGroup, os.Stdout,
		usernames.Manager.NameForGID, af.DisplayN)
	return nil
}

func (st *statsCmds) userOrGroup(af *viewFlags, stats statsFileFormat, name string, mapper func(string) (int64, error)) error {
	sdb := stats.Stats
	when := stats.Date

	id, err := mapper(name)
	if err != nil {
		return err
	}

	banner(os.Stdout, "=", "Usage by %v as of: %v\n", name, when)
	st.formatPerIDStats(sdb.PerUser, os.Stdout, usernames.Manager.NameForUID, map[int64]bool{id: true}, af.DisplayN)
	return nil
}

type heapFormatter[T comparable] struct{}

func (hf heapFormatter[T]) formatHeap(heap *heap.MinMax[int64, T], out io.Writer, kf func(size int64) string, vf func(T) string, n int) {
	i := 0
	for heap.Len() > 0 {
		k, v := heap.PopMax()
		fmt.Fprintf(out, "%v: %v\n", kf(k), vf(v))
		i++
		if i >= n {
			break
		}
	}
}

func (hf heapFormatter[T]) formatHeaps(h *reports.Heaps[T], out io.Writer, valueFormatter func(T) string, n int) {
	banner(out, "-", "Bytes used\n")
	hf.formatHeap(h.Bytes, out, fmtSize, valueFormatter, n)
	banner(out, "-", "\nBytes used on underlying filesystem\n")
	hf.formatHeap(h.StorageBytes, out, fmtSize, valueFormatter, n)
	banner(out, "-", "\nNumber of Files\n")
	hf.formatHeap(h.Files, out, fmtCount, valueFormatter, n)
	banner(out, "-", "\nNumber of Prefixes/Directories\n")
	hf.formatHeap(h.Prefixes, out, fmtCount, valueFormatter, n)
}

func (hf heapFormatter[T]) formatTotals(h *reports.Heaps[T], out io.Writer) {
	banner(out, "-", "Totals\n")
	fmt.Fprintf(out, "Bytes:     %v (%v)\n", fmtSize(h.TotalBytes), fmtKiBytes(h.TotalBytes))
	fmt.Fprintf(out, "Storage:   %v (%v)\n", fmtSize(h.TotalStorageBytes), fmtKiBytes(h.TotalStorageBytes))
	fmt.Fprintf(out, "Files:     %v\n", fmtCount(h.TotalFiles))
	fmt.Fprintf(out, "Prefixes:  %v\n", fmtCount(h.TotalPrefixes))
	fmt.Fprintf(out, "Total:     %v\n", fmtCount(h.TotalFiles+h.TotalPrefixes))
	fmt.Fprintf(out, "Links:     %v\n", fmtCount(h.TotalHardlinks))
	fmt.Fprintf(out, "Link dirs: %v\n\n", fmtCount(h.TotalHardlinkDirs))
}

func formatDistributedHeaps(dh *reports.DistributedHeaps, out io.Writer, n int) {
	printSection := func(title string, list []reports.DistributedStats, metric func(reports.DistributedStats) (string, string, float64)) {
		if len(list) == 0 {
			return
		}
		banner(out, "-", "%s\n", title)
		for _, ds := range list {
			distVal, maxLeafVal, amp := metric(ds)
			fmt.Fprintf(out, "%s (distributed; max-leaf: %s, %.1fx across %s dirs, max-child: %.0f%%): %s\n",
				distVal, maxLeafVal, amp, fmtCount(ds.SubtreeDirs), ds.MaxChildShare*100, ds.Prefix)
		}
	}
	printSection("Distributed Bytes used", reports.TopDistributed(dh.Bytes, n), func(ds reports.DistributedStats) (string, string, float64) {
		return fmtSize(ds.DistributedBytes), fmtSize(ds.MaxLeafBytes), ds.BytesAmplification
	})
	printSection("\nDistributed Storage Bytes used", reports.TopDistributed(dh.StorageBytes, n), func(ds reports.DistributedStats) (string, string, float64) {
		return fmtSize(ds.DistributedStorage), fmtSize(ds.MaxLeafStorage), ds.StorageAmplification
	})
	printSection("\nDistributed Files", reports.TopDistributed(dh.Files, n), func(ds reports.DistributedStats) (string, string, float64) {
		return fmtCount(ds.DistributedFiles), fmtCount(ds.MaxLeafFiles), ds.FilesAmplification
	})
}

func (st *statsCmds) formatPerIDStats(s reports.PerIDStats, out io.Writer, nameForID func(int64) string, ids map[int64]bool, n int) {
	for id, h := range s.ByPrefix {
		if len(ids) != 0 && !ids[id] {
			continue
		}
		banner(out, "=", "\n%v\n", nameForID(id))
		heapFormatter[string]{}.formatHeaps(h, out, func(v string) string { return v }, n)
	}
}
