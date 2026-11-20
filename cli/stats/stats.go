// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package stats implements idu's stats commands: compute, which computes
// statistics from the database and saves them to a file, view, which
// displays them, and suggest, which suggests configuration settings based
// on the state of the database.
package stats

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/internal/usernames"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmd/idu/summary"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/localfs"
)

func saveStats(dir, file string, stats cli.StatsFile) error {
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
		return stats.Encode(out)
	}
	buf := &bytes.Buffer{}
	if err := stats.Encode(buf); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	basename := stats.Date.Format(cli.StatsFileLayout)
	basename = strings.TrimSuffix(basename, ".idustats") + ".idustats"
	filename := filepath.Join(dir, basename)
	if err := os.WriteFile(filename, buf.Bytes(), 0660); err != nil { //nolint:gosec
		return err
	}
	sl := filepath.Join(dir, "latest.idustats")
	os.Remove(sl)
	if err := os.Symlink(basename, sl); err != nil {
		if runtime.GOOS != "windows" {
			return err
		}
		// Creating symlinks on Windows requires developer mode or
		// administrator privileges, so fall back to a copy.
		return os.WriteFile(sl, buf.Bytes(), 0660) //nolint:gosec
	}
	return nil
}

// Commands implements the stats commands, ie. compute, view and suggest.
type Commands struct {
	env *cli.Env
	// readers is the number of goroutines used to read prefix metadata
	// concurrently, defaultPrefixReaders if zero.
	readers int
}

// defaultPrefixReaders is the default number of goroutines used to read
// prefix metadata concurrently. Stats computation is generally limited by
// the single goroutine that must process the metadata in order, so there is
// little benefit to more readers unless the database is not cached in memory.
const defaultPrefixReaders = 4

// DisplayFlags are the flags that control how stats are displayed.
type DisplayFlags struct {
	DisplayN int  `subcmd:"display,10,number of top entries to display"`
	Progress bool `subcmd:"progress,false,show progress"`
}

// ComputeFlags are the flags for the stats compute command.
type ComputeFlags struct {
	cli.FSFlag
	ComputeN  int             `subcmd:"n,2000,number of top entries to compute"`
	Scan      bool            `subcmd:"scan,false,'compute the stats from all of the summaries for the prefix, rather than from the rankings and recursive totals maintained by analyze, which are used by default for the root of a filesystem but do not include the distributed subtree statistics and limit the per user and group rankings to topn_per_id entries'"`
	Progress  bool            `subcmd:"progress,false,show progress"`
	StatsDir  string          `subcmd:"stats-dir,,'directory that stats files are written to, defaults to stats_dir from the configuration or stats'"`
	StatsFile string          `subcmd:"stats-file,,'write stats to the specified file, rather than a directory, use - for stdout'"`
	Prefix    flags.Repeating `subcmd:"prefix,,'prefix match expression'"`
}

// ViewFlags are the flags for the stats view command.
type ViewFlags struct {
	DisplayFlags
	FS       string `subcmd:"fs,,'the name of the configured filesystem to operate on, overrides the global --fs flag'"`
	StatsDir string `subcmd:"stats-dir,,'directory that stats files are read from, defaults to stats_dir from the configuration of the filesystem specified by --fs; --fs is not required if this flag is set'"`
	User     string `subcmd:"user,,display stats for the specified user"`
	Group    string `subcmd:"group,,display stats for the specified group"`
	Info     bool   `subcmd:"info,false,display metadata for the stats file"`
}

// New returns the Commands that use env.
func New(env *cli.Env) *Commands {
	return &Commands{env: env}
}

// Compute implements the stats compute command, values must be a
// *ComputeFlags.
func (st *Commands) Compute(ctx context.Context, values interface{}, args []string) error {
	// TODO(cnicolaou): generalize this to other filesystems.
	fs := localfs.New()
	return st.computeFS(ctx, fs, values.(*ComputeFlags), args)
}

func (st *Commands) computeFS(ctx context.Context, fwfs filewalk.FS, cf *ComputeFlags, args []string) error {

	parser := stats.NewParser(ctx, fwfs)

	env := st.env.ForFS(cf.FS)
	ctx, f, target, err := env.LookupPrefix(ctx, args[0])
	if err != nil {
		return err
	}
	cfg := f.Config()
	rdb, err := env.OpenReadOnlyDB(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to open database for %v in %v: %w", iduconfig.Label(f), cfg.Database, err)
	}
	defer rdb.Close(ctx)

	match, err := stats.NewMatcher(parser,
		stats.WithExpression(args[1:]...),
		stats.WithEmptyValue(true),
		stats.WithFilewalkFS(fwfs),
		stats.WithHardlinkHandling(!cfg.CountHardlinkAsFiles))
	if err != nil {
		return err
	}

	useStored := !cf.Scan && isFilesystemRoot(cfg, f.Location(), target)
	sdb, source, perIDN, err := st.computeStats(ctx, rdb, cfg, match, target, useStored, cf.ComputeN, cf.Progress)
	if err != nil {
		return err
	}

	// Save stats.
	stats := cli.StatsFile{
		Prefix:     target,
		Date:       time.Now(),
		Expression: match.String(),
		Stats:      sdb,
		Source:     source,
		PerIDN:     perIDN,
	}
	return saveStats(selectStatsDir(cf.StatsDir, cfg.StatsDir), cf.StatsFile, stats)
}

// computeStats computes stats for prefix and returns them together with
// their source and, when cli.StatsFromStored is used, the stored per-ID
// ranking limit. If useStored is true, and prefix is the root of the
// filesystem, they are constructed from the rankings and recursive totals
// maintained by analyze when those are available and up to date. Otherwise
// they are computed from the precomputed summaries unless they cannot be
// used, in which case all of the file metadata is rescanned: summaries are
// computed without any filter expression and only record what's needed to
// deduplicate hardlinks if configured to do so.
func (st *Commands) computeStats(ctx context.Context, db idudb.Reader, cfg *iduconfig.Settings, match stats.Matcher, prefix string, useStored bool, topN int, progress bool) (*report.AllStats, string, int, error) {
	if match.HasExpression() || !cfg.CountHardlinkAsFiles && !cfg.RecordHardlinks() {
		sdb, err := st.computeFromPrefixes(ctx, db, cfg, match, prefix, topN, progress)
		return sdb, cli.StatsFromSummaries, 0, err
	}
	if useStored {
		sdb, perIDN, err := st.computeFromStored(ctx, db, cfg, prefix, topN)
		if err == nil {
			return sdb, cli.StatsFromStored, perIDN, nil
		}
		if !errors.Is(err, errStoredUnusable) {
			return nil, "", 0, err
		}
		fmt.Fprintf(os.Stderr, "computing stats from the summaries: %v\n", err)
	}
	sdb, err := st.computeFromSummaries(ctx, db, cfg, prefix, topN, progress)
	return sdb, cli.StatsFromSummaries, 0, err
}

// isFilesystemRoot returns true if target is the root of the filesystem,
// which is the only prefix for which the stored rankings and recursive
// totals can be used, since they deduplicate hardlinks across the entire
// filesystem.
func isFilesystemRoot(cfg *iduconfig.Settings, root, target string) bool {
	sep := cfg.SeparatorOrDefault()
	rk, _ := stats.SummaryKeys(sep, root)
	tk, _ := stats.SummaryKeys(sep, target)
	return rk == tk
}

func (st *Commands) computeFromSummaries(ctx context.Context, db idudb.Reader, cfg *iduconfig.Settings, prefix string, topN int, progress bool) (*report.AllStats, error) {
	sr := summary.New(db, cfg)
	if err := sr.Check(ctx); err != nil {
		return nil, err
	}
	sdb := report.NewAllStats(prefix, topN)
	agg := report.NewSubtreeAggregator(prefix, topN)
	n := 0
	for e, err := range sr.Walk(ctx, prefix) {
		if err != nil {
			return nil, err
		}
		if progress && (n != 0 && n%1000 == 0) {
			fmt.Printf("processed % 10v summary entries\n", cli.FmtCount(int64(n)))
		}
		n++
		ps := e.Summary
		sdb.RecordDirect(e.Path, ps.Direct, ps.PerUser, ps.PerGroup)
		agg.Step(e.Key, e.Path, ps.Direct)
	}
	if progress {
		fmt.Printf("processed % 10v summary entries\n", cli.FmtCount(int64(n)))
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
// when using the summaries. The metadata is read and decoded concurrently,
// but the stats, including hardlink deduplication, are computed in order.
func (st *Commands) computeFromPrefixes(ctx context.Context, db idudb.Reader, cfg *iduconfig.Settings, match stats.Matcher, prefix string, topN int, progress bool) (*report.AllStats, error) {
	if err := idudb.CheckSummaryKeys(ctx, db); err != nil {
		return nil, err
	}
	calc, sep := cfg.CalculatorOrDefault(), cfg.SeparatorOrDefault()
	sdb := report.NewAllStats(prefix, topN)
	agg := report.NewSubtreeAggregator(prefix, topN)
	exactKey, childPrefix := stats.SummaryKeys(sep, prefix)
	readers := st.readers
	if readers == 0 {
		readers = min(defaultPrefixReaders, runtime.GOMAXPROCS(0))
	}
	n := 0
	err := readPrefixesInOrder(ctx, db, sep, exactKey, childPrefix, readers, func(e *prefixEntry) {
		if progress && (n != 0 && n%1000 == 0) {
			fmt.Printf("processed % 10v entries\n", cli.FmtCount(int64(n)))
		}
		n++
		switch {
		case e.missing:
			fmt.Fprintf(os.Stderr, "missing metadata for %v\n", e.prefix)
			return
		case e.decodeErr != nil:
			fmt.Fprintf(os.Stderr, "failed to unmarshal value: %v\n", e.decodeErr)
			return
		}
		if !e.pi.Match(match) {
			return
		}
		totals, users, groups := e.pi.Totals(calc, match)
		sdb.RecordDirect(e.prefix, totals, users, groups)
		agg.Step(e.summaryKey, e.prefix, totals)
	})
	if err != nil {
		return nil, err
	}
	if progress {
		fmt.Printf("processed % 10v entries\n", cli.FmtCount(int64(n)))
	}
	agg.Finish()
	sdb.Subtree = agg.Subtree
	sdb.Distributed = agg.Distributed
	sdb.Finalize()
	return sdb, nil
}

// statsSource describes the source of the stats in a stats file.
func statsSource(sf cli.StatsFile) string {
	switch sf.Source {
	case cli.StatsFromStored:
		return fmt.Sprintf("the rankings and recursive totals maintained by analyze (at most %v entries per user and group)", sf.PerIDN)
	case "", cli.StatsFromSummaries:
		return "the summaries"
	}
	return sf.Source
}

func selectStatsDir(statsDirFlag, cfgStatsDir string) string {
	if len(statsDirFlag) > 0 {
		return statsDirFlag
	}
	if len(cfgStatsDir) > 0 {
		return cfgStatsDir
	}
	return "stats"
}

func resolveDirectStatsFile(statsDirFlag, arg string) (string, bool) {
	if arg == "-" {
		return "-", true
	}
	if info, err := os.Stat(arg); err == nil && !info.IsDir() {
		return arg, true
	}
	if len(statsDirFlag) > 0 && !filepath.IsAbs(arg) {
		candidate := filepath.Join(statsDirFlag, arg)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, true
		}
	}
	return "", false
}

// resolveStatsFilename returns the stats file to be viewed. With no
// arguments it is the latest.idustats file in the stats directory. With a
// single argument it is that file if it exists (or is -), otherwise it is
// the named file in the stats directory. The stats directory is the one
// specified by --stats-dir, if set, in which case --fs is not required,
// otherwise it is the stats directory for the filesystem specified by --fs.
func resolveStatsFilename(ctx context.Context, env *cli.Env, fsFlag, statsDirFlag string, args []string) (string, error) {
	if len(args) > 1 {
		return "", fmt.Errorf("too many arguments: expected [<filename>]")
	}
	name := "latest.idustats"
	if len(args) == 1 {
		if f, ok := resolveDirectStatsFile(statsDirFlag, args[0]); ok {
			return f, nil
		}
		name = args[0]
		if filepath.IsAbs(name) {
			return name, nil
		}
	}
	if len(statsDirFlag) > 0 {
		return filepath.Join(statsDirFlag, name), nil
	}
	pcfg, _, err := env.ForFS(fsFlag).LookupFS("")
	if err != nil {
		if loadErr := cli.ConfigLoadError(ctx); loadErr != nil {
			return "", loadErr
		}
		return "", err
	}
	return filepath.Join(selectStatsDir(statsDirFlag, pcfg.Config().StatsDir), name), nil
}

// View implements the stats view command, values must be a *ViewFlags.
func (st *Commands) View(ctx context.Context, values interface{}, args []string) error {
	af := values.(*ViewFlags)
	if len(af.User) != 0 && len(af.Group) != 0 {
		return fmt.Errorf("only one of --user or --group may be specified")
	}

	filename, err := resolveStatsFilename(ctx, st.env, af.FS, af.StatsDir, args)
	if err != nil {
		return err
	}
	stats, err := cli.LoadStats(filename)
	if err != nil {
		return err
	}

	if af.Info {
		fmt.Printf("Date       : %v\n", stats.Date)
		fmt.Printf("Prefix     : %v\n", stats.Prefix)
		fmt.Printf("Expression : %v\n", stats.Expression)
		fmt.Printf("Source     : %v\n", statsSource(stats))
		fmt.Println()
	}

	sdb := stats.Stats
	when := stats.Date

	if len(af.User) != 0 {
		return st.userOrGroup(af, stats, af.User, usernames.Manager.UIDForName, sdb.PerUser, usernames.Manager.NameForUID)
	}

	if len(af.Group) != 0 {
		return st.userOrGroup(af, stats, af.Group, usernames.Manager.GIDForName, sdb.PerGroup, usernames.Manager.NameForGID)
	}

	heapFormatter[string]{fmtSize: st.env.FmtSize}.formatTotals(sdb.Prefix, os.Stdout)

	cli.Banner(os.Stdout, "=", "Usage by top %v Prefixes as of: %v\n", af.DisplayN, when)
	heapFormatter[string]{fmtSize: st.env.FmtSize}.formatHeaps(sdb.Prefix, os.Stdout, func(v string) string { return v }, af.DisplayN)

	if sdb.Subtree != nil {
		cli.Banner(os.Stdout, "=", "\nUsage by top %v Subtrees as of: %v\n", af.DisplayN, when)
		heapFormatter[string]{fmtSize: st.env.FmtSize}.formatHeaps(sdb.Subtree, os.Stdout, func(v string) string { return v }, af.DisplayN)
	}

	if sdb.Distributed != nil {
		cli.Banner(os.Stdout, "=", "\nDistributed Subtree Hotspots (top %v, modest fanout) as of: %v\n", af.DisplayN, when)
		formatDistributedHeaps(sdb.Distributed, os.Stdout, st.env.FmtSize, af.DisplayN)
	} else if stats.Source == cli.StatsFromStored {
		fmt.Printf("\nDistributed subtree hotspots were not computed, use stats compute --scan to compute them.\n")
	}

	cli.Banner(os.Stdout, "=", "\nUsage by top %v users as of: %v\n", af.DisplayN, when)
	heapFormatter[int64]{fmtSize: st.env.FmtSize}.formatHeaps(sdb.ByUser, os.Stdout,
		usernames.Manager.NameForUID, af.DisplayN)

	cli.Banner(os.Stdout, "=", "\nUsage by top %v groups as of: %v\n", af.DisplayN, when)
	heapFormatter[int64]{fmtSize: st.env.FmtSize}.formatHeaps(sdb.ByGroup, os.Stdout,
		usernames.Manager.NameForGID, af.DisplayN)
	return nil
}

func (st *Commands) userOrGroup(af *ViewFlags, stats cli.StatsFile, name string, mapper func(string) (int64, error), perID report.PerIDStats, nameForID func(int64) string) error {
	when := stats.Date

	id, err := mapper(name)
	if err != nil {
		return err
	}

	cli.Banner(os.Stdout, "=", "Usage by %v as of: %v\n", name, when)
	st.formatPerIDStats(perID, os.Stdout, nameForID, map[int64]bool{id: true}, af.DisplayN)
	return nil
}

type heapFormatter[T comparable] struct {
	fmtSize func(int64) string
}

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

func (hf heapFormatter[T]) formatHeaps(h *report.Heaps[T], out io.Writer, valueFormatter func(T) string, n int) {
	cli.Banner(out, "-", "Bytes used\n")
	hf.formatHeap(h.Bytes, out, hf.fmtSize, valueFormatter, n)
	cli.Banner(out, "-", "\nBytes used on underlying filesystem\n")
	hf.formatHeap(h.StorageBytes, out, hf.fmtSize, valueFormatter, n)
	cli.Banner(out, "-", "\nNumber of Files\n")
	hf.formatHeap(h.Files, out, cli.FmtCount, valueFormatter, n)
	cli.Banner(out, "-", "\nNumber of Prefixes/Directories\n")
	hf.formatHeap(h.Prefixes, out, cli.FmtCount, valueFormatter, n)
	if h.SparseBytes != nil && h.SparseBytes.Len() > 0 {
		cli.Banner(out, "-", "\nBytes in sparse files not allocated on the underlying filesystem\n")
		hf.formatHeap(h.SparseBytes, out, hf.fmtSize, valueFormatter, n)
	}
}

func (hf heapFormatter[T]) formatTotals(h *report.Heaps[T], out io.Writer) {
	cli.Banner(out, "-", "Totals\n")
	fmt.Fprintf(out, "Bytes:     %v (%v)\n", hf.fmtSize(h.TotalBytes), cli.FmtKiBytes(h.TotalBytes))
	fmt.Fprintf(out, "Storage:   %v (%v)\n", hf.fmtSize(h.TotalStorageBytes), cli.FmtKiBytes(h.TotalStorageBytes))
	fmt.Fprintf(out, "Files:     %v\n", cli.FmtCount(h.TotalFiles))
	fmt.Fprintf(out, "Prefixes:  %v\n", cli.FmtCount(h.TotalPrefixes))
	fmt.Fprintf(out, "Total:     %v\n", cli.FmtCount(h.TotalFiles+h.TotalPrefixes))
	fmt.Fprintf(out, "Links:     %v\n", cli.FmtCount(h.TotalHardlinks))
	fmt.Fprintf(out, "Link dirs: %v\n", cli.FmtCount(h.TotalHardlinkDirs))
	fmt.Fprintf(out, "Sparse:    %v (%v not allocated)\n\n", cli.FmtCount(h.TotalSparseFiles), strings.TrimSpace(hf.fmtSize(h.TotalSparseBytes)))
}

func formatDistributedHeaps(dh *report.DistributedHeaps, out io.Writer, fmtSize func(int64) string, n int) {
	printSection := func(title string, list []report.DistributedStats, metric func(report.DistributedStats) (string, string, float64)) {
		if len(list) == 0 {
			return
		}
		cli.Banner(out, "-", "%s\n", title)
		for _, ds := range list {
			distVal, maxLeafVal, amp := metric(ds)
			fmt.Fprintf(out, "%s (distributed; max-leaf: %s, %.1fx across %s dirs, max-child: %.0f%%): %s\n",
				distVal, maxLeafVal, amp, cli.FmtCount(ds.SubtreeDirs), ds.MaxChildShare*100, ds.Prefix)
		}
	}
	printSection("Distributed Bytes used", report.TopDistributed(dh.Bytes, n), func(ds report.DistributedStats) (string, string, float64) {
		return fmtSize(ds.DistributedBytes), fmtSize(ds.MaxLeafBytes), ds.BytesAmplification
	})
	printSection("\nDistributed Storage Bytes used", report.TopDistributed(dh.StorageBytes, n), func(ds report.DistributedStats) (string, string, float64) {
		return fmtSize(ds.DistributedStorage), fmtSize(ds.MaxLeafStorage), ds.StorageAmplification
	})
	printSection("\nDistributed Files", report.TopDistributed(dh.Files, n), func(ds report.DistributedStats) (string, string, float64) {
		return cli.FmtCount(ds.DistributedFiles), cli.FmtCount(ds.MaxLeafFiles), ds.FilesAmplification
	})
}

func (st *Commands) formatPerIDStats(s report.PerIDStats, out io.Writer, nameForID func(int64) string, ids map[int64]bool, n int) {
	for id, h := range s.ByPrefix {
		if len(ids) != 0 && !ids[id] {
			continue
		}
		cli.Banner(out, "=", "\n%v\n", nameForID(id))
		heapFormatter[string]{fmtSize: st.env.FmtSize}.formatHeaps(h, out, func(v string) string { return v }, n)
	}
}
