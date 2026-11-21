// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package stats implements idu's stats commands: compute, which computes
// statistics from the database and saves them to a file, view, which
// displays them, and suggest, which suggests configuration settings based
// on the state of the database.
package stats

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/snapshot"
	"cloudeng.io/cmd/idu/usernames"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/localfs"
)

// saveSnapshot writes sf to file, - for stdout, if file is set, and
// otherwise as a new snapshot in dir.
func saveSnapshot(dir, file string, sf snapshot.File) (string, error) {
	if len(file) > 0 {
		out := os.Stdout
		if file != "-" {
			var err error
			out, err = os.Create(file)
			if err != nil {
				return "", err
			}
			defer out.Close()
		}
		return file, sf.Encode(out)
	}
	return snapshot.NewStore(dir).Save(sf)
}

// Commands implements the stats commands, ie. compute, view and suggest.
type Commands struct {
	env *cli.Env
}

// DisplayFlags are the flags that control how stats are displayed.
type DisplayFlags struct {
	DisplayN int  `subcmd:"display,10,number of top entries to display"`
	Progress bool `subcmd:"progress,false,show progress"`
}

// ComputeFlags are the flags for the stats compute command.
type ComputeFlags struct {
	cli.FSFlag
	ComputeN    int             `subcmd:"n,2000,number of top entries to compute"`
	Scan        bool            `subcmd:"scan,false,'compute the stats from all of the summaries for the prefix, rather than from the rankings and recursive totals maintained by analyze, which are used by default for the root of a filesystem but do not include the distributed subtree statistics and limit the per user and group rankings to topn_per_id entries'"`
	Progress    bool            `subcmd:"progress,false,show progress"`
	SnapshotDir string          `subcmd:"snapshot-dir,,'directory that the snapshot is written to, defaults to snapshot_dir from the configuration'"`
	StatsFile   string          `subcmd:"stats-file,,'write the snapshot to the specified file, rather than the snapshot directory, use - for stdout'"`
	Prefix      flags.Repeating `subcmd:"prefix,,'prefix match expression'"`
}

// ViewFlags are the flags for the stats view command.
type ViewFlags struct {
	DisplayFlags
	FS          string `subcmd:"fs,,'the name of the configured filesystem to operate on, overrides the global --fs flag'"`
	SnapshotDir string `subcmd:"snapshot-dir,,'directory that snapshots are read from, and written to, defaults to snapshot_dir from the configuration of the filesystem specified by --fs; --fs is not required if this flag is set and the configuration cannot be loaded, in which case the latest snapshot in it is displayed'"`
	User        string `subcmd:"user,,'display stats for the specified user, subset by --summary, --bytes, --storage and --file-counts'"`
	Group       string `subcmd:"group,,'display stats for the specified group, subset by --summary, --bytes, --storage and --file-counts'"`
	Info        bool   `subcmd:"info,false,display metadata for the stats file"`
	Summary     bool   `subcmd:"summary,false,'display the totals for the entire filesystem, or the user or group specified by --user or --group'"`
	Bytes       bool   `subcmd:"bytes,false,'display the top entries by the number of bytes reported, ie. file sizes'"`
	Storage     bool   `subcmd:"storage,false,'display the top entries by the number of bytes stored on the underlying filesystem'"`
	FileCounts  bool   `subcmd:"file-counts,false,'display the top entries by the number of files'"`
	Hotspots    bool   `subcmd:"hotspots,false,'display the distributed subtree hotspots, ie. subtrees whose usage is at least twice that of their largest directory; computing them requires a full pass over the database, which is saved as a snapshot, unless the snapshot used already includes them'"`
	Hardlinks   bool   `subcmd:"hardlinks,false,'display the number of hardlinks, the top entries by the number of hardlinks and any files found by analyze to have links that it has not seen; the rankings require a full pass over the database, which is saved as a snapshot, unless the snapshot used already includes them'"`
}

// selection determines what stats view displays.
type selection struct {
	summary, bytes, storage, files, hotspots, hardlinks bool
	// all is true if no selection flags were specified, in which case
	// everything, other than the hardlinks section, is displayed,
	// including the rankings by number of prefixes and sparse bytes.
	all bool
}

func newSelection(af *ViewFlags) selection {
	sel := selection{
		summary:   af.Summary,
		bytes:     af.Bytes,
		storage:   af.Storage,
		files:     af.FileCounts,
		hotspots:  af.Hotspots,
		hardlinks: af.Hardlinks,
	}
	sel.all = !sel.summary && !sel.anyMetric() && !sel.hotspots && !sel.hardlinks
	return sel
}

func (sel selection) anyMetric() bool {
	return sel.bytes || sel.storage || sel.files
}

// rankings returns true if the rankings of prefixes, subtrees, users and
// groups are to be displayed, ie. if any of --bytes, --storage or
// --file-counts were specified without --hotspots or --hardlinks, which
// they otherwise subset.
func (sel selection) rankings() bool {
	return sel.all || (sel.anyMetric() && !sel.hotspots && !sel.hardlinks)
}

// showHotspots returns true if the distributed hotspots are to be displayed.
func (sel selection) showHotspots() bool {
	return sel.rankings() || sel.hotspots
}

// metric returns true if the ranking selected by flag is to be displayed.
func (sel selection) metric(flag bool) bool {
	return sel.all || flag || !sel.anyMetric()
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

	env := st.env.ForFS(cf.FS)
	ctx, f, target, err := env.LookupPrefix(ctx, args[0])
	if err != nil {
		return err
	}
	cfg := f.Config()
	dir := cf.SnapshotDir
	if dir == "" {
		dir = cfg.SnapshotDir
	}
	if dir == "" && cf.StatsFile == "" {
		return fmt.Errorf("snapshot_dir is not set for %v, set it, or use --snapshot-dir or --stats-file", iduconfig.Label(f))
	}
	rdb, err := env.OpenReadOnlyDB(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to open database for %v in %v: %w", iduconfig.Label(f), cfg.Database, err)
	}
	defer rdb.Close(ctx)

	match, err := snapshot.NewMatcher(ctx, fwfs, cfg, args[1:]...)
	if err != nil {
		return err
	}

	isRoot := snapshot.IsFilesystemRoot(cfg, f.Location(), target)
	c := snapshot.Computer{Progress: cf.Progress}
	sdb, source, perIDN, err := c.Compute(ctx, rdb, cfg, match, target, !cf.Scan && isRoot, cf.ComputeN)
	if err != nil {
		return err
	}
	_, err = saveSnapshot(dir, cf.StatsFile, snapshot.File{
		Prefix:     target,
		Date:       time.Now(),
		Expression: match.String(),
		Stats:      sdb,
		Source:     source,
		Whole:      isRoot && !match.HasExpression(),
		PerIDN:     perIDN,
	})
	return err
}

func resolveDirectStatsFile(snapshotDirFlag, arg string) (string, bool) {
	if arg == "-" {
		return "-", true
	}
	if info, err := os.Stat(arg); err == nil && !info.IsDir() {
		return arg, true
	}
	if len(snapshotDirFlag) > 0 && !filepath.IsAbs(arg) {
		candidate := filepath.Join(snapshotDirFlag, arg)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, true
		}
	}
	return "", false
}

// loadSnapshot returns the snapshot to be viewed and its path, which is
// empty for stats constructed from the database without being saved.
//
// With a single argument it is that file if it exists (or is -), and
// otherwise the named file in the snapshot directory, ie. the one
// specified by --snapshot-dir or the snapshot_dir of the filesystem
// specified by --fs.
//
// With no arguments it is the most recent snapshot of the entire
// filesystem that is more recent than the last update to its database,
// stats constructed from the database if there is no such snapshot, or a
// new snapshot if constructing them requires a full pass over the
// database, see snapshot.Computer.Current. If the configuration cannot be
// loaded and --snapshot-dir is set, it is the latest snapshot in that
// directory.
func (st *Commands) loadSnapshot(ctx context.Context, af *ViewFlags, args []string) (snapshot.File, string, error) {
	if len(args) > 1 {
		return snapshot.File{}, "", fmt.Errorf("too many arguments: expected [<filename>]")
	}
	env := st.env.ForFS(af.FS)
	f, _, lookupErr := env.LookupFS("")
	if lookupErr != nil {
		if loadErr := cli.ConfigLoadError(ctx); loadErr != nil {
			lookupErr = loadErr
		}
	}
	dir := af.SnapshotDir
	if dir == "" && lookupErr == nil {
		dir = f.Config().SnapshotDir
	}
	if len(args) == 1 {
		filename, ok := resolveDirectStatsFile(af.SnapshotDir, args[0])
		if !ok {
			switch {
			case filepath.IsAbs(args[0]):
				filename = args[0]
			case dir != "":
				filename = filepath.Join(dir, args[0])
			case lookupErr != nil:
				return snapshot.File{}, "", lookupErr
			default:
				return snapshot.File{}, "", fmt.Errorf("%v: not found and snapshot_dir is not set for %v", args[0], iduconfig.Label(f))
			}
		}
		sf, err := snapshot.Load(filename)
		return sf, filename, err
	}
	if lookupErr != nil {
		if af.SnapshotDir == "" {
			return snapshot.File{}, "", lookupErr
		}
		filename := snapshot.NewStore(af.SnapshotDir).LatestPath()
		sf, err := snapshot.Load(filename)
		return sf, filename, err
	}
	ctx, f, _, err := env.LookupPrefix(ctx, "")
	if err != nil {
		return snapshot.File{}, "", err
	}
	c := snapshot.Computer{Progress: af.Progress, Hotspots: af.Hotspots, Hardlinks: af.Hardlinks}
	return c.Current(ctx, f, dir, af.DisplayN, env.OpenOptions()...)
}

// View implements the stats view command, values must be a *ViewFlags.
func (st *Commands) View(ctx context.Context, values interface{}, args []string) error {
	af := values.(*ViewFlags)
	if len(af.User) != 0 && len(af.Group) != 0 {
		return fmt.Errorf("only one of --user or --group may be specified")
	}

	stats, filename, err := st.loadSnapshot(ctx, af, args)
	if err != nil {
		return err
	}
	if filename == "" {
		filename = "none, computed from the database"
	}

	if af.Info {
		fmt.Printf("Date       : %v\n", stats.Date)
		fmt.Printf("Prefix     : %v\n", stats.Prefix)
		fmt.Printf("Expression : %v\n", stats.Expression)
		fmt.Printf("Source     : %v\n", stats.DescribeSource())
		fmt.Printf("Snapshot   : %v\n", filename)
		fmt.Println()
	}

	sel := newSelection(af)
	if len(af.User) != 0 {
		return st.userOrGroup(af, sel, stats, af.User, usernames.Manager.UIDForName, stats.Stats.PerUser, usernames.Manager.NameForUID)
	}
	if len(af.Group) != 0 {
		return st.userOrGroup(af, sel, stats, af.Group, usernames.Manager.GIDForName, stats.Stats.PerGroup, usernames.Manager.NameForGID)
	}
	st.viewFilesystem(os.Stdout, sel, stats, af.DisplayN)
	if sel.hardlinks {
		st.viewHardlinks(ctx, os.Stdout, af, stats)
	}
	return nil
}

// viewFilesystem displays the selected stats for the entire filesystem.
func (st *Commands) viewFilesystem(out io.Writer, sel selection, sf snapshot.File, n int) {
	sdb, when := sf.Stats, sf.Date
	hf := heapFormatter[string]{fmtSize: st.env.FmtSize, sel: sel}
	if sel.all || sel.summary {
		hf.formatTotals(sdb.Prefix, out)
	}
	if sel.rankings() {
		cli.Banner(out, "=", "Usage by top %v Prefixes as of: %v\n", n, when)
		hf.formatHeaps(sdb.Prefix, out, func(v string) string { return v }, n)
		if sdb.Subtree != nil {
			cli.Banner(out, "=", "\nUsage by top %v Subtrees as of: %v\n", n, when)
			hf.formatHeaps(sdb.Subtree, out, func(v string) string { return v }, n)
		}
	}
	if sel.showHotspots() {
		if sdb.Distributed != nil {
			cli.Banner(out, "=", "\nDistributed Subtree Hotspots (top %v, modest fanout) as of: %v\n", n, when)
			formatDistributedHeaps(sdb.Distributed, out, st.env.FmtSize, sel, n)
		} else {
			fmt.Fprintf(out, "\nDistributed subtree hotspots were not computed, use stats view --hotspots, or stats compute --scan, to compute them.\n")
		}
	}
	if sel.rankings() {
		idf := heapFormatter[int64]{fmtSize: st.env.FmtSize, sel: sel}
		cli.Banner(out, "=", "\nUsage by top %v users as of: %v\n", n, when)
		idf.formatHeaps(sdb.ByUser, out, usernames.Manager.NameForUID, n)
		cli.Banner(out, "=", "\nUsage by top %v groups as of: %v\n", n, when)
		idf.formatHeaps(sdb.ByGroup, out, usernames.Manager.NameForGID, n)
	}
}

// hardlinksUnavailable is displayed when the snapshot does not include the
// rankings by number of hardlinks.
const hardlinksUnavailable = "The rankings by number of hardlinks are not included in this snapshot, use stats view --hardlinks, or stats compute --scan, to compute them.\n"

// viewHardlinks displays the number of hardlinks, the top entries by
// number of hardlinks and any missing links found by analyze.
func (st *Commands) viewHardlinks(ctx context.Context, out io.Writer, af *ViewFlags, sf snapshot.File) {
	sdb, n := sf.Stats, af.DisplayN
	cli.Banner(out, "=", "\nHardlinks as of: %v\n", sf.Date)
	fmt.Fprintf(out, "Links:     %v (counted once)\n", cli.FmtCount(sdb.Prefix.TotalHardlinks))
	fmt.Fprintf(out, "Link dirs: %v (eg. bind mounts)\n", cli.FmtCount(sdb.Prefix.TotalHardlinkDirs))
	if sdb.Prefix.Hardlinks == nil {
		fmt.Fprint(out, "\n"+hardlinksUnavailable)
	} else {
		pf := heapFormatter[string]{}
		cli.Banner(out, "-", "\nTop %v prefixes by number of hardlinks\n", n)
		pf.formatHeap(sdb.Prefix.Hardlinks, out, cli.FmtCount, func(v string) string { return v }, n)
		if sdb.Subtree != nil && sdb.Subtree.Hardlinks != nil {
			cli.Banner(out, "-", "\nTop %v subtrees by number of hardlinks\n", n)
			pf.formatHeap(sdb.Subtree.Hardlinks, out, cli.FmtCount, func(v string) string { return v }, n)
		}
		idf := heapFormatter[int64]{}
		for _, ids := range []struct {
			what string
			h    *report.Heaps[int64]
			name func(int64) string
		}{{"users", sdb.ByUser, usernames.Manager.NameForUID}, {"groups", sdb.ByGroup, usernames.Manager.NameForGID}} {
			if ids.h != nil && ids.h.Hardlinks != nil {
				cli.Banner(out, "-", "\nTop %v %v by number of hardlinks\n", n, ids.what)
				idf.formatHeap(ids.h.Hardlinks, out, cli.FmtCount, ids.name, n)
			}
		}
	}
	st.viewMissingLinks(ctx, out, af)
}

// viewMissingLinks displays a summary of the files found by analyze to
// have links that it has not seen, if the configuration and database for
// the filesystem are available.
func (st *Commands) viewMissingLinks(ctx context.Context, out io.Writer, af *ViewFlags) {
	env := st.env.ForFS(af.FS)
	ctx, f, _, err := env.LookupPrefix(ctx, "")
	if err != nil {
		return
	}
	cfg := f.Config()
	if cfg.CountHardlinkAsFiles {
		fmt.Fprintf(out, "\nHardlinks are counted as separate files (count_hardlinks_as_files) and hence are not deduplicated.\n")
		return
	}
	if !cfg.RecordHardlinks() || cfg.TopNOrDefault() == 0 {
		return
	}
	db, err := env.OpenReadOnlyDB(ctx, cfg)
	if err != nil {
		return
	}
	defer db.Close(ctx)
	ml, err := idudb.ReadMissingLinks(ctx, db)
	if err != nil {
		fmt.Fprintf(out, "\nFailed to read the missing links: %v\n", err)
		return
	}
	var files int
	var links uint64
	for _, p := range ml.Prefixes {
		files += p.Files
		links += p.Links
	}
	if files == 0 {
		fmt.Fprintf(out, "\nNo files with links that analyze has not seen were found.\n")
		return
	}
	fmt.Fprintf(out, "\n%v files, in %v prefixes, have %v links that analyze has not seen; they may be counted more than once, see idu stats suggest.\n",
		cli.FmtCount(int64(files)), len(ml.Prefixes), links)
}

func (st *Commands) userOrGroup(af *ViewFlags, sel selection, stats snapshot.File, name string, mapper func(string) (int64, error), perID report.PerIDStats, nameForID func(int64) string) error {
	when := stats.Date

	id, err := mapper(name)
	if err != nil {
		return err
	}

	cli.Banner(os.Stdout, "=", "Usage by %v as of: %v\n", name, when)
	st.formatPerIDStats(perID, os.Stdout, sel, nameForID, map[int64]bool{id: true}, af.DisplayN)
	return nil
}

type heapFormatter[T comparable] struct {
	fmtSize func(int64) string
	sel     selection
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
	sel := hf.sel
	sep := ""
	section := func(show bool, title string, heap *heap.MinMax[int64, T], kf func(int64) string) {
		if !sel.metric(show) {
			return
		}
		cli.Banner(out, "-", "%s%s\n", sep, title)
		hf.formatHeap(heap, out, kf, valueFormatter, n)
		sep = "\n"
	}
	section(sel.bytes, "Bytes used", h.Bytes, hf.fmtSize)
	section(sel.storage, "Bytes used on underlying filesystem", h.StorageBytes, hf.fmtSize)
	section(sel.files, "Number of Files", h.Files, cli.FmtCount)
	if !sel.all {
		return
	}
	section(true, "Number of Prefixes/Directories", h.Prefixes, cli.FmtCount)
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

func formatDistributedHeaps(dh *report.DistributedHeaps, out io.Writer, fmtSize func(int64) string, sel selection, n int) {
	empty := func(h *heap.MinMax[int64, report.DistributedStats]) bool { return h == nil || h.Len() <= 0 }
	if empty(dh.Bytes) && empty(dh.StorageBytes) && empty(dh.Files) {
		fmt.Fprintf(out, "No distributed subtree hotspots were found.\n")
		return
	}
	sep := ""
	printSection := func(show bool, title string, list []report.DistributedStats, metric func(report.DistributedStats) (string, string, float64)) {
		if len(list) == 0 || !sel.metric(show) {
			return
		}
		cli.Banner(out, "-", "%s%s\n", sep, title)
		sep = "\n"
		for _, ds := range list {
			distVal, maxLeafVal, amp := metric(ds)
			fmt.Fprintf(out, "%s (distributed; max-leaf: %s, %.1fx across %s dirs, max-child: %.0f%%): %s\n",
				distVal, maxLeafVal, amp, cli.FmtCount(ds.SubtreeDirs), ds.MaxChildShare*100, ds.Prefix)
		}
	}
	printSection(sel.bytes, "Distributed Bytes used", report.TopDistributed(dh.Bytes, n), func(ds report.DistributedStats) (string, string, float64) {
		return fmtSize(ds.DistributedBytes), fmtSize(ds.MaxLeafBytes), ds.BytesAmplification
	})
	printSection(sel.storage, "Distributed Storage Bytes used", report.TopDistributed(dh.StorageBytes, n), func(ds report.DistributedStats) (string, string, float64) {
		return fmtSize(ds.DistributedStorage), fmtSize(ds.MaxLeafStorage), ds.StorageAmplification
	})
	printSection(sel.files, "Distributed Files", report.TopDistributed(dh.Files, n), func(ds report.DistributedStats) (string, string, float64) {
		return cli.FmtCount(ds.DistributedFiles), cli.FmtCount(ds.MaxLeafFiles), ds.FilesAmplification
	})
}

func (st *Commands) formatPerIDStats(s report.PerIDStats, out io.Writer, sel selection, nameForID func(int64) string, ids map[int64]bool, n int) {
	hf := heapFormatter[string]{fmtSize: st.env.FmtSize, sel: sel}
	for id, h := range s.ByPrefix {
		if len(ids) != 0 && !ids[id] {
			continue
		}
		cli.Banner(out, "=", "\n%v\n", nameForID(id))
		if sel.summary {
			hf.formatTotals(h, out)
		}
		if sel.rankings() {
			hf.formatHeaps(h, out, func(v string) string { return v }, n)
		}
		if sel.hardlinks {
			fmt.Fprintf(out, "Links:     %v (counted once)\n", cli.FmtCount(h.TotalHardlinks))
			if h.Hardlinks == nil {
				fmt.Fprint(out, hardlinksUnavailable)
			} else {
				cli.Banner(out, "-", "\nTop %v prefixes by number of hardlinks\n", n)
				hf.formatHeap(h.Hardlinks, out, cli.FmtCount, func(v string) string { return v }, n)
			}
		}
	}
}

// Snapshot computes stats for the entire filesystem f from its database
// and saves them as a new snapshot in its snapshot_dir, returning its
// path. It implements analyze.SnapshotFunc.
func (st *Commands) Snapshot(ctx context.Context, f iduconfig.Filesystem) (string, error) {
	dir := f.Config().SnapshotDir
	if dir == "" {
		return "", fmt.Errorf("snapshot_dir is not set for %v", iduconfig.Label(f))
	}
	return snapshot.Computer{}.Take(ctx, f, dir, st.env.OpenOptions()...)
}
