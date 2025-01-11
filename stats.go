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
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/cmd/idu/internal/usernames"
	"cloudeng.io/cmdutil/flags"
	"cloudeng.io/file/diskusage"
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
	StatsDir  string          `subcmd:"stats-dir,stats,'directory that stats files are written to'"`
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

	ctx, cfg, rdb, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], true)
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

	sdb, err := st.computeStats(ctx, rdb, match, args[0], cfg.Calculator(), cf.ComputeN, cf.Progress)
	if err != nil {
		rdb.Close(ctx)
		return err
	}
	rdb.Close(ctx)

	// Save stats.
	stats := statsFileFormat{
		Prefix:     args[0],
		Date:       time.Now(),
		Expression: match.String(),
		Stats:      sdb,
	}
	return saveStats(cf.StatsDir, cf.StatsFile, stats)
}

func (st *statsCmds) computeStats(ctx context.Context, db database.DB, match boolexpr.Matcher, prefix string, calc diskusage.Calculator, topN int, progress bool) (*reports.AllStats, error) {
	sdb := reports.NewAllStats(prefix, topN)
	n := 0
	err := db.Stream(ctx, prefix, func(_ context.Context, k string, v []byte) {
		if progress && (n != 0 && n%1000 == 0) {
			fmt.Printf("processed % 10v entries\n", fmtCount(int64(n)))
		}
		n++
		var pi prefixinfo.T
		if err := pi.UnmarshalBinary(v); err != nil {
			fmt.Fprintf(os.Stderr, "failed to unmarshal value for %v: %v\n", k, err)
			return
		}
		if !match.Prefix(k, &pi) {
			return
		}
		if err := sdb.Update(k, pi, calc, match); err != nil {
			fmt.Fprintf(os.Stderr, "failed to compute stats for %v: %v\n", k, err)
			return
		}
	})

	if progress {
		fmt.Printf("processed % 10v entries\n", fmtCount(int64(n)))
	}

	sdb.Finalize()
	return sdb, err
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

func (st *statsCmds) formatPerIDStats(s reports.PerIDStats, out io.Writer, nameForID func(int64) string, ids map[int64]bool, n int) {
	for id, h := range s.ByPrefix {
		if len(ids) != 0 && !ids[id] {
			continue
		}
		banner(out, "=", "\n%v\n", nameForID(id))
		heapFormatter[string]{}.formatHeaps(h, out, func(v string) string { return v }, n)
	}
}
