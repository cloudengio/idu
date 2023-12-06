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
	"strings"
	"time"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/file/diskusage"
)

type statsFileFormat struct {
	Prefix     string
	Date       time.Time
	Expression string
	Stats      *reports.AllStats
}

func loadStats(filename string) (statsFileFormat, error) {
	buf, err := os.ReadFile(filename)
	if err != nil {
		return statsFileFormat{}, err
	}
	var stats statsFileFormat
	if err := gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&stats); err != nil {
		return statsFileFormat{}, err
	}
	return stats, nil
}

func saveStats(filename, softlink string, stats statsFileFormat) error {
	if len(filename) == 0 {
		filename = stats.Date.Format(time.DateTime)
	}
	filename = strings.TrimSuffix(filename, ".idustats") + ".idustats"
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(stats); err != nil {
		return err
	}
	if err := os.WriteFile(filename, buf.Bytes(), 0660); err != nil {
		return err
	}
	if len(softlink) == 0 {
		return nil
	}
	os.Remove(softlink)
	return os.Symlink(filename, softlink)
}

type statsCmds struct {
}

type StatsFlags struct {
	DisplayN int  `subcmd:"display,10,number of top entries to display"`
	Progress bool `subcmd:"progress,false,show progress"`
}

type computeFlags struct {
	ComputeN         int    `subcmd:"n,2000,number of top entries to compute"`
	Progress         bool   `subcmd:"progress,false,show progress"`
	SaveAs           string `subcmd:"save-as,,'save files to the specified filename, otherwise the current date and time will be used as the filename'"`
	CreateLatestLink string `subcmd:"create-link,latest.idustats,create a soft-link to the stats file created"`
}

type viewFlags struct {
	StatsFlags
	User  string `subcmd:"user,,display stats for the specified user"`
	Group string `subcmd:"group,,display stats for the specified group"`
}

type userFlags struct {
	StatsFlags
}

type groupFlags struct {
	StatsFlags
}

type listStatsFlags struct {
	internal.TimeRangeFlags
}

type eraseFlags struct {
	Force bool `subcmd:"force,false,force deletion of all stats"`
}

func (st *statsCmds) compute(ctx context.Context, values interface{}, args []string) error {
	cf := values.(*computeFlags)

	expr, err := createExpr(args[1:])

	ctx, cfg, rdb, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], true)
	if err != nil {
		return err
	}

	if args[0] != cfg.Prefix {
		fmt.Printf("warning: computing and storing stats for %v and not for %v\n", cfg.Prefix, args[0])
	}

	sdb, err := st.computeStats(ctx, rdb, cfg.Prefix, cfg.Calculator(), cf.ComputeN, cf.Progress)
	if err != nil {
		rdb.Close(ctx)
		return err
	}
	rdb.Close(ctx)

	// Save stats.
	stats := statsFileFormat{
		Prefix:     args[0],
		Date:       time.Now(),
		Expression: expr.String(),
		Stats:      sdb,
	}
	return saveStats(cf.SaveAs, cf.CreateLatestLink, stats)
}

func (st *statsCmds) computeStats(ctx context.Context, db database.DB, prefix string, calc diskusage.Calculator, topN int, progress bool) (*reports.AllStats, error) {

	hasStorabeBytes := calc.String() != "identity"
	sdb := reports.NewAllStats(prefix, hasStorabeBytes, topN)
	n := 0
	err := db.Stream(ctx, prefix, func(_ context.Context, k string, v []byte) {
		var pi prefixinfo.T
		if err := pi.UnmarshalBinary(v); err != nil {
			fmt.Fprintf(os.Stderr, "failed to unmarshal value for %v: %v\n", k, err)
			return
		}
		if err := sdb.Update(k, pi, calc); err != nil {
			fmt.Fprintf(os.Stderr, "failed to compute stats for %v: %v\n", k, err)
			return
		}
		if progress && (n != 0 && n%1000 == 0) {
			fmt.Printf("processed % 10v entries\n", fmtCount(int64(n)))
		}
		n++
	})
	fmt.Printf("processed % 10v entries\n", fmtCount(int64(n)))
	sdb.Finalize()
	return sdb, err
}

func (st *statsCmds) view(ctx context.Context, values interface{}, args []string) error {
	af := values.(*viewFlags)
	if len(af.User) != 0 && len(af.Group) != 0 {
		return fmt.Errorf("only one of --user or --group may be specified")
	}

	stats, err := loadStats(args[0])
	if err != nil {
		return err
	}

	sdb := stats.Stats
	when := stats.Date

	if len(af.User) != 0 {
		return st.userOrGroup(ctx, af, stats, af.User, globalUserManager.uidForName)
	}

	if len(af.Group) != 0 {
		return st.userOrGroup(ctx, af, stats, af.Group, globalUserManager.gidForName)
	}

	heapFormatter[string]{}.formatTotals(sdb.Prefix, os.Stdout)

	banner(os.Stdout, "=", "Usage by top %v Prefixes as of: %v\n", af.DisplayN, when)
	heapFormatter[string]{}.formatHeaps(sdb.Prefix, os.Stdout, func(v string) string { return v }, af.DisplayN)

	banner(os.Stdout, "=", "\nUsage by top %v users as of: %v\n", af.DisplayN, when)
	heapFormatter[uint32]{}.formatHeaps(sdb.ByUser, os.Stdout, globalUserManager.nameForUID, af.DisplayN)

	banner(os.Stdout, "=", "\nUsage by top %v groups as of: %v\n", af.DisplayN, when)
	heapFormatter[uint32]{}.formatHeaps(sdb.ByGroup, os.Stdout, globalUserManager.nameForGID, af.DisplayN)
	return nil
}

func idmap(ids []string, mapper func(string) (uint32, error)) (map[uint32]bool, error) {
	idm := make(map[uint32]bool)
	for _, a := range ids {
		id, err := mapper(a)
		if err != nil {
			return nil, fmt.Errorf("unrecoginised id: %v", a)
		}
		idm[id] = true

	}
	return idm, nil
}

func (st *statsCmds) userOrGroup(ctx context.Context, af *viewFlags, stats statsFileFormat, name string, mapper func(string) (uint32, error)) error {
	sdb := stats.Stats
	when := stats.Date

	id, err := mapper(name)
	if err != nil {
		return err
	}

	banner(os.Stdout, "=", "Usage by %v as of: %v\n", name, when)
	st.formatPerIDStats(sdb.PerUser, os.Stdout, globalUserManager.nameForUID, map[uint32]bool{id: true}, af.DisplayN)
	return nil
}

type heapFormatter[T comparable] struct{}

func (hf heapFormatter[T]) formatHeap(heap *heap.MinMax[int64, T], out io.Writer, kf func(size int64) string, vf func(T) string, n int) {
	i := 0
	for heap.Len() > 0 {
		k, v := heap.PopMax()
		fmt.Printf("%v: %v\n", kf(k), vf(v))
		i++
		if i >= n {
			break
		}
	}
}

func (hf heapFormatter[T]) formatHeaps(h *reports.Heaps[T], out io.Writer, valueFormatter func(T) string, n int) {
	banner(out, "-", "Bytes used\n")
	hf.formatHeap(h.Bytes, out, fmtSize, valueFormatter, n)
	if h.StorageBytes != nil {
		banner(out, "-", "\nBytes used on underlying filesystem\n")
		hf.formatHeap(h.StorageBytes, out, fmtSize, valueFormatter, n)
	}
	banner(out, "-", "\nNumber of Files\n")
	hf.formatHeap(h.Files, out, fmtCount, valueFormatter, n)
	banner(out, "-", "\nNumber of Prefixes/Directories\n")
	hf.formatHeap(h.Prefixes, out, fmtCount, valueFormatter, n)
}

func (hf heapFormatter[T]) formatTotals(h *reports.Heaps[T], out io.Writer) {
	banner(out, "-", "Totals\n")
	fmt.Fprintf(out, "Bytes:    %v\n", fmtSize(h.TotalBytes))
	if h.StorageBytes != nil {
		fmt.Fprintf(out, "Storage:  %v\n", fmtSize(h.TotalStorageBytes))
	}
	fmt.Fprintf(out, "Files:    %v\n", fmtCount(h.TotalFiles))
	fmt.Fprintf(out, "Prefixes: %v\n\n", fmtCount(h.TotalPrefixes))
}

func (st *statsCmds) formatPerIDStats(s reports.PerIDStats, out io.Writer, nameForID func(uint32) string, ids map[uint32]bool, n int) {
	for id, h := range s.ByPrefix {
		if len(ids) != 0 && !ids[id] {
			continue
		}
		banner(out, "=", "\n%v\n", nameForID(id))
		heapFormatter[string]{}.formatHeaps(h, out, func(v string) string { return v }, n)
	}
}
