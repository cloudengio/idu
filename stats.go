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
	"time"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/file/diskusage"
)

type statsCmds struct {
}

type StatsFlags struct {
	DisplayN int `subcmd:"display,10,number of top entries to display"`
}

type computeFlags struct {
	ComputeN int `subcmd:"n,2000,number of top entries to compute"`
}

type aggregateFlags struct {
	StatsFlags
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

func (st *statsCmds) list(ctx context.Context, values interface{}, args []string) error {
	lf := values.(*listStatsFlags)
	ctx, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], true)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	from, to, set, err := lf.TimeRangeFlags.FromTo()
	if err != nil {
		return err
	}
	if set {
		return db.VisitStats(ctx, from, to,
			func(_ context.Context, when time.Time, detail []byte) bool {
				fmt.Printf("%v: size: %v\n", when, fmtSize(int64(len(detail))))
				return true
			})
	}

	when, detail, err := db.LastStats(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("%v: size: %v\n", when, fmtSize(int64(len(detail))))
	return nil
}

func (st *statsCmds) erase(ctx context.Context, values interface{}, args []string) error {
	ef := values.(*eraseFlags)
	if !ef.Force {
		return fmt.Errorf("use --force to erase all stats")
	}
	ctx, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], false)
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	return db.Clear(ctx, false, false, true)
}

func (st *statsCmds) compute(ctx context.Context, values interface{}, args []string) error {
	cf := values.(*computeFlags)

	ctx, cfg, rdb, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], true)
	if err != nil {
		return err
	}

	if args[0] != cfg.Prefix {
		fmt.Printf("warning: computing and storing stats for %v and not for %v\n", cfg.Prefix, args[0])
	}

	_, when, _, err := rdb.LastLog(ctx)
	if err != nil {
		rdb.Close(ctx)
		return fmt.Errorf("error readling latest log entry: %v", err)
	}

	sdb, err := st.computeStats(ctx, rdb, cfg.Prefix, cfg.Calculator(), cf.ComputeN)
	if err != nil {
		rdb.Close(ctx)
		return err
	}
	rdb.Close(ctx)

	// Save stats.
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(sdb); err != nil {
		return err
	}
	ctx, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, cfg.Prefix, false)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	return db.SaveStats(ctx, when, buf.Bytes())
}

func (st *statsCmds) computeStats(ctx context.Context, db database.DB, prefix string, calc diskusage.Calculator, topN int) (*reports.AllStats, error) {

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
		if n != 0 && n%1000 == 0 {
			fmt.Printf("processed % 10v entries\n", fmtCount(int64(n)))
		}
		n++
		return
	})
	fmt.Printf("processed % 10v entries\n", fmtCount(int64(n)))
	sdb.Finalize()
	return sdb, err
}

func (st *statsCmds) getOrComputeStats(ctx context.Context, prefix string, n int) (time.Time, *reports.AllStats, error) {
	ctx, cfg, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, prefix, true)
	if err != nil {
		return time.Time{}, nil, err
	}
	defer db.Close(ctx)
	if cfg.Prefix == prefix {
		var sdb reports.AllStats
		var buf []byte
		when, buf, err := db.LastStats(ctx)
		if err != nil {
			return time.Time{}, nil, err
		}
		if err := gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&sdb); err != nil {
			return time.Time{}, nil, err
		}
		return when, &sdb, nil
	}

	// Compute stats.
	sdb, err := st.computeStats(ctx, db, prefix, cfg.Calculator(), n)
	return time.Now(), sdb, err
}

func (st *statsCmds) aggregate(ctx context.Context, values interface{}, args []string) error {
	af := values.(*aggregateFlags)
	when, sdb, err := st.getOrComputeStats(ctx, args[0], af.DisplayN)
	if err != nil {
		return err
	}
	heapFormatter[string]{}.formatTotals(sdb.Prefix, os.Stdout)

	banner(os.Stdout, "=", "Usage by top %v Prefixes as of: %v \n", af.DisplayN, when)
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

func (st *statsCmds) user(ctx context.Context, values interface{}, args []string) error {
	uf := values.(*userFlags)
	when, sdb, err := st.getOrComputeStats(ctx, args[0], uf.DisplayN)
	if err != nil {
		return err
	}
	ids, err := idmap(args[1:], globalUserManager.uidForName)
	if err != nil {
		return err
	}
	banner(os.Stdout, "=", "Usage by users (top %v items per user) as of: %v\n", uf.DisplayN, when)
	st.formatPerIDStats(sdb.PerUser, os.Stdout, globalUserManager.nameForUID, ids, uf.DisplayN)
	return nil
}

func (st *statsCmds) group(ctx context.Context, values interface{}, args []string) error {
	gf := values.(*groupFlags)
	when, sdb, err := st.getOrComputeStats(ctx, args[0], gf.DisplayN)
	if err != nil {
		return err
	}
	ids, err := idmap(args[1:], globalUserManager.gidForName)
	if err != nil {
		return err
	}
	banner(os.Stdout, "=", "Usage by groups (top %v items per group) as of: %v\n", gf.DisplayN, when)
	st.formatPerIDStats(sdb.PerGroup, os.Stdout, globalUserManager.nameForGID, ids, gf.DisplayN)
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
	banner(out, "-", "\nNumer of Prefixes/Directories\n")
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
