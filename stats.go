// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/database/boltdb"
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
	ctx, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], boltdb.ReadOnly())
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	from, to, err := lf.TimeRangeFlags.FromTo()
	if err != nil {
		return err
	}
	return db.VisitStats(ctx, from, to,
		func(_ context.Context, when time.Time, detail []byte) bool {
			fmt.Printf("%v: size: %v\n", when, fmtSize(int64(len(detail))))
			return true
		})
}

func (st *statsCmds) erase(ctx context.Context, values interface{}, args []string) error {
	ef := values.(*eraseFlags)
	if !ef.Force {
		return fmt.Errorf("use --force to erase all stats")
	}
	ctx, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0])
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	return db.Clear(ctx, false, false, true)
}

func (st *statsCmds) compute(ctx context.Context, values interface{}, args []string) error {
	cf := values.(*computeFlags)

	ctx, cfg, rdb, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, args[0], boltdb.ReadOnly())
	if err != nil {
		return err
	}

	if args[0] != cfg.Prefix {
		fmt.Printf("warning: computing and storing stats for %v and not for %v\n", cfg.Prefix, args[0])
	}

	_, when, _, err := rdb.LastLog(ctx)
	if err != nil {
		rdb.Close(ctx)
		return err
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
	ctx, _, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, cfg.Prefix)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	return db.SaveStats(ctx, when, buf.Bytes())
}

func (st *statsCmds) computeStats(ctx context.Context, db database.DB, prefix string, calc diskusage.Calculator, topN int) (*stats, error) {

	hasStorabeBytes := calc.String() != "identity"
	sdb := newStats(prefix, hasStorabeBytes, topN)

	err := db.Scan(ctx, prefix, func(_ context.Context, k string, v []byte) bool {
		if !strings.HasPrefix(k, prefix) {
			return false
		}
		var pi internal.PrefixInfo
		if err := pi.UnmarshalBinary(v); err != nil {
			fmt.Fprintf(os.Stderr, "failed to unmarshal value for %v: %v\n", k, err)
			return false
		}
		totals, us, gs, err := pi.ComputeStats(calc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to compute stats for %v: %v\n", k, err)
			return false
		}
		sdb.prefixStats(k,
			pi.Size()+totals.Bytes,
			calc.Calculate(pi.Size())+totals.StorageBytes,
			totals.Files,
			totals.Prefixes)
		sdb.userStats(k, us)
		sdb.groupStats(k, gs)
		return true
	})

	sdb.finalize()
	return sdb, err
}

func (st *statsCmds) getOrComputeStats(ctx context.Context, prefix string, n int) (time.Time, *stats, error) {
	ctx, cfg, db, err := internal.OpenPrefixAndDatabase(ctx, globalConfig, prefix, boltdb.ReadOnly())
	if err != nil {
		return time.Time{}, nil, err
	}
	defer db.Close(ctx)
	if cfg.Prefix == prefix {
		var sdb stats
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
	sdb.Prefix.formatTotals(os.Stdout)

	banner(os.Stdout, "=", "Usage by top %v Prefixes as of: %v \n", af.DisplayN, when)
	sdb.Prefix.formatHeaps(os.Stdout, func(v string) string { return v }, af.DisplayN)

	banner(os.Stdout, "=", "\nUsage by top %v users as of: %v\n", af.DisplayN, when)
	sdb.ByUser.formatHeaps(os.Stdout, globalUserManager.nameForUID, af.DisplayN)

	banner(os.Stdout, "=", "\nUsage by top %v groups as of: %v\n", af.DisplayN, when)
	sdb.ByGroup.formatHeaps(os.Stdout, globalUserManager.nameForGID, af.DisplayN)
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
	sdb.PerUser.format(os.Stdout, globalUserManager.nameForUID, ids, uf.DisplayN)
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
	sdb.PerGroup.format(os.Stdout, globalUserManager.nameForGID, ids, gf.DisplayN)
	return nil
}

type heaps[T comparable] struct {
	MaxN                          int
	Prefix                        string
	TotalBytes, TotalStorageBytes int64
	TotalFiles, TotalPrefixes     int64
	Bytes                         *heap.MinMax[int64, T]
	StorageBytes                  *heap.MinMax[int64, T]
	Files                         *heap.MinMax[int64, T]
	Prefixes                      *heap.MinMax[int64, T]
}

type idStats struct {
	Prefix          string
	MaxN            int
	HasStorageBytes bool
	nameForID       func(uint32) string
	ByPrefix        map[uint32]*heaps[string]
}

type stats struct {
	MaxN     int
	Prefix   *heaps[string]
	PerUser  idStats
	PerGroup idStats
	ByUser   *heaps[uint32]
	ByGroup  *heaps[uint32]

	userTotals  map[uint32]internal.Stats
	groupTotals map[uint32]internal.Stats
}

func newHeaps[T comparable](prefix string, storageBytes bool, n int) *heaps[T] {
	h := &heaps[T]{
		MaxN:     n,
		Prefix:   prefix,
		Bytes:    heap.NewMinMax[int64, T](),
		Files:    heap.NewMinMax[int64, T](),
		Prefixes: heap.NewMinMax[int64, T](),
	}
	if storageBytes {
		h.StorageBytes = heap.NewMinMax[int64, T]()
	}
	return h
}

func (h *heaps[T]) push(item T, bytes, storageBytes, files, prefixes int64) {
	h.Bytes.PushMaxN(bytes, item, h.MaxN)
	if h.StorageBytes != nil {
		h.StorageBytes.PushMaxN(storageBytes, item, h.MaxN)
	}
	h.Files.PushMaxN(files, item, h.MaxN)
	h.Prefixes.PushMaxN(prefixes, item, h.MaxN)
	h.TotalBytes += bytes
	h.TotalStorageBytes += storageBytes
	h.TotalFiles += files
	h.TotalPrefixes += prefixes
}

func (h *heaps[T]) popAll(heap *heap.MinMax[int64, T], n int) (keys []int64, vals []T) {
	i := 0
	for heap.Len() > 0 {
		if i++; n > 0 && i >= n {
			break
		}
		k, v := heap.PopMax()
		keys = append(keys, k)
		vals = append(vals, v)

	}
	return
}

func banner(out io.Writer, ul string, format string, args ...any) {
	buf := strings.Builder{}
	o := fmt.Sprintf(format, args...)
	buf.WriteString(o)
	buf.WriteString(strings.Repeat(ul, len(o)))
	buf.WriteRune('\n')
	out.Write([]byte(buf.String()))
}

func (h *heaps[T]) formatHeap(heap *heap.MinMax[int64, T], out io.Writer, kf func(size int64) string, vf func(T) string, n int) {
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
func (h *heaps[T]) formatHeaps(out io.Writer, valueFormatter func(T) string, n int) {
	banner(out, "-", "Bytes used\n")
	h.formatHeap(h.Bytes, out, fmtSize, valueFormatter, n)
	if h.StorageBytes != nil {
		banner(out, "-", "\nBytes used on underlying filesystem\n")
		h.formatHeap(h.StorageBytes, out, fmtSize, valueFormatter, n)
	}
	banner(out, "-", "\nNumber of Files\n")
	h.formatHeap(h.Files, out, fmtCount, valueFormatter, n)
	banner(out, "-", "\nNumer of Prefixes/Directories\n")
	h.formatHeap(h.Prefixes, out, fmtCount, valueFormatter, n)
}

func (h *heaps[T]) formatTotals(out io.Writer) {
	banner(out, "-", "Totals\n")
	fmt.Fprintf(out, "Bytes:    %v\n", fmtSize(h.TotalBytes))
	if h.StorageBytes != nil {
		fmt.Fprintf(out, "Storage:  %v\n", fmtSize(h.TotalStorageBytes))
	}
	fmt.Fprintf(out, "Files:    %v\n", fmtCount(h.TotalFiles))
	fmt.Fprintf(out, "Prefixes: %v\n\n", fmtCount(h.TotalPrefixes))
}

func appendString(buf []byte, s string) []byte {
	buf = binary.AppendVarint(buf, int64(len(s)))
	return append(buf, s...)
}

func decodeString(data []byte) (int, string) {
	l, n := binary.Varint(data)
	return n + int(l), string(data[n : n+int(l)])
}

func newIdStats(prefix string, storageBytes bool, n int) idStats {
	return idStats{
		Prefix:          prefix,
		HasStorageBytes: storageBytes,
		MaxN:            n,
		ByPrefix:        make(map[uint32]*heaps[string]),
	}
}

func (s *idStats) push(id uint32, prefix string, size, storageBytes, files, children int64) {
	if _, ok := s.ByPrefix[id]; !ok {
		s.ByPrefix[id] = newHeaps[string](s.Prefix, s.HasStorageBytes, s.MaxN)
	}
	s.ByPrefix[id].push(prefix, size, storageBytes, files, children)
}

func (s *idStats) format(out io.Writer, nameForID func(uint32) string, ids map[uint32]bool, n int) {
	for id, h := range s.ByPrefix {
		if len(ids) != 0 && !ids[id] {
			continue
		}
		banner(out, "=", "\n%v\n", nameForID(id))
		h.formatHeaps(out, func(v string) string { return v }, n)
	}
}

func newStats(prefix string, withStorageBytes bool, n int) *stats {
	return &stats{
		MaxN:        n,
		Prefix:      newHeaps[string](prefix, withStorageBytes, n),
		PerUser:     newIdStats(prefix, withStorageBytes, n),
		PerGroup:    newIdStats(prefix, withStorageBytes, n),
		ByUser:      newHeaps[uint32](prefix, withStorageBytes, n),
		ByGroup:     newHeaps[uint32](prefix, withStorageBytes, n),
		userTotals:  map[uint32]internal.Stats{},
		groupTotals: map[uint32]internal.Stats{},
	}
}

func (s *stats) prefixStats(prefix string, size, storageBytes, files, children int64) {
	s.Prefix.push(prefix, size, storageBytes, files, children)
}

func addToMap(stats map[uint32]internal.Stats, uid uint32, size, storageBytes, files, children int64) {
	s := stats[uid]
	s.Bytes += size
	s.StorageBytes += storageBytes
	s.Files += files
	s.Prefixes += children
	stats[uid] = s
}

func (s *stats) userStats(prefix string, us internal.StatsList) {
	for _, u := range us {
		s.PerUser.push(u.ID, prefix, u.Bytes, u.StorageBytes, u.Files, u.Prefixes)
		addToMap(s.userTotals, u.ID, u.Bytes, u.StorageBytes, u.Files, u.Prefixes)
	}
}

func (s *stats) groupStats(prefix string, ug internal.StatsList) {
	for _, g := range ug {
		s.PerGroup.push(g.ID, prefix, g.Bytes, g.StorageBytes, g.Files, g.Prefixes)
		addToMap(s.groupTotals, g.ID, g.Bytes, g.StorageBytes, g.Files, g.Prefixes)
	}
}

func (s *stats) finalize() {
	for id, stats := range s.userTotals {
		s.ByUser.push(id, stats.Bytes, stats.StorageBytes, stats.Files, stats.Prefixes)
	}
	for id, stats := range s.groupTotals {
		s.ByGroup.push(id, stats.Bytes, stats.StorageBytes, stats.Files, stats.Prefixes)
	}
}
