// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package reports_test

import (
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"slices"
	"sort"
	"testing"
	"time"

	"cloudeng.io/algo/container/heap"
	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file"
	"cloudeng.io/file/diskusage"
	"golang.org/x/exp/maps"
)

func newInfo(name string, size int64, mode fs.FileMode, modTime time.Time, uid, gid uint32) file.Info {
	return file.NewInfo(name, size, mode, modTime, prefixinfo.NewSysInfo(uid, gid, 0, 0))
}

func createPrefixInfo(t *testing.T, uid, gid uint32, name string, contents ...[]file.Info) prefixinfo.T {
	now := time.Now().Truncate(0)
	info := newInfo(name, 3, fs.ModeDir|0700, now.Truncate(0), uid, gid)
	pi, err := prefixinfo.New(name, info)
	if err != nil {
		t.Fatal(err)
	}

	for _, c := range contents {
		pi.AppendInfoList(c)
	}
	return pi
}

type times2 struct{}

func (times2) Calculate(n int64) int64 { return 2 * n }

func (times2) String() string {
	return "times2"
}

type testStats struct {
	prefix                                            string
	bytes, storageBytes, files, prefixes, prefixBytes int64
}

func (ts *testStats) update(bytes, storageBytes, files, prefixes, prefixBytes int64) {
	ts.bytes += bytes
	ts.storageBytes += storageBytes
	ts.files += files
	ts.prefixes += prefixes
	ts.prefixBytes += prefixBytes
}

func computeStats(t *testing.T, sdb *reports.AllStats, calc diskusage.Calculator, keys []string, match stats.Matcher, pis ...prefixinfo.T) {
	for i, pi := range pis {
		k := keys[i]
		if err := sdb.Update(k, pi, calc, match); err != nil {
			t.Fatal(err)
		}
	}
	sdb.Finalize()
}

func compareIDs[T comparable](t *testing.T, m map[uint32]*reports.Heaps[T], want ...uint32) {
	got := maps.Keys(m)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		_, _, l, _ := runtime.Caller(1)
		t.Errorf("line %v: got %v, want %v", l, got, want)
	}
}

func compareHeap[T comparable](t *testing.T, h *heap.MinMax[int64, T], n int, ws []int64, wp ...T) {
	_, _, l, _ := runtime.Caller(1)
	s, p := reports.PopN(h, n)
	if got, want := s, ws; !slices.Equal(got, want) {
		t.Errorf("line %v: got %v, want %v", l, got, want)
	}
	if got, want := p, wp; !slices.Equal(got, want) {
		t.Errorf("line %v: got %v, want %v", l, got, want)
	}
}

func compareHeapTotals[T comparable](t *testing.T, h *reports.Heaps[T], totals testStats) {
	_, _, l, _ := runtime.Caller(1)
	if h.TotalBytes != totals.bytes {
		t.Errorf("line %v: got %v, want %v", l, h.TotalBytes, totals.bytes)
	}
	if h.StorageBytes != nil && h.TotalStorageBytes != totals.storageBytes {
		t.Errorf("line %v: got %v, want %v", l, h.TotalStorageBytes, totals.storageBytes)
	}
	if h.TotalFiles != totals.files {
		t.Errorf("line %v: got %v, want %v", l, h.TotalFiles, totals.files)
	}
	if h.TotalPrefixes != totals.prefixes {
		t.Errorf("line %v: got %v, want %v", l, h.TotalPrefixes, totals.prefixes)
	}
	if h.TotalPrefixBytes != totals.prefixBytes {
		t.Errorf("line %v: got %v, want %v", l, h.TotalPrefixBytes, totals.prefixBytes)
	}
}

func comparePerIDTotals(t *testing.T, pis reports.PerIDStats, totals testStats) {
	_, _, l, _ := runtime.Caller(1)
	var tb, tsb, tf, tp, tpb int64
	for _, v := range pis.ByPrefix {
		tb += v.TotalBytes
		tsb += v.TotalStorageBytes
		tf += v.TotalFiles
		tp += v.TotalPrefixes
		tpb += v.TotalPrefixBytes
	}
	if tb != totals.bytes {
		t.Errorf("line %v: got %v, want %v", l, tb, totals.bytes)
	}
	if tsb != totals.storageBytes {
		t.Errorf("line %v: got %v, want %v", l, tsb, totals.storageBytes)
	}
	if tf != totals.files {
		t.Errorf("line %v: got %v, want %v", l, tf, totals.files)
	}
	if tp != totals.prefixes {
		t.Errorf("line %v: got %v, want %v", l, tp, totals.prefixes)
	}
	if tpb != totals.prefixBytes {
		t.Errorf("line %v: got %v, want %v", l, tpb, totals.prefixBytes)
	}

}

func nInfos(n int, mode fs.FileMode, uid, gid uint32) (fis []file.Info) {
	modTime := time.Now()
	for i := 0; i < n; i++ {
		fis = append(fis, newInfo(fmt.Sprintf("f%v", i), int64(i+1), mode, modTime, uid, gid))

	}
	return
}

func nInfoF(n int, uid, gid uint32) (fis []file.Info) {
	return nInfos(n, 0700, uid, gid)
}

func nInfoD(n int, uid, gid uint32) (fis []file.Info) {
	return nInfos(n, 0700|os.ModeDir, uid, gid)
}

func fib(n int) int64 {
	r := 0
	for i := 0; i < n; i++ {
		r += i + 1
	}
	return int64(r)
}

func TestReportStatsSingleID(t *testing.T) {
	calc := times2{}
	var uid, gid uint32 = 100, 2

	npi := createPrefixInfo

	// Number of files and prefixes for each of the prefixinfo.Ts to be
	// be created.
	nf, nd := []int{2, 6, 9, 7}, []int{2, 7, 5, 3}

	// Create the prefixes and compute the totals.
	pikeys := []string{}
	totals := testStats{}
	for i := 0; i < len(nf); i++ {
		pikeys = append(pikeys, fmt.Sprintf("p%v", i))
		totals.update(fib(nf[i]), fib(nf[i])*2, int64(nf[i]), int64(nd[i]), fib(nd[i]))
	}

	for _, tc := range []struct {
		uid, gid uint32
	}{
		{uid, gid},
		{uid + 1, gid + 1},
	} {
		pis := []prefixinfo.T{}
		for i := 0; i < len(nf); i++ {
			pis = append(pis, npi(t, uid, gid, pikeys[i], nInfoF(nf[i], tc.uid, tc.gid), nInfoD(nd[i], tc.uid, tc.gid)))
		}

		sdb := reports.NewAllStats("test", true, 5)
		computeStats(t, sdb, calc, pikeys, boolexpr.AlwaysMatch{}, pis...)

		compareIDs(t, sdb.PerUser.ByPrefix, tc.uid)
		compareIDs(t, sdb.PerGroup.ByPrefix, tc.gid)

		for _, h := range []*reports.Heaps[string]{
			sdb.Prefix,
			sdb.PerUser.ByPrefix[tc.uid],
			sdb.PerGroup.ByPrefix[tc.gid],
		} {
			compareHeapTotals(t, h, totals)
			compareHeap(t, h.Bytes, 3, []int64{fib(9), fib(7), fib(6)}, "p2", "p3", "p1")
			compareHeap(t, h.StorageBytes, 3, []int64{fib(9) * 2, fib(7) * 2, fib(6) * 2}, "p2", "p3", "p1")
			compareHeap(t, h.Files, 3, []int64{9, 7, 6}, "p2", "p3", "p1")
			compareHeap(t, h.Prefixes, 10, []int64{7, 5, 3, 2}, "p1", "p2", "p3", "p0")
			compareHeap(t, h.PrefixBytes, 10, []int64{fib(7), fib(5), fib(3), fib(2)}, "p1", "p2", "p3", "p0")
		}

		for _, tcid := range []struct {
			h  *reports.Heaps[uint32]
			id uint32
		}{
			{sdb.ByUser, tc.uid},
			{sdb.ByGroup, tc.gid},
		} {
			compareHeapTotals(t, tcid.h, totals)
			compareHeap(t, tcid.h.Bytes, 10, []int64{totals.bytes}, tcid.id)
			compareHeap(t, tcid.h.StorageBytes, 10, []int64{totals.storageBytes}, tcid.id)
			compareHeap(t, tcid.h.Files, 10, []int64{totals.files}, tcid.id)
			compareHeap(t, tcid.h.Prefixes, 10, []int64{totals.prefixes}, tcid.id)
			compareHeap(t, tcid.h.PrefixBytes, 10, []int64{totals.prefixBytes}, tcid.id)
		}

		sdb = reports.NewAllStats("test", true, 5)
		matcher, err := boolexpr.CreateMatcher(boolexpr.NewParser(nil), boolexpr.WithExpression("user=33"))
		if err != nil {
			t.Fatal(err)
		}

		zeroes := testStats{}
		for i := 0; i < len(nf); i++ {
			pikeys = append(pikeys, fmt.Sprintf("p%v", i))
			totals.update(0, 0, 0, 0, 0)
		}
		computeStats(t, sdb, calc, pikeys, matcher, pis...)
		for _, h := range []*reports.Heaps[string]{
			sdb.Prefix,
			sdb.PerUser.ByPrefix[tc.uid],
			sdb.PerGroup.ByPrefix[tc.gid],
		} {
			if h != nil {
				compareHeapTotals(t, h, zeroes)
			}
		}
	}
}

func cloneIDDetails(d map[uint32][]testStats) map[uint32][]testStats {
	n := map[uint32][]testStats{}
	for k, v := range d {
		n[k] = slices.Clone(v)
	}
	return n
}

func TestReportStatsMultipleIDs(t *testing.T) {
	var uid, gid uint32 = 100, 2

	nf, nd := []int{2, 6, 9, 7, 3, 10, 5}, []int{2, 7, 5, 3, 10, 6, 4}

	// Create the prefixes
	nuid, ngid := uid, gid
	uids, gids := []uint32{uid}, []uint32{gid} // unique ids
	uidl, gidl := []uint32{}, []uint32{}       // lists of ids, used to compute stats below
	pikeys := []string{}
	pis := []prefixinfo.T{}
	for i := 0; i < len(nf); i++ {
		uidl = append(uidl, nuid)
		gidl = append(gidl, ngid)
		pikeys = append(pikeys, fmt.Sprintf("p%v", i))
		pis = append(pis, createPrefixInfo(t, uid, gid, pikeys[i],
			nInfoF(nf[i], nuid, ngid),
			nInfoD(nd[i], nuid, ngid)))
		if i%2 == 1 {
			nuid++
			ngid++
			uids = append(uids, nuid)
			gids = append(gids, ngid)
		}
	}

	// Compute the totals.
	var totals testStats
	for i := 0; i < len(nf); i++ {
		totals.update(fib(nf[i]), fib(nf[i])*2, int64(nf[i]), int64(nd[i]), fib(nd[i]))
	}

	// Compute the per id stats.
	perIDTotals := map[uint32]testStats{}
	perIDDetails := map[uint32][]testStats{}
	for i := 0; i < len(nf); i++ {
		for _, id := range []uint32{uidl[i], gidl[i]} {
			ut := perIDTotals[id]
			ut.update(fib(nf[i]), fib(nf[i])*2, int64(nf[i]), int64(nd[i]), fib(nd[i]))
			perIDTotals[id] = ut
			d := testStats{prefix: pikeys[i]}
			d.update(fib(nf[i]), fib(nf[i])*2, int64(nf[i]), int64(nd[i]), fib(nd[i]))
			perIDDetails[id] = append(perIDDetails[id], d)
		}
	}

	sizeOrdered, fileOrdered, prefixedOrdered :=
		cloneIDDetails(perIDDetails), cloneIDDetails(perIDDetails), cloneIDDetails(perIDDetails)

	for _, id := range append(uidl, gidl...) {
		sort.Slice(sizeOrdered[id],
			func(i, j int) bool { return sizeOrdered[id][i].bytes > sizeOrdered[id][j].bytes })
		sort.Slice(fileOrdered[id],
			func(i, j int) bool { return fileOrdered[id][i].files > fileOrdered[id][j].files })
		sort.Slice(prefixedOrdered[id],
			func(i, j int) bool { return prefixedOrdered[id][i].prefixes > prefixedOrdered[id][j].prefixes })
	}

	testAllIDs(t, pikeys, pis, totals, uids, gids, perIDTotals, sizeOrdered,
		fileOrdered, prefixedOrdered)

	testIDExpr(t, pikeys, pis, uids, gids, perIDTotals, sizeOrdered, fileOrdered, prefixedOrdered)
}

func testSingleID(t *testing.T, match stats.Matcher, group bool, pikeys []string, pis []prefixinfo.T, id uint32, perIDTotal testStats, sizeOrdered, fileOrdered, prefixedOrdered []testStats) {
	calc := times2{}

	sdb := reports.NewAllStats("test", true, 5)

	computeStats(t, sdb, calc, pikeys, match, pis...)

	compareHeapTotals(t, sdb.Prefix, perIDTotal)

	comparePerIDTotals(t, sdb.PerUser, perIDTotal)
	comparePerIDTotals(t, sdb.PerGroup, perIDTotal)

	so, fo, po := sizeOrdered, fileOrdered, prefixedOrdered

	var h *reports.Heaps[string]
	if group {
		compareIDs(t, sdb.PerGroup.ByPrefix, id)
		h = sdb.PerGroup.ByPrefix[id]
	} else {
		compareIDs(t, sdb.PerUser.ByPrefix, id)
		h = sdb.PerUser.ByPrefix[id]
	}

	if len(so) == 1 {
		compareHeap(t, h.Bytes, 3, []int64{so[0].bytes}, so[0].prefix)
		compareHeap(t, h.StorageBytes, 3, []int64{so[0].storageBytes}, so[0].prefix)
		compareHeap(t, h.Files, 3, []int64{fo[0].files}, fo[0].prefix)
		compareHeap(t, h.Prefixes, 10, []int64{po[0].prefixes}, po[0].prefix)
		compareHeap(t, h.PrefixBytes, 10, []int64{po[0].prefixBytes}, po[0].prefix)
	} else {
		compareHeap(t, h.Bytes, 3, []int64{so[0].bytes, so[1].bytes}, so[0].prefix, so[1].prefix)
		compareHeap(t, h.StorageBytes, 3, []int64{so[0].storageBytes, so[1].storageBytes}, so[0].prefix, so[1].prefix)
		compareHeap(t, h.Files, 3, []int64{fo[0].files, fo[1].files}, fo[0].prefix, fo[1].prefix)
		compareHeap(t, h.Prefixes, 10, []int64{po[0].prefixes, po[1].prefixes}, po[0].prefix, po[1].prefix)
		compareHeap(t, h.PrefixBytes, 10, []int64{po[0].prefixBytes, po[1].prefixBytes}, po[0].prefix, po[1].prefix)
	}
}

func testIDExpr(t *testing.T, pikeys []string, pis []prefixinfo.T, uids, gids []uint32, perIDTotals map[uint32]testStats, sizeOrdered, fileOrdered, prefixedOrdered map[uint32][]testStats) {
	parser := boolexpr.NewParser(nil)

	for _, uid := range uids {
		matcher, err := boolexpr.CreateMatcher(parser, boolexpr.WithExpression(fmt.Sprintf("user=%d", uid)))
		if err != nil {
			t.Fatal(err)
		}
		testSingleID(t, matcher, false, pikeys, pis, uid, perIDTotals[uid], sizeOrdered[uid], fileOrdered[uid], prefixedOrdered[uid])
	}

	for _, gid := range gids {
		matcher, err := boolexpr.CreateMatcher(parser, boolexpr.WithExpression(fmt.Sprintf("group=%d", gid)))
		if err != nil {
			t.Fatal(err)
		}
		testSingleID(t, matcher, true, pikeys, pis, gid, perIDTotals[gid], sizeOrdered[gid], fileOrdered[gid], prefixedOrdered[gid])
	}
}

func testAllIDs(t *testing.T, pikeys []string, pis []prefixinfo.T, totals testStats, uids, gids []uint32, perIDTotals map[uint32]testStats, sizeOrdered, fileOrdered, prefixedOrdered map[uint32][]testStats) {
	calc := times2{}

	sdb := reports.NewAllStats("test", true, 5)
	computeStats(t, sdb, calc, pikeys, boolexpr.AlwaysMatch{}, pis...)

	compareHeapTotals(t, sdb.Prefix, totals)

	compareIDs(t, sdb.PerUser.ByPrefix, uids...)
	compareIDs(t, sdb.PerGroup.ByPrefix, gids...)

	// All user/group stats should sum to the totals.
	comparePerIDTotals(t, sdb.PerUser, totals)
	comparePerIDTotals(t, sdb.PerGroup, totals)

	// Now look at per ID stats.
	for _, uid := range uids {
		h := sdb.PerUser.ByPrefix[uid]
		totals := perIDTotals[uid]

		compareHeapTotals(t, h, totals)

		so, fo, po := sizeOrdered[uid], fileOrdered[uid], prefixedOrdered[uid]

		if len(so) == 1 {
			compareHeap(t, h.Bytes, 3, []int64{so[0].bytes}, so[0].prefix)
			compareHeap(t, h.StorageBytes, 3, []int64{so[0].storageBytes}, so[0].prefix)
			compareHeap(t, h.Files, 3, []int64{fo[0].files}, fo[0].prefix)
			compareHeap(t, h.Prefixes, 10, []int64{po[0].prefixes}, po[0].prefix)
			compareHeap(t, h.PrefixBytes, 10, []int64{po[0].prefixBytes}, po[0].prefix)
		} else {
			compareHeap(t, h.Bytes, 3, []int64{so[0].bytes, so[1].bytes}, so[0].prefix, so[1].prefix)
			compareHeap(t, h.StorageBytes, 3, []int64{so[0].storageBytes, so[1].storageBytes}, so[0].prefix, so[1].prefix)
			compareHeap(t, h.Files, 3, []int64{fo[0].files, fo[1].files}, fo[0].prefix, fo[1].prefix)
			compareHeap(t, h.Prefixes, 10, []int64{po[0].prefixes, po[1].prefixes}, po[0].prefix, po[1].prefix)
			compareHeap(t, h.PrefixBytes, 10, []int64{po[0].prefixBytes, po[1].prefixBytes}, po[0].prefix, po[1].prefix)
		}
	}
}
