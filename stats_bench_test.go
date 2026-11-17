// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file"
)

// createPrefixTreeDB creates a database containing a two level tree of
// fanout*fanout directories, each with nfiles files.
func createPrefixTreeDB(ctx context.Context, b testing.TB, fanout, nfiles int) iduconfig.Root {
	b.Helper()
	cfg := iduconfig.Root{Root: "/root", Settings: iduconfig.Settings{Database: filepath.Join(b.TempDir(), "db"), Separator: "/"}}
	// Create the database via idudb so that it contains the summary
	// metadata and then write the prefixes and their summaries directly.
	idb, err := openWritableDB(ctx, &cfg.Settings)
	if err != nil {
		b.Fatal(err)
	}
	if err := idb.Close(ctx); err != nil {
		b.Fatal(err)
	}
	sdb, err := openRawDB(ctx, cfg.Database)
	if err != nil {
		b.Fatal(err)
	}
	var buf bytes.Buffer
	now := time.Now()
	ino := uint64(1)
	newPrefix := func(path string, files int) prefixinfo.T {
		ino++
		pi := prefixinfo.New(path, file.NewInfo(filepath.Base(path), 4096, 0o755|1<<31, now,
			file.XAttr{UID: 1, GID: 2, Device: 1, FileID: ino, Blocks: 8}))
		entries := make(file.InfoList, files)
		for i := range entries {
			ino++
			entries[i] = file.NewInfo(fmt.Sprintf("f%04d", i), 1024, 0o644, now,
				file.XAttr{UID: 1, GID: 2, Device: 1, FileID: ino, Blocks: 2, Hardlinks: 1})
		}
		pi.SetInfoList(entries)
		return pi
	}
	set := func(path string, files int) {
		pi := newPrefix(path, files)
		buf.Reset()
		if err := pi.AppendBinary(&buf); err != nil {
			b.Fatal(err)
		}
		if err := sdb.Set(ctx, path, buf.Bytes(), true); err != nil {
			b.Fatal(err)
		}
		summary := stats.ComputeSummary(path, &pi, cfg.CalculatorOrDefault(), stats.AlwaysMatch(), cfg.RecordHardlinks())
		exactKey, _ := stats.SummaryKeys(cfg.SeparatorOrDefault(), path)
		if err := sdb.SetSummary(ctx, exactKey, summary.AppendBinary(nil), true); err != nil {
			b.Fatal(err)
		}
	}
	set("/root", nfiles)
	for i := range fanout {
		parent := fmt.Sprintf("/root/d%03d", i)
		set(parent, nfiles)
		for j := range fanout {
			set(fmt.Sprintf("%s/e%03d", parent, j), nfiles)
		}
	}
	if err := sdb.Close(ctx); err != nil {
		b.Fatal(err)
	}
	return cfg
}

func BenchmarkComputeFromPrefixes(b *testing.B) {
	ctx := context.Background()
	for _, nfiles := range []int{10, 100} {
		cfg := createPrefixTreeDB(ctx, b, 100, nfiles)
		db, err := openReadOnlyDB(ctx, cfg.Database)
		if err != nil {
			b.Fatal(err)
		}
		name := fmt.Sprintf("dirs=%d/files=%d", 100*100+100+1, nfiles)
		b.Run("impl=per-get/"+name, func(b *testing.B) {
			for b.Loop() {
				if _, err := computeFromPrefixesPerGet(ctx, db, &cfg.Settings, hardlinkMatcher(ctx, b), cfg.Root, 10); err != nil {
					b.Fatal(err)
				}
			}
		})
		for _, readers := range []int{1, 2, 4, 8, runtime.GOMAXPROCS(0)} {
			st := &statsCmds{readers: readers}
			b.Run(fmt.Sprintf("impl=readers-%02d/%s", readers, name), func(b *testing.B) {
				for b.Loop() {
					if _, err := st.computeFromPrefixes(ctx, db, &cfg.Settings, hardlinkMatcher(ctx, b), cfg.Root, 10, false); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
		_ = db.Close(ctx)
	}
}

func BenchmarkComputeFromSummaries(b *testing.B) {
	ctx := context.Background()
	for _, nfiles := range []int{10, 100} {
		cfg := createPrefixTreeDB(ctx, b, 100, nfiles)
		db, err := openReadOnlyDB(ctx, cfg.Database)
		if err != nil {
			b.Fatal(err)
		}
		st := &statsCmds{}
		b.Run(fmt.Sprintf("dirs=%d/files=%d", 100*100+100+1, nfiles), func(b *testing.B) {
			for b.Loop() {
				if _, err := st.computeFromSummaries(ctx, db, &cfg.Settings, cfg.Root, 10, false); err != nil {
					b.Fatal(err)
				}
			}
		})
		_ = db.Close(ctx)
	}
}

// computeFromPrefixesPerGet is the previous, serial, implementation of
// computeFromPrefixes that uses a database transaction per prefix and is
// retained for comparison.
func computeFromPrefixesPerGet(ctx context.Context, db idudb.Reader, cfg *iduconfig.Settings, match stats.Matcher, prefix string, topN int) (*reports.AllStats, error) {
	calc, sep := cfg.CalculatorOrDefault(), cfg.SeparatorOrDefault()
	sdb := reports.NewAllStats(prefix, topN)
	agg := reports.NewSubtreeAggregator(prefix, topN)
	exactKey, childPrefix := stats.SummaryKeys(sep, prefix)
	var buf bytes.Buffer
	var getErr error
	err := db.ScanSummaries(ctx, exactKey, childPrefix, func(ctx context.Context, sk string, _ []byte) bool {
		k := stats.DecodeSummaryKey(sep, sk)
		buf.Reset()
		if getErr = db.Get(ctx, k, &buf); getErr != nil {
			return false
		}
		var pi prefixinfo.T
		if err := pi.UnmarshalBinary(buf.Bytes()); err != nil {
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
	agg.Finish()
	sdb.Subtree = agg.Subtree
	sdb.Distributed = agg.Distributed
	sdb.Finalize()
	return sdb, err
}
