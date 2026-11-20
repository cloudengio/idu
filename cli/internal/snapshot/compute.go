// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmd/idu/summary"
)

// Computer computes stats from a database.
type Computer struct {
	// Readers is the number of goroutines used to read prefix metadata
	// concurrently, defaultPrefixReaders if zero.
	Readers int
	// Progress requests that progress be displayed.
	Progress bool
	// Hotspots and Hardlinks require that the stats returned by Current
	// include the distributed subtree hotspots and the rankings by number
	// of hardlinks respectively. Neither is included in the stats
	// constructed from the rankings maintained by analyze and hence either
	// requires a full pass over the database.
	Hotspots, Hardlinks bool
}

// needsFullPass returns a description of why a full pass over the
// database is required, if it is.
func (c Computer) needsFullPass() (string, bool) {
	switch {
	case c.Hotspots && c.Hardlinks:
		return "the distributed hotspots and hardlink rankings require a full pass", true
	case c.Hotspots:
		return "the distributed hotspots require a full pass", true
	case c.Hardlinks:
		return "the hardlink rankings require a full pass", true
	}
	return "", false
}

// Satisfies returns true if sf includes everything required by c.
func (c Computer) Satisfies(sf File) bool {
	if sf.Stats == nil {
		return false
	}
	if c.Hotspots && sf.Stats.Distributed == nil {
		return false
	}
	if c.Hardlinks && (sf.Stats.Prefix == nil || sf.Stats.Prefix.Hardlinks == nil) {
		return false
	}
	return true
}

// defaultPrefixReaders is the default number of goroutines used to read
// prefix metadata concurrently. Stats computation is generally limited by
// the single goroutine that must process the metadata in order, so there is
// little benefit to more readers unless the database is not cached in memory.
const defaultPrefixReaders = 4

// Compute computes stats for prefix and returns them together with
// their source and, when FromStoredSource is used, the stored per-ID
// ranking limit. If useStored is true, and prefix is the root of the
// filesystem, they are constructed from the rankings and recursive totals
// maintained by analyze when those are available and up to date. Otherwise
// they are computed from the precomputed summaries unless they cannot be
// used, in which case all of the file metadata is rescanned: summaries are
// computed without any filter expression and only record what's needed to
// deduplicate hardlinks if configured to do so.
func (c Computer) Compute(ctx context.Context, db idudb.Reader, cfg *iduconfig.Settings, match stats.Matcher, prefix string, useStored bool, topN int) (*report.AllStats, string, int, error) {
	if match.HasExpression() || !cfg.CountHardlinkAsFiles && !cfg.RecordHardlinks() {
		sdb, err := c.FromPrefixes(ctx, db, cfg, match, prefix, topN)
		return sdb, FromSummariesSource, 0, err
	}
	if useStored {
		sdb, perIDN, err := c.FromStored(ctx, db, cfg, prefix, topN)
		if err == nil {
			return sdb, FromStoredSource, perIDN, nil
		}
		if !errors.Is(err, ErrStoredUnusable) {
			return nil, "", 0, err
		}
		fmt.Fprintf(os.Stderr, "computing stats from the summaries: %v\n", err)
	}
	sdb, err := c.FromSummaries(ctx, db, cfg, prefix, topN)
	return sdb, FromSummariesSource, 0, err
}

// IsFilesystemRoot returns true if target is the root of the filesystem,
// which is the only prefix for which the stored rankings and recursive
// totals can be used, since they deduplicate hardlinks across the entire
// filesystem.
func IsFilesystemRoot(cfg *iduconfig.Settings, root, target string) bool {
	sep := cfg.SeparatorOrDefault()
	rk, _ := stats.SummaryKeys(sep, root)
	tk, _ := stats.SummaryKeys(sep, target)
	return rk == tk
}

// FromSummaries computes stats for prefix from the precomputed summaries.
func (c Computer) FromSummaries(ctx context.Context, db idudb.Reader, cfg *iduconfig.Settings, prefix string, topN int) (*report.AllStats, error) {
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
		if c.Progress && (n != 0 && n%1000 == 0) {
			fmt.Printf("processed % 10v summary entries\n", cli.FmtCount(int64(n)))
		}
		n++
		ps := e.Summary
		sdb.RecordDirect(e.Path, ps.Direct, ps.PerUser, ps.PerGroup)
		agg.Step(e.Key, e.Path, ps.Direct)
	}
	if c.Progress {
		fmt.Printf("processed % 10v summary entries\n", cli.FmtCount(int64(n)))
	}
	agg.Finish()
	sdb.Subtree = agg.Subtree
	sdb.Distributed = agg.Distributed
	sdb.Finalize()
	return sdb, nil
}

// FromPrefixes computes stats from all of the file metadata stored for
// each prefix. The prefixes are visited in DFS pre-order using the summary
// keys, which exist for every prefix, so that memory usage is proportional to
// the depth of the tree and hardlinks are attributed to the same prefixes as
// when using the summaries. The metadata is read and decoded concurrently,
// but the stats, including hardlink deduplication, are computed in order.
func (c Computer) FromPrefixes(ctx context.Context, db idudb.Reader, cfg *iduconfig.Settings, match stats.Matcher, prefix string, topN int) (*report.AllStats, error) {
	if err := idudb.CheckSummaryKeys(ctx, db); err != nil {
		return nil, err
	}
	calc, sep := cfg.CalculatorOrDefault(), cfg.SeparatorOrDefault()
	sdb := report.NewAllStats(prefix, topN)
	agg := report.NewSubtreeAggregator(prefix, topN)
	exactKey, childPrefix := stats.SummaryKeys(sep, prefix)
	readers := c.Readers
	if readers == 0 {
		readers = min(defaultPrefixReaders, runtime.GOMAXPROCS(0))
	}
	n := 0
	err := readPrefixesInOrder(ctx, db, sep, exactKey, childPrefix, readers, func(e *prefixEntry) {
		if c.Progress && (n != 0 && n%1000 == 0) {
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
	if c.Progress {
		fmt.Printf("processed % 10v entries\n", cli.FmtCount(int64(n)))
	}
	agg.Finish()
	sdb.Subtree = agg.Subtree
	sdb.Distributed = agg.Distributed
	sdb.Finalize()
	return sdb, nil
}
