// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"path/filepath"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/file/diskusage"
	"cloudeng.io/file/filewalk"
)

// Option configures a DB created by New.
type Option func(*options)

type options struct {
	calc            diskusage.Calculator
	sep             string
	hardlinks       bool
	compactInterval time.Duration
	topn            topnConfig
}

func newOptions(opts []Option) options {
	o := options{
		calc: diskusage.NewIdentity(),
		sep:  string(filepath.Separator),
	}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// WithCalculator sets the disk usage calculator used for the precomputed
// summaries. It defaults to diskusage.NewIdentity().
func WithCalculator(calc diskusage.Calculator) Option {
	return func(o *options) {
		if calc != nil {
			o.calc = calc
		}
	}
}

// WithSeparator sets the filename separator, it defaults to filepath.Separator.
func WithSeparator(sep string) Option {
	return func(o *options) {
		if sep != "" {
			o.sep = sep
		}
	}
}

// WithRecordHardlinks requests that the precomputed summaries record
// potential hardlinks so that they can be deduplicated without rescanning
// the database.
func WithRecordHardlinks(v bool) Option {
	return func(o *options) {
		o.hardlinks = v
	}
}

// WithCompactionInterval requests that the database be compacted when it is
// closed via LogAndClose if more than the specified duration has elapsed
// since it was last written. A zero value disables compaction.
func WithCompactionInterval(d time.Duration) Option {
	return func(o *options) {
		o.compactInterval = d
	}
}

// WithTopN requests that the top n prefixes, by each stats.Metric, be
// maintained incrementally by every analysis, both overall and for each
// user and group, using heaps that hold capacity and perIDCapacity entries
// respectively. The capacities should exceed n and perIDN so that entries
// that shrink or are deleted can be replaced without recomputing the
// heaps, see iduconfig.Settings.TopNCapacity. A value of zero for n
// disables the maintenance of the rankings.
func WithTopN(n, capacity, perIDN, perIDCapacity int) Option {
	return func(o *options) {
		o.topn = topnConfig{
			n:             max(n, 0),
			capacity:      max(capacity, n, 0),
			perIDN:        max(perIDN, 0),
			perIDCapacity: max(perIDCapacity, perIDN, 0),
		}
	}
}

// OptionsFromConfig returns the database options specified by cfg.
func OptionsFromConfig(cfg *iduconfig.Settings) []Option {
	return []Option{
		WithCalculator(cfg.CalculatorOrDefault()),
		WithSeparator(cfg.SeparatorOrDefault()),
		WithRecordHardlinks(cfg.RecordHardlinks()),
		WithCompactionInterval(cfg.CompactionInterval()),
		WithTopN(cfg.TopNOrDefault(), cfg.TopNCapacity(), cfg.TopNPerIDOrDefault(), cfg.TopNPerIDCapacity()),
	}
}

// AnalyzeOption configures an Analyzer.
type AnalyzeOption func(*analyzeOptions)

type analyzeOptions struct {
	fs                       filewalk.FS
	exclude                  func(path string) bool
	concurrentScans          int
	scanSize                 int
	concurrentStats          int
	concurrentStatsThreshold int
	force                    bool
	slowScan                 time.Duration
	progress                 ScanProgress
	observer                 Observer
}

// DefaultSlowScanThreshold is the default duration after which scans
// are reported as being slow.
const DefaultSlowScanThreshold = 10 * time.Second

// WithFS sets the filesystem to be analyzed, it defaults to the local
// filesystem.
func WithFS(fs filewalk.FS) AnalyzeOption {
	return func(o *analyzeOptions) {
		o.fs = fs
	}
}

// WithExclusions sets a function that is called for every prefix and file
// encountered; those for which it returns true are ignored.
func WithExclusions(exclude func(path string) bool) AnalyzeOption {
	return func(o *analyzeOptions) {
		o.exclude = exclude
	}
}

// WithConcurrentScans sets the maximum number of concurrent scan operations,
// zero selects filewalk's default.
func WithConcurrentScans(n int) AnalyzeOption {
	return func(o *analyzeOptions) {
		o.concurrentScans = n
	}
}

// WithScanSize sets the maximum number of items to fetch from the filesystem
// in a single operation, zero selects filewalk's default.
func WithScanSize(n int) AnalyzeOption {
	return func(o *analyzeOptions) {
		o.scanSize = n
	}
}

// WithConcurrentStats sets the maximum number of concurrent stat operations,
// zero selects asyncstat's default.
func WithConcurrentStats(n int) AnalyzeOption {
	return func(o *analyzeOptions) {
		o.concurrentStats = n
	}
}

// WithConcurrentStatsThreshold sets the minimum number of files before stats
// are performed concurrently, the value is passed directly to
// asyncstat.WithAsyncThreshold.
func WithConcurrentStatsThreshold(n int) AnalyzeOption {
	return func(o *analyzeOptions) {
		o.concurrentStatsThreshold = n
	}
}

// WithForce requests that all prefixes be reanalyzed even if they are
// unchanged since they were last analyzed.
func WithForce(v bool) AnalyzeOption {
	return func(o *analyzeOptions) {
		o.force = v
	}
}

// WithSlowScanThreshold sets the duration after which a scan is reported
// as slow, it defaults to DefaultSlowScanThreshold.
func WithSlowScanThreshold(d time.Duration) AnalyzeOption {
	return func(o *analyzeOptions) {
		o.slowScan = d
	}
}

// WithProgress sets the ScanProgress implementation to be notified of
// the progress of the analysis.
func WithProgress(p ScanProgress) AnalyzeOption {
	return func(o *analyzeOptions) {
		if p != nil {
			o.progress = p
		}
	}
}

// AnalyzeOptionsFromConfig returns the analyze options specified by cfg.
// The filesystem (WithFS) and any options not represented in the
// configuration (eg. WithForce, WithProgress) must be supplied separately.
func AnalyzeOptionsFromConfig(cfg *iduconfig.Settings) []AnalyzeOption {
	return []AnalyzeOption{
		WithExclusions(cfg.Exclude),
		WithConcurrentScans(cfg.ConcurrentScans),
		WithScanSize(cfg.ScanSize),
		WithConcurrentStats(cfg.ConcurrentStats),
		WithConcurrentStatsThreshold(cfg.ConcurrentStatsThreshold),
	}
}
