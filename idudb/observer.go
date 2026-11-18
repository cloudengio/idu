// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"fmt"
	"iter"

	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/logging/ctxlog"
)

// ScanInfo describes an analysis, see Observer.ScanStarted.
type ScanInfo struct {
	// Root is the root of the filesystem being analyzed.
	Root string
	// Target is the prefix being analyzed, ie. Root or a prefix beneath it.
	Target string
	// Rewrite is true if the stored summaries were computed with a
	// different configuration (eg. calculator or hardlink settings) and
	// hence all of the summaries for Target will be rewritten. The old
	// summaries reported by PrefixChanged are then not comparable with
	// the new ones and any state derived from them should be rebuilt.
	Rewrite bool
}

// Observer is notified of every change that an Analyzer makes to the
// summaries stored in a DB. It allows for state derived from the summaries,
// such as aggregates, indexes or exports, to be maintained incrementally.
//
// PrefixChanged and SubtreeDeleted may be called concurrently, by multiple
// goroutines, between the calls to ScanStarted and ScanDone. They
// deliberately do not return errors: an observer cannot abort an analysis
// once it has started, since the database of filesystem metadata is the
// primary record and is always kept as complete as possible, whereas any
// state derived from it can be rebuilt. An observer that fails to maintain
// its state should record the failure, return it from ScanDone and rebuild
// that state, eg. on the next analysis. Only ScanStarted can prevent an
// analysis, by returning an error before it begins.
type Observer interface {
	// ScanStarted is called before the analysis starts. If it returns an
	// error the analysis is not performed and ScanDone is not called.
	ScanStarted(ctx context.Context, scan ScanInfo) error

	// PrefixChanged is called after the summary for path has been written,
	// with the summary that it replaced, old, if any, and the new summary, cur.
	// It is only called for prefixes whose summary was rewritten, ie. those
	// that changed since the previous analysis or all prefixes if
	// ScanInfo.Rewrite is true. old is nil if there was no previous
	// summary or it could not be read. Neither old nor cur may be retained
	// after PrefixChanged returns.
	PrefixChanged(ctx context.Context, path string, old, cur *stats.PrefixSummary)

	// SubtreeDeleted is called before the prefix path, and all of the
	// prefixes beneath it, are deleted from the database. summaries
	// iterates over the summaries being deleted, path's own included, in
	// depth-first pre-order; it reads them from the database and hence
	// incurs no cost if not used. It is only valid for the duration of the
	// call and the summaries it returns may not be retained. If it cannot
	// read or decode a summary then iteration stops and the deletion fails
	// and is reported as an error by the analysis, and hence to ScanDone.
	SubtreeDeleted(ctx context.Context, path string, summaries iter.Seq2[string, *stats.PrefixSummary])

	// ScanDone is called once the analysis has completed and all of the
	// changes it made have been persisted. scanErr is nil if the entire
	// Target was analyzed successfully, otherwise the analysis was
	// incomplete or interrupted and the changes reported may not include
	// all of those made to the database.
	ScanDone(ctx context.Context, scanErr error) error
}

// WithObserver sets the Observer to be notified of all changes
// made to the summaries by the Analyzer.
func WithObserver(o Observer) AnalyzeOption {
	return func(ao *analyzeOptions) {
		ao.observer = o
	}
}

// getSummary returns the summary stored for exactKey, if any.
func (sdb *DB) getSummary(ctx context.Context, exactKey string) (*stats.PrefixSummary, error) {
	var ps *stats.PrefixSummary
	var decodeErr error
	err := sdb.db.ScanSummaries(ctx, exactKey, "", func(_ context.Context, _ string, val []byte) bool {
		ps = &stats.PrefixSummary{}
		if decodeErr = ps.UnmarshalBinary(val); decodeErr != nil {
			ps = nil
		}
		return false
	})
	if err == nil {
		err = decodeErr
	}
	return ps, err
}

// previousSummary returns the summary stored for exactKey, if any, for
// reporting to the observer. Errors are logged rather than returned since
// they should not prevent the new summary from being written.
func (sdb *DB) previousSummary(ctx context.Context, key, exactKey string) *stats.PrefixSummary {
	ps, err := sdb.getSummary(ctx, exactKey)
	if err != nil {
		ctxlog.Warn(ctx, "failed to read previous summary", "path", key, "error", err)
		return nil
	}
	return ps
}

// deletedSummaries returns an iterator over the summaries stored for
// prefix and its descendants. Any error encountered is stored in *errp.
func (sdb *DB) deletedSummaries(ctx context.Context, prefix string, errp *error) iter.Seq2[string, *stats.PrefixSummary] {
	return func(yield func(string, *stats.PrefixSummary) bool) {
		exactKey, childPrefix := stats.SummaryKeys(sdb.sep, prefix)
		var ps stats.PrefixSummary
		var decodeErr error
		err := sdb.db.ScanSummaries(ctx, exactKey, childPrefix, func(_ context.Context, key string, val []byte) bool {
			path := stats.DecodeSummaryKey(sdb.sep, key)
			if decodeErr = ps.UnmarshalBinary(val); decodeErr != nil {
				decodeErr = fmt.Errorf("%v: %w", path, decodeErr)
				return false
			}
			return yield(path, &ps)
		})
		if err == nil {
			err = decodeErr
		}
		if err != nil {
			*errp = err
		}
	}
}
