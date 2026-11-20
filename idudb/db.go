// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package idudb provides support for building and maintaining a database
// of filesystem metadata that can be used for incrementally computing disk
// usage statistics. The database itself is accessed via the Database and
// Reader interfaces, implementations of which (e.g. backends/badgerdb) are
// created by a BackendFactory. Open and OpenWritable open the database
// specified by an iduconfig.Settings for reading and for analysis
// respectively. All operations that modify the database are implemented by
// this package; other packages should only read the database via the
// Reader interface, or the higher level APIs provided by the summary and
// report packages. Log messages are written to the
// logger, if any, in the context supplied to each method, see
// cloudeng.io/logging/ctxlog.
package idudb

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file/diskusage"
	"cloudeng.io/logging/ctxlog"
)

// DB represents a writable idu database. A DB is not safe for use by
// multiple, concurrent, Analyzers.
type DB struct {
	db              Database
	compactInterval time.Duration
	lastWritten     time.Time
	calc            diskusage.Calculator
	sep             string
	match           stats.Matcher
	spec            string
	hardlinkInfo    stats.HardlinkRecording
	// recordAll, if set, returns true for the prefixes whose summaries
	// record every file when hardlinkInfo is stats.RecordLinkedFiles.
	recordAll func(prefix string) bool
	// links, if set, records the link counts of the files recorded by
	// the summaries written during an analysis, see missingLinks.
	links *linkCounts
	// rewriteSummaries is set when the stored summaries were computed with
	// a different specification and must be recomputed for unchanged prefixes.
	rewriteSummaries bool
	invalidateOnce   sync.Once
	// observer, if set, is notified of every change to the summaries. It
	// is set by Analyze for the duration of an analysis.
	observer Observer
	topn     topnConfig
	// previousSummaryFailed is set if the summary replaced by a new one
	// could not be read during the current analysis, in which case any
	// state derived from it, such as the top-N rankings, may contain
	// stale entries.
	previousSummaryFailed atomic.Bool
	// progress, if set, is notified of the phases of an analysis and of
	// the maintenance that follows it, see startPhase.
	progress ScanProgress
}

// startPhase reports, and logs, the start of a phase of an analysis, or
// of the maintenance that follows it, and returns a function that logs its
// completion and duration.
func (sdb *DB) startPhase(ctx context.Context, name string) func() {
	if sdb.progress != nil {
		sdb.progress.Phase(name)
	}
	ctxlog.Info(ctx, "phase started", "phase", name)
	start := time.Now()
	return func() {
		ctxlog.Info(ctx, "phase done", "phase", name, "duration", time.Since(start))
	}
}

// New returns a DB that uses the supplied, writable, database backend. The
// returned DB takes ownership of db, which will be closed by calling either
// Close or LogAndClose. If New returns an error, db is not closed and remains
// the responsibility of the caller. New returns ErrLegacyDatabase for
// databases created by older versions of idu, see CheckFormat.
func New(ctx context.Context, db Database, opts ...Option) (*DB, error) {
	o := newOptions(opts)
	if err := CheckFormat(ctx, db); err != nil {
		return nil, err
	}
	spec := summarySpec(o.calc.String(), o.hardlinks)
	recordAll := o.recordAll
	if o.hardlinks == stats.RecordLinkedFiles {
		spec = withRecordAll(spec, o.recordAllID)
	} else {
		recordAll = nil
	}
	rewrite, err := initSummaryMeta(ctx, db, spec)
	if err != nil {
		return nil, err
	}
	var lastWritten time.Time
	if start, stop, _, err := db.LastLog(ctx); err == nil {
		if !stop.IsZero() {
			lastWritten = stop
		} else if !start.IsZero() {
			lastWritten = start
		}
	}
	return &DB{
		db:               db,
		compactInterval:  o.compactInterval,
		lastWritten:      lastWritten,
		calc:             o.calc,
		sep:              o.sep,
		match:            stats.AlwaysMatch(),
		spec:             spec,
		hardlinkInfo:     o.hardlinks,
		recordAll:        recordAll,
		rewriteSummaries: rewrite,
		topn:             o.topn,
	}, nil
}

// markSummariesComplete records that every summary in the database
// was computed with the current configuration. It must only be called
// after a complete, uninterrupted scan of the entire configured prefix.
func (sdb *DB) markSummariesComplete(ctx context.Context) error {
	if !sdb.rewriteSummaries {
		return nil
	}
	// Ensure that all of the rewritten summaries are persisted before
	// recording that they are complete.
	defer sdb.startPhase(ctx, PhaseFlushSummaries)()
	if err := sdb.db.Flush(ctx); err != nil {
		return err
	}
	if err := sdb.db.SetMeta(ctx, metaKeySummaries, []byte(sdb.spec)); err != nil {
		return err
	}
	sdb.rewriteSummaries = false
	return nil
}

func (sdb *DB) logError(ctx context.Context, key string, when time.Time, detail []byte) error {
	return sdb.db.LogError(ctx, key, when, detail)
}

type valueLogGCRunner interface {
	RunValueLogGC(discardRatio float64) error
}

type compactor interface {
	Compact(ctx context.Context) error
}

func (sdb *DB) logAndMaintain(ctx context.Context, start, stop time.Time, detail []byte) error {
	// Always attempt to persist the run log entry even if ctx was canceled,
	// so interrupted analyze runs leave an audit trail before closing.
	if err := sdb.db.Log(context.WithoutCancel(ctx), start, stop, detail); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	if sdb.compactInterval != 0 && !sdb.lastWritten.IsZero() && time.Since(sdb.lastWritten) > sdb.compactInterval {
		if c, ok := sdb.db.(compactor); ok {
			ctxlog.Info(ctx, "compacting database", "interval", sdb.compactInterval, "last_written", sdb.lastWritten)
			defer sdb.startPhase(ctx, PhaseCompact)()
			return c.Compact(ctx)
		}
	}
	if gc, ok := sdb.db.(valueLogGCRunner); ok {
		defer sdb.startPhase(ctx, PhaseValueLogGC)()
		return gc.RunValueLogGC(0.5)
	}
	return nil
}

// LogAndClose records a log entry for an operation that ran from start to
// stop with the supplied detail, performs any required maintenance (such as
// compaction or garbage collection) and closes the database.
func (sdb *DB) LogAndClose(ctx context.Context, start, stop time.Time, detail []byte) error {
	opErr := sdb.logAndMaintain(ctx, start, stop, detail)
	closeErr := sdb.db.Close(ctx)
	return errors.Join(opErr, closeErr)
}

// Close closes the database.
func (sdb *DB) Close(ctx context.Context) error {
	return sdb.db.Close(ctx)
}

// childPrefix returns the key prefix shared by all of the descendants
// of prefix, but not by its siblings (e.g. /a/ rather than /a which
// would also match /a-b).
func (sdb *DB) childPrefix(prefix string) string {
	return strings.TrimRight(prefix, sdb.sep) + sdb.sep
}

// deletePrefix deletes prefix and all of the prefixes beneath it, but not
// siblings that share it as a string prefix (e.g. /a-b for /a).
func (sdb *DB) deletePrefix(ctx context.Context, prefix string) error {
	// Deletes are not batched, so flush any pending batched writes for
	// prefix or its descendants to prevent them from being written after,
	// and hence undoing, the delete.
	if err := sdb.db.Flush(ctx); err != nil {
		return err
	}
	if sdb.links != nil {
		sdb.links.deleted(prefix)
	}
	if sdb.observer != nil {
		var scanErr error
		sdb.observer.SubtreeDeleted(ctx, prefix, sdb.deletedSummaries(ctx, prefix, &scanErr))
		if scanErr != nil {
			return scanErr
		}
	}
	if err := sdb.db.Delete(ctx, prefix); err != nil {
		return err
	}
	if err := sdb.db.DeletePrefix(ctx, sdb.childPrefix(prefix)); err != nil {
		return err
	}
	exactKey, childPrefix := stats.SummaryKeys(sdb.sep, prefix)
	if err := sdb.db.DeleteSummary(ctx, exactKey, childPrefix); err != nil {
		return err
	}
	return sdb.db.DeleteSubtrees(ctx, exactKey, childPrefix)
}

// deleteErrors deletes the errors logged for prefix and all of the prefixes
// beneath it, but not for siblings that share it as a string prefix.
func (sdb *DB) deleteErrors(ctx context.Context, prefix string) error {
	if err := sdb.db.DeleteError(ctx, prefix); err != nil {
		return err
	}
	return sdb.db.DeleteErrors(ctx, sdb.childPrefix(prefix))
}

func (sdb *DB) getPrefixInfo(ctx context.Context, key string, pi *prefixinfo.T) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	if err := sdb.db.Get(ctx, key, buf); err != nil {
		return false, err
	}
	if len(buf.Bytes()) == 0 {
		// Key does not exist or is a bucket, which should never happen here.
		return false, nil
	}
	return true, pi.UnmarshalBinary(buf.Bytes())
}

func (sdb *DB) setPrefixInfo(ctx context.Context, key string, unchanged bool, pi *prefixinfo.T) error {
	if unchanged {
		if !sdb.rewriteSummaries {
			return nil
		}
		// pi contains the previously stored files and the current
		// sub-directories, which is all that's needed for the summary.
		return sdb.setSummary(ctx, key, pi)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	if err := pi.AppendBinary(buf); err != nil {
		return err
	}
	if err := sdb.db.Set(ctx, key, buf.Bytes(), true); err != nil {
		return err
	}
	return sdb.setSummary(ctx, key, pi)
}

func (sdb *DB) setSummary(ctx context.Context, key string, pi *prefixinfo.T) error {
	rec := sdb.hardlinkInfo
	if sdb.recordAll != nil && sdb.recordAll(key) {
		rec = stats.RecordAllFiles
	}
	summary := stats.ComputeSummary(key, pi, sdb.calc, sdb.match, rec)
	exactKey, _ := stats.SummaryKeys(sdb.sep, key)
	var old *stats.PrefixSummary
	if sdb.observer != nil {
		// Each prefix's summary is written at most once per analysis and
		// all writes are flushed when an analysis completes, so the
		// previous summary cannot be in a pending batched write.
		old = sdb.previousSummary(ctx, key, exactKey)
	}
	err := sdb.db.SetSummary(ctx, exactKey, summary.AppendBinary(nil), true)
	if err == nil && sdb.observer != nil {
		sdb.observer.PrefixChanged(ctx, key, old, &summary)
	}
	if err == nil && sdb.links != nil {
		sdb.links.record(key, pi)
	}
	if err != nil {
		// Ensure that stats will not use the now incomplete summaries
		// and that the next analyze will rewrite them.
		// The failure may be due to cancelation, so don't let that prevent
		// recording that the summaries are no longer valid.
		sdb.invalidateOnce.Do(func() {
			if ierr := sdb.db.SetMeta(context.WithoutCancel(ctx), metaKeySummaries, []byte(invalidSummariesSpec)); ierr != nil {
				ctxlog.Error(ctx, "failed to invalidate summaries", "error", ierr)
			}
		})
	}
	return err
}

var bufPool = sync.Pool{
	New: func() any {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return new(bytes.Buffer)
	},
}
