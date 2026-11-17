// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package idudb provides support for building and maintaining a database
// of filesystem metadata that can be used for incrementally computing disk
// usage statistics. The database itself is accessed via the Database and
// Reader interfaces, implementations of which (e.g. backends/badgerdb) are
// created by the caller and passed to New. All operations that modify the
// database are implemented by this package; other packages should only
// read the database via the Reader interface.
package idudb

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file/diskusage"
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
	hardlinkInfo    bool
	// rewriteSummaries is set when the stored summaries were computed with
	// a different specification and must be recomputed for unchanged prefixes.
	rewriteSummaries bool
	invalidateOnce   sync.Once
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
		rewriteSummaries: rewrite,
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
			internal.Log(ctx, internal.LogProgress, "compacting database", "interval", sdb.compactInterval, "last_written", sdb.lastWritten)
			return c.Compact(ctx)
		}
	}
	if gc, ok := sdb.db.(valueLogGCRunner); ok {
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
	if err := sdb.db.Delete(ctx, prefix); err != nil {
		return err
	}
	if err := sdb.db.DeletePrefix(ctx, sdb.childPrefix(prefix)); err != nil {
		return err
	}
	exactKey, childPrefix := stats.SummaryKeys(sdb.sep, prefix)
	return sdb.db.DeleteSummary(ctx, exactKey, childPrefix)
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
	summary := stats.ComputeSummary(key, pi, sdb.calc, sdb.match, sdb.hardlinkInfo)
	exactKey, _ := stats.SummaryKeys(sdb.sep, key)
	err := sdb.db.SetSummary(ctx, exactKey, summary.AppendBinary(nil), true)
	if err != nil {
		// Ensure that stats will not use the now incomplete summaries
		// and that the next analyze will rewrite them.
		// The failure may be due to cancelation, so don't let that prevent
		// recording that the summaries are no longer valid.
		sdb.invalidateOnce.Do(func() {
			if ierr := sdb.db.SetMeta(context.WithoutCancel(ctx), metaKeySummaries, []byte(invalidSummariesSpec)); ierr != nil {
				internal.Log(ctx, internal.LogError, "failed to invalidate summaries", "error", ierr)
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
