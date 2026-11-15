// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/database/badgerdb"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file/diskusage"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

var badgerLogName = defaultLogName + "-badger"

func logDB(ctx context.Context, level slog.Level, msg string, args ...interface{}) {
	if level < Verbosity {
		return
	}
	var pcs [1]uintptr
	if logSourceCode {
		runtime.Callers(2, pcs[:]) // skip [Callers, infof]
	}
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	r.Add(args...)
	_ = getOrCreateLogger(badgerLogName).Handler().Handle(ctx, r)
}

type badgerLogger struct{}

func (l *badgerLogger) Errorf(f string, a ...interface{}) {
	if LogError < Verbosity {
		return
	}
	logDB(context.Background(), LogError, "badger", "msg", fmt.Sprintf(f, a...))
}

func (l *badgerLogger) Warningf(f string, a ...interface{}) {
	if slog.LevelWarn < Verbosity {
		return
	}
	logDB(context.Background(), slog.LevelWarn, "badger", "msg", fmt.Sprintf(f, a...))
}

func (l *badgerLogger) Infof(f string, a ...interface{}) {
	if slog.LevelInfo < Verbosity {
		return
	}
	logDB(context.Background(), slog.LevelInfo, "badger", "msg", fmt.Sprintf(f, a...))
}

func (l *badgerLogger) Debugf(f string, a ...interface{}) {
	if slog.LevelDebug < Verbosity {
		return
	}
	logDB(context.Background(), slog.LevelDebug, "badger", "msg", fmt.Sprintf(f, a...))
}

func badgerdbOptions(readonly bool, opts ...badgerdb.Option) []badgerdb.Option {
	if readonly {
		opts = append(opts, badgerdb.ReadOnly())
	}
	return opts
}

func openBadgerDB(_ context.Context, cfg config.Prefix, readonly bool) (database.DB, error) {
	opts := badgerdbOptions(readonly)
	bopts := badger.DefaultOptions(cfg.Database).
		WithLogger(&badgerLogger{}).
		WithDetectConflicts(false).
		WithMetricsEnabled(false).
		WithIndexCacheSize(64 << 20)
	if !readonly {
		bopts = bopts.
			WithCompactL0OnClose(true).
			WithCompression(options.ZSTD).
			WithBlockCacheSize(32 << 20)
	}
	opts = append(opts, badgerdb.WithBadgerOptions(bopts))
	return badgerdb.Open(cfg.Database, opts...)
}

func UseBadgerDB() {
	databaseFactory = openBadgerDB
}

var databaseFactory = openBadgerDB

func OpenDatabase(ctx context.Context, cfg config.Prefix, readonly bool) (database.DB, error) {
	doneCh := make(chan struct {
		db  database.DB
		err error
	}, 1)
	go func() {
		db, err := databaseFactory(ctx, cfg, readonly)
		doneCh <- struct {
			db  database.DB
			err error
		}{db, err}
	}()
	start := time.Now()
	delayed := false
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case res := <-doneCh:
			if delayed {
				fmt.Println()
			}
			if res.err != nil {
				return nil, res.err
			}
			if err := checkDatabaseFormat(ctx, res.db); err != nil {
				_ = res.db.Close(ctx)
				return nil, fmt.Errorf("%v: %w", cfg.Database, err)
			}
			return res.db, nil
		case <-time.After(time.Second):
			fmt.Printf("waiting for database to open: %v: %s\t\t\r", cfg.Database, time.Since(start).Truncate(time.Second))
			delayed = true
		}
	}
}

func OpenPrefixAndDatabase(ctx context.Context, all config.T, prefix string, readonly bool) (context.Context, config.Prefix, string, database.DB, error) {
	ctx, cfg, target, err := LookupPrefix(ctx, all, prefix)
	if err != nil {
		return ctx, config.Prefix{}, "", nil, err
	}
	db, err := OpenDatabase(ctx, cfg, readonly)
	if err != nil {
		return ctx, config.Prefix{}, "", nil, fmt.Errorf("failed to open database for %v in %v: %v", cfg.Prefix, cfg.Database, err)
	}
	return ctx, cfg, target, db, nil
}

type ScanDB interface {
	GetPrefixInfo(ctx context.Context, key string, pi *prefixinfo.T) (bool, error)
	SetPrefixInfo(ctx context.Context, key string, unchanged bool, pi *prefixinfo.T) error
	LogError(ctx context.Context, key string, when time.Time, detail []byte) error
	LogAndClose(ctx context.Context, start, stop time.Time, detail []byte) error
	DeletePrefix(ctx context.Context, prefix string) error
	DeleteErrors(ctx context.Context, prefix string) error
	// MarkSummariesComplete records that every summary in the database
	// was computed with the current configuration. It must only be called
	// after a complete, uninterrupted scan of the entire configured prefix.
	MarkSummariesComplete(ctx context.Context) error
	Close(ctx context.Context) error
}

const metaKeySummaries = "summaries/v1"

type scanDB struct {
	db              database.DB
	compactInterval time.Duration
	lastWritten     time.Time
	calc            diskusage.Calculator
	sep             string
	match           boolexpr.Matcher
	spec            string
	hardlinkInfo    bool
	// rewriteSummaries is set when the stored summaries were computed with
	// a different specification and must be recomputed for unchanged prefixes.
	rewriteSummaries bool
	invalidateOnce   sync.Once
}

const (
	hardlinksSpecSuffix = ";hardlinks"
	// invalidSummariesSpec is stored when a summary could not be written
	// so that the next analyze rewrites them all.
	invalidSummariesSpec = "invalid"
)

// SummarySpec returns the specification that the precomputed summaries must
// be created with for cfg: they depend on the disk usage calculator and
// whether hardlinks are recorded.
func SummarySpec(cfg *config.Prefix) string {
	spec := cfg.CalculatorOrDefault().String()
	if cfg.RecordHardlinks() {
		spec += hardlinksSpecSuffix
	}
	return spec
}

// ErrStaleSummaries is returned when the precomputed summaries are
// incomplete or were computed using a different configuration.
var ErrStaleSummaries = errors.New("summaries are incomplete or were computed with a different configuration (layout or hardlink settings): rerun analyze for the entire prefix")

// ErrLegacyDatabase is returned when opening a database that was created
// before precomputed summaries were introduced. Such databases are not
// supported and must be deleted and recreated by rerunning analyze.
var ErrLegacyDatabase = errors.New("legacy database format without summaries: delete it and rerun analyze")

// checkDatabaseFormat returns ErrLegacyDatabase if db contains prefix data
// but no summary metadata.
func checkDatabaseFormat(ctx context.Context, db database.DB) error {
	var metaBuf bytes.Buffer
	if err := db.GetMeta(ctx, metaKeySummaries, &metaBuf); err != nil {
		return err
	}
	if metaBuf.Len() > 0 {
		return nil
	}
	hasData := false
	if err := db.Scan(ctx, "", func(_ context.Context, _ string, _ []byte) bool {
		hasData = true
		return false
	}); err != nil {
		return err
	}
	if hasData {
		return ErrLegacyDatabase
	}
	return nil
}

// initSummaryMeta returns true if the existing summaries were computed
// with a specification other than spec and hence need to be rewritten.
// OpenDatabase rejects legacy databases, so a missing metadata entry
// denotes a new, empty, database.
func initSummaryMeta(ctx context.Context, db database.DB, spec string) (bool, error) {
	var metaBuf bytes.Buffer
	if err := db.GetMeta(ctx, metaKeySummaries, &metaBuf); err != nil {
		return false, err
	}
	if metaBuf.Len() == 0 {
		return false, db.SetMeta(ctx, metaKeySummaries, []byte(spec))
	}
	return metaBuf.String() != spec, nil
}

// CheckSummaries returns ErrStaleSummaries unless db contains complete
// precomputed summaries computed with calcID that also record hardlinks
// if needLinks is true.
func CheckSummaries(ctx context.Context, db database.DB, calcID string, needLinks bool) error {
	var metaBuf bytes.Buffer
	if err := db.GetMeta(ctx, metaKeySummaries, &metaBuf); err != nil {
		return err
	}
	switch metaBuf.String() {
	case calcID + hardlinksSpecSuffix:
		return nil
	case calcID:
		if !needLinks {
			return nil
		}
	}
	return ErrStaleSummaries
}

// CheckSummaryKeys returns ErrStaleSummaries if a summary could not be
// written and hence the summary keys may not include every prefix.
func CheckSummaryKeys(ctx context.Context, db database.DB) error {
	var metaBuf bytes.Buffer
	if err := db.GetMeta(ctx, metaKeySummaries, &metaBuf); err != nil {
		return err
	}
	if metaBuf.String() == invalidSummariesSpec {
		return ErrStaleSummaries
	}
	return nil
}

func NewScanDB(ctx context.Context, cfg config.Prefix) (ScanDB, error) {
	db, err := OpenDatabase(ctx, cfg, false)
	if err != nil {
		return nil, err
	}
	spec := SummarySpec(&cfg)
	rewrite, err := initSummaryMeta(ctx, db, spec)
	if err != nil {
		_ = db.Close(ctx)
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
	return &scanDB{
		db:               db,
		compactInterval:  cfg.CompactionInterval(),
		lastWritten:      lastWritten,
		calc:             cfg.CalculatorOrDefault(),
		sep:              cfg.SeparatorOrDefault(),
		match:            boolexpr.AlwaysMatch(nil),
		spec:             spec,
		hardlinkInfo:     cfg.RecordHardlinks(),
		rewriteSummaries: rewrite,
	}, nil
}

func (sdb *scanDB) MarkSummariesComplete(ctx context.Context) error {
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

func (sdb *scanDB) LogError(ctx context.Context, key string, when time.Time, detail []byte) error {
	return sdb.db.LogError(ctx, key, when, detail)
}

type valueLogGCRunner interface {
	RunValueLogGC(discardRatio float64) error
}

type compactor interface {
	Compact(ctx context.Context) error
}

func (sdb *scanDB) LogAndClose(ctx context.Context, start, stop time.Time, detail []byte) error {
	if err := sdb.db.Log(ctx, start, stop, detail); err != nil {
		return err
	}
	if ctx.Err() == nil {
		compacted := false
		if sdb.compactInterval != 0 && !sdb.lastWritten.IsZero() && time.Since(sdb.lastWritten) > sdb.compactInterval {
			if c, ok := sdb.db.(compactor); ok {
				Log(ctx, LogProgress, "compacting database", "interval", sdb.compactInterval, "last_written", sdb.lastWritten)
				if err := c.Compact(ctx); err != nil {
					return err
				}
				compacted = true
			}
		}
		if !compacted {
			if gc, ok := sdb.db.(valueLogGCRunner); ok {
				if err := gc.RunValueLogGC(0.5); err != nil {
					return err
				}
			}
		}
	}
	if err := sdb.db.Close(ctx); err != nil {
		return err
	}
	return nil
}

// childPrefix returns the key prefix shared by all of the descendants
// of prefix, but not by its siblings (e.g. /a/ rather than /a which
// would also match /a-b).
func (sdb *scanDB) childPrefix(prefix string) string {
	return strings.TrimRight(prefix, sdb.sep) + sdb.sep
}

// DeletePrefix deletes prefix and all of the prefixes beneath it, but not
// siblings that share it as a string prefix (e.g. /a-b for /a).
func (sdb *scanDB) DeletePrefix(ctx context.Context, prefix string) error {
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

// DeleteErrors deletes the errors logged for prefix and all of the prefixes
// beneath it, but not for siblings that share it as a string prefix.
func (sdb *scanDB) DeleteErrors(ctx context.Context, prefix string) error {
	if err := sdb.db.DeleteError(ctx, prefix); err != nil {
		return err
	}
	return sdb.db.DeleteErrors(ctx, sdb.childPrefix(prefix))
}

func (sdb *scanDB) GetPrefixInfo(ctx context.Context, key string, pi *prefixinfo.T) (bool, error) {
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

func (sdb *scanDB) SetPrefixInfo(ctx context.Context, key string, unchanged bool, pi *prefixinfo.T) error {
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

func (sdb *scanDB) setSummary(ctx context.Context, key string, pi *prefixinfo.T) error {
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
				Log(ctx, LogError, "failed to invalidate summaries", "error", ierr)
			}
		})
	}
	return err
}

func (sdb *scanDB) Close(ctx context.Context) error {
	return sdb.db.Close(ctx)
}

var bufPool = sync.Pool{
	New: func() any {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return new(bytes.Buffer)
	},
}
