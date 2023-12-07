// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/database/badgerdb"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"github.com/dgraph-io/badger/v4"
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
	_ = getOrCreateLogger(badgerLogName, ctx).Handler().Handle(ctx, r)
}

type badgerLogger struct{}

func (l *badgerLogger) Errorf(f string, a ...interface{}) {
	m := fmt.Sprintf(f, a...)
	logDB(context.Background(), LogError, "badger", "msg", m)
}

func (l *badgerLogger) Warningf(f string, a ...interface{}) {
	m := fmt.Sprintf(f, a...)
	logDB(context.Background(), slog.LevelWarn, "badger", "msg", m)
}

func (l *badgerLogger) Infof(f string, a ...interface{}) {
	m := fmt.Sprintf(f, a...)
	logDB(context.Background(), slog.LevelInfo, "badger", "msg", m)
}

func (l *badgerLogger) Debugf(f string, a ...interface{}) {
	m := fmt.Sprintf(f, a...)
	logDB(context.Background(), slog.LevelWarn, "badger", "msg", m)
}

func badgerdbOptions(readonly bool, opts ...badgerdb.Option) []badgerdb.Option {
	if readonly {
		opts = append(opts, badgerdb.ReadOnly())
	}
	return opts
}

func openBadgerDB(ctx context.Context, cfg config.Prefix, readonly bool) (database.DB, error) {
	opts := badgerdbOptions(readonly)
	bopts := badger.DefaultOptions(cfg.Database)
	bopts = bopts.WithLogger(&badgerLogger{})
	bopts = bopts.WithLoggingLevel(badger.ERROR)
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
			return res.db, res.err
		case <-time.After(time.Second):
			fmt.Printf("waiting for database to open: %v: %s\t\t\r", cfg.Database, time.Since(start).Truncate(time.Second))
			delayed = true
		}
	}
}

func OpenPrefixAndDatabase(ctx context.Context, all config.T, prefix string, readonly bool) (context.Context, config.Prefix, database.DB, error) {
	ctx, cfg, err := LookupPrefix(ctx, all, prefix)
	if err != nil {
		return ctx, config.Prefix{}, nil, err
	}
	db, err := OpenDatabase(ctx, cfg, readonly)
	if err != nil {
		return ctx, config.Prefix{}, nil, fmt.Errorf("failed to open database for %v in %v: %v\n", cfg.Prefix, cfg.Database, err)
	}
	return ctx, cfg, db, nil
}

type ScanDB interface {
	GetPrefixInfo(ctx context.Context, key string, pi *prefixinfo.T) (bool, error)
	SetPrefixInfo(ctx context.Context, key string, unchanged bool, pi *prefixinfo.T) error
	LogError(ctx context.Context, key string, when time.Time, detail []byte) error
	LogAndClose(ctx context.Context, start, stop time.Time, detail []byte) error
	DeletePrefix(ctx context.Context, prefix string) error
	DeleteErrors(ctx context.Context, prefix string) error
	Close(ctx context.Context) error
}

type scanDB struct {
	db database.DB
}

func NewScanDB(ctx context.Context, cfg config.Prefix) (ScanDB, error) {
	db, err := OpenDatabase(ctx, cfg, false)
	if err != nil {
		return nil, err
	}
	return &scanDB{
		db: db,
	}, nil
}

func (sdb *scanDB) LogError(ctx context.Context, key string, when time.Time, detail []byte) error {
	return sdb.db.LogError(ctx, key, when, detail)
}

func (sdb *scanDB) LogAndClose(ctx context.Context, start, stop time.Time, detail []byte) error {
	if err := sdb.db.Log(ctx, start, stop, detail); err != nil {
		return err
	}
	if err := sdb.db.Close(ctx); err != nil {
		return err
	}
	return nil
}

func (sdb *scanDB) DeletePrefix(ctx context.Context, prefix string) error {
	return sdb.db.DeletePrefix(ctx, prefix)
}

func (sdb *scanDB) DeleteErrors(ctx context.Context, prefix string) error {
	return sdb.db.DeleteErrors(ctx, prefix)
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
	if err := sdb.db.GetBuf(ctx, key, buf); err != nil {
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
		return nil
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
	return sdb.db.SetBatch(ctx, key, buf.Bytes())
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
