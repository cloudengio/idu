// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/database/badgerdb"
	"cloudeng.io/cmd/idu/internal/database/boltdb"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/errors"
	"github.com/dgraph-io/badger/v4"
)

func boltdbOptions(readonly bool, opts ...boltdb.Option) []boltdb.Option {
	if readonly {
		opts = append(opts, boltdb.ReadOnly())
	}
	return opts
}

func openBoltDB(ctx context.Context, cfg config.Prefix, readonly bool) (database.DB, error) {
	opts := boltdbOptions(readonly, boltdb.WithTimeout(10*time.Second))
	return boltdb.Open(cfg.Database, opts...)
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
	opts = append(opts, badgerdb.WithBadgerOptions(bopts))
	return badgerdb.Open(cfg.Database, opts...)
}

func UseBadgerDB() {
	databaseFactory = openBadgerDB
}

func UseBoltDB() {
	databaseFactory = openBoltDB
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

type WriteOnlyDB struct {
	from, to string
	rddb     database.DB
	wrdb     database.DB
	inplace  bool
}

func NewScanDB(ctx context.Context, cfg config.Prefix, inplace bool) (ScanDB, error) {
	if inplace {
		db, err := OpenDatabase(ctx, cfg, false)
		if err != nil {
			return nil, err
		}
		return &WriteOnlyDB{
			inplace: true,
			wrdb:    db,
		}, nil
	}

	var rddb database.DB
	_, err := os.Stat(cfg.Database)
	if err != nil {
		// Create an empty read-only database when none exists.
		if !os.IsNotExist(err) {
			return nil, err
		}
		db, err := OpenDatabase(ctx, cfg, false)
		if err != nil {
			return nil, err
		}
		if err := db.Close(ctx); err != nil {
			return nil, err
		}
	}

	rddb, err = OpenDatabase(ctx, cfg, true)
	if err != nil {
		return nil, err
	}

	wrCfg := cfg
	wrCfg.Database = cfg.Database + ".new"
	from, to := cfg.Database, wrCfg.Database
	wrdb, err := OpenDatabase(ctx, wrCfg, false)
	if err != nil {
		return nil, err
	}

	if err := copyMetadata(ctx, cfg.Prefix, rddb, wrdb); err != nil {
		return nil, fmt.Errorf("failed copying metadata to new database: %v", err)
	}
	return &WriteOnlyDB{
		inplace: false,
		from:    from,
		to:      to,
		rddb:    rddb,
		wrdb:    wrdb,
	}, nil
}

func copyMetadata(ctx context.Context, prefix string, from, to database.DB) error {
	errs := &errors.M{}
	from.VisitErrors(ctx, prefix,
		func(_ context.Context, key string, when time.Time, detail []byte) bool {
			err := to.LogError(ctx, key, when, detail)
			errs.Append(err)
			return true
		})

	from.VisitLogs(ctx, time.Time{}, time.Now(), func(ctx context.Context, begin, end time.Time, detail []byte) bool {
		err := to.Log(ctx, begin, end, detail)
		errs.Append(err)
		return true
	})

	from.VisitStats(ctx, time.Time{}, time.Now(),
		func(ctx context.Context, when time.Time, detail []byte) bool {
			err := to.SaveStats(ctx, when, detail)
			errs.Append(err)
			return true
		})

	return errs.Err()
}

func (wro *WriteOnlyDB) LogError(ctx context.Context, key string, when time.Time, detail []byte) error {
	return wro.wrdb.LogError(ctx, key, when, detail)
}

func (wro *WriteOnlyDB) LogAndClose(ctx context.Context, start, stop time.Time, detail []byte) error {
	if err := wro.wrdb.Log(ctx, start, stop, detail); err != nil {
		return err
	}
	if err := wro.wrdb.Close(ctx); err != nil {
		return err
	}
	if wro.inplace {
		return nil
	}
	if err := wro.rddb.Close(ctx); err != nil {
		return err
	}
	os.Rename(wro.from, wro.from+".bak")
	return os.Rename(wro.to, wro.from)
}

func (wro *WriteOnlyDB) DeletePrefix(ctx context.Context, prefix string) error {
	return wro.wrdb.DeletePrefix(ctx, prefix)
}

func (wro *WriteOnlyDB) DeleteErrors(ctx context.Context, prefix string) error {
	return wro.wrdb.DeleteErrors(ctx, prefix)
}

func (wro *WriteOnlyDB) GetPrefixInfo(ctx context.Context, key string, pi *prefixinfo.T) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	db := wro.rddb
	if wro.inplace {
		db = wro.wrdb
	}
	if err := db.GetBuf(ctx, key, buf); err != nil {
		return false, err
	}
	if len(buf.Bytes()) == 0 {
		// Key does not exist or is a bucket, which should never happen here.
		return false, nil
	}
	return true, pi.UnmarshalBinary(buf.Bytes())
}

func (wro *WriteOnlyDB) SetPrefixInfo(ctx context.Context, key string, unchanged bool, pi *prefixinfo.T) error {
	if wro.inplace && unchanged {
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
	return wro.wrdb.SetBatch(ctx, key, buf.Bytes())
}

func (wro *WriteOnlyDB) Close(ctx context.Context) error {
	return wro.wrdb.Close(ctx)
}

var bufPool = sync.Pool{
	New: func() any {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return new(bytes.Buffer)
	},
}
