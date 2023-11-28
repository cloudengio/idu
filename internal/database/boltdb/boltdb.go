// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boltdb

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/database/types"
	bolt "go.etcd.io/bbolt"
)

// Option represents a specific option accepted by Open.
type Option func(o *database.Options[Options])

var ReadOnly = database.ReadOnly[Options]
var WithTimeout = database.WithTimeout[Options]
var BatchDelay = func(d time.Duration) Option {
	return func(o *database.Options[Options]) {
		o.Sub.batchDelaySet = true
		o.Sub.batchDelay = d
	}
}

type Options struct {
	batchDelaySet bool
	batchDelay    time.Duration
}

// Database represents a bolt database.
type Database struct {
	database.Options[Options]
	filename string
	bdb      *bolt.DB
}

var (
	bucketPaths  = "__paths__"
	bucketLog    = "__log__"
	bucketErrors = "__errors_"
	bucketStats  = "__stats__"

	nestedBuckets = []string{bucketPaths, bucketErrors, bucketStats, bucketLog}
)

func initBuckets(bdb *bolt.DB) error {
	bdb.Update(func(tx *bolt.Tx) error {
		for _, nested := range nestedBuckets {
			if _, err := tx.CreateBucketIfNotExists([]byte(nested)); err != nil {
				return err
			}
		}
		return nil
	})
	return nil
}

// Open opens the specified database. If the database does not exist it will
// be created.
func Open[T Options](location string, opts ...Option) (database.DB, error) {
	dir := filepath.Dir(location)
	if len(dir) > 0 && dir != "." {
		os.MkdirAll(dir, 0770)
	}
	db := &Database{
		filename: location,
	}
	for _, fn := range opts {
		fn(&db.Options)
	}
	bopts := bolt.Options{}
	if db.Timeout > 0 {
		bopts.Timeout = db.Timeout
	}
	var mode fs.FileMode = 0600
	if db.ReadOnly {
		mode = 0400
		bopts.ReadOnly = true
	}
	bopts.NoFreelistSync = false
	bopts.FreelistType = bolt.FreelistMapType
	var bdb *bolt.DB
	var err error
	bdb, err = bolt.Open(db.filename, mode, &bopts)
	if err != nil {
		return nil, err
	}

	if db.Sub.batchDelaySet {
		bdb.MaxBatchDelay = db.Sub.batchDelay
	}

	if !db.ReadOnly {
		if err := initBuckets(bdb); err != nil {
			return nil, err
		}
	}
	db.bdb = bdb
	return db, nil
}

func (db *Database) canceled(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Close closes the database.
func (db *Database) Close(ctx context.Context) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	return db.bdb.Close()
}

// Bolt exposes the encapsulated bolt.DB.
func (db *Database) Bolt() *bolt.DB {
	return db.bdb
}

func (db *Database) set(ctx context.Context, bucket, key string, info []byte) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	return db.bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket)).Put([]byte(key), info)
	})
}

func (db *Database) get(ctx context.Context, bucket, key string, buf *bytes.Buffer) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	err := db.bdb.View(func(tx *bolt.Tx) error {
		pb := tx.Bucket([]byte(bucket))
		if v := pb.Get([]byte(key)); v != nil {
			buf.Grow(len(v))
			buf.Write(v)
		}
		return nil
	})
	return err
}

func (db *Database) Set(ctx context.Context, key string, info []byte) error {
	return db.set(ctx, bucketPaths, key, info)
}

func (db *Database) Get(ctx context.Context, key string) ([]byte, error) {
	var buf bytes.Buffer
	err := db.get(ctx, bucketPaths, key, &buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (db *Database) GetBuf(ctx context.Context, key string, buf *bytes.Buffer) error {
	err := db.get(ctx, bucketPaths, key, buf)
	if err != nil {
		return err
	}
	return nil
}

func (db *Database) SetBatch(ctx context.Context, key string, buf []byte) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	return db.bdb.Batch(func(tx *bolt.Tx) error {
		err := tx.Bucket([]byte(bucketPaths)).Put([]byte(key), buf)
		return err
	})
}

func (db *Database) Delete(ctx context.Context, keys ...string) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	return db.bdb.Update(func(tx *bolt.Tx) error {
		paths := tx.Bucket([]byte(bucketPaths))
		for _, key := range keys {
			if err := paths.Delete([]byte(key)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *Database) DeletePrefix(ctx context.Context, prefix string) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	tx, err := db.bdb.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	paths := tx.Bucket([]byte(bucketPaths))
	cursor := paths.Cursor()
	k, _ := cursor.Seek([]byte(prefix))
	for ; k != nil && bytes.HasPrefix(k, []byte(prefix)); k, _ = cursor.Next() {
		paths.Delete(k)
	}
	return tx.Commit()
}

func (db *Database) DeleteErrors(_ context.Context, prefix string) error {
	tx, err := db.bdb.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	errorsBucket := tx.Bucket([]byte(bucketErrors))

	cursor := errorsBucket.Cursor()
	k, _ := cursor.Seek([]byte(prefix))
	for ; k != nil && bytes.HasPrefix(k, []byte(prefix)); k, _ = cursor.Next() {
		errorsBucket.Delete(k)
	}
	return tx.Commit()
}

func (db *Database) SaveStats(ctx context.Context, when time.Time, value []byte) error {
	pl := types.StatsPayload{
		When:    when,
		Payload: value,
	}
	var buf bytes.Buffer
	if err := types.Encode(&buf, pl); err != nil {
		return err
	}
	key := when.Format(time.RFC3339)
	return db.set(ctx, bucketStats, key, buf.Bytes())
}

func (db *Database) LastStats(ctx context.Context) (time.Time, []byte, error) {
	_, v, err := db.getLast(ctx, bucketStats)
	if err != nil {
		return time.Time{}, nil, err
	}
	var pl types.StatsPayload
	if err := types.Decode(v, &pl); err != nil {
		return time.Time{}, nil, err
	}
	return pl.When, pl.Payload, nil
}

func (db *Database) VisitStats(ctx context.Context, start, stop time.Time, visitor func(ctx context.Context, when time.Time, detail []byte) bool) error {
	return db.scanTimeRange(ctx, bucketStats, start, stop, func(ctx context.Context, v []byte) error {
		var pl types.StatsPayload
		if err := types.Decode(v, &pl); err != nil {
			return err
		}
		if !visitor(ctx, pl.When, pl.Payload) {
			return nil
		}
		return nil
	})
}

func (db *Database) Log(ctx context.Context, start, stop time.Time, detail []byte) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	tx, err := db.bdb.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	logs := tx.Bucket([]byte(bucketLog))

	pl := types.LogPayload{
		Start:   start,
		Stop:    stop,
		Payload: detail,
	}

	var buf bytes.Buffer
	if err := types.Encode(&buf, pl); err != nil {
		return err
	}
	key := start.Format(time.RFC3339)
	if err := logs.Put([]byte(key), buf.Bytes()); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *Database) initScan(tx *bolt.Tx, bucket, start string) (cursor *bolt.Cursor, k, v []byte, err error) {
	cursor = tx.Bucket([]byte(bucket)).Cursor()
	if len(start) == 0 {
		k, v = cursor.First()
	} else {
		k, v = cursor.Seek([]byte(start))
	}
	return
}

func (db *Database) Scan(ctx context.Context, path string, visitor func(ctx context.Context, key string, val []byte) bool) error {
	return db.bdb.View(func(tx *bolt.Tx) error {
		cursor, k, v, err := db.initScan(tx, bucketPaths, path)
		if err != nil {
			return err
		}
		for ; k != nil; k, v = cursor.Next() {
			if !visitor(ctx, string(k), v) {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	})
}
func (db *Database) Stream(ctx context.Context, prefix string, visitor func(ctx context.Context, key string, val []byte)) error {
	p := []byte(prefix)
	return db.bdb.View(func(tx *bolt.Tx) error {
		cursor, k, v, err := db.initScan(tx, bucketPaths, prefix)
		if err != nil {
			return err
		}
		for ; k != nil && bytes.HasPrefix(k, p); k, v = cursor.Next() {
			visitor(ctx, string(k), v)
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	})
}

func (db *Database) getLast(_ context.Context, bucket string) (key, val []byte, err error) {
	err = db.bdb.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(bucket)).Cursor()
		k, v := c.Last()
		key = make([]byte, len(k))
		copy(key, k)
		val = make([]byte, len(v))
		copy(val, v)
		return nil
	})
	return
}

func (db *Database) scanTimeRange(ctx context.Context, bucket string, start, stop time.Time, visitor func(ctx context.Context, payload []byte) error) error {
	startKey := start.Format(time.RFC3339)
	stopKey := []byte(stop.Format(time.RFC3339))
	return db.bdb.View(func(tx *bolt.Tx) error {
		cursor, k, v, err := db.initScan(tx, bucket, startKey)
		if err != nil {
			return err
		}
		for ; k != nil && bytes.Compare(k, stopKey) <= 0; k, v = cursor.Next() {

			if err := visitor(ctx, v); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	})
}

func (db *Database) LastLog(ctx context.Context) (start, stop time.Time, detail []byte, err error) {
	_, v, err := db.getLast(ctx, bucketLog)
	if err != nil {
		return
	}
	var pl types.LogPayload
	if err := types.Decode(v, &pl); err != nil {
		return time.Time{}, time.Time{}, nil, err
	}
	start = pl.Start
	stop = pl.Stop
	detail = pl.Payload
	return
}

func (db *Database) VisitLogs(ctx context.Context, start, stop time.Time, visitor func(ctx context.Context, begin, end time.Time, detail []byte) bool) error {
	return db.scanTimeRange(ctx, bucketLog, start, stop, func(ctx context.Context, payload []byte) error {
		var pl types.LogPayload
		if err := types.Decode(payload, &pl); err != nil {
			return err
		}
		if !visitor(ctx, pl.Start, pl.Stop, pl.Payload) {
			return nil
		}
		return nil
	})
}

func (db *Database) LogError(ctx context.Context, key string, when time.Time, detail []byte) error {
	pl := types.ErrorPayload{
		When:    when,
		Key:     key,
		Payload: detail,
	}
	var buf bytes.Buffer
	if err := types.Encode(&buf, pl); err != nil {
		return err
	}
	return db.bdb.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucketErrors)).Put([]byte(key), buf.Bytes())
	})
}

func (db *Database) VisitErrors(ctx context.Context, key string,
	visitor func(ctx context.Context, key string, when time.Time, detail []byte) bool) error {
	return db.bdb.View(func(tx *bolt.Tx) error {
		cursor, k, v, err := db.initScan(tx, bucketErrors, key)
		if err != nil {
			return err
		}
		for ; k != nil; k, v = cursor.Next() {
			var pl types.ErrorPayload
			if err := types.Decode(v, &pl); err != nil {
				return err
			}
			if !visitor(ctx, pl.Key, pl.When, pl.Payload) {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return nil
	})
}

func (db *Database) clearBucket(tx *bolt.Tx, bucket string) error {
	if err := tx.DeleteBucket([]byte(bucket)); err != nil {
		return err
	}
	_, err := tx.CreateBucket([]byte(bucket))
	return err
}

func (db *Database) Clear(_ context.Context, logs, errors, stats bool) error {
	tx, err := db.bdb.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if logs {
		if err := db.clearBucket(tx, bucketLog); err != nil {
			return err
		}
	}

	if errors {
		if err := db.clearBucket(tx, bucketErrors); err != nil {
			return err
		}
	}

	if stats {
		if err := db.clearBucket(tx, bucketStats); err != nil {
			return err
		}
	}

	return tx.Commit()
}
