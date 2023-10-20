// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package boltdb

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"cloudeng.io/cmd/idu/internal/database"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/exp/slices"
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
	prefix   string
	bdb      *bolt.DB
}

var (
	bucketPaths  = "__paths__"
	bucketLog    = "__log__"
	bucketErrors = "__errors_"
	bucketStats  = "__stats__"
	bucketUsers  = "__users__"
	bucketGroups = "__groups__"

	nestedBuckets = []string{bucketPaths, bucketErrors, bucketStats, bucketUsers, bucketGroups, bucketLog}
)

func initBuckets(bdb *bolt.DB, prefix string) error {
	bdb.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(prefix))
		if err != nil {
			return err
		}
		for _, nested := range nestedBuckets {
			if _, err = bucket.CreateBucketIfNotExists([]byte(nested)); err != nil {
				return err
			}
		}
		return nil
	})
	return nil
}

// Open opens the specified database. If the database does not exist it will
// be created.
func Open[T Options](location, prefix string, opts ...Option) (database.DB, error) {
	dir := filepath.Dir(location)
	if len(dir) > 0 && dir != "." {
		os.MkdirAll(dir, 0700)
	}
	db := &Database{
		filename: location,
		prefix:   prefix,
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
	bopts.NoFreelistSync = true
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
		if err := initBuckets(bdb, prefix); err != nil {
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
		return tx.Bucket([]byte(db.prefix)).Bucket([]byte(bucket)).Put([]byte(key), info)
	})
}

func (db *Database) get(ctx context.Context, bucket, key string) ([]byte, error) {
	if err := db.canceled(ctx); err != nil {
		return nil, err
	}
	var info []byte
	err := db.bdb.View(func(tx *bolt.Tx) error {
		pb := tx.Bucket([]byte(db.prefix)).Bucket([]byte(bucket))
		if v := pb.Get([]byte(key)); v != nil {
			info = slices.Clone(v)
		}
		return nil
	})
	return info, err
}

func (db *Database) Set(ctx context.Context, key string, info []byte) error {
	return db.set(ctx, bucketPaths, key, info)
}

func (db *Database) Get(ctx context.Context, key string) ([]byte, error) {
	return db.get(ctx, bucketPaths, key)
}

func (db *Database) SetBatch(ctx context.Context, key string, info []byte) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	return db.bdb.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(db.prefix)).Bucket([]byte(bucketPaths)).Put([]byte(key), info)
	})
}

func (db *Database) Delete(ctx context.Context, keys ...string) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	return db.bdb.Update(func(tx *bolt.Tx) error {
		pb := tx.Bucket([]byte(db.prefix))
		if pb == nil {
			return fmt.Errorf("no bucket for prefix: %v", db.prefix)
		}
		paths := pb.Bucket([]byte(bucketPaths))
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
	pb := tx.Bucket([]byte(db.prefix))
	if pb == nil {
		return fmt.Errorf("no bucket for prefix: %v", db.prefix)
	}
	paths := pb.Bucket([]byte(bucketPaths))
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
	pb := tx.Bucket([]byte(db.prefix))
	if pb == nil {
		return fmt.Errorf("no bucket for prefix: %v", db.prefix)
	}
	errorsBucket := pb.Bucket([]byte(bucketErrors))

	cursor := errorsBucket.Cursor()
	k, _ := cursor.Seek([]byte(prefix))
	for ; k != nil && bytes.HasPrefix(k, []byte(prefix)); k, _ = cursor.Next() {
		errorsBucket.Delete(k)
	}
	return tx.Commit()
}

type statsPayload struct {
	When    time.Time
	Payload []byte
}

func (db *Database) SaveStats(ctx context.Context, when time.Time, value []byte) error {
	pl := statsPayload{
		When:    when,
		Payload: value,
	}
	key := pl.When.Format(time.RFC3339)
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(pl); err != nil {
		return err
	}
	return db.set(ctx, bucketStats, key, buf.Bytes())
}

func (db *Database) LastStats(ctx context.Context) (time.Time, []byte, error) {
	_, v, err := db.getLast(ctx, bucketStats)
	if err != nil {
		return time.Time{}, nil, err
	}
	dec := gob.NewDecoder(bytes.NewReader(v))
	var pl statsPayload
	if err := dec.Decode(&pl); err != nil {
		return time.Time{}, nil, err
	}
	return pl.When, pl.Payload, nil
}

func (db *Database) VisitStats(ctx context.Context, start, stop time.Time, visitor func(ctx context.Context, when time.Time, detail []byte) bool) error {
	return db.scanTimeRange(ctx, bucketStats, start, stop, func(ctx context.Context, payload []byte) error {
		dec := gob.NewDecoder(bytes.NewReader(payload))
		var pl statsPayload
		if err := dec.Decode(&pl); err != nil {
			return err
		}
		if !visitor(ctx, pl.When, pl.Payload) {
			return nil
		}
		return nil
	})
}

type logPayload struct {
	Start, Stop time.Time
	Payload     []byte
}

func (db *Database) LogAndClose(ctx context.Context, start, stop time.Time, detail []byte) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	tx, err := db.bdb.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pb := tx.Bucket([]byte(db.prefix))
	if pb == nil {
		return fmt.Errorf("no bucket for prefix: %v", db.prefix)
	}
	logs := pb.Bucket([]byte(bucketLog))

	pl := logPayload{
		Start:   start,
		Stop:    stop,
		Payload: detail,
	}

	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	enc.Encode(pl)

	key := start.Format(time.RFC3339)
	if err := logs.Put([]byte(key), buf.Bytes()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	err = db.bdb.Close()
	db.bdb = nil
	return err
}

func (db *Database) initScan(tx *bolt.Tx, bucket, start string) (cursor *bolt.Cursor, k, v []byte, err error) {
	pb := tx.Bucket([]byte(db.prefix))
	if pb == nil {
		return nil, nil, nil, fmt.Errorf("no bucket for prefix: %v", db.prefix)
	}
	cursor = pb.Bucket([]byte(bucket)).Cursor()
	if len(start) == 0 {
		k, v = cursor.First()
	} else {
		k, v = cursor.Seek([]byte(start))
	}
	return
}

func (db *Database) getLast(_ context.Context, bucket string) (key, val []byte, err error) {
	err = db.bdb.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(db.prefix)).Bucket([]byte(bucket)).Cursor()
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
	dec := gob.NewDecoder(bytes.NewReader(v))
	var pl logPayload
	if err := dec.Decode(&pl); err != nil {
		return time.Time{}, time.Time{}, nil, err
	}
	start = pl.Start
	stop = pl.Stop
	detail = pl.Payload
	return
}

func (db *Database) VisitLogs(ctx context.Context, start, stop time.Time, visitor func(ctx context.Context, begin, end time.Time, detail []byte) bool) error {
	return db.scanTimeRange(ctx, bucketLog, start, stop, func(ctx context.Context, payload []byte) error {
		dec := gob.NewDecoder(bytes.NewReader(payload))
		var pl logPayload
		if err := dec.Decode(&pl); err != nil {
			return err
		}
		if !visitor(ctx, pl.Start, pl.Stop, pl.Payload) {
			return nil
		}
		return nil
	})
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

type errorPayload struct {
	When    time.Time
	Key     string
	Payload []byte
}

func (db *Database) LogError(ctx context.Context, key string, when time.Time, detail []byte) error {
	return db.bdb.Batch(func(tx *bolt.Tx) error {
		pb := tx.Bucket([]byte(db.prefix))
		if pb == nil {
			return fmt.Errorf("no bucket for prefix: %v", db.prefix)
		}
		pl := errorPayload{
			When:    when,
			Key:     key,
			Payload: detail,
		}
		buf := &bytes.Buffer{}
		enc := gob.NewEncoder(buf)
		if err := enc.Encode(pl); err != nil {
			return err
		}
		return pb.Bucket([]byte(bucketErrors)).Put([]byte(key), buf.Bytes())
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
			dec := gob.NewDecoder(bytes.NewReader(v))
			var pl errorPayload
			dec.Decode(&pl)
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

func (db *Database) clearBucket(pb *bolt.Bucket, bucket string) error {
	if err := pb.DeleteBucket([]byte(bucket)); err != nil {
		return err
	}
	_, err := pb.CreateBucket([]byte(bucket))
	return err
}

func (db *Database) Clear(_ context.Context, logs, errors, stats bool) error {
	tx, err := db.bdb.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	pb := tx.Bucket([]byte(db.prefix))
	if pb == nil {
		return fmt.Errorf("no bucket for prefix: %v", db.prefix)
	}

	if logs {
		if err := db.clearBucket(pb, bucketLog); err != nil {
			return err
		}
	}

	if errors {
		if err := db.clearBucket(pb, bucketErrors); err != nil {
			return err
		}
	}

	if stats {
		if err := db.clearBucket(pb, bucketStats); err != nil {
			return err
		}
	}

	return tx.Commit()
}
