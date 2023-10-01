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
	boltOpt       bolt.Options
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
	bucketPaths      = "__paths__"
	bucketLog        = "__log__"
	bucketErrorsWhen = "__errors_when__"
	bucketErrorsKey  = "__errors_key__"
	bucketStats      = "__stats__"
	bucketUsers      = "__users__"
	bucketGroups     = "__groups__"

	nestedBuckets = []string{bucketPaths, bucketErrorsKey, bucketErrorsWhen, bucketStats, bucketUsers, bucketGroups, bucketLog}
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

// Close closes the database.
func (db *Database) Close(ctx context.Context) error {
	return db.bdb.Close()
}

// Bolt exposes the encapsulated bolt.DB.
func (db *Database) Bolt() *bolt.DB {
	return db.bdb
}

func (db *Database) Set(_ context.Context, key string, info []byte) error {
	return db.bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(db.prefix)).Bucket([]byte(bucketPaths)).Put([]byte(key), info)
	})
}

func (db *Database) Get(_ context.Context, key string) ([]byte, error) {
	var info []byte
	err := db.bdb.View(func(tx *bolt.Tx) error {
		pb := tx.Bucket([]byte(db.prefix)).Bucket([]byte(bucketPaths))
		if v := pb.Get([]byte(key)); v != nil {
			info = slices.Clone(v)
		}
		return nil
	})
	return info, err
}

func (db *Database) SetBatch(_ context.Context, key string, info []byte) error {
	return db.bdb.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(db.prefix)).Bucket([]byte(bucketPaths)).Put([]byte(key), info)
	})
}

func (db *Database) Delete(_ context.Context, keys ...string) error {
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

func (db *Database) DeletePrefix(_ context.Context, prefix string) error {
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

type logPayload struct {
	Start, Stop time.Time
	Payload     []byte
}

func (db *Database) LogAndClose(ctx context.Context, start, stop time.Time, detail []byte) error {
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

func (db *Database) LastLog(_ context.Context) (start, stop time.Time, detail []byte, err error) {
	err = db.bdb.View(func(tx *bolt.Tx) error {
		logs := tx.Bucket([]byte(db.prefix)).Bucket([]byte(bucketLog))
		c := logs.Cursor()
		_, v := c.Last()
		dec := gob.NewDecoder(bytes.NewReader(v))
		var pl logPayload
		dec.Decode(&pl)
		start = pl.Start
		stop = pl.Stop
		detail = make([]byte, len(pl.Payload))
		copy(detail, pl.Payload)
		return nil
	})
	return
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

func (db *Database) VisitLogs(ctx context.Context, start, stop time.Time, visitor func(ctx context.Context, begin, end time.Time, detail []byte) bool) error {
	startKey := start.Format(time.RFC3339)
	stopKey := []byte(stop.Format(time.RFC3339))
	return db.bdb.View(func(tx *bolt.Tx) error {
		cursor, k, v, err := db.initScan(tx, bucketLog, startKey)
		if err != nil {
			return err
		}
		for ; k != nil && bytes.Compare(k, stopKey) <= 0; k, v = cursor.Next() {
			dec := gob.NewDecoder(bytes.NewReader(v))
			var pl logPayload
			dec.Decode(&pl)
			if !visitor(ctx, pl.Start, pl.Stop, pl.Payload) {
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

func (db *Database) Scan(ctx context.Context, path string, visitor func(ctx context.Context, key string, val []byte) bool) error {
	return db.bdb.View(func(tx *bolt.Tx) error {
		cursor, k, v, err := db.initScan(tx, bucketPaths, path)
		if err != nil {
			return err
		}
		fmt.Printf("PS: %s\n", k)
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

func (db *Database) LogError(ctx context.Context, when time.Time, key string, detail []byte) error {
	tx, err := db.bdb.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

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
	enc.Encode(pl)
	timeKey := when.Format(time.RFC3339)
	if err := pb.Bucket([]byte(bucketErrorsWhen)).Put([]byte(timeKey), buf.Bytes()); err != nil {
		return err
	}
	if err := pb.Bucket([]byte(bucketErrorsKey)).Put([]byte(key), buf.Bytes()); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *Database) VisitErrorsWhen(ctx context.Context, start, stop time.Time,
	visitor func(ctx context.Context, when time.Time, key string, detail []byte) bool) error {
	startKey := start.Format(time.RFC3339)
	stopKey := []byte(stop.Format(time.RFC3339))
	return db.bdb.View(func(tx *bolt.Tx) error {
		cursor, k, v, err := db.initScan(tx, bucketErrorsWhen, startKey)
		if err != nil {
			return err
		}
		for ; k != nil && bytes.Compare(k, stopKey) <= 0; k, v = cursor.Next() {
			dec := gob.NewDecoder(bytes.NewReader(v))
			var pl errorPayload
			dec.Decode(&pl)
			if !visitor(ctx, pl.When, pl.Key, pl.Payload) {
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

func (db *Database) VisitErrorsKey(ctx context.Context, key string,
	visitor func(ctx context.Context, when time.Time, key string, detail []byte) bool) error {
	return db.bdb.View(func(tx *bolt.Tx) error {
		cursor, k, v, err := db.initScan(tx, bucketErrorsKey, key)
		if err != nil {
			return err
		}
		for ; k != nil; k, v = cursor.Next() {
			dec := gob.NewDecoder(bytes.NewReader(v))
			var pl errorPayload
			dec.Decode(&pl)
			if !visitor(ctx, pl.When, pl.Key, pl.Payload) {
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

func (db *Database) Clear(_ context.Context, logs, errors bool) error {
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
		if err := db.clearBucket(pb, bucketErrorsKey); err != nil {
			return err
		}
		if err := db.clearBucket(pb, bucketErrorsWhen); err != nil {
			return err
		}
	}

	return tx.Commit()
}
