// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package badgerdb

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/database/types"
	"cloudeng.io/errors"
	"cloudeng.io/os/lockedfile"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/z"
)

// Option represents a specific option accepted by Open.
type Option func(o *database.Options[Options])

var ReadOnly = database.ReadOnly[Options]

// WithBadgerOptions specifies the options to be used when opening the
// database. Note, that it overrides all other badger specific options when
// used.
func WithBadgerOptions(opts badger.Options) Option {
	return func(o *database.Options[Options]) {
		o.Sub.Options = opts
	}
}

type Options struct {
	badger.Options
}

// Database represents a badger database.
type Database struct {
	database.Options[Options]
	location string
	bdb      *badger.DB
	batch    *writeBatch
	lock     *lockedfile.Mutex
	unlockMu sync.Mutex
	unlock   func()
}

var (
	logPrefix   = "__log__"
	errorPrefix = "__errors_"
	statsPrefix = "__stats__"
)

func isBucket(key []byte) bool {
	if key[0] != '_' || key[1] != '_' {
		return false
	}
	if bytes.HasPrefix(key, []byte(logPrefix)) {
		return true
	}
	if bytes.HasPrefix(key, []byte(errorPrefix)) {
		return true
	}
	if bytes.HasPrefix(key, []byte(statsPrefix)) {
		return true
	}
	return false
}

func keyForBucket(prefix, key string) []byte {
	return []byte(prefix + key)
}

// Open opens the specified database. If the database does not exist it will
// be created.
func Open[T Options](location string, opts ...Option) (database.DB, error) {
	if len(location) > 0 && location != "." {
		os.MkdirAll(location, 0700)
	}
	db := &Database{
		location: location,
	}
	db.Options.Sub.Options = badger.DefaultOptions(location)
	for _, fn := range opts {
		fn(&db.Options)
	}
	db.Options.Sub.Options = db.Options.Sub.Options.WithReadOnly(db.Options.ReadOnly)

	lockfile := filepath.Join(location, "lock")
	db.lock = lockedfile.MutexAt(lockfile)
	var unlock func()
	var err error
	if db.Options.ReadOnly {
		unlock, err = db.lock.RLockCreate()
	} else {
		unlock, err = db.lock.Lock()
	}
	if err != nil {
		return nil, err
	}
	db.unlock = unlock
	bdb, err := badger.Open(db.Options.Sub.Options)
	if err != nil {
		return nil, err
	}
	db.bdb = bdb
	db.batch = newWriteBatch(bdb)
	return db, nil
}

func (db *Database) BadgerDB() *badger.DB {
	return db.bdb
}

func (db *Database) canceled(ctx context.Context) error {
	select {
	case <-ctx.Done():
		db.batch.flush()
		return ctx.Err()
	default:
		return nil
	}
}

func (db *Database) set(ctx context.Context, key, val []byte) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	return db.bdb.Update(func(tx *badger.Txn) error {
		return tx.Set(key, val)
	})
}

func (db *Database) get(ctx context.Context, key string, buf *bytes.Buffer) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	err := db.bdb.View(func(tx *badger.Txn) error {
		item, err := tx.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		return item.Value(func(val []byte) error {
			buf.Grow(len(val))
			buf.Write(val)
			return nil
		})
	})
	return err
}

func (db *Database) Set(ctx context.Context, key string, val []byte) error {
	return db.set(ctx, []byte(key), val)
}

func (db *Database) Get(ctx context.Context, key string) ([]byte, error) {
	var buf bytes.Buffer
	err := db.get(ctx, key, &buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (db *Database) GetBuf(ctx context.Context, key string, buf *bytes.Buffer) error {
	err := db.get(ctx, key, buf)
	if err != nil {
		return err
	}
	return nil
}

func (db *Database) SetBatch(ctx context.Context, key string, buf []byte) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	return db.batch.set([]byte(key), buf)
}

func (db *Database) Delete(ctx context.Context, keys ...string) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	return db.bdb.Update(func(tx *badger.Txn) error {
		for _, key := range keys {
			if err := tx.Delete([]byte(key)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *Database) DeletePrefix(ctx context.Context, prefix string) error {
	return db.deletePrefix(ctx, []byte(prefix))
}

func (db *Database) deletePrefix(ctx context.Context, prefix []byte) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	tx := db.bdb.NewTransaction(true)
	defer tx.Discard()
	it := tx.NewIterator(badger.DefaultIteratorOptions)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if err := tx.Delete(it.Item().KeyCopy(nil)); err != nil {
			return err
		}
	}
	it.Close()
	return tx.Commit()
}

func (db *Database) DeleteErrors(ctx context.Context, prefix string) error {
	return db.deletePrefix(ctx, keyForBucket(errorPrefix, prefix))
}

var errScanDone = errors.New("scan done")

func (db *Database) scanFrom(ctx context.Context, prefix []byte, visitor func(ctx context.Context, key string, val []byte) error) error {
	return db.bdb.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		if len(prefix) > 0 {
			it.Seek(prefix)
		} else {
			it.Rewind()
		}
		for it.Seek(prefix); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				return visitor(ctx, string(k), v)
			})
			if err != nil {
				if err == errScanDone {
					break
				}
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

func (db *Database) scanTimeRange(ctx context.Context, bucket string, start, stop time.Time, visitor func(ctx context.Context, key string, val []byte) error) error {
	startKey := keyForBucket(bucket, start.Format(time.RFC3339))
	stopKey := keyForBucket(bucket, stop.Format(time.RFC3339))
	return db.bdb.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(startKey); it.Valid(); it.Next() {
			k := it.Item().Key()
			if bytes.Compare(k, stopKey) > 0 {
				break
			}
			item := it.Item()
			err := item.Value(func(v []byte) error {
				return visitor(ctx, string(k), v)
			})
			if err != nil {
				if err == errScanDone {
					break
				}
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

func (db *Database) Scan(ctx context.Context, path string, visitor func(ctx context.Context, key string, val []byte) bool) error {
	return db.scanFrom(ctx, []byte(path), func(ctx context.Context, key string, val []byte) error {
		if isBucket([]byte(key)) {
			return nil
		}
		if !visitor(ctx, key, val) {
			return errScanDone
		}
		return nil
	})
}

func (db *Database) Stream(ctx context.Context, path string, visitor func(ctx context.Context, key string, val []byte)) error {
	stream := db.bdb.NewStream()
	stream.Prefix = []byte(path)
	stream.ChooseKey = func(item *badger.Item) bool {
		return !isBucket(item.Key())
	}
	stream.KeyToList = nil
	stream.Send = func(buf *z.Buffer) error {
		list, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}
		for _, kv := range list.Kv {
			if kv.StreamDone {
				return nil
			}
			visitor(ctx, string(kv.Key), kv.Value)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		return nil
	}
	return stream.Orchestrate(ctx)
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
	dbkey := keyForBucket(errorPrefix, key)
	return db.set(ctx, dbkey, buf.Bytes())
}

func (db *Database) VisitErrors(ctx context.Context, key string,
	visitor func(ctx context.Context, key string, when time.Time, detail []byte) bool) error {
	dbkey := keyForBucket(errorPrefix, key)
	return db.scanFrom(ctx, dbkey, func(ctx context.Context, key string, val []byte) error {
		if !strings.HasPrefix(key, errorPrefix) {
			return errScanDone
		}
		var pl types.ErrorPayload
		if err := types.Decode(val, &pl); err != nil {
			return err
		}
		if !visitor(ctx, pl.Key, pl.When, pl.Payload) {
			return errScanDone
		}
		return nil
	})
}

func (db *Database) lastKey(prefix string) (string, error) {
	var lastKey []byte
	p := []byte(prefix)
	err := db.bdb.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		var l []byte
		for it.Seek(p); it.ValidForPrefix(p); it.Next() {
			l = it.Item().Key()
		}
		lastKey = make([]byte, len(l))
		copy(lastKey, l)
		return nil
	})
	return string(lastKey), err
}

func (db *Database) Log(ctx context.Context, start, stop time.Time, detail []byte) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	pl := types.LogPayload{
		Start:   start,
		Stop:    stop,
		Payload: detail,
	}
	var buf bytes.Buffer
	if err := types.Encode(&buf, pl); err != nil {
		return err
	}
	key := keyForBucket(logPrefix, start.Format(time.RFC3339))
	return db.set(ctx, key, buf.Bytes())
}

func (db *Database) LastLog(ctx context.Context) (start, stop time.Time, detail []byte, err error) {
	lk, err := db.lastKey(logPrefix)
	if err != nil {
		return time.Time{}, time.Time{}, nil, err
	}
	var buf bytes.Buffer
	if err := db.get(ctx, lk, &buf); err != nil {
		return time.Time{}, time.Time{}, nil, err
	}
	var pl types.LogPayload
	if err := types.Decode(buf.Bytes(), &pl); err != nil {
		return time.Time{}, time.Time{}, nil, err
	}
	start = pl.Start
	stop = pl.Stop
	detail = pl.Payload
	return
}

func (db *Database) VisitLogs(ctx context.Context, start, stop time.Time, visitor func(ctx context.Context, begin, end time.Time, detail []byte) bool) error {
	return db.scanTimeRange(ctx, logPrefix, start, stop, func(ctx context.Context, key string, val []byte) error {
		if !strings.HasPrefix(key, logPrefix) {
			return errScanDone
		}
		var pl types.LogPayload
		if err := types.Decode(val, &pl); err != nil {
			return err
		}
		if !visitor(ctx, pl.Start, pl.Stop, pl.Payload) {
			return errScanDone
		}
		return nil
	})
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
	key := keyForBucket(statsPrefix, when.Format(time.RFC3339))
	return db.set(ctx, key, buf.Bytes())
}

func (db *Database) LastStats(ctx context.Context) (time.Time, []byte, error) {
	lk, err := db.lastKey(statsPrefix)
	if err != nil {
		return time.Time{}, nil, err
	}
	var buf bytes.Buffer
	if err := db.get(ctx, lk, &buf); err != nil {
		return time.Time{}, nil, err
	}
	var pl types.StatsPayload
	if err := types.Decode(buf.Bytes(), &pl); err != nil {
		return time.Time{}, nil, err
	}
	return pl.When, pl.Payload, nil
}

func (db *Database) VisitStats(ctx context.Context, start, stop time.Time, visitor func(ctx context.Context, when time.Time, detail []byte) bool) error {
	return db.scanTimeRange(ctx, statsPrefix, start, stop, func(ctx context.Context, key string, val []byte) error {
		if !strings.HasPrefix(key, statsPrefix) {
			return errScanDone
		}
		var pl types.StatsPayload
		if err := types.Decode(val, &pl); err != nil {
			return err
		}
		if !visitor(ctx, pl.When, pl.Payload) {
			return errScanDone
		}
		return nil
	})
}

// Close closes the database.
func (db *Database) Close(ctx context.Context) error {
	db.unlockMu.Lock()
	defer db.unlockMu.Unlock()
	if db.unlock == nil {
		return nil
	}
	var errs errors.M
	errs.Append(db.batch.flush())
	errs.Append(db.bdb.Close())
	db.unlock()
	db.unlock = nil
	return errs.Err()
}

func (db *Database) Clear(_ context.Context, logs, errors, stats bool) error {
	if logs {
		if err := db.bdb.DropPrefix([]byte(logPrefix)); err != nil {
			return err
		}
	}

	if errors {
		if err := db.bdb.DropPrefix([]byte(errorPrefix)); err != nil {
			return err
		}
	}

	if stats {
		if err := db.bdb.DropPrefix([]byte(statsPrefix)); err != nil {
			return err
		}
	}

	return nil
}
