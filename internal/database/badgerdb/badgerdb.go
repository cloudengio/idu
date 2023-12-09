// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package badgerdb

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
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
var WithTimeout = database.WithTimeout[Options]

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

// The database is paritioned into 4 'buckets':
// 1. inode bucket, keyed by inode and device numbers. This is by far
//    the largest since it has an entry for every file.
// 2. the prefix bucket, keyed by prefix. This contains an entry for
//    every prefix.
// 3. the log bucket, keyed by timestamp. This contains an entry for
//    every log entry, ie. iteration of updates of the database.
// 4. the error bucket, keyed by timestamp. This contains an entry for
//    every error encountered in the most recent update of the database.
//
// Keys are assigned to each bucket by prepending an identifying byte
// to the key.

const (
	inodeBucket  = 0xf0
	prefixBucket = 0xf1
	logBucket    = 0xf2
	errorBucket  = 0xf3
)

var bufPool = sync.Pool{
	New: func() any {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return new(bytes.Buffer)
	},
}

func keyForBucket(bucket byte, key []byte) *bytes.Buffer {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteByte(bucket)
	buf.Write(key)
	return buf
}

// Open opens the specified database. If the database does not exist it will
// be created.
func Open[T Options](location string, opts ...Option) (database.DB, error) {
	if len(location) > 0 && location != "." {
		os.MkdirAll(location, 0770)
	}
	db := &Database{
		location: location,
	}
	db.Options.Sub.Options = badger.DefaultOptions(location)
	for _, fn := range opts {
		fn(&db.Options)
	}
	db.Options.Sub.Options = db.Options.Sub.Options.WithReadOnly(db.Options.ReadOnly)

	lockfile := filepath.Join(location, "applock")
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
	osopts := osOptions(db.Options.Sub.Options)
	bdb, err := badger.Open(osopts)
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

func (db *Database) get(ctx context.Context, key []byte, buf *bytes.Buffer) error {
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

func (db *Database) Set(ctx context.Context, prefix string, val []byte, batch bool) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	kb := keyForBucket(prefixBucket, []byte(prefix))
	defer bufPool.Put(kb)
	if batch {
		return db.batch.set(kb.Bytes(), val)
	}
	return db.set(ctx, kb.Bytes(), val)
}

func (db *Database) Get(ctx context.Context, prefix string, buf *bytes.Buffer) error {
	kb := keyForBucket(prefixBucket, []byte(prefix))
	defer bufPool.Put(kb)
	err := db.get(ctx, kb.Bytes(), buf)
	if err != nil {
		return err
	}
	return nil
}

/*
func (db *Database) Delete(ctx context.Context, keys ...string) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	return db.bdb.Update(func(tx *badger.Txn) error {
		kb := bufPool.Get().(*bytes.Buffer)
		defer bufPool.Put(kb)
		for _, key := range keys {
			kb.Reset()
			kb.WriteByte(prefixBucket)
			kb.WriteString(key)
			if err := tx.Delete(kb.Bytes()); err != nil {
				return err
			}
		}
		return nil
	})
}*/

func (db *Database) DeletePrefix(ctx context.Context, prefix string) error {
	kb := keyForBucket(prefixBucket, []byte(prefix))
	defer bufPool.Put(kb)
	return db.deletePrefix(ctx, kb.Bytes())
}

func (db *Database) deleteBatch(prefix []byte) (bool, error) {
	tx := db.bdb.NewTransaction(true)
	defer tx.Discard()
	it := tx.NewIterator(badger.DefaultIteratorOptions)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if err := tx.Delete(it.Item().KeyCopy(nil)); err != nil {
			it.Close()
			if err == badger.ErrTxnTooBig {
				return false, tx.Commit()
			}
			return true, err
		}
	}
	it.Close()
	return true, tx.Commit()
}

func (db *Database) deletePrefix(ctx context.Context, prefix []byte) error {
	for {
		if err := db.canceled(ctx); err != nil {
			return err
		}
		done, err := db.deleteBatch(prefix)
		if done || err != nil {
			return err
		}
	}
}

func (db *Database) DeleteErrors(ctx context.Context, prefix string) error {
	kb := keyForBucket(errorBucket, []byte(prefix))
	defer bufPool.Put(kb)
	return db.deletePrefix(ctx, kb.Bytes())
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

func (db *Database) scanTimeRange(ctx context.Context, bucket byte, start, stop time.Time, visitor func(ctx context.Context, key string, val []byte) error) error {
	startKb := keyForBucket(bucket, []byte(start.Format(time.RFC3339)))
	defer bufPool.Put(startKb)
	stopKb := keyForBucket(bucket, []byte(stop.Format(time.RFC3339)))
	defer bufPool.Put(stopKb)
	return db.bdb.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(startKb.Bytes()); it.Valid(); it.Next() {
			k := it.Item().Key()
			if bytes.Compare(k, stopKb.Bytes()) > 0 {
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
		if key[0] != prefixBucket {
			return nil
		}
		if !visitor(ctx, key[1:], val) {
			return errScanDone
		}
		return nil
	})
}

func (db *Database) Stream(ctx context.Context, path string, visitor func(ctx context.Context, key string, val []byte)) error {
	stream := db.bdb.NewStream()
	stream.Prefix = []byte(path)
	stream.ChooseKey = func(item *badger.Item) bool {
		return item.Key()[0] == prefixBucket
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
			visitor(ctx, string(kv.Key[1:]), kv.Value)
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
	kb := keyForBucket(errorBucket, []byte(key))
	defer bufPool.Put(kb)
	return db.set(ctx, kb.Bytes(), buf.Bytes())
}

func (db *Database) VisitErrors(ctx context.Context, key string,
	visitor func(ctx context.Context, key string, when time.Time, detail []byte) bool) error {
	kb := keyForBucket(errorBucket, []byte(key))
	defer bufPool.Put(kb)
	return db.scanFrom(ctx, kb.Bytes(), func(ctx context.Context, key string, val []byte) error {
		if key[0] != errorBucket {
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

func (db *Database) lastKey(prefix byte) ([]byte, error) {
	var lastKey []byte
	p := []byte{prefix}
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
	return lastKey, err
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
	kb := keyForBucket(logBucket, []byte(start.Format(time.RFC3339)))
	defer bufPool.Put(kb)
	return db.set(ctx, kb.Bytes(), buf.Bytes())
}

func (db *Database) LastLog(ctx context.Context) (start, stop time.Time, detail []byte, err error) {
	lk, err := db.lastKey(logBucket)
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
	return db.scanTimeRange(ctx, logBucket, start, stop, func(ctx context.Context, key string, val []byte) error {
		if key[0] != logBucket {
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

func (db *Database) Clear(_ context.Context, logs, errors bool) error {
	if logs {
		if err := db.bdb.DropPrefix([]byte{logBucket}); err != nil {
			return err
		}
	}

	if errors {
		if err := db.bdb.DropPrefix([]byte{errorBucket}); err != nil {
			return err
		}
	}
	return nil
}
