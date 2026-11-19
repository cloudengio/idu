// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package badgerdb

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"cloudeng.io/cmd/idu/idudb/backends/badgerdb/internal/types"
	"cloudeng.io/errors"
	"cloudeng.io/os/lockedfile"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"google.golang.org/protobuf/proto"
)

// Option represents a specific option accepted by Open.
type Option func(o *options)

type options struct {
	ReadOnly bool
	Timeout  time.Duration
	Sub      Options
}

// ReadOnly requests that the database is opened in read-only mode.
func ReadOnly() Option {
	return func(o *options) {
		o.ReadOnly = true
	}
}

// WithTimeout requests that the database be opened with the specified timeout.
func WithTimeout(t time.Duration) Option {
	return func(o *options) {
		o.Timeout = t
	}
}

// WithBadgerOptions specifies the options to be used when opening the
// database. Note, that it overrides all other badger specific options when
// used.
func WithBadgerOptions(opts badger.Options) Option {
	return func(o *options) {
		o.Sub.Options = opts
	}
}

type Options struct {
	badger.Options
}

// Database represents a badger database.
type Database struct {
	options
	location string
	bdb      *badger.DB
	// batchMu guards replacing batch, which is safe for concurrent use
	// in its own right, when it is flushed.
	batchMu   sync.RWMutex
	batch     *writeBatch
	lock      *lockedfile.Mutex
	unlock    func()
	closeOnce sync.Once
	closeErr  error
}

// The database is partitioned into 7 'buckets':
// 1. inode bucket, keyed by inode and device numbers. This is by far
//    the largest since it has an entry for every file.
// 2. the prefix bucket, keyed by prefix. This contains an entry for
//    every prefix.
// 3. the log bucket, keyed by timestamp. This contains an entry for
//    every log entry, ie. iteration of updates of the database.
// 4. the error bucket, keyed by timestamp. This contains an entry for
//    every error encountered in the most recent update of the database.
// 5. the summary bucket, keyed by summary key. This contains a precomputed
//    summary for every prefix.
// 6. the meta bucket, keyed by name. This contains metadata describing
//    the database, such as how its summaries were computed.
// 7. the subtree bucket, keyed by summary key. This contains the recursive
//    totals for every prefix.
//
// Keys are assigned to each bucket by prepending an identifying byte
// to the key.

const (
	inodeBucket      = 0xf0
	prefixBucket     = 0xf1
	logBucket        = 0xf2
	errorBucket      = 0xf3
	summaryBucket    = 0xf4
	metaBucket       = 0xf5
	subtreeBucket    = 0xf6
	logKeyTimeFormat = "2006-01-02T15:04:05.000000000Z"
)

func logKeyForTime(bucket byte, ts time.Time) *bytes.Buffer {
	return keyForBucket(bucket, []byte(ts.UTC().Format(logKeyTimeFormat)))
}

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

func openLockedBadger(db *Database, location string, timedOut <-chan struct{}) error {
	lockfile := filepath.Join(location, "applock")
	db.lock = lockedfile.MutexAt(lockfile)
	var unlock func()
	var err error
	if db.ReadOnly {
		unlock, err = db.lock.RLockCreate()
	} else {
		unlock, err = db.lock.Lock()
	}
	if err != nil {
		return err
	}
	select {
	case <-timedOut:
		unlock()
		return context.DeadlineExceeded
	default:
	}
	db.unlock = unlock
	osopts := osOptions(db.Sub.Options)
	bdb, err := badger.Open(osopts)
	if err != nil {
		unlock()
		db.unlock = nil
		return err
	}
	db.bdb = bdb
	if !db.ReadOnly {
		db.batch = newWriteBatch(bdb)
	}
	return nil
}

// Open opens the specified database. If the database does not exist it will
// be created unless opened read-only.
func Open(location string, opts ...Option) (*Database, error) {
	db := &Database{
		location: location,
	}
	db.Sub.Options = badger.DefaultOptions(location)
	for _, fn := range opts {
		fn(&db.options)
	}
	db.Sub.Options = db.Sub.WithReadOnly(db.ReadOnly)

	if db.ReadOnly {
		if _, err := os.Stat(location); err != nil {
			return nil, err
		}
	} else if len(location) > 0 && location != "." {
		if err := os.MkdirAll(location, 0770); err != nil {
			return nil, err
		}
	}

	if db.Timeout <= 0 {
		if err := openLockedBadger(db, location, nil); err != nil {
			return nil, err
		}
		return db, nil
	}

	timedOut := make(chan struct{})
	ch := make(chan error, 1)
	go func() {
		ch <- openLockedBadger(db, location, timedOut)
	}()
	select {
	case err := <-ch:
		if err != nil {
			return nil, err
		}
		return db, nil
	case <-time.After(db.Timeout):
		close(timedOut)
		go func() {
			if err := <-ch; err == nil {
				_ = db.Close(context.Background())
			}
		}()
		return nil, fmt.Errorf("timed out opening database %s after %s", location, db.Timeout)
	}
}

func (db *Database) BadgerDB() *badger.DB {
	return db.bdb
}

// RunValueLogGC runs Badger's value log garbage collection repeatedly until
// no more files meet discardRatio.
func (db *Database) RunValueLogGC(discardRatio float64) error {
	for {
		err := db.bdb.RunValueLogGC(discardRatio)
		if err == badger.ErrNoRewrite {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// Compact flushes pending batched writes, flattens Badger's LSM tree, and
// runs value log garbage collection.
func (db *Database) Compact(ctx context.Context) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	if db.ReadOnly {
		return badger.ErrReadOnlyTxn
	}
	if err := db.Flush(ctx); err != nil {
		return err
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if err := db.bdb.Flatten(workers); err != nil {
		return err
	}
	return db.RunValueLogGC(0.5)
}

func (db *Database) canceled(ctx context.Context) error {
	select {
	case <-ctx.Done():
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
		item, err := tx.Get(key)
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
	if db.ReadOnly {
		return badger.ErrReadOnlyTxn
	}
	kb := keyForBucket(prefixBucket, []byte(prefix))
	defer bufPool.Put(kb)
	if batch {
		return db.batchSet(kb.Bytes(), val)
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

func (db *Database) deleteKey(bucket byte, key string) error {
	kb := keyForBucket(bucket, []byte(key))
	defer bufPool.Put(kb)
	return db.bdb.Update(func(tx *badger.Txn) error {
		if _, err := tx.Get(kb.Bytes()); err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		return tx.Delete(kb.Bytes())
	})
}

// Delete deletes the value stored for exactly prefix.
func (db *Database) Delete(ctx context.Context, prefix string) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	if db.ReadOnly {
		return badger.ErrReadOnlyTxn
	}
	return db.deleteKey(prefixBucket, prefix)
}

// SetSummary stores a precomputed summary value in the summary bucket.
func (db *Database) SetSummary(ctx context.Context, key string, val []byte, batch bool) error {
	return db.setInBucket(ctx, summaryBucket, key, val, batch)
}

// DeleteSummary deletes exactKey and any descendant keys matching childPrefix
// from the summary bucket without affecting sibling prefixes.
func (db *Database) DeleteSummary(ctx context.Context, exactKey, childPrefix string) error {
	return db.deleteSummaryKeys(ctx, summaryBucket, exactKey, childPrefix)
}

// SetSubtree stores the recursive totals for key in the subtree bucket.
func (db *Database) SetSubtree(ctx context.Context, key string, val []byte, batch bool) error {
	return db.setInBucket(ctx, subtreeBucket, key, val, batch)
}

// DeleteSubtrees deletes exactKey and any descendant keys matching childPrefix
// from the subtree bucket without affecting sibling prefixes.
func (db *Database) DeleteSubtrees(ctx context.Context, exactKey, childPrefix string) error {
	return db.deleteSummaryKeys(ctx, subtreeBucket, exactKey, childPrefix)
}

func (db *Database) setInBucket(ctx context.Context, bucket byte, key string, val []byte, batch bool) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	if db.ReadOnly {
		return badger.ErrReadOnlyTxn
	}
	kb := keyForBucket(bucket, []byte(key))
	defer bufPool.Put(kb)
	if batch {
		return db.batchSet(kb.Bytes(), val)
	}
	return db.set(ctx, kb.Bytes(), val)
}

func (db *Database) deleteSummaryKeys(ctx context.Context, bucket byte, exactKey, childPrefix string) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	if db.ReadOnly {
		return badger.ErrReadOnlyTxn
	}
	if exactKey != "" && (childPrefix == "" || !strings.HasPrefix(exactKey, childPrefix)) {
		if err := db.deleteKey(bucket, exactKey); err != nil {
			return err
		}
	}
	if childPrefix != "" {
		cb := keyForBucket(bucket, []byte(childPrefix))
		defer bufPool.Put(cb)
		return db.deletePrefix(ctx, cb.Bytes())
	}
	return nil
}

// ClearSubtrees deletes all of the entries in the subtree bucket.
func (db *Database) ClearSubtrees(ctx context.Context) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	if db.ReadOnly {
		return badger.ErrReadOnlyTxn
	}
	return db.bdb.DropPrefix([]byte{subtreeBucket})
}

// SetMeta stores a metadata entry in the metadata bucket.
func (db *Database) SetMeta(ctx context.Context, key string, val []byte) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	if db.ReadOnly {
		return badger.ErrReadOnlyTxn
	}
	kb := keyForBucket(metaBucket, []byte(key))
	defer bufPool.Put(kb)
	return db.set(ctx, kb.Bytes(), val)
}

// GetMeta retrieves a metadata entry from the metadata bucket into buf.
func (db *Database) GetMeta(ctx context.Context, key string, buf *bytes.Buffer) error {
	kb := keyForBucket(metaBucket, []byte(key))
	defer bufPool.Put(kb)
	return db.get(ctx, kb.Bytes(), buf)
}

func (db *Database) batchSet(key, val []byte) error {
	db.batchMu.RLock()
	defer db.batchMu.RUnlock()
	if db.batch == nil {
		return badger.ErrDBClosed
	}
	return db.batch.set(key, val)
}

// Flush persists any pending batched writes. It is safe to call
// concurrently with batched writes.
func (db *Database) Flush(ctx context.Context) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	db.batchMu.Lock()
	defer db.batchMu.Unlock()
	if db.ReadOnly {
		return nil
	}
	if db.batch == nil {
		return badger.ErrDBClosed
	}
	err := db.batch.flush()
	if db.bdb != nil && !db.bdb.IsClosed() {
		db.batch = newWriteBatch(db.bdb)
	} else {
		db.batch = nil
	}
	return err
}

func (db *Database) DeletePrefix(ctx context.Context, prefix string) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	if db.ReadOnly {
		return badger.ErrReadOnlyTxn
	}
	kb := keyForBucket(prefixBucket, []byte(prefix))
	defer bufPool.Put(kb)
	return db.deletePrefix(ctx, kb.Bytes())
}

func (db *Database) deleteBatch(prefix []byte) (bool, error) {
	tx := db.bdb.NewTransaction(true)
	defer tx.Discard()
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = prefix
	it := tx.NewIterator(opts)
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

// DeleteError deletes the error logged for exactly key.
func (db *Database) DeleteError(ctx context.Context, key string) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	if db.ReadOnly {
		return badger.ErrReadOnlyTxn
	}
	return db.deleteKey(errorBucket, key)
}

func (db *Database) DeleteErrors(ctx context.Context, prefix string) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	if db.ReadOnly {
		return badger.ErrReadOnlyTxn
	}
	kb := keyForBucket(errorBucket, []byte(prefix))
	defer bufPool.Put(kb)
	return db.deletePrefix(ctx, kb.Bytes())
}

var errScanDone = errors.New("scan done")

func (db *Database) scanFrom(ctx context.Context, bucket byte, prefix []byte, visitor func(ctx context.Context, key string, val []byte) error) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	kb := keyForBucket(bucket, prefix)
	defer bufPool.Put(kb)
	prefix = kb.Bytes()
	return db.bdb.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{bucket}
		// Values are small and stored in the LSM tree rather than the value
		// log, and prefetching them creates a goroutine per item, which
		// is far more expensive than reading them synchronously.
		opts.PrefetchValues = false
		it := tx.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.Valid(); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
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
		}
		return nil
	})
}

func (db *Database) scanTimeRange(ctx context.Context, bucket byte, start, stop time.Time, visitor func(ctx context.Context, key string, val []byte) error) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	startKb := logKeyForTime(bucket, start)
	defer bufPool.Put(startKb)
	stopKb := logKeyForTime(bucket, stop)
	defer bufPool.Put(stopKb)
	return db.bdb.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{bucket}
		// Values are small and stored in the LSM tree rather than the value
		// log, and prefetching them creates a goroutine per item, which
		// is far more expensive than reading them synchronously.
		opts.PrefetchValues = false
		it := tx.NewIterator(opts)
		defer it.Close()
		for it.Seek(startKb.Bytes()); it.Valid(); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
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
		}
		return nil
	})
}

const defaultStreamMaxSize = 16 << 20 // 16 MiB

func (db *Database) Scan(ctx context.Context, path string, visitor func(ctx context.Context, key string, val []byte) bool) error {
	return db.scanFrom(ctx, prefixBucket, []byte(path), func(ctx context.Context, key string, val []byte) error {
		if key[0] != prefixBucket {
			return errScanDone
		}
		if !visitor(ctx, key[1:], val) {
			return errScanDone
		}
		return nil
	})
}

// inSummaryRange reports whether the summary key k is exactKey or
// a descendant matching childPrefix.
func inSummaryRange(exactKey, childPrefix, k string) bool {
	if exactKey == "" && childPrefix == "" {
		return true
	}
	return k == exactKey || (childPrefix != "" && strings.HasPrefix(k, childPrefix))
}

// ScanSummaries iterates over entries in the summary bucket starting at
// exactKey and stopping once keys no longer match exactKey or childPrefix.
func (db *Database) ScanSummaries(ctx context.Context, exactKey, childPrefix string, visitor func(ctx context.Context, key string, val []byte) bool) error {
	return db.scanSummaryKeys(ctx, summaryBucket, exactKey, childPrefix, visitor)
}

// ScanSubtrees iterates over entries in the subtree bucket starting at
// exactKey and stopping once keys no longer match exactKey or childPrefix.
func (db *Database) ScanSubtrees(ctx context.Context, exactKey, childPrefix string, visitor func(ctx context.Context, key string, val []byte) bool) error {
	return db.scanSummaryKeys(ctx, subtreeBucket, exactKey, childPrefix, visitor)
}

// scanSummaryKeys iterates over the entries, keyed by summary key, in
// bucket starting at exactKey and stopping once keys no longer match
// exactKey or childPrefix.
func (db *Database) scanSummaryKeys(ctx context.Context, bucket byte, exactKey, childPrefix string, visitor func(ctx context.Context, key string, val []byte) bool) error {
	return db.scanFrom(ctx, bucket, []byte(exactKey), func(ctx context.Context, key string, val []byte) error {
		if len(key) == 0 || key[0] != bucket {
			return errScanDone
		}
		k := key[1:]
		if !inSummaryRange(exactKey, childPrefix, k) {
			return errScanDone
		}
		if !visitor(ctx, k, val) {
			return errScanDone
		}
		return nil
	})
}

// GetBatch calls fn with the value stored for each of keys, or nil if there
// is none, reading all of them within a single read transaction since
// creating a transaction per value is expensive.
func (db *Database) GetBatch(ctx context.Context, keys []string, fn func(i int, val []byte) error) error {
	return db.getBatch(ctx, prefixBucket, keys, fn)
}

// GetSubtrees is like GetBatch but for the subtree bucket.
func (db *Database) GetSubtrees(ctx context.Context, keys []string, fn func(i int, val []byte) error) error {
	return db.getBatch(ctx, subtreeBucket, keys, fn)
}

func (db *Database) getBatch(ctx context.Context, bucket byte, keys []string, fn func(i int, val []byte) error) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	kb := &bytes.Buffer{}
	return db.bdb.View(func(tx *badger.Txn) error {
		for i, key := range keys {
			kb.Reset()
			kb.WriteByte(bucket)
			kb.WriteString(key)
			item, err := tx.Get(kb.Bytes())
			if err == badger.ErrKeyNotFound {
				if err := fn(i, nil); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			if err := item.Value(func(v []byte) error { return fn(i, v) }); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *Database) Stream(ctx context.Context, path string, visitor func(ctx context.Context, key string, val []byte)) error {
	stream := db.bdb.NewStream()
	kb := keyForBucket(prefixBucket, []byte(path))
	defer bufPool.Put(kb)
	stream.Prefix = kb.Bytes()
	stream.NumGo = runtime.NumCPU()
	stream.MaxSize = defaultStreamMaxSize
	var kv pb.KV
	stream.Send = func(buf *z.Buffer) error {
		if err := buf.SliceIterate(func(s []byte) error {
			kv.Reset()
			if err := proto.Unmarshal(s, &kv); err != nil {
				return err
			}
			if kv.StreamDone {
				return nil
			}
			visitor(ctx, string(kv.Key[1:]), kv.Value)
			return nil
		}); err != nil {
			return err
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
	return db.scanFrom(ctx, errorBucket, []byte(key), func(ctx context.Context, key string, val []byte) error {
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
	end := []byte{prefix + 1}
	err := db.bdb.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(end)
		if it.Valid() && bytes.Equal(it.Item().Key(), end) {
			it.Next()
		}
		if it.ValidForPrefix(p) {
			lastKey = it.Item().KeyCopy(nil)
		}
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
	kb := logKeyForTime(logBucket, start)
	defer bufPool.Put(kb)
	return db.set(ctx, kb.Bytes(), buf.Bytes())
}

func (db *Database) LastLog(ctx context.Context) (start, stop time.Time, detail []byte, err error) {
	lk, err := db.lastKey(logBucket)
	if err != nil {
		return time.Time{}, time.Time{}, nil, err
	}
	if len(lk) == 0 {
		return time.Time{}, time.Time{}, nil, nil
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
func (db *Database) Close(_ context.Context) error {
	db.closeOnce.Do(func() {
		var errs errors.M
		db.batchMu.Lock()
		if db.batch != nil {
			errs.Append(db.batch.flush())
			db.batch = nil
		}
		db.batchMu.Unlock()
		if db.bdb != nil {
			errs.Append(db.bdb.Close())
		}
		if db.unlock != nil {
			db.unlock()
			db.unlock = nil
		}
		db.closeErr = errs.Err()
	})
	return db.closeErr
}

func (db *Database) Clear(ctx context.Context, logs, errors bool) error {
	if err := db.canceled(ctx); err != nil {
		return err
	}
	if db.ReadOnly {
		return badger.ErrReadOnlyTxn
	}
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
