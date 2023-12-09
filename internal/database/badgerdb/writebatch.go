// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package badgerdb

import (
	"slices"

	"github.com/dgraph-io/badger/v4"
)

type writeBatch struct {
	bdb      *badger.DB
	batch    *badger.WriteBatch
	maxSize  int64
	maxCount int64
}

func newWriteBatch(bdb *badger.DB) *writeBatch {
	return &writeBatch{
		bdb:      bdb,
		batch:    bdb.NewWriteBatch(),
		maxSize:  bdb.MaxBatchSize(),
		maxCount: bdb.MaxBatchCount(),
	}
}

func (wb *writeBatch) set(key, value []byte) error {
	k := slices.Clone(key)
	v := slices.Clone(value)
	return wb.batch.Set(k, v)
}

func (wb *writeBatch) flush() error {
	return wb.batch.Flush()
}
