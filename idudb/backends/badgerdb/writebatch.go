// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package badgerdb

import (
	"slices"

	"github.com/dgraph-io/badger/v4"
)

type writeBatch struct {
	batch *badger.WriteBatch
}

func newWriteBatch(bdb *badger.DB) *writeBatch {
	return &writeBatch{
		batch: bdb.NewWriteBatch(),
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
