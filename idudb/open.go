// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"bytes"
	"context"
	"errors"
)

// ErrLegacyDatabase is returned for a database that was created before
// precomputed summaries were introduced. Such databases are not supported
// and must be deleted and recreated by rerunning analyze.
var ErrLegacyDatabase = errors.New("legacy database format without summaries: delete it and rerun analyze")

// CheckFormat returns ErrLegacyDatabase if db contains prefix data but
// no summary metadata, i.e. it was created by an older version of idu.
// It should be called on every newly opened database before it is used;
// New, EraseLogs and EraseErrors call it automatically.
func CheckFormat(ctx context.Context, db Reader) error {
	var metaBuf bytes.Buffer
	if err := db.GetMeta(ctx, metaKeySummaries, &metaBuf); err != nil {
		return err
	}
	if metaBuf.Len() > 0 {
		return nil
	}
	hasData := false
	if err := db.Scan(ctx, "", func(_ context.Context, _ string, _ []byte) bool {
		hasData = true
		return false
	}); err != nil {
		return err
	}
	if hasData {
		return ErrLegacyDatabase
	}
	return nil
}

// EraseLogs deletes all of the log entries in db. It does not close db.
func EraseLogs(ctx context.Context, db Database) error {
	if err := CheckFormat(ctx, db); err != nil {
		return err
	}
	return db.Clear(ctx, true, false)
}

// EraseErrors deletes all of the error entries in db. It does not close db.
func EraseErrors(ctx context.Context, db Database) error {
	if err := CheckFormat(ctx, db); err != nil {
		return err
	}
	return db.Clear(ctx, false, true)
}
