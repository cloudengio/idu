// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"bytes"
	"context"
	"time"
)

// Reader represents the read-only operations supported by a database
// backend, such as the one implemented by backends/badgerdb.
type Reader interface {
	// Get retrieves the value associated with prefix storing
	// its contents in the supplied bytes.Buffer.
	Get(ctx context.Context, prefix string, buf *bytes.Buffer) error

	// GetBatch calls fn with the value associated with each of prefixes,
	// or nil if there is none, in order. val is only valid for the duration
	// of the call to fn. It is more efficient than calling Get for each
	// prefix.
	GetBatch(ctx context.Context, prefixes []string, fn func(i int, val []byte) error) error

	// Scan can be used to iterate over all keys in the database starting at
	// the specified key.
	Scan(ctx context.Context, key string, visitor func(ctx context.Context, key string, val []byte) bool) error

	// Stream can be used to iterate over all keys in the database concurrently
	// that have the specified prefix.
	Stream(ctx context.Context, prefix string, visitor func(ctx context.Context, key string, val []byte)) error

	// LastLog returns the most recently recorded log entry.
	LastLog(ctx context.Context) (start, stop time.Time, detail []byte, err error)

	// VisitLogs calls visitor for every log entry between start and stop. The
	// visitor func should return false if it wants to stop the iteration over
	// log entries.
	VisitLogs(ctx context.Context, start, stop time.Time,
		visitor func(ctx context.Context, begin, end time.Time, detail []byte) bool) error

	// VisitErrors calls visitor for every error starting at key. The
	// visitor func should return false if it wants to stop the iteration over
	// errors.
	VisitErrors(ctx context.Context, key string, visitor func(ctx context.Context, key string, when time.Time, val []byte) bool) error

	// GetSummaries calls fn with the summary stored in the summary bucket
	// for each of keys, which are summary keys, or nil if there are none,
	// in order. val is only valid for the duration of the call to fn.
	GetSummaries(ctx context.Context, keys []string, fn func(i int, val []byte) error) error

	// ScanSummaries iterates over summary entries in lexicographical key order,
	// starting at exactKey and stopping once keys no longer match exactKey or
	// childPrefix (when either is non-empty).
	ScanSummaries(ctx context.Context, exactKey, childPrefix string, visitor func(ctx context.Context, key string, val []byte) bool) error

	// GetSubtrees calls fn with the recursive totals stored in the
	// subtree bucket for each of keys, which are summary keys, or nil if
	// there are none, in order. val is only valid for the duration of the
	// call to fn.
	GetSubtrees(ctx context.Context, keys []string, fn func(i int, val []byte) error) error

	// ScanSubtrees iterates over the recursive totals in the subtree bucket
	// in lexicographical key order, starting at exactKey and stopping once
	// keys no longer match exactKey or childPrefix (when either is
	// non-empty).
	ScanSubtrees(ctx context.Context, exactKey, childPrefix string, visitor func(ctx context.Context, key string, val []byte) bool) error

	// ScanLinks iterates over the entries in the link bucket whose keys
	// start with prefix, in lexicographical key order. The link bucket
	// contains the index used to deduplicate hardlinks, whose keys and
	// values are defined by the idudb package.
	ScanLinks(ctx context.Context, prefix string, visitor func(ctx context.Context, key string, val []byte) bool) error

	// GetMeta retrieves a metadata value from the metadata bucket into buf.
	GetMeta(ctx context.Context, key string, buf *bytes.Buffer) error

	// Close closes the database.
	Close(context.Context) error
}

// Writer represents the operations that modify a database.
type Writer interface {
	// Set stores the value associated with prefix. If batch is true then
	// calls from multiple goroutines may be merged together. This is only
	// useful when Set is called from multiple goroutines.
	Set(ctx context.Context, prefix string, value []byte, batch bool) error

	// Delete deletes the value associated with exactly prefix, if any.
	Delete(ctx context.Context, prefix string) error

	// DeletePrefix deletes all keys that have the specified prefix.
	DeletePrefix(ctx context.Context, prefix string) error

	// Flush persists any pending batched writes. It is safe to call
	// concurrently with batched writes.
	Flush(ctx context.Context) error

	// DeleteError deletes the error logged for exactly key, if any.
	DeleteError(ctx context.Context, key string) error

	// DeleteErrors deletes all errors that have the specified prefix.
	DeleteErrors(ctx context.Context, prefix string) error

	// Log should be used to record the start and stop time for
	// a given operation and associated details/description.
	Log(ctx context.Context, start, stop time.Time, detail []byte) error

	// LogError records an error.
	LogError(ctx context.Context, key string, when time.Time, detail []byte) error

	// Clear clears all of the log or error entries.
	Clear(ctx context.Context, logs, errors bool) error

	// SetSummary stores a precomputed summary record for key in the summary bucket.
	SetSummary(ctx context.Context, key string, value []byte, batch bool) error

	// DeleteSummary deletes the summary entry matching exactKey and any
	// descendant summary entries matching childPrefix.
	DeleteSummary(ctx context.Context, exactKey, childPrefix string) error

	// SetSubtree stores the recursive totals for key in the subtree bucket.
	SetSubtree(ctx context.Context, key string, value []byte, batch bool) error

	// DeleteSubtrees deletes the subtree entry matching exactKey and any
	// descendant subtree entries matching childPrefix.
	DeleteSubtrees(ctx context.Context, exactKey, childPrefix string) error

	// ClearSubtrees deletes all of the entries in the subtree bucket.
	ClearSubtrees(ctx context.Context) error

	// SetLink stores value for key in the link bucket. If batch is true
	// the write may be merged with others, as for Set.
	SetLink(ctx context.Context, key string, value []byte, batch bool) error

	// DeleteLink deletes exactly key, if present, from the link bucket.
	DeleteLink(ctx context.Context, key string) error

	// ClearLinks deletes all of the entries in the link bucket.
	ClearLinks(ctx context.Context) error

	// SetMeta stores a metadata value in the metadata bucket.
	SetMeta(ctx context.Context, key string, value []byte) error
}

// Database represents a database backend that supports both read and write
// operations. Only the idudb package should use the Writer methods.
type Database interface {
	Reader
	Writer
}
