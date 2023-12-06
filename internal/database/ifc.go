// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package database

import (
	"bytes"
	"context"
	"time"
)

// ReadOnly requests that the database is opened in read-only mode.
func ReadOnly[T any]() func(o *Options[T]) {
	return func(o *Options[T]) {
		o.ReadOnly = true
	}
}

// WithTimeout requests that the database be opened with the specified timeout.
func WithTimeout[T any](t time.Duration) func(o *Options[T]) {
	return func(o *Options[T]) {
		o.Timeout = t
	}
}

type Options[T any] struct {
	ReadOnly bool
	Timeout  time.Duration
	Sub      T
}

// DB represents a database.
type DB interface {
	// Set stores the value associated with key.
	Set(ctx context.Context, key string, value []byte) error

	// Get retrieves the value associated with key.
	Get(ctx context.Context, key string) ([]byte, error)

	// GeGetBuft retrieves the value associated with key storing
	// its contents in the supplied bytes.Buffer.
	GetBuf(ctx context.Context, key string, buf *bytes.Buffer) error

	// Delete deletes the specified entries.
	Delete(ctx context.Context, keys ...string) error

	// DeletePrefix deletes all keys that have the specified prefix.
	DeletePrefix(ctx context.Context, prefix string) error

	// DeleteErrors deletes all errors that have the specified prefix.
	DeleteErrors(ctx context.Context, prefix string) error

	SaveStats(ctx context.Context, when time.Time, value []byte) error
	LastStats(ctx context.Context) (time.Time, []byte, error)
	VisitStats(ctx context.Context, start, stop time.Time,
		visitor func(ctx context.Context, when time.Time, value []byte) bool) error

	// SetBatch is like Set but allows for batching of concurrent calls to
	// SetBatch. It should only be used when called from multiple goroutines.
	SetBatch(ctx context.Context, key string, buf []byte) error

	// Scan can be used to iterate over all keys in the database starting at
	// the specified key.
	Scan(ctx context.Context, key string, visitor func(ctx context.Context, key string, val []byte) bool) error

	// Stream can be used to iterate over all keys in the database concurrently
	// that have the specified prefix.
	Stream(ctx context.Context, prefix string, visitor func(ctx context.Context, key string, val []byte)) error

	// Log should be used to record the start and stop time for
	// a given operation and associated details/description.
	Log(ctx context.Context, start, stop time.Time, detail []byte) error

	// LastLog returns the most recently recorded log entry.
	LastLog(ctx context.Context) (start, stop time.Time, detail []byte, err error)

	// VisitLogs calls visitor for every log entry between start and stop. The
	// visitor func should return false if it wants to stop the iteration over
	// log entries.
	VisitLogs(ctx context.Context, start, stop time.Time,
		visitor func(ctx context.Context, begin, end time.Time, detail []byte) bool) error

	// LogError records an error.
	LogError(ctx context.Context, key string, when time.Time, detail []byte) error

	// VisitErrors calls visitor for every error starting at key. The
	// visitor func should return false if it wants to stop the iteration over
	// errors.
	VisitErrors(ctx context.Context, key string, visitor func(ctx context.Context, key string, when time.Time, val []byte) bool) error

	// Clear clears all of the log or error entries.
	Clear(ctx context.Context, logs, errors, stats bool) error

	// Close closes the database.
	Close(context.Context) error
}
