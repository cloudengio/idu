// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package database

import (
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

	// Delete deletes the specified key/value entries.
	Delete(ctx context.Context, keys ...string) error

	DeletePrefix(ctx context.Context, prefix string) error

	// SetBatch is like Set but allows for batching of concurrent calls to
	// SetBatch. It should only be used when called from multiple goroutines.
	SetBatch(ctx context.Context, key string, value []byte) error

	// Scan can be used to iterate over all keys in the database.
	Scan(ctx context.Context, key string, visitor func(ctx context.Context, key string, val []byte) bool) error

	// LogAndClose should be used to record the start and stop time for
	// a given operation and associated details/description. The database
	// will be closed once the log entry has been committeed.
	LogAndClose(ctx context.Context, start, stop time.Time, detail []byte) error

	// LastLog returns the most recently recorded log entry.
	LastLog(ctx context.Context) (start, stop time.Time, detail []byte, err error)

	// VisitLogs calls visitor for every log entry between start and stop. The
	// visitor func should return false if it wants to stop the iteration over
	// log entries.
	VisitLogs(ctx context.Context, start, stop time.Time,
		visitor func(ctx context.Context, begin, end time.Time, detail []byte) bool) error

	// LogError records an error, errors are stored in two ways: by key and by
	// when this function is called.
	LogError(ctx context.Context, when time.Time, key string, detail []byte) error

	// VisitErrorsWhen calls visitor for every error between start and stop. The
	// visitor func should return false if it wants to stop the iteration over
	// errors.
	VisitErrorsWhen(ctx context.Context, start, stop time.Time,
		visitor func(ctx context.Context, when time.Time, key string, detail []byte) bool) error

	// VisitErrorsKey calls visitor for every error starting at key. The
	// visitor func should return false if it wants to stop the iteration over
	// errors.
	VisitErrorsKey(ctx context.Context, key string, visitor func(ctx context.Context, when time.Time, key string, val []byte) bool) error

	// SetUser records a user and the group ids for the groups that they belong.
	SetUser(ctx context.Context, user string, id int64, gids []int64) error

	// SetGroup records a group and its gid.
	SetGroup(ctx context.Context, user string, gid int64) error

	// VisitUsers calls visitor for every user starting at user.
	VisitUsers(ctx context.Context, user string, visitor func(ctx context.Context, user string, id int64, gids []int64) bool) error

	// VisitGroups calls visitor for every group starting at group.
	VisitGroups(ctx context.Context, group string, visitor func(ctx context.Context, group string, gid int64) bool) error

	// Clear clears all of the log or error entries.
	Clear(ctx context.Context, logs, errors bool) error

	// Close closes the database.
	Close(context.Context) error
}
