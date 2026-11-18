// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
)

// BackendFactory creates the database backend for the database stored at
// path, opening it for reading only if readonly is true.
type BackendFactory func(ctx context.Context, path string, readonly bool) (Database, error)

// ErrNoBackend is returned when a database is opened without specifying a
// BackendFactory, see WithBackendFactory.
var ErrNoBackend = errors.New("no database backend specified, see idudb.WithBackendFactory")

// NewBackendFactory adapts a typed backend constructor, such as
// badgerdb.NewBackend, into a BackendFactory while ensuring a nil concrete
// pointer is not returned inside a non-nil Database interface on error.
func NewBackendFactory[T Database](fn func(context.Context, string, bool) (T, error)) BackendFactory {
	if fn == nil {
		return nil
	}
	return func(ctx context.Context, path string, readonly bool) (Database, error) {
		db, err := fn(ctx, path, readonly)
		if err != nil {
			return nil, err
		}
		return db, nil
	}
}

// OpenOption configures OpenBackend, Open and OpenWritable.
type OpenOption func(*openOptions)

type openOptions struct {
	factory  BackendFactory
	progress io.Writer
}

func newOpenOptions(opts []OpenOption) openOptions {
	o := openOptions{}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// WithBackendFactory sets the factory used to create the database backend,
// eg. one that opens a badger database using badgerdb.NewBackend. It must
// be specified, otherwise ErrNoBackend is returned.
func WithBackendFactory(f BackendFactory) OpenOption {
	return func(o *openOptions) {
		if f != nil {
			o.factory = f
		}
	}
}

// WithOpenProgress requests that a message be written to w, once a second,
// while waiting for a database to open, eg. because another process holds
// its lock. By default no messages are written.
func WithOpenProgress(w io.Writer) OpenOption {
	return func(o *openOptions) {
		o.progress = w
	}
}

// OpenBackend opens the database backend for the database stored at path,
// waiting until it is opened or ctx is canceled. If ctx is canceled first
// then any database that is subsequently opened is closed. OpenBackend does
// not check the format of the database, see CheckFormat.
func OpenBackend(ctx context.Context, path string, readonly bool, opts ...OpenOption) (Database, error) {
	o := newOpenOptions(opts)
	if o.factory == nil {
		return nil, ErrNoBackend
	}
	type result struct {
		db  Database
		err error
	}
	doneCh := make(chan result, 1)
	go func() {
		db, err := o.factory(ctx, path, readonly)
		doneCh <- result{db, err}
	}()
	start := time.Now()
	delayed := false
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			closeCtx := context.WithoutCancel(ctx)
			go func() {
				res := <-doneCh
				if res.err == nil && res.db != nil {
					_ = res.db.Close(closeCtx)
				}
			}()
			return nil, ctx.Err()
		case res := <-doneCh:
			if delayed {
				fmt.Fprintln(o.progress)
			}
			return res.db, res.err
		case <-ticker.C:
			if o.progress != nil {
				fmt.Fprintf(o.progress, "waiting for database to open: %v: %s\t\t\r", path, time.Since(start).Truncate(time.Second))
				delayed = true
			}
		}
	}
}

func validateSettings(cfg *iduconfig.Settings) error {
	if cfg == nil {
		return errors.New("idudb: nil settings configuration")
	}
	return nil
}

// Open opens the database specified by cfg for reading only and checks its
// format, see CheckFormat.
func Open(ctx context.Context, cfg *iduconfig.Settings, opts ...OpenOption) (Reader, error) {
	if err := validateSettings(cfg); err != nil {
		return nil, err
	}
	db, err := OpenBackend(ctx, cfg.Database, true, opts...)
	if err != nil {
		return nil, err
	}
	if err := CheckFormat(ctx, db); err != nil {
		_ = db.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("%v: %w", cfg.Database, err)
	}
	return db, nil
}

// OpenWritable opens, or creates, the database specified by cfg for use by
// an Analyzer, configured using OptionsFromConfig(cfg) followed by any
// supplied dbOpts.
func OpenWritable(ctx context.Context, cfg *iduconfig.Settings, opts []OpenOption, dbOpts ...Option) (*DB, error) {
	if err := validateSettings(cfg); err != nil {
		return nil, err
	}
	raw, err := OpenBackend(ctx, cfg.Database, false, opts...)
	if err != nil {
		return nil, err
	}
	db, err := New(ctx, raw, append(OptionsFromConfig(cfg), dbOpts...)...)
	if err != nil {
		_ = raw.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("%v: %w", cfg.Database, err)
	}
	return db, nil
}
