// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package cli provides the support shared by idu's command groups (see
// cloudeng.io/cmd/idu/cli/...): the environment configured by main,
// locating the filesystem and database to operate on, opening databases,
// per-filesystem logging, formatting sizes and counts and the format of
// stats files.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/idudb/defaultdb"
	"cloudeng.io/cmdutil"
	"cloudeng.io/file/diskusage"
)

// Env is the environment shared by all of the commands. It is created by
// main, which sets Config and FS once the configuration has been loaded
// and the global flags parsed, ie. before any command runs.
type Env struct {
	// Config is the loaded configuration.
	Config iduconfig.T
	// FS is the name of the filesystem to operate on, ie. the value of
	// the global --fs flag.
	FS string
	// DatabaseFactory creates the database backend.
	DatabaseFactory idudb.BackendFactory
	// OpenProgress is where messages are written while waiting for a
	// database to open.
	OpenProgress io.Writer
	// BytesPrinter formats a size in bytes as a value and units, see
	// FmtSize.
	BytesPrinter func(size int64) (float64, string)
}

// NewEnv returns an Env that uses badger databases, writes database open
// progress to os.Stdout and displays sizes in decimal units.
func NewEnv() *Env {
	return &Env{
		DatabaseFactory: defaultdb.BackendFactory(),
		OpenProgress:    os.Stdout,
		BytesPrinter:    DecimalBytes,
	}
}

// DecimalBytes formats size using decimal units, eg. KB.
func DecimalBytes(size int64) (float64, string) {
	return diskusage.Decimal(size).Standardize()
}

// BinaryBytes formats size using binary units, eg. KiB.
func BinaryBytes(size int64) (float64, string) {
	return diskusage.Binary(size).Standardize()
}

// LookupPrefix uses iduconfig.T.Lookup to locate the configuration for the
// filesystem specified by FS and the location within it specified by
// prefix. It also arranges for the returned context to use that
// filesystem's logging configuration, if any, see WithLoggers.
func (e *Env) LookupPrefix(ctx context.Context, prefix string) (context.Context, iduconfig.Filesystem, string, error) {
	f, target, err := e.LookupFS(prefix)
	if err != nil {
		return ctx, nil, "", err
	}
	ctx, err = withPrefixLogger(ctx, f)
	return ctx, f, target, err
}

// LookupFS is like LookupPrefix but does not configure logging.
func (e *Env) LookupFS(prefix string) (iduconfig.Filesystem, string, error) {
	return LookupFSFor(e.Config, e.FS, prefix)
}

// LookupFSFor locates the configuration for the filesystem named fsName,
// which may be empty if only one is configured, and the location within
// it specified by prefix.
func LookupFSFor(all iduconfig.T, fsName, prefix string) (iduconfig.Filesystem, string, error) {
	f, target, err := all.Lookup(fsName, prefix)
	if errors.Is(err, iduconfig.ErrFilesystemNameRequired) {
		err = fmt.Errorf("%w, use --fs=<name> to specify one", err)
	}
	return f, target, err
}

// PrefixArg returns the optional <prefix> argument, or the empty string,
// which refers to the root of the filesystem, if it was not supplied.
func PrefixArg(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}

// OpenOptions returns the options used to open all databases.
func (e *Env) OpenOptions() []idudb.OpenOption {
	return []idudb.OpenOption{
		idudb.WithBackendFactory(e.DatabaseFactory),
		idudb.WithOpenProgress(e.OpenProgress),
	}
}

// OpenDatabase opens the database backend at path, waiting until it is
// opened or ctx is canceled and displaying a message while waiting.
func (e *Env) OpenDatabase(ctx context.Context, path string, readonly bool) (idudb.Database, error) {
	return idudb.OpenBackend(ctx, path, readonly, e.OpenOptions()...)
}

// OpenReadOnlyDB opens the database specified by cfg for reading only.
func (e *Env) OpenReadOnlyDB(ctx context.Context, cfg *iduconfig.Settings) (idudb.Reader, error) {
	return idudb.Open(ctx, cfg, e.OpenOptions()...)
}

// OpenWritableDB opens the database specified by cfg for use by analyze.
func (e *Env) OpenWritableDB(ctx context.Context, cfg *iduconfig.Settings) (*idudb.DB, error) {
	return idudb.OpenWritable(ctx, cfg, e.OpenOptions())
}

// OpenPrefixReadOnly resolves prefix to its configuration and the target
// path to operate on and opens the corresponding database for reading.
func (e *Env) OpenPrefixReadOnly(ctx context.Context, prefix string) (context.Context, *iduconfig.Settings, string, idudb.Reader, error) {
	ctx, f, target, err := e.LookupPrefix(ctx, prefix)
	if err != nil {
		return ctx, nil, "", nil, err
	}
	cfg := f.Config()
	db, err := e.OpenReadOnlyDB(ctx, cfg)
	if err != nil {
		return ctx, nil, "", nil, fmt.Errorf("failed to open database for %v in %v: %w", iduconfig.Label(f), cfg.Database, err)
	}
	return ctx, cfg, target, db, nil
}

type configLoadError struct {
	err error
}

// WithConfigLoadError returns a context that records err, the error, if
// any, encountered when loading the configuration. It allows commands that
// can run without a configuration to report it when they need one.
func WithConfigLoadError(ctx context.Context, err error) context.Context {
	return cmdutil.ContextWithValue(ctx, configLoadError{err: err})
}

// ConfigLoadError returns the error recorded by WithConfigLoadError, if any.
func ConfigLoadError(ctx context.Context) error {
	if loadErr, ok := cmdutil.ContextValue[configLoadError](ctx); ok {
		return loadErr.err
	}
	return nil
}

// FSFlag is embedded in the flags of every command that operates on a
// configured filesystem, it overrides the global --fs flag.
type FSFlag struct {
	FS string `subcmd:"fs,,'the name of the configured filesystem to operate on, overrides the global --fs flag; it may be omitted if only one filesystem is configured'"`
}

// ForFS returns e if fs is empty, or otherwise a copy of e that operates
// on the filesystem named fs, ie. one whose FS is fs.
func (e *Env) ForFS(fs string) *Env {
	if fs == "" {
		return e
	}
	c := *e
	c.FS = fs
	return &c
}
