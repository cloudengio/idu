// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sync"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmdutil"
	"cloudeng.io/logging/ctxlog"
)

// Loggers records the flags used to override logging configurations and
// the loggers created for a command so that they can all be closed when
// the command completes.
type Loggers struct {
	fs    *flag.FlagSet
	flags cmdutil.LoggingFlags
	mu    sync.Mutex
	l     []*cmdutil.Logger
}

// NewLoggers returns a Loggers that applies any of flags that are set in
// fs to every logging configuration.
func NewLoggers(fs *flag.FlagSet, flags cmdutil.LoggingFlags) *Loggers {
	return &Loggers{fs: fs, flags: flags}
}

// NewLogger creates a logger for cfg, overridden by any of the logging
// flags that are set.
func (ls *Loggers) NewLogger(cfg cmdutil.LoggingConfig) (*cmdutil.Logger, error) {
	if cfg.Format == "" {
		// Match the default of the --log-format flag.
		cfg.Format = ls.flags.Format
	}
	l, err := cfg.WithFlagOverrides(ls.fs, ls.flags).NewLogger()
	if err != nil {
		return nil, err
	}
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.l = append(ls.l, l)
	return l, nil
}

// Close closes all of the loggers created by NewLogger.
func (ls *Loggers) Close() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	var errs []error
	for _, l := range ls.l {
		errs = append(errs, l.Close())
	}
	ls.l = nil
	return errors.Join(errs...)
}

// WithLoggers returns a context that carries ls so that LookupPrefix can
// create the loggers for filesystems with their own logging configuration.
func WithLoggers(ctx context.Context, ls *Loggers) context.Context {
	return cmdutil.ContextWithValue(ctx, ls)
}

// LogsToFile reports whether a logger configured with cfg and overridden by
// ls's flags writes to a file rather than the terminal.
func (ls *Loggers) LogsToFile(cfg cmdutil.LoggingConfig) bool {
	if ls != nil {
		cfg = cfg.WithFlagOverrides(ls.fs, ls.flags)
	}
	return cfg.TempFile != "" || (cfg.File != "" && cfg.File != "-")
}

// LogsToFileFor reports whether the effective logger in ctx for a filesystem
// with optional perFS logging configuration writes to a file rather than
// the terminal.
func LogsToFileFor(ctx context.Context, global cmdutil.LoggingConfig, perFS *cmdutil.LoggingConfig) bool {
	cfg := global
	if perFS != nil {
		cfg = *perFS
	}
	ls, _ := cmdutil.ContextValue[*Loggers](ctx)
	return ls.LogsToFile(cfg)
}

// withPrefixLogger returns a context whose logger is the one specified by the
// logging section of the prefix's configuration, if it has one, overridden
// by any --log-* flags set on the command line. Otherwise ctx is returned
// unchanged and the global logger continues to be used.
func withPrefixLogger(ctx context.Context, f iduconfig.Filesystem) (context.Context, error) {
	cfg := f.Config()
	if cfg.Logging == nil {
		return ctx, nil
	}
	ls, ok := cmdutil.ContextValue[*Loggers](ctx)
	if !ok {
		return ctx, nil
	}
	l, err := ls.NewLogger(*cfg.Logging)
	if err != nil {
		return ctx, fmt.Errorf("error setting up logger for %v: %w", iduconfig.Label(f), err)
	}
	return ctxlog.WithLogger(ctx, l.Logger), nil
}
