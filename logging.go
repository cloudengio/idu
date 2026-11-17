// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sync"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmdutil"
	"cloudeng.io/cmdutil/subcmd"
	"cloudeng.io/logging/ctxlog"
)

// loggers records the flags used to override logging configurations and
// the loggers created for a command so that they can all be closed when
// the command completes.
type loggers struct {
	fs *flag.FlagSet
	mu sync.Mutex
	l  []*cmdutil.Logger
}

func (ls *loggers) newLogger(cfg cmdutil.LoggingConfig) (*cmdutil.Logger, error) {
	if cfg.Format == "" {
		// Match the default of the --log-format flag.
		cfg.Format = globalFlags.Format
	}
	l, err := cfg.WithFlagOverrides(ls.fs, globalFlags.LoggingFlags).NewLogger()
	if err != nil {
		return nil, err
	}
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.l = append(ls.l, l)
	return l, nil
}

func (ls *loggers) close() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	var errs []error
	for _, l := range ls.l {
		errs = append(errs, l.Close())
	}
	ls.l = nil
	return errors.Join(errs...)
}

// loggingHook is a subcmd.PreHook that creates the logger specified by the
// logging section of the configuration file, overridden by any --log-* flags
// set on the command line. It must run after the configuration is loaded.
func loggingHook(ctx context.Context) (context.Context, string, subcmd.PostHook, error) {
	const id = "logging"
	noop := func(context.Context) (string, error) { return id, nil }
	global := subcmd.GlobalFlagSetFromContext(ctx)
	if global == nil {
		return ctx, id, noop, fmt.Errorf("global flagset not in context")
	}
	ls := &loggers{fs: global.FlagSet()}
	cfg, _ := cmdutil.ContextValue[iduconfig.T](ctx)
	l, err := ls.newLogger(cfg.Logging)
	if err != nil {
		return ctx, id, noop, fmt.Errorf("error setting up logger: %w", err)
	}
	ctx = cmdutil.ContextWithValue(ctx, ls)
	ctx = ctxlog.WithLogger(ctx, l.Logger)
	return ctx, id, func(context.Context) (string, error) {
		if err := ls.close(); err != nil {
			return id, fmt.Errorf("error closing logger: %w", err)
		}
		return id, nil
	}, nil
}

// withPrefixLogger returns a context whose logger is the one specified by the
// logging section of the prefix's configuration, if it has one, overridden
// by any --log-* flags set on the command line. Otherwise ctx is returned
// unchanged and the global logger continues to be used.
func withPrefixLogger(ctx context.Context, cfg iduconfig.Prefix) (context.Context, error) {
	if cfg.Logging == nil {
		return ctx, nil
	}
	ls, ok := cmdutil.ContextValue[*loggers](ctx)
	if !ok {
		return ctx, nil
	}
	l, err := ls.newLogger(*cfg.Logging)
	if err != nil {
		return ctx, fmt.Errorf("error setting up logger for %v: %w", cfg.Prefix, err)
	}
	return ctxlog.WithLogger(ctx, l.Logger), nil
}

// lookupPrefix is like internal.LookupPrefix but also arranges for the
// returned context to use the prefix's logging configuration, if any.
func lookupPrefix(ctx context.Context, all iduconfig.T, target string) (context.Context, iduconfig.Prefix, string, error) {
	ctx, cfg, target, err := internal.LookupPrefix(ctx, all, target)
	if err != nil {
		return ctx, cfg, target, err
	}
	ctx, err = withPrefixLogger(ctx, cfg)
	return ctx, cfg, target, err
}
