// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmdutil"
	"cloudeng.io/cmdutil/subcmd"
	"cloudeng.io/logging/ctxlog"
)

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
	ls := cli.NewLoggers(global.FlagSet(), globalFlags.LoggingFlags)
	cfg, _ := cmdutil.ContextValue[iduconfig.T](ctx)
	l, err := ls.NewLogger(cfg.Logging)
	if err != nil {
		return ctx, id, noop, fmt.Errorf("error setting up logger: %w", err)
	}
	ctx = cli.WithLoggers(ctx, ls)
	ctx = ctxlog.WithLogger(ctx, l.Logger)
	return ctx, id, func(context.Context) (string, error) {
		if err := ls.Close(); err != nil {
			return id, fmt.Errorf("error closing logger: %w", err)
		}
		return id, nil
	}, nil
}
