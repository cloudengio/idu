// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	debugpkg "runtime/debug"
	"strings"
	"syscall"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmdutil"
	"cloudeng.io/cmdutil/profiling"
	"cloudeng.io/cmdutil/subcmd"
)

// setDefaultPreHooks sets the pre-hooks for each of idu's command trees.
// Pre-hooks set for a command apply to all of the commands beneath it, so
// the defaults are set for the entire tree and then overridden. Pre-hooks
// run in order and their post-hooks in the reverse order: the
// configuration is loaded first, then the logger it specifies is created,
// then the runtime settings, profiling, the http server and signal handling
// are set up.
func setDefaultPreHooks(cmdSet *subcmd.CommandSetYAML) {
	cmdSet.SetPreHooks(preHooks(configHook(true))...)
	// stats view and the reports commands can operate on snapshots, or
	// reports, without a configuration and report the error encountered
	// loading it only if they need it, see cli.ConfigLoadError.
	cmdSet.Set("stats", "view").MustSetPreHooks(preHooks(configHook(false))...)
	cmdSet.Set("reports").MustSetPreHooks(preHooks(configHook(false))...)
	// config show displays the configuration, and the files that it was
	// read from, even if it could not be loaded; config describe and
	// expression-syntax need nothing.
	cmdSet.Set("config").MustSetPreHooks()
	cmdSet.Set("config", "show").MustSetPreHooks(configHook(false))
	cmdSet.Set("expression-syntax").MustSetPreHooks()
}

// preHooks returns config followed by the pre-hooks used by every command
// that operates on a filesystem.
func preHooks(config subcmd.PreHook) []subcmd.PreHook {
	return []subcmd.PreHook{config, loggingHook, runtimeHook, profilingHook, httpHook, signalsHook}
}

func noopPostHook(id string) subcmd.PostHook {
	return func(context.Context) (string, error) { return id, nil }
}

// configHook returns a pre-hook that loads the configuration, adds it, the
// files it was read from and the error, if any, encountered loading it to
// the context, see cli.ConfigLoadError, and sets env's Config and FS. The
// error is returned if required is true.
func configHook(required bool) subcmd.PreHook {
	return func(ctx context.Context) (context.Context, string, subcmd.PostHook, error) {
		const id = "config"
		cfg, loc, err := iduconfig.Load(ctx, &globalFlags.Config)
		env.Config = cfg
		env.FS = globalFlags.FS
		ctx = cmdutil.ContextWithValue(ctx, cfg)
		ctx = cmdutil.ContextWithValue(ctx, loc)
		ctx = cli.WithConfigLoadError(ctx, err)
		if !required {
			err = nil
		}
		return ctx, id, noopPostHook(id), err
	}
}

// runtimeHook applies the --gcpercent and --units flags.
func runtimeHook(ctx context.Context) (context.Context, string, subcmd.PostHook, error) {
	const id = "runtime"
	debugpkg.SetGCPercent(globalFlags.GCPercent)
	switch globalFlags.Units.Value {
	case unitsDecimal:
		env.BytesPrinter = cli.DecimalBytes
	case unitsBinary:
		env.BytesPrinter = cli.BinaryBytes
	}
	return ctx, id, noopPostHook(id), nil
}

// profilingHook starts the profiles requested by --profile, its post-hook
// writes them.
func profilingHook(ctx context.Context) (context.Context, string, subcmd.PostHook, error) {
	const id = "profiling"
	var saves []func() error
	post := func(context.Context) (string, error) {
		var errs []error
		for _, save := range saves {
			errs = append(errs, save())
		}
		return id, errors.Join(errs...)
	}
	for _, profile := range globalFlags.ExitProfile.Profiles {
		if !profiling.IsPredefined(profile.Name) {
			fmt.Printf("warning profile %v defaults to CPU profiling since it is not one of the predefined profile types: %v\n", profile.Name, strings.Join(profiling.PredefinedProfiles(), ", "))
		}
		save, err := profiling.Start(profile.Name, profile.Filename)
		if err != nil {
			return ctx, id, post, err
		}
		fmt.Printf("profiling: %v %v\n", profile.Name, profile.Filename)
		saves = append(saves, save)
	}
	return ctx, id, post, nil
}

// httpHook serves /debug/vars and the profiling endpoints on the address
// specified by --http, if any, until the command has finished.
func httpHook(ctx context.Context) (context.Context, string, subcmd.PostHook, error) {
	const id = "http"
	addr := globalFlags.HTTP
	if len(addr) == 0 {
		return ctx, id, noopPostHook(id), nil
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return ctx, id, noopPostHook(id), err
	}
	// gosec G114
	go http.Serve(ln, nil) //nolint:gosec,errcheck
	return ctx, id, func(context.Context) (string, error) {
		_ = ln.Close()
		return id, nil
	}, nil
}

// signalsHook cancels the command's context when it is interrupted.
func signalsHook(ctx context.Context) (context.Context, string, subcmd.PostHook, error) {
	const id = "signals"
	ctx, cancel := context.WithCancel(ctx)
	cmdutil.HandleSignals(cancel, os.Interrupt, syscall.SIGTERM)
	return ctx, id, func(context.Context) (string, error) {
		cancel()
		return id, nil
	}, nil
}
