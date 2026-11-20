// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package tui implements idu's terminal user interface, see tui-design.md.
// It provides an interactive view of a filesystem's usage, as a tree of its
// prefixes together with the summary of, and top-N rankings beneath, the
// highlighted prefix, and the ability to run analyze.
package tui

import (
	"context"
	"io"
	"log/slog"

	"cloudeng.io/cmd/idu/cli/analyze"
	"cloudeng.io/cmd/idu/cli/stats"
	"cloudeng.io/cmd/idu/cli/tui/internal/model"
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/logging/ctxlog"
	"github.com/gdamore/tcell/v2"
)

// Flags are the flags for the tui command.
type Flags struct {
	cli.FSFlag
	TopN int `subcmd:"n,20,number of top entries to display in each of the top-N tables"`
}

// Commands implements the tui command.
type Commands struct {
	env *cli.Env
	// logToFile returns true if log output is written to a file, and
	// hence need not be discarded while the tui owns the terminal.
	logToFile func() bool

	// The following may be overridden by tests.
	newScreen  func() (tcell.Screen, error)
	openSource func(ctx context.Context, env *cli.Env, f iduconfig.Filesystem, log io.Writer) (model.Source, error)
	analyze    func(ctx context.Context, p analyze.Progress, out io.Writer, flags *analyze.Flags) error
}

// Option configures the Commands created by New.
type Option func(*Commands)

// WithLogToFile sets a function that returns true if log output is
// written to a file, rather than the terminal, and hence need not be
// discarded while the tui is running. It is called when the tui starts.
func WithLogToFile(fn func() bool) Option {
	return func(c *Commands) {
		c.logToFile = fn
	}
}

// New returns the Commands that use env.
func New(env *cli.Env, opts ...Option) *Commands {
	c := &Commands{env: env}
	for _, fn := range opts {
		fn(c)
	}
	return c
}

// TUI implements the tui command, values must be a *Flags.
func (c *Commands) TUI(ctx context.Context, values interface{}, args []string) error {
	fl := values.(*Flags)
	env := c.env.ForFS(fl.FS)
	ctx, f, target, err := env.LookupPrefix(ctx, cli.PrefixArg(args))
	if err != nil {
		return err
	}
	logsToFile := false
	if c.logToFile != nil {
		logsToFile = c.logToFile()
	} else {
		logsToFile = cli.LogsToFileFor(ctx, env.Config.Logging, f.Config().Logging)
	}
	if !logsToFile {
		// Anything written to the terminal corrupts the display.
		ctx = ctxlog.WithLogger(ctx, slog.New(slog.DiscardHandler))
	}
	screen, err := c.screen()
	if err != nil {
		return err
	}
	// Anything written to the terminal, eg. while waiting for a database
	// to open, corrupts the display.
	quiet := *env
	quiet.OpenProgress = io.Discard
	env = &quiet
	u := newUI(ctx, uiConfig{
		env:        env,
		fs:         f,
		target:     target,
		topN:       max(fl.TopN, 1),
		screen:     screen,
		openSource: c.sourceOpener(),
		analyze:    c.analyzer(env, f),
	})
	return u.run()
}

func (c *Commands) screen() (tcell.Screen, error) {
	if c.newScreen != nil {
		return c.newScreen()
	}
	return tcell.NewScreen()
}

func (c *Commands) sourceOpener() func(context.Context, *cli.Env, iduconfig.Filesystem, io.Writer) (model.Source, error) {
	if c.openSource != nil {
		return c.openSource
	}
	return func(ctx context.Context, env *cli.Env, f iduconfig.Filesystem, log io.Writer) (model.Source, error) {
		return model.Open(ctx, env, f, f.Config().SnapshotDir, log)
	}
}

// analyzer returns the function used to run analyze for f.
func (c *Commands) analyzer(env *cli.Env, f iduconfig.Filesystem) func(context.Context, analyze.Progress, io.Writer, *analyze.Flags) error {
	if c.analyze != nil {
		return c.analyze
	}
	return func(ctx context.Context, p analyze.Progress, out io.Writer, flags *analyze.Flags) error {
		opts := []analyze.Option{
			analyze.WithOutput(out),
			analyze.WithSnapshot(stats.New(env).Snapshot),
		}
		if p != nil {
			opts = append(opts, analyze.WithProgress(func(context.Context, bool) analyze.Progress { return p }))
		}
		alz := analyze.New(env, opts...)
		return alz.Analyze(ctx, flags, []string{f.Location()})
	}
}
