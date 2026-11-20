// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cli

import (
	"context"
	"flag"
	"testing"

	"cloudeng.io/cmdutil"
)

func TestLogsToFileFor(t *testing.T) {
	newLoggers := func(args ...string) *Loggers {
		fs := flag.NewFlagSet("test", flag.ContinueOnError)
		var lf cmdutil.LoggingFlags
		fs.StringVar(&lf.File, "log-file", "", "")
		fs.StringVar(&lf.TempFile, "log-tempfile", "", "")
		fs.StringVar(&lf.Format, "log-format", "text", "")
		if err := fs.Parse(args); err != nil {
			t.Fatal(err)
		}
		return NewLoggers(fs, lf)
	}

	ctx := context.Background()
	filePerFS := &cmdutil.LoggingConfig{File: "/var/log/fs.log"}
	termGlobal := cmdutil.LoggingConfig{}
	fileGlobal := cmdutil.LoggingConfig{File: "/var/log/global.log"}

	// Per-filesystem log file overrides terminal global config.
	if !LogsToFileFor(WithLoggers(ctx, newLoggers()), termGlobal, filePerFS) {
		t.Errorf("expected per-FS log file to report true")
	}
	// Explicit --log-file=- overrides configured log file back to terminal.
	if LogsToFileFor(WithLoggers(ctx, newLoggers("--log-file=-")), fileGlobal, nil) {
		t.Errorf("expected --log-file=- override to report false")
	}
	if LogsToFileFor(WithLoggers(ctx, newLoggers("--log-file=-")), termGlobal, filePerFS) {
		t.Errorf("expected --log-file=- override to report false for per-FS")
	}
	// Global file config with no overrides reports true.
	if !LogsToFileFor(WithLoggers(ctx, newLoggers()), fileGlobal, nil) {
		t.Errorf("expected global log file to report true")
	}
	// --log-tempfile reports true even with terminal config.
	if !LogsToFileFor(WithLoggers(ctx, newLoggers("--log-tempfile=idu-*.log")), termGlobal, nil) {
		t.Errorf("expected --log-tempfile to report true")
	}
	// Terminal config with no overrides reports false.
	if LogsToFileFor(WithLoggers(ctx, newLoggers()), termGlobal, nil) {
		t.Errorf("expected terminal logging to report false")
	}
}
