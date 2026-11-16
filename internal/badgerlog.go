// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"
)

var badgerLogName = defaultLogName + "-badger"

func logDB(ctx context.Context, level slog.Level, msg string, args ...interface{}) {
	if level < Verbosity {
		return
	}
	var pcs [1]uintptr
	if logSourceCode {
		runtime.Callers(2, pcs[:]) // skip [Callers, infof]
	}
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	r.Add(args...)
	_ = getOrCreateLogger(badgerLogName).Handler().Handle(ctx, r)
}

// BadgerLogger implements badger.Logger by writing to a dedicated
// log file subject to the configured Verbosity.
type BadgerLogger struct{}

func (l *BadgerLogger) Errorf(f string, a ...interface{}) {
	if LogError < Verbosity {
		return
	}
	logDB(context.Background(), LogError, "badger", "msg", fmt.Sprintf(f, a...))
}

func (l *BadgerLogger) Warningf(f string, a ...interface{}) {
	if slog.LevelWarn < Verbosity {
		return
	}
	logDB(context.Background(), slog.LevelWarn, "badger", "msg", fmt.Sprintf(f, a...))
}

func (l *BadgerLogger) Infof(f string, a ...interface{}) {
	if slog.LevelInfo < Verbosity {
		return
	}
	logDB(context.Background(), slog.LevelInfo, "badger", "msg", fmt.Sprintf(f, a...))
}

func (l *BadgerLogger) Debugf(f string, a ...interface{}) {
	if slog.LevelDebug < Verbosity {
		return
	}
	logDB(context.Background(), slog.LevelDebug, "badger", "msg", fmt.Sprintf(f, a...))
}
