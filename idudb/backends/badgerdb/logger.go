// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package badgerdb

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// Logger implements badger.Logger by writing to an slog.Logger, with
// the attribute component=badger, subject to that logger's level.
type Logger struct {
	logger *slog.Logger
}

var _ badger.Logger = (*Logger)(nil)

// NewLogger returns a badger.Logger that writes to logger.
func NewLogger(logger *slog.Logger) *Logger {
	return &Logger{logger: logger.With("component", "badger")}
}

func (l *Logger) log(level slog.Level, format string, args ...any) {
	ctx := context.Background()
	if !l.logger.Enabled(ctx, level) {
		return
	}
	// badger's messages are terminated by newlines.
	l.logger.Log(ctx, level, strings.TrimSpace(fmt.Sprintf(format, args...)))
}

// Errorf implements badger.Logger.
func (l *Logger) Errorf(format string, args ...any) { l.log(slog.LevelError, format, args...) }

// Warningf implements badger.Logger.
func (l *Logger) Warningf(format string, args ...any) { l.log(slog.LevelWarn, format, args...) }

// Infof implements badger.Logger.
func (l *Logger) Infof(format string, args ...any) { l.log(slog.LevelInfo, format, args...) }

// Debugf implements badger.Logger.
func (l *Logger) Debugf(format string, args ...any) { l.log(slog.LevelDebug, format, args...) }
