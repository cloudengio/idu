// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

const (
	LogPrefix   = slog.Level(-1)
	LogProgress = slog.LevelInfo
	LogError    = slog.LevelError
)

var levelNames = map[slog.Leveler]string{
	LogProgress: "PROGRESS",
	LogError:    "ERROR",
	LogPrefix:   "PREFIX",
}

var Verbosity slog.Level = LogError
var LogDir string

type logger struct {
	sync.Mutex
	*slog.Logger
}

var globalLogger = &logger{}

func getOrCreateLogger(name string, ctx context.Context) *slog.Logger {
	globalLogger.Lock()
	defer globalLogger.Unlock()
	if globalLogger.Logger != nil {
		return globalLogger.Logger
	}
	f, name, err := createNamedLogfile(name, LogDir, time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log file: %v, %v\n", name, err)
	}
	globalLogger.Logger = newLogger(f)
	return globalLogger.Logger
}

func Log(ctx context.Context, level slog.Level, msg string, args ...interface{}) {
	if level < Verbosity {
		return
	}
	var pcs [1]uintptr
	if logSourceCode {
		runtime.Callers(2, pcs[:]) // skip [Callers, infof]
	}
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	r.Add(args...)
	_ = getOrCreateLogger(defaultLogName, ctx).Handler().Handle(ctx, r)
}

var (
	logSourceCode  = false
	pid            = os.Getpid()
	defaultLogName = filepath.Base(os.Args[0]) + "-" + os.Getenv("USER")
)

// logName returns a new log file name containing tag, with start time t, and
// the name for the symlink for tag.
func logName(logName string, t time.Time) (name, link string) {
	name = fmt.Sprintf("%s.log.%04d%02d%02d-%02d%02d%02d.%d",
		logName,
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		t.Second(),
		pid)
	return name, logName + ".log"
}

func createNamedLogfile(name, dir string, t time.Time) (f *os.File, fname string, err error) {
	name, link := logName(name, t)
	fname = filepath.Join(dir, name)
	f, err = os.Create(fname)
	if err == nil {
		symlink := filepath.Join(dir, link)
		os.Remove(symlink)        // ignore err
		os.Symlink(name, symlink) // ignore err
		return f, fname, nil
	}
	return nil, fname, fmt.Errorf("log: cannot create log: %v", err)
}

func newLogger(f *os.File) *slog.Logger {
	opts := &slog.HandlerOptions{
		AddSource: logSourceCode,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.LevelKey {
				level := a.Value.Any().(slog.Level)
				levelLabel, exists := levelNames[level]
				if !exists {
					levelLabel = level.String()
				}
				a.Value = slog.StringValue(levelLabel)
			}
			return a
		}}
	return slog.New(slog.NewJSONHandler(f, opts))
}
