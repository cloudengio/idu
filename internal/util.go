// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmdutil/flags"
)

type TimeRangeFlags struct {
	Since time.Duration `subcmd:"since,0s,'display entries since the specified duration, it takes precedence over from/to'"`
	From  flags.Time    `subcmd:"from,,display entries starting at this time/date"`
	To    flags.Time    `subcmd:"to,,display entries ending at this time/date"`
}

func (tr *TimeRangeFlags) FromTo() (from, to time.Time, set bool, err error) {
	set = tr.Since != 0 || !tr.From.IsDefault() || !tr.To.IsDefault()
	to = time.Now()
	if !set {
		return
	}
	if tr.Since < 0 {
		return time.Time{}, time.Time{}, false, fmt.Errorf("since duration must be non-negative: %v", tr.Since)
	}
	if tr.Since > 0 {
		from = to.Add(-tr.Since)
		return
	}
	if !tr.From.IsDefault() {
		from = tr.From.Get().(time.Time)
	}
	if !tr.To.IsDefault() {
		to = tr.To.Get().(time.Time)
	}
	return
}

// trimTrailingSep strips trailing path separators while preserving root paths
// such as "/" or "C:\" without altering internal separators (important on
// Windows when configured prefixes use forward slashes).
func trimTrailingSep(path string) string {
	trimmed := strings.TrimRight(path, `/\`)
	if trimmed == "" && len(path) > 0 {
		return path[:1]
	}
	if len(path) >= 3 && path[1] == ':' && (path[2] == '/' || path[2] == '\\') && len(trimmed) == 2 {
		return path[:3]
	}
	return trimmed
}

// LookupPrefix resolves target (either a configuration name or a filesystem
// path) to the matching Prefix configuration and the filesystem path to operate
// on (Prefix.Prefix when matched by name, or the resolved path when matched
// by prefix).
func LookupPrefix(ctx context.Context, all iduconfig.T, target string) (context.Context, iduconfig.Prefix, string, error) {
	if cfg, ok := all.ForName(target); ok {
		return ctx, cfg, trimTrailingSep(cfg.Prefix), nil
	}
	if filepath.IsLocal(target) || len(target) == 0 {
		if dir, err := os.Getwd(); err == nil {
			target = filepath.Join(dir, target)
		}
	}
	target = trimTrailingSep(target)
	cfg, ok := all.ForPrefix(target)
	if !ok {
		return ctx, cfg, "", fmt.Errorf("no configuration for %v", target)
	}
	return ctx, cfg, target, nil
}

type prefixInfo struct {
	name string
	prefixinfo.T
}

// PrefixInfoAsFSInfo returns a fs.FileInfo for the supplied PrefixInfo.
func PrefixInfoAsFSInfo(pi prefixinfo.T, name string) fs.FileInfo {
	return &prefixInfo{T: pi, name: name}
}

func (pi *prefixInfo) Name() string {
	return pi.name
}

func (pi *prefixInfo) IsDir() bool {
	return pi.Mode().IsDir()
}

func (pi *prefixInfo) Sys() any {
	return nil
}
