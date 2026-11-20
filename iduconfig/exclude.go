// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package iduconfig

import (
	"path/filepath"
	"runtime"
)

// pseudoFilesystems returns the prefixes of the kernel provided filesystems
// on goos. They are never scanned: /dev/fd on macOS, for example, refers to
// the open files of the scanning process itself, including its database.
func pseudoFilesystems(goos string) []string {
	switch goos {
	case "darwin":
		return []string{"/dev"}
	case "linux":
		return []string{"/dev", "/proc", "/sys"}
	}
	return nil
}

// databaseDirs returns db and, if different, db with symbolic links resolved,
// as an absolute path.
func databaseDirs(db string) []string {
	if db == "" {
		return nil
	}
	abs, err := filepath.Abs(db)
	if err != nil {
		abs = db
	}
	dirs := []string{cleanPath(abs)}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil && cleanPath(resolved) != dirs[0] {
		dirs = append(dirs, cleanPath(resolved))
	}
	return dirs
}

// ExcludeFunc returns a function that reports whether a path is to be
// ignored when scanning: those that match one of the configured Exclusions
// and, always, the database directory, whose size is reported separately,
// and the pseudo filesystems such as /dev.
func (p *Settings) ExcludeFunc() func(path string) bool {
	return p.excludeFunc(runtime.GOOS)
}

func (p *Settings) excludeFunc(goos string) func(path string) bool {
	always := append(pseudoFilesystems(goos), databaseDirs(p.Database)...)
	return func(path string) bool {
		if p.Exclude(path) {
			return true
		}
		path = cleanPath(path)
		for _, prefix := range always {
			if containsPath(prefix, path, caseInsensitivePaths) {
				return true
			}
		}
		return false
	}
}
