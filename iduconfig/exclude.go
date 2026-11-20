// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package iduconfig

import (
	"errors"
	"io/fs"
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

// evalSymlinksExisting resolves symbolic links for p, or for its nearest
// existing ancestor when p itself does not yet exist on disk.
func evalSymlinksExisting(p string) (string, bool) {
	resolved, err := filepath.EvalSymlinks(p)
	if err == nil {
		return resolved, true
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", false
	}
	parent := filepath.Dir(p)
	if parent == p {
		return "", false
	}
	resolvedParent, ok := evalSymlinksExisting(parent)
	if !ok {
		return "", false
	}
	return filepath.Join(resolvedParent, filepath.Base(p)), true
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
	if resolved, ok := evalSymlinksExisting(abs); ok && cleanPath(resolved) != dirs[0] {
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
