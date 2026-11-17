// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package iduconfig

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// ErrFilesystemNameRequired is returned by Lookup when no filesystem name
// is supplied and more than one filesystem is configured.
var ErrFilesystemNameRequired = errors.New("a filesystem name is required when more than one filesystem is configured")

// Lookup returns the configuration for the filesystem named fsname, which
// will be either a *Root or a *KeyRange, and the location within that
// filesystem that is specified by prefix.
//
// If fsname is empty and exactly one filesystem is configured then that
// filesystem is used, otherwise ErrFilesystemNameRequired is returned.
//
// For a *Root, an empty prefix refers to the root itself, a relative prefix
// is interpreted relative to the root and an absolute prefix is used as is.
// In all cases the resulting path must be contained within the root. The
// returned path is cleaned and has no trailing separator (other than for a
// root such as / or C:\).
//
// For a *KeyRange, an empty prefix refers to the entire range and is
// returned as the empty string. Otherwise prefix is a key prefix that is
// used as is and all of the keys that start with it must be within the
// range.
func (t T) Lookup(fsname, prefix string) (Filesystem, string, error) {
	f, err := t.lookupFS(fsname)
	if err != nil {
		return nil, "", err
	}
	target, err := f.lookup(prefix)
	if err != nil {
		return nil, "", fmt.Errorf("filesystem %v: %w", Label(f), err)
	}
	return f, target, nil
}

func (r *Root) lookup(prefix string) (string, error) {
	root := trimTrailingSep(r.Root)
	var path string
	switch {
	case len(prefix) == 0:
		path = root
	case filepath.IsAbs(prefix):
		// Avoid filepath.Clean here since on Windows it would rewrite
		// forward slashes that may be used in the configured roots.
		path = trimTrailingSep(prefix)
	default:
		path = trimTrailingSep(filepath.Join(root, prefix))
	}
	if !containsPath(filepath.Clean(root), filepath.Clean(path), caseInsensitivePaths) {
		return "", fmt.Errorf("%v is not contained within the root", prefix)
	}
	return path, nil
}

func (kr *KeyRange) lookup(prefix string) (string, error) {
	if len(prefix) == 0 {
		return "", nil
	}
	if !kr.ContainsPrefix(prefix) {
		return "", fmt.Errorf("the keys starting with %q are not all contained within the range", prefix)
	}
	return prefix, nil
}

func (t T) lookupFS(fsname string) (Filesystem, error) {
	if len(fsname) > 0 {
		if f, ok := t.ForName(fsname); ok {
			return f, nil
		}
		return nil, fmt.Errorf("no filesystem named %q is configured, configured filesystems: %v", fsname, t.filesystemNames())
	}
	switch len(t.Filesystems) {
	case 0:
		return nil, errors.New("no filesystems are configured")
	case 1:
		return t.Filesystems[0], nil
	}
	return nil, fmt.Errorf("%w: %v", ErrFilesystemNameRequired, t.filesystemNames())
}

func (t T) filesystemNames() string {
	names := make([]string, 0, len(t.Filesystems))
	for _, f := range t.Filesystems {
		names = append(names, Label(f))
	}
	return strings.Join(names, ", ")
}

// containsPath reports whether path is root or is contained within root,
// comparing whole path components only so that /tmpx is not considered to
// be within /tmp.
func containsPath(root, path string, caseInsensitive bool) bool {
	equal := func(a, b string) bool {
		if caseInsensitive {
			return strings.EqualFold(a, b)
		}
		return a == b
	}
	if equal(root, path) {
		return true
	}
	if len(path) <= len(root) || !equal(path[:len(root)], root) {
		return false
	}
	if isSep(root[len(root)-1]) {
		// root is / or C:\ etc.
		return true
	}
	return isSep(path[len(root)])
}

func isSep(c byte) bool {
	return c == '/' || c == filepath.Separator
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
