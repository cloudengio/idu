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
// is interpreted relative to the root and an absolute prefix, including one
// that starts with a separator but has no volume name on Windows, is used
// as is.
// In all cases the resulting path is cleaned (see filepath.Clean), and hence
// has no trailing separator other than for a root such as / or C:\, and
// must be contained within the root. On Windows, the returned path uses
// forward slashes if the root or absolute prefix uses only forward slashes.
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
	root := cleanPath(r.Root)
	path := root
	switch {
	case isRooted(prefix):
		path = cleanPath(prefix)
	case len(prefix) > 0:
		// filepath.Join cleans its result.
		path = matchSeparators(root, filepath.Join(root, prefix))
	}
	if !containsPath(root, path, caseInsensitivePaths) {
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
// be within /tmp. Both root and path must already be cleaned.
func containsPath(root, path string, caseInsensitive bool) bool {
	// Allow for a mix of / and \ on Windows, ToSlash is a no-op elsewhere.
	root, path = filepath.ToSlash(root), filepath.ToSlash(path)
	if pathEqual(root, path, caseInsensitive) {
		return true
	}
	if len(path) <= len(root) || !pathEqual(path[:len(root)], root, caseInsensitive) {
		return false
	}
	if root[len(root)-1] == '/' {
		// root is / or C:/ etc.
		return true
	}
	return path[len(root)] == '/'
}

func pathEqual(a, b string, caseInsensitive bool) bool {
	if caseInsensitive {
		return strings.EqualFold(a, b)
	}
	return a == b
}

// isRooted reports whether p is an absolute path or, on Windows, a path that
// starts with a separator but has no volume name (eg. /tmp or \tmp), which
// filepath.IsAbs does not consider to be absolute but which must not be
// interpreted relative to a configured root, which may itself be of that
// form.
func isRooted(p string) bool {
	return filepath.IsAbs(p) || (len(p) > 0 && (p[0] == '/' || p[0] == filepath.Separator))
}

// cleanPath returns filepath.Clean(p), but on Windows, where filepath.Clean
// converts / to \, it preserves the use of forward slashes if p uses only
// forward slashes, as may be the case for configured roots.
func cleanPath(p string) string {
	return matchSeparators(p, filepath.Clean(p))
}

// matchSeparators returns p using forward slashes if orig uses only
// forward slashes and the platform separator is not /.
func matchSeparators(orig, p string) string {
	if filepath.Separator != '/' && strings.ContainsRune(orig, '/') && !strings.ContainsRune(orig, filepath.Separator) {
		return filepath.ToSlash(p)
	}
	return p
}
