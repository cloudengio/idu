// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package mcpserver

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"cloudeng.io/cmd/idu/iduconfig"
)

// lookup returns the filesystem to use for a tool call, see selectFS, and
// the location within it specified by prefix: an empty prefix refers to
// the entire filesystem, a relative one is relative to its root.
func (s *Server) lookup(fs, prefix string) (iduconfig.Filesystem, string, error) {
	f, err := s.selectFS(fs, prefix)
	if err != nil {
		return nil, "", err
	}
	// Resolve prefix within f alone, which need not have a name.
	return iduconfig.T{Filesystems: iduconfig.Filesystems{f}}.Lookup("", prefix)
}

// selectFS returns the filesystem to use for a tool call:
//
//  1. fs, if set, is the name, or the location, ie. root, of a configured
//     filesystem;
//  2. otherwise an absolute prefix selects the filesystem with the longest
//     root that contains it;
//  3. otherwise the default filesystem, see defaultFS.
func (s *Server) selectFS(fs, prefix string) (iduconfig.Filesystem, error) {
	if fs != "" {
		if f, ok := s.cfg.ForName(fs); ok {
			return f, nil
		}
		for _, f := range s.cfg.Filesystems {
			if samePath(f.Location(), fs) {
				return f, nil
			}
		}
		return nil, fmt.Errorf("no filesystem named, or with the root, %q is configured; configured filesystems: %v", fs, s.labels())
	}
	if isAbs(prefix) {
		if r, ok := s.cfg.ForPrefix(prefix); ok {
			return r, nil
		}
		return nil, fmt.Errorf("%v is not within any of the configured filesystems: %v", prefix, s.labels())
	}
	f, _, err := s.defaultFS()
	return f, err
}

// defaultFS returns the filesystem used when a tool call specifies neither
// a filesystem nor an absolute prefix and a description of why it is the
// default. It is, in order of preference: the one named by
// WithDefaultFilesystem, the only one configured, or the local filesystem,
// ie. the one whose root is / (or a volume such as C:\ on Windows).
func (s *Server) defaultFS() (iduconfig.Filesystem, string, error) {
	if s.opts.defaultFS != "" {
		f, err := s.selectFS(s.opts.defaultFS, "")
		return f, "configured as the default", err
	}
	switch len(s.cfg.Filesystems) {
	case 0:
		return nil, "", fmt.Errorf("no filesystems are configured")
	case 1:
		return s.cfg.Filesystems[0], "the only filesystem configured", nil
	}
	for _, f := range s.cfg.Filesystems {
		if r, ok := f.(*iduconfig.Root); ok && isVolumeRoot(r.Root) {
			return f, "the local filesystem, its root is " + r.Root, nil
		}
	}
	return nil, "", fmt.Errorf("%w and none of them is the local filesystem, ie. has a root of /: set fs to one of: %v, or use an absolute prefix", iduconfig.ErrFilesystemNameRequired, s.labels())
}

func (s *Server) labels() string {
	labels := make([]string, len(s.cfg.Filesystems))
	for i, f := range s.cfg.Filesystems {
		labels[i] = iduconfig.Label(f)
	}
	return strings.Join(labels, ", ")
}

// isAbs returns true if path is absolute, including a path that starts
// with a separator, but has no volume name, on Windows.
func isAbs(path string) bool {
	return filepath.IsAbs(path) || strings.HasPrefix(path, "/") || strings.HasPrefix(path, `\`)
}

// isVolumeRoot returns true if root is the root of a filesystem volume, ie.
// / on unix systems.
func isVolumeRoot(root string) bool {
	if root == "" {
		return false
	}
	c := filepath.Clean(root)
	return filepath.Dir(c) == c
}

func samePath(a, b string) bool {
	ca, cb := filepath.Clean(a), filepath.Clean(b)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(ca, cb)
	}
	return ca == cb
}
