// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package statedir manages directories of timestamped entries, files or
// directories, such as stats snapshots and reports, together with a
// symbolic link, named latest, to the most recent entry.
package statedir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

// LatestName is the name of the link to the most recent entry.
const LatestName = "latest"

// ErrNoSymlinks is returned, wrapped, by SetLatest when the latest link
// cannot be created because symbolic links are not supported, eg. on
// Windows without developer mode or administrator privileges.
var ErrNoSymlinks = errors.New("symbolic links are not supported")

// Dir is a directory of entries whose names are their timestamps,
// formatted using a time layout, followed by an optional extension.
type Dir struct {
	root, layout, ext string
	legacy            []string
}

// New returns a Dir for the directory root, whose entries are named using
// layout and ext. Entries whose names use any of the legacy layouts are
// also recognized by Entries, but new entries are always named using
// layout.
func New(root, layout, ext string, legacy ...string) Dir {
	return Dir{root: root, layout: layout, ext: ext, legacy: legacy}
}

// Root returns the directory.
func (d Dir) Root() string {
	return d.root
}

// Name returns the name of the entry for when, which is formatted in
// local time.
func (d Dir) Name(when time.Time) string {
	return when.Local().Format(d.layout) + d.ext
}

// Path returns the path of the entry for when.
func (d Dir) Path(when time.Time) string {
	return filepath.Join(d.root, d.Name(when))
}

// LatestPath returns the path of the link to the most recent entry.
func (d Dir) LatestPath() string {
	return filepath.Join(d.root, LatestName)
}

// WriteFile writes data as the entry for when and makes it the latest
// entry. Existing entries are never overwritten: if there is already an
// entry with the same name, since names have the resolution of the layout,
// eg. a millisecond, the entry is written for the first subsequent time,
// at that resolution, for which there is none. The entry is written to a
// temporary file that is then renamed so that readers never see a
// partially written entry. Where symbolic links are not supported the
// latest entry is a copy instead.
func (d Dir) WriteFile(when time.Time, data []byte) (string, error) {
	if err := os.MkdirAll(d.root, 0o750); err != nil {
		return "", err
	}
	path := d.Path(when)
	for {
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			break
		}
		when = d.next(when)
		path = d.Path(when)
	}
	if err := writeAtomic(path, data); err != nil {
		return "", err
	}
	if err := d.SetLatest(when); err != nil {
		if !errors.Is(err, ErrNoSymlinks) {
			return path, err
		}
		return path, writeAtomic(d.LatestPath(), data)
	}
	return path, nil
}

// next returns the earliest time after when, in steps of a millisecond,
// whose entry has a different name to that of when.
func (d Dir) next(when time.Time) time.Time {
	name := d.Name(when)
	for {
		when = when.Add(time.Millisecond)
		if d.Name(when) != name {
			return when
		}
	}
}

func writeAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-"+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o640); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// MkdirAll creates the entry for when as a directory, together with the
// supplied sub-directories within it, and returns its path. It does not
// update the latest link, see SetLatest.
func (d Dir) MkdirAll(when time.Time, subdirs ...string) (string, error) {
	path := d.Path(when)
	for _, sd := range append([]string{""}, subdirs...) {
		if err := os.MkdirAll(filepath.Join(path, sd), 0o770); err != nil {
			return "", err
		}
	}
	return path, nil
}

// SetLatest makes the entry for when the latest entry by replacing the
// latest link. It returns an error that wraps ErrNoSymlinks if the link
// could not be created because symbolic links are not supported.
func (d Dir) SetLatest(when time.Time) error {
	link := d.LatestPath()
	tmp := filepath.Join(d.root, fmt.Sprintf(".%s-%d", LatestName, os.Getpid()))
	os.Remove(tmp)
	if err := os.Symlink(d.Name(when), tmp); err != nil {
		if runtime.GOOS == "windows" {
			return fmt.Errorf("failed to link %v to %v: %w: %w", link, d.Name(when), ErrNoSymlinks, err)
		}
		return err
	}
	if err := os.Rename(tmp, link); err != nil {
		os.Remove(tmp)
		// A copy, rather than a link, of a directory, eg. one created on
		// Windows, cannot be replaced by rename.
		if err := os.RemoveAll(link); err != nil {
			return err
		}
		if err := os.Symlink(d.Name(when), link); err != nil {
			return err
		}
	}
	return nil
}

// parse parses name using the layout, or any of the legacy layouts.
// Layouts without a time zone are formatted in local time.
func (d Dir) parse(name string) (time.Time, bool) {
	for _, layout := range append([]string{d.layout}, d.legacy...) {
		if when, err := time.ParseInLocation(layout, name, time.Local); err == nil {
			return when, true
		}
	}
	return time.Time{}, false
}

// Entry is an entry in a Dir.
type Entry struct {
	When  time.Time
	Name  string
	Path  string
	IsDir bool
}

// Entries returns the entries in the directory, most recent first. Names
// that do not match the layout and extension, such as the latest link,
// are ignored. A directory that does not exist has no entries.
func (d Dir) Entries() ([]Entry, error) {
	des, err := os.ReadDir(d.root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var entries []Entry
	for _, de := range des {
		name := de.Name()
		if de.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(name, d.ext) {
			continue
		}
		when, ok := d.parse(strings.TrimSuffix(name, d.ext))
		if !ok {
			continue
		}
		entries = append(entries, Entry{When: when, Name: name, Path: filepath.Join(d.root, name), IsDir: de.IsDir()})
	}
	slices.SortFunc(entries, func(a, b Entry) int { return b.When.Compare(a.When) })
	return entries, nil
}
