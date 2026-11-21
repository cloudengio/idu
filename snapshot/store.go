// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package snapshot

import (
	"bytes"
	"time"

	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/internal/statedir"
	"cloudeng.io/cmd/idu/stats"
)

// Ext is the extension of snapshot files.
const Ext = ".idustats"

// Store is a directory of snapshots, each named by the time it was
// created, with a link, named latest, to the most recent.
type Store struct {
	dir statedir.Dir
}

// NewStore returns the Store for dir.
func NewStore(dir string) Store {
	return Store{dir: statedir.New(dir, cli.StatsFileLayout, Ext, cli.LegacyStatsFileLayouts...)}
}

// Dir returns the directory of the store.
func (s Store) Dir() string {
	return s.dir.Root()
}

// LatestPath returns the path of the link to the most recent snapshot.
func (s Store) LatestPath() string {
	return s.dir.LatestPath()
}

// Save writes f as a new snapshot, named by f.Date, and makes it the latest.
func (s Store) Save(f File) (string, error) {
	var buf bytes.Buffer
	if err := f.Encode(&buf); err != nil {
		return "", err
	}
	return s.dir.WriteFile(f.Date, buf.Bytes())
}

// FreshFor returns the most recent snapshot of the entire filesystem
// rooted at root, ie. one for which Whole is true, that was created after
// since and for which accept, if not nil, returns true, if there is one.
func (s Store) FreshFor(sep, root string, since time.Time, accept func(File) bool) (File, string, bool, error) {
	entries, err := s.dir.Entries()
	if err != nil {
		return File{}, "", false, err
	}
	rootKey, _ := stats.SummaryKeys(sep, root)
	// Entry names have a resolution of a millisecond.
	oldest := since.Truncate(time.Millisecond)
	for _, e := range entries {
		if e.IsDir {
			continue
		}
		if e.When.Before(oldest) {
			break
		}
		f, err := Load(e.Path)
		if err != nil {
			return File{}, "", false, err
		}
		if key, _ := stats.SummaryKeys(sep, f.Prefix); f.Whole && key == rootKey && f.Date.After(since) && (accept == nil || accept(f)) {
			return f, e.Path, true, nil
		}
	}
	return File{}, "", false, nil
}
