// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package tui implements an interactive, ncdu-style, browser for the
// directory hierarchy recorded by idu analyze.
package tui

import (
	"context"

	"cloudeng.io/cmd/idu/stats"
)

// Entry describes a directory. Own holds the totals for the files directly
// within it and Tree those for its entire subtree, including Own.
type Entry struct {
	Name string // final path component, or the full path for Listing.Self.
	Path string
	Own  stats.Totals
	Tree stats.Totals
	Dirs int64 // number of directories in the subtree, excluding itself.
}

// HasChildren returns true if the directory contains sub-directories
// that can be browsed.
func (e Entry) HasChildren() bool {
	return e.Dirs > 0
}

// Listing is the contents of a directory as displayed by the browser.
type Listing struct {
	Self     Entry
	Children []Entry
}

// Source provides directory listings to the browser. List may be slow,
// for example for the root of a large tree, and is therefore only ever
// called from a tea.Cmd, never from Update or View. Implementations must
// honor ctx cancellation.
//
// The browser depends only on this interface so that the storage used
// to compute listings can change, e.g. to read rolled-up tree totals
// stored by analyze, or a different database, without changing the UI.
type Source interface {
	List(ctx context.Context, path string) (Listing, error)
}
