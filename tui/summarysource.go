// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package tui

import (
	"context"
	"fmt"
	"strings"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/hardlinks"
	"cloudeng.io/cmd/idu/stats"
)

// SummarySource is a Source that computes listings from the per-directory
// summaries stored by analyze.
//
// The summaries record only the files directly within each directory, so
// List scans the entire subtree of the requested path to compute the tree
// totals of each child. Summary keys sort in DFS pre-order with each
// child's subtree contiguous, so a single sequential scan suffices, but
// listing the root of a tree is as expensive as 'idu stats compute'. The
// browser caches listings so that this cost is paid once per directory.
// Once analyze stores rolled-up tree totals a Source that reads them
// directly can replace this one.
//
// Hardlinks are deduplicated within the subtree being listed, i.e. as if
// it were a standalone tree, provided that the summaries record hardlinks
// (see iduconfig.Prefix.RecordHardlinks). Otherwise each link is counted.
type SummarySource struct {
	db     idudb.Reader
	sep    string
	calcID string
	dedup  bool
}

// NewSummarySource returns a SummarySource for the database db configured
// by cfg.
func NewSummarySource(db idudb.Reader, cfg *iduconfig.Prefix) *SummarySource {
	return &SummarySource{
		db:     db,
		sep:    cfg.SeparatorOrDefault(),
		calcID: cfg.CalculatorOrDefault().String(),
		dedup:  cfg.RecordHardlinks(),
	}
}

// List implements Source.
func (s *SummarySource) List(ctx context.Context, path string) (Listing, error) {
	if err := idudb.CheckSummaries(ctx, s.db, s.calcID, s.dedup); err != nil {
		return Listing{}, err
	}
	var hl *hardlinks.Incremental
	if s.dedup {
		hl = &hardlinks.Incremental{}
	}
	exactKey, childPrefix := stats.SummaryKeys(s.sep, path)
	l := Listing{Self: Entry{Name: path, Path: path}}
	var cur *Entry
	var visitErr error
	err := s.db.ScanSummaries(ctx, exactKey, childPrefix, func(_ context.Context, k string, v []byte) bool {
		var ps stats.PrefixSummary
		if err := ps.UnmarshalBinary(v); err != nil {
			visitErr = fmt.Errorf("%v: %w", stats.DecodeSummaryKey(s.sep, k), err)
			return false
		}
		if ps.CalcID != s.calcID {
			visitErr = fmt.Errorf("%v: %w", stats.DecodeSummaryKey(s.sep, k), idudb.ErrStaleSummaries)
			return false
		}
		if hl != nil {
			ps.ApplyHardlinks(hl)
		}
		l.Self.Tree.Add(ps.Direct)
		if k == exactKey {
			l.Self.Own = ps.Direct
			return true
		}
		l.Self.Dirs++
		name, _, nested := strings.Cut(strings.TrimPrefix(k, childPrefix), "\x00")
		if cur == nil || cur.Name != name {
			l.Children = append(l.Children, Entry{
				Name: name,
				Path: stats.DecodeSummaryKey(s.sep, childPrefix+name),
			})
			cur = &l.Children[len(l.Children)-1]
		}
		cur.Tree.Add(ps.Direct)
		if nested {
			cur.Dirs++
		} else {
			cur.Own = ps.Direct
		}
		return true
	})
	if err == nil {
		err = visitErr
	}
	if err != nil {
		return Listing{}, err
	}
	return l, nil
}
