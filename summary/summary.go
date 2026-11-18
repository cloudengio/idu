// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package summary provides typed, read-only, access to the per-prefix
// summaries stored in an idu database. It hides the encoding of the
// summaries and their keys and checks that they were computed with the
// configuration that the database is being read with.
package summary

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"strings"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/hardlinks"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
)

// Entry is a single prefix summary.
type Entry struct {
	// Path is the filesystem path, or key prefix, of the summary.
	Path string
	// Key is the database key of the summary. Keys sort in depth-first
	// pre-order and can be used with report.SubtreeAggregator.
	Key string
	// Depth is the number of path components below the prefix that was
	// passed to Walk, ie. 0 for that prefix itself, 1 for its children etc.
	Depth   int
	Summary stats.PrefixSummary
}

// Reader provides access to the summaries stored in a database.
type Reader struct {
	db     idudb.Reader
	sep    string
	calcID string
	dedup  bool
}

// New returns a Reader for db, which must contain the database specified by
// cfg. Hardlinks are deduplicated by Walk unless cfg.CountHardlinkAsFiles is
// set.
func New(db idudb.Reader, cfg *iduconfig.Settings) *Reader {
	return &Reader{
		db:     db,
		sep:    cfg.SeparatorOrDefault(),
		calcID: cfg.CalculatorOrDefault().String(),
		dedup:  !cfg.CountHardlinkAsFiles,
	}
}

// Open opens the database specified by cfg for reading only and returns a
// Reader for it. The options must include idudb.WithBackendFactory. The
// database is closed by calling Close.
func Open(ctx context.Context, cfg *iduconfig.Settings, opts ...idudb.OpenOption) (*Reader, error) {
	db, err := idudb.Open(ctx, cfg, opts...)
	if err != nil {
		return nil, err
	}
	return New(db, cfg), nil
}

// DB returns the underlying database.
func (r *Reader) DB() idudb.Reader {
	return r.db
}

// Close closes the underlying database.
func (r *Reader) Close(ctx context.Context) error {
	return r.db.Close(ctx)
}

// Check returns idudb.ErrStaleSummaries unless the database contains a
// complete set of summaries computed with the configuration the Reader was
// created with.
func (r *Reader) Check(ctx context.Context) error {
	return idudb.CheckSummaries(ctx, r.db, r.calcID, r.dedup)
}

func (r *Reader) decode(path string, val []byte, ps *stats.PrefixSummary) error {
	if err := ps.UnmarshalBinary(val); err != nil {
		return fmt.Errorf("%v: %w", path, err)
	}
	if ps.CalcID != r.calcID || r.dedup && !ps.HardlinkInfo {
		return fmt.Errorf("%v: %w", path, idudb.ErrStaleSummaries)
	}
	return nil
}

// Get returns the summary for path, if there is one. Hardlinks are not
// deduplicated since doing so requires visiting every summary that
// precedes path, see Walk.
func (r *Reader) Get(ctx context.Context, path string) (stats.PrefixSummary, bool, error) {
	exactKey, _ := stats.SummaryKeys(r.sep, path)
	var ps stats.PrefixSummary
	found := false
	var decodeErr error
	err := r.db.ScanSummaries(ctx, exactKey, "", func(_ context.Context, _ string, val []byte) bool {
		found = true
		decodeErr = r.decode(path, val, &ps)
		return false
	})
	if err == nil {
		err = decodeErr
	}
	if err != nil || !found {
		return stats.PrefixSummary{}, false, err
	}
	return ps, true, nil
}

// WalkOption configures Walk.
type WalkOption func(*walkOptions)

type walkOptions struct {
	maxDepth int
}

// WithMaxDepth limits Walk to summaries that are at most depth levels
// below the prefix being walked, a negative value, the default, means no
// limit. Note that the cost of a walk is proportional to the size of the
// entire subtree regardless of depth.
func WithMaxDepth(depth int) WalkOption {
	return func(o *walkOptions) {
		o.maxDepth = depth
	}
}

// Walk returns an iterator over the summaries for path and all of its
// descendants in depth-first pre-order. Unless the Reader was created for
// a configuration that counts hardlinks as files, hardlinks are
// deduplicated such that the first link encountered in that order is the
// one that is counted. Iteration stops at the first error, which is
// returned as the final value of the iterator.
func (r *Reader) Walk(ctx context.Context, path string, opts ...WalkOption) iter.Seq2[Entry, error] {
	o := walkOptions{maxDepth: -1}
	for _, fn := range opts {
		fn(&o)
	}
	return func(yield func(Entry, error) bool) {
		var hl *hardlinks.Incremental
		if r.dedup {
			hl = &hardlinks.Incremental{}
		}
		exactKey, childPrefix := stats.SummaryKeys(r.sep, path)
		stopped := false
		var walkErr error
		err := r.db.ScanSummaries(ctx, exactKey, childPrefix, func(_ context.Context, key string, val []byte) bool {
			depth := keyDepth(exactKey, childPrefix, key)
			tooDeep := o.maxDepth >= 0 && depth > o.maxDepth
			if tooDeep && hl == nil {
				return true
			}
			e := Entry{Path: stats.DecodeSummaryKey(r.sep, key), Key: key, Depth: depth}
			if walkErr = r.decode(e.Path, val, &e.Summary); walkErr != nil {
				return false
			}
			if hl != nil {
				// Every summary must be visited, in order, for the
				// deduplication to be consistent with a complete walk.
				e.Summary.ApplyHardlinks(hl)
			}
			if tooDeep {
				return true
			}
			if !yield(e, nil) {
				stopped = true
				return false
			}
			return true
		})
		if stopped {
			return
		}
		if err == nil {
			err = walkErr
		}
		if err != nil {
			yield(Entry{}, err)
		}
	}
}

// keyDepth returns the depth of key relative to exactKey.
func keyDepth(exactKey, childPrefix, key string) int {
	if key == exactKey || len(key) < len(childPrefix) {
		return 0
	}
	return strings.Count(key[len(childPrefix):], "\x00") + 1
}

// Children returns the summaries for the immediate children of path, in
// the order in which they were recorded by the most recent analysis, along
// with the number of children that have no summary. Its cost is
// proportional to the number of children rather than the size of the
// subtree rooted at path. Hardlinks are not deduplicated, see Get.
func (r *Reader) Children(ctx context.Context, path string) ([]Entry, int, error) {
	var buf bytes.Buffer
	if err := r.db.Get(ctx, path, &buf); err != nil {
		return nil, 0, err
	}
	if buf.Len() == 0 {
		return nil, 0, nil
	}
	var pi prefixinfo.T
	if err := pi.UnmarshalBinary(buf.Bytes()); err != nil {
		return nil, 0, fmt.Errorf("%v: %w", path, err)
	}
	var children []Entry
	missing := 0
	for _, fi := range pi.PrefixesOnly() {
		child := join(r.sep, path, fi.Name())
		ps, ok, err := r.Get(ctx, child)
		if err != nil {
			return nil, 0, err
		}
		if !ok {
			missing++
			continue
		}
		key, _ := stats.SummaryKeys(r.sep, child)
		children = append(children, Entry{Path: child, Key: key, Depth: 1, Summary: ps})
	}
	return children, missing, nil
}

func join(sep, path, name string) string {
	if strings.HasSuffix(path, sep) {
		return path + name
	}
	return path + sep + name
}
