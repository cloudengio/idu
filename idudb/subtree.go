// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"cloudeng.io/cmd/idu/stats"
)

// The recursive totals for every prefix, ie. the sum of the direct totals
// of the prefix and all of its descendants, are stored in the subtree
// bucket keyed by summary key. They are maintained incrementally by
// Analyze, when top-N rankings are enabled, and rebuilt from the summaries
// when that is not possible. Hardlinks are deduplicated using the link
// index, see linkChanges.
const metaKeySubtrees = "subtrees/v2"

const (
	subtreesValid = "valid"
	// subtreesDirty is stored while an analysis is updating the
	// recursive totals and remains if that analysis is interrupted or
	// fails, until they are rebuilt.
	subtreesDirty = "dirty"
)

// ErrStaleSubtrees is returned when the recursive totals are missing or
// may be out of date.
var ErrStaleSubtrees = errors.New("the recursive totals are missing or may be out of date: rerun analyze for the entire prefix")

// subtreesState returns the stored state of the recursive totals, which
// is empty if there are none.
func subtreesState(ctx context.Context, db Reader) (string, error) {
	val, err := getMeta(ctx, db, metaKeySubtrees)
	return string(val), err
}

func setSubtreesState(ctx context.Context, db Database, state string) error {
	return db.SetMeta(ctx, metaKeySubtrees, []byte(state))
}

// CheckSubtrees returns ErrStaleSubtrees unless db contains complete,
// up to date, recursive totals.
func CheckSubtrees(ctx context.Context, db Reader) error {
	state, err := subtreesState(ctx, db)
	if err != nil {
		return err
	}
	if state != subtreesValid {
		return ErrStaleSubtrees
	}
	return nil
}

// decodeSubtree decodes the recursive totals stored for path.
func decodeSubtree(path string, val []byte) (stats.Totals, error) {
	var t stats.Totals
	if err := t.UnmarshalBinary(val); err != nil {
		return t, fmt.Errorf("%v: recursive totals: %w", path, err)
	}
	return t, nil
}

// subtreeChanges accumulates the changes to the recursive totals made by
// an analysis, so that they can be applied once it has completed. Adding
// changes is order independent and hence they can be accumulated as they
// are reported by the concurrent walker. It is not safe for concurrent use.
type subtreeChanges struct {
	rootKey string
	// changes maps the summary key of every prefix whose recursive totals
	// have changed to the change.
	changes map[string]stats.Totals
	// deleted contains the summary keys of the prefixes whose subtrees
	// have been deleted.
	deleted map[string]bool
}

func newSubtreeChanges(rootKey string) *subtreeChanges {
	return &subtreeChanges{
		rootKey: rootKey,
		changes: map[string]stats.Totals{},
		deleted: map[string]bool{},
	}
}

// addToAncestors adds d to the changes for key, if self is true, and for
// every one of its ancestors up to and including the root.
func (sc *subtreeChanges) addToAncestors(key string, self bool, d stats.Totals) {
	if d.IsZero() {
		return
	}
	d.ID = 0
	if !self {
		if key == sc.rootKey {
			return
		}
		var ok bool
		if key, ok = stats.ParentSummaryKey(key); !ok {
			return
		}
	}
	for {
		c := sc.changes[key]
		c.Add(d)
		sc.changes[key] = c
		if key == sc.rootKey {
			return
		}
		parent, ok := stats.ParentSummaryKey(key)
		if !ok {
			return
		}
		key = parent
	}
}

// prefixChanged records that the direct totals for the prefix with summary
// key key changed from old, which is nil for a new prefix, to cur.
func (sc *subtreeChanges) prefixChanged(key string, old *stats.Totals, cur stats.Totals) {
	d := cur
	if old != nil {
		d.Sub(*old)
	}
	sc.addToAncestors(key, true, d)
}

// subtreeDeleted records that the subtree rooted at key, whose recursive
// totals are removed, has been deleted.
func (sc *subtreeChanges) subtreeDeleted(key string, removed stats.Totals) {
	var d stats.Totals
	d.Sub(removed)
	sc.addToAncestors(key, false, d)
	sc.deleted[key] = true
}

// isDeleted returns true if key, or any of its ancestors, was deleted.
func (sc *subtreeChanges) isDeleted(key string) bool {
	if len(sc.deleted) == 0 {
		return false
	}
	for {
		if sc.deleted[key] {
			return true
		}
		parent, ok := stats.ParentSummaryKey(key)
		if !ok || key == sc.rootKey {
			return false
		}
		key = parent
	}
}

// errInconsistentSubtrees is returned by apply if applying the changes
// results in negative totals, ie. if the stored recursive totals were
// inconsistent with the summaries.
var errInconsistentSubtrees = errors.New("recursive totals are inconsistent with the summaries")

// applyBatchSize is the number of recursive totals read at a time by apply.
const applyBatchSize = 1000

// apply adds the accumulated changes to the recursive totals stored in db,
// calling fn with the path and new recursive totals of every prefix that
// still exists. It does not flush the writes.
func (sc *subtreeChanges) apply(ctx context.Context, db Database, sep string, fn func(path string, t stats.Totals)) error {
	keys := slices.Sorted(maps.Keys(sc.changes))
	keys = slices.DeleteFunc(keys, sc.isDeleted)
	for batch := range slices.Chunk(keys, applyBatchSize) {
		cur := make([]stats.Totals, len(batch))
		err := db.GetSubtrees(ctx, batch, func(i int, val []byte) error {
			if val == nil {
				return nil
			}
			var err error
			cur[i], err = decodeSubtree(stats.DecodeSummaryKey(sep, batch[i]), val)
			return err
		})
		if err != nil {
			return err
		}
		for i, key := range batch {
			t := cur[i]
			t.Add(sc.changes[key])
			path := stats.DecodeSummaryKey(sep, key)
			if t.IsNegative() {
				return fmt.Errorf("%v: %w", path, errInconsistentSubtrees)
			}
			if t.IsZero() {
				if err := db.DeleteSubtrees(ctx, key, ""); err != nil {
					return err
				}
				continue
			}
			if err := db.SetSubtree(ctx, key, t.AppendBinary(nil), true); err != nil {
				return err
			}
			fn(path, t)
		}
	}
	return nil
}

// subtreeFrame is a prefix whose recursive totals are being computed by a
// subtreeAggregator.
type subtreeFrame struct {
	key, path string
	totals    stats.Totals
}

// subtreeAggregator computes the recursive totals for every prefix from
// their direct totals, which must be supplied in depth-first pre-order,
// ie. in summary key order, using memory proportional to the depth of the
// tree.
type subtreeAggregator struct {
	stack []subtreeFrame
	// done is called with the recursive totals for every prefix once
	// they are complete, ie. in depth-first post-order.
	done func(key, path string, t stats.Totals) error
}

// step adds the next prefix in depth-first pre-order.
func (sa *subtreeAggregator) step(key, path string, direct stats.Totals) error {
	for len(sa.stack) > 0 && !stats.IsSummaryDescendant(sa.stack[len(sa.stack)-1].key, key) {
		if err := sa.pop(); err != nil {
			return err
		}
	}
	direct.ID = 0
	sa.stack = append(sa.stack, subtreeFrame{key: key, path: path, totals: direct})
	return nil
}

// finish completes the recursive totals of the prefixes remaining on the
// stack.
func (sa *subtreeAggregator) finish() error {
	for len(sa.stack) > 0 {
		if err := sa.pop(); err != nil {
			return err
		}
	}
	return nil
}

func (sa *subtreeAggregator) pop() error {
	n := len(sa.stack) - 1
	f := sa.stack[n]
	sa.stack = sa.stack[:n]
	if n > 0 {
		sa.stack[n-1].totals.Add(f.totals)
	}
	return sa.done(f.key, f.path, f.totals)
}
