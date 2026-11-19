// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"

	"cloudeng.io/cmd/idu/internal/topn"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/logging/ctxlog"
)

// topnConfig configures the incrementally maintained top-N rankings.
type topnConfig struct {
	n, capacity           int
	perIDN, perIDCapacity int
}

func (c topnConfig) enabled() bool {
	return c.n > 0
}

func (c topnConfig) matches(s topn.State) bool {
	return s.N == c.n && s.Capacity == c.capacity &&
		s.PerIDN == c.perIDN && s.PerIDCapacity == c.perIDCapacity
}

// idRankings holds the per-user or per-group rankings, which are loaded
// from the database on first use.
type idRankings struct {
	stored   map[int64]bool // ids with rankings stored in the database
	loaded   map[int64]*topn.Rankings
	modified map[int64]bool
	key      func(int64) string
}

func newIDRankings(stored []int64, key func(int64) string) idRankings {
	ir := idRankings{
		stored:   map[int64]bool{},
		loaded:   map[int64]*topn.Rankings{},
		modified: map[int64]bool{},
		key:      key,
	}
	for _, id := range stored {
		ir.stored[id] = true
	}
	return ir
}

// get returns the rankings for id, loading them from db if necessary. An id
// with no stored rankings has never had any usage and hence its rankings
// start out empty and exact.
func (ir *idRankings) get(ctx context.Context, db Database, id int64, capacity int) (*topn.Rankings, error) {
	if r, ok := ir.loaded[id]; ok {
		return r, nil
	}
	r := topn.NewRankings(capacity)
	if ir.stored[id] {
		if _, err := getRankings(ctx, db, ir.key(id), r); err != nil {
			return nil, err
		}
	}
	ir.loaded[id] = r
	return r, nil
}

func (ir *idRankings) needsRefresh(n int) bool {
	for _, r := range ir.loaded {
		if r.NeedsRefresh(n) {
			return true
		}
	}
	return false
}

// write writes the modified rankings to db and returns the ids that have
// stored rankings after doing so. Rankings that no longer convey any
// information are deleted by storing an empty value.
func (ir *idRankings) write(ctx context.Context, db Database) ([]int64, error) {
	for id := range ir.modified {
		r := ir.loaded[id]
		if r.Empty() {
			if ir.stored[id] {
				if err := db.SetMeta(ctx, ir.key(id), nil); err != nil {
					return nil, err
				}
			}
			delete(ir.stored, id)
			continue
		}
		if err := db.SetMeta(ctx, ir.key(id), r.AppendBinary(nil)); err != nil {
			return nil, err
		}
		ir.stored[id] = true
	}
	return slices.Sorted(maps.Keys(ir.stored)), nil
}

// topnObserver is an Observer that maintains the state derived from the
// summaries stored in a DB, namely:
//   - the recursive totals for every prefix, see subtreeChanges;
//   - the top-N rankings of the prefixes by their direct and recursive
//     totals, and by their direct totals for every user and group;
//   - the totals, across the entire filesystem, for every user and group.
//
// Changes reported during an analysis are applied to the rankings and the
// per-id totals immediately, and accumulated for the recursive totals.
// All of them are written to the database by ScanDone.
//
// The rankings, and per-id totals, are instead recomputed from scratch by
// ScanDone (see refreshTopN) if they are missing, invalid, were created
// with a different configuration, if incremental maintenance has left any
// ranking with fewer than N entries that are known to be exact, or if the
// recursive totals are being rebuilt. The recursive totals are rebuilt,
// as part of the same pass over the summaries, if they are missing or were
// left dirty by an interrupted analysis, if all of the summaries are
// being rewritten, if any of the summaries replaced during the analysis
// could not be read, or if they are found to be inconsistent.
type topnObserver struct {
	sdb *DB
	cfg topnConfig

	mu       sync.Mutex
	refresh  bool // recompute the rankings and per-id totals
	rebuild  bool // recompute the recursive totals, implies refresh
	state    topn.State
	prefixes *topn.Rankings
	subtrees *topn.Rankings
	users    idRankings
	groups   idRankings
	ids      *topn.IDTotals
	changes  *subtreeChanges
}

func newTopNObserver(sdb *DB) *topnObserver {
	return &topnObserver{sdb: sdb, cfg: sdb.topn}
}

// reset discards all rankings and per-id totals, replacing those described
// by prev with empty ones.
func (t *topnObserver) reset(prev topn.State) {
	t.state = prev
	t.prefixes = topn.NewRankings(t.cfg.capacity)
	t.subtrees = topn.NewRankings(t.cfg.capacity)
	t.users = newIDRankings(prev.Users, topn.UserKey)
	t.groups = newIDRankings(prev.Groups, topn.GroupKey)
	t.ids = topn.NewIDTotals()
}

func getMeta(ctx context.Context, db Reader, key string) ([]byte, error) {
	var buf bytes.Buffer
	if err := db.GetMeta(ctx, key, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// getTopNState returns the stored state of the rankings, if any.
func getTopNState(ctx context.Context, db Reader) (topn.State, bool, error) {
	var s topn.State
	val, err := getMeta(ctx, db, topn.StateKey)
	if err != nil || len(val) == 0 {
		return s, false, err
	}
	if err := s.UnmarshalBinary(val); err != nil {
		return s, false, err
	}
	return s, true, nil
}

// getRankings reads the rankings stored for key, if any, into r.
func getRankings(ctx context.Context, db Reader, key string, r *topn.Rankings) (bool, error) {
	val, err := getMeta(ctx, db, key)
	if err != nil || len(val) == 0 {
		return false, err
	}
	if err := r.UnmarshalBinary(val); err != nil {
		return false, fmt.Errorf("%v: %w", key, err)
	}
	return true, nil
}

func setTopNState(ctx context.Context, db Database, s topn.State) error {
	return db.SetMeta(ctx, topn.StateKey, s.AppendBinary(nil))
}

// load reads the stored rankings and per-id totals.
func (t *topnObserver) load(ctx context.Context) error {
	db := t.sdb.db
	for _, r := range []struct {
		key string
		rk  *topn.Rankings
	}{{topn.PrefixesKey, t.prefixes}, {topn.SubtreesKey, t.subtrees}} {
		found, err := getRankings(ctx, db, r.key, r.rk)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%v: not found", r.key)
		}
	}
	val, err := getMeta(ctx, db, topn.IDTotalsKey)
	if err != nil {
		return err
	}
	if len(val) == 0 {
		return fmt.Errorf("%v: not found", topn.IDTotalsKey)
	}
	return t.ids.UnmarshalBinary(val)
}

func (t *topnObserver) ScanStarted(ctx context.Context, scan ScanInfo) error {
	db := t.sdb.db
	state, ok, err := getTopNState(ctx, db)
	if err != nil {
		ctxlog.Warn(ctx, "failed to read top-N state, the rankings will be refreshed", "error", err)
	}
	subState, serr := subtreesState(ctx, db)
	if serr != nil {
		ctxlog.Warn(ctx, "failed to read the state of the recursive totals, they will be rebuilt", "error", serr)
	}
	t.reset(state)
	t.rebuild = serr != nil || subState != subtreesValid || scan.Rewrite
	t.refresh = t.rebuild || err != nil || !ok || !state.Valid || !t.cfg.matches(state)
	rootKey, _ := stats.SummaryKeys(t.sdb.sep, scan.Root)
	t.changes = newSubtreeChanges(rootKey)
	if !t.refresh {
		if err := t.load(ctx); err != nil {
			ctxlog.Warn(ctx, "failed to read top-N rankings, they will be refreshed", "error", err)
			t.refresh = true
		}
	}
	// Mark the rankings and recursive totals as invalid until ScanDone
	// has written them.
	state.Valid = false
	if err := setTopNState(ctx, db, state); err != nil {
		return err
	}
	return setSubtreesState(ctx, db, subtreesDirty)
}

// fail records that incremental maintenance has failed and hence that the
// rankings must be refreshed. It must be called with t.mu held.
func (t *topnObserver) fail(ctx context.Context, err error) {
	ctxlog.Warn(ctx, "failed to update top-N rankings, they will be refreshed", "error", err)
	t.refresh = true
}

func (t *topnObserver) setPerID(ctx context.Context, ir *idRankings, path string, old, cur stats.PerIDTotals) {
	db := t.sdb.db
	for _, id := range cur {
		r, err := ir.get(ctx, db, id.ID, t.cfg.perIDCapacity)
		if err != nil {
			t.fail(ctx, err)
			return
		}
		r.Set(path, id)
		ir.modified[id.ID] = true
	}
	for _, id := range old {
		if slices.ContainsFunc(cur, func(c stats.Totals) bool { return c.ID == id.ID }) {
			continue
		}
		t.deletePerID(ctx, ir, path, id.ID)
		if t.refresh {
			return
		}
	}
}

func (t *topnObserver) deletePerID(ctx context.Context, ir *idRankings, path string, id int64) {
	if !ir.stored[id] {
		if _, ok := ir.loaded[id]; !ok {
			return
		}
	}
	r, err := ir.get(ctx, t.sdb.db, id, t.cfg.perIDCapacity)
	if err != nil {
		t.fail(ctx, err)
		return
	}
	r.Delete(path)
	ir.modified[id] = true
}

func (t *topnObserver) PrefixChanged(ctx context.Context, path string, old, cur *stats.PrefixSummary) {
	key, _ := stats.SummaryKeys(t.sdb.sep, path)
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.rebuild {
		var od *stats.Totals
		if old != nil {
			od = &old.Direct
		}
		t.changes.prefixChanged(key, od, cur.Direct)
	}
	if t.refresh {
		return
	}
	t.prefixes.Set(path, cur.Direct)
	var oldUsers, oldGroups stats.PerIDTotals
	if old != nil {
		oldUsers, oldGroups = old.PerUser, old.PerGroup
		t.ids.Sub(oldUsers, oldGroups)
	}
	t.ids.Add(cur.PerUser, cur.PerGroup)
	t.setPerID(ctx, &t.users, path, oldUsers, cur.PerUser)
	if t.refresh {
		return
	}
	t.setPerID(ctx, &t.groups, path, oldGroups, cur.PerGroup)
}

func (t *topnObserver) SubtreeDeleted(ctx context.Context, path string, summaries iter.Seq2[string, *stats.PrefixSummary]) {
	if t.isRebuilding() {
		return
	}
	// The recursive totals of path are the sum of the direct totals of
	// all of the prefixes being deleted.
	var removed stats.Totals
	for p, ps := range summaries {
		removed.Add(ps.Direct)
		t.mu.Lock()
		if !t.refresh {
			t.prefixes.Delete(p)
			t.subtrees.Delete(p)
			t.ids.Sub(ps.PerUser, ps.PerGroup)
			for _, u := range ps.PerUser {
				t.deletePerID(ctx, &t.users, p, u.ID)
				if t.refresh {
					break
				}
			}
			for _, g := range ps.PerGroup {
				if t.refresh {
					break
				}
				t.deletePerID(ctx, &t.groups, p, g.ID)
			}
		}
		t.mu.Unlock()
	}
	// If the iteration fails the deletion, and hence the analysis, fails
	// and the changes are not applied.
	key, _ := stats.SummaryKeys(t.sdb.sep, path)
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.rebuild {
		t.changes.subtreeDeleted(key, removed)
	}
}

func (t *topnObserver) isRebuilding() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rebuild
}

func (t *topnObserver) ScanDone(ctx context.Context, scanErr error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if scanErr != nil || t.sdb.rewriteSummaries {
		// The rankings and recursive totals remain marked as invalid
		// since not all of the changes, or summaries, are known to be
		// complete.
		return nil
	}
	if t.sdb.previousSummaryFailed.Load() {
		// The previous totals of the prefix are unknown and hence so are
		// the changes to the recursive totals and the ids that the prefix
		// was previously attributed to, so it may remain, with a stale
		// value, in the rankings of ids that no longer use it.
		ctxlog.Warn(ctx, "previous summaries could not be read, the recursive totals and top-N rankings will be rebuilt")
		t.rebuild, t.refresh = true, true
	}
	if !t.rebuild {
		if err := t.applySubtrees(ctx); err != nil {
			if !errors.Is(err, errInconsistentSubtrees) {
				return err
			}
			ctxlog.Warn(ctx, "the recursive totals and top-N rankings will be rebuilt", "error", err)
			t.rebuild, t.refresh = true, true
		}
	}
	if !t.refresh && t.ids.IsNegative() {
		ctxlog.Warn(ctx, "per-user or per-group totals are inconsistent with the summaries, the top-N rankings will be refreshed")
		t.refresh = true
	}
	if !t.refresh {
		t.refresh = t.prefixes.NeedsRefresh(t.cfg.n) ||
			t.subtrees.NeedsRefresh(t.cfg.n) ||
			t.users.needsRefresh(t.cfg.perIDN) ||
			t.groups.needsRefresh(t.cfg.perIDN)
	}
	if t.refresh {
		return refreshTopN(ctx, t.sdb, t.state, t.rebuild)
	}
	if err := t.write(ctx); err != nil {
		return err
	}
	return setSubtreesState(ctx, t.sdb.db, subtreesValid)
}

// applySubtrees applies the accumulated changes to the stored recursive
// totals and the subtree rankings.
func (t *topnObserver) applySubtrees(ctx context.Context) error {
	db := t.sdb.db
	err := t.changes.apply(ctx, db, t.sdb.sep, func(path string, tot stats.Totals) {
		if !t.refresh {
			t.subtrees.Set(path, tot)
		}
	})
	if err != nil {
		return err
	}
	return db.Flush(ctx)
}

func (t *topnObserver) write(ctx context.Context) error {
	db := t.sdb.db
	if err := db.SetMeta(ctx, topn.PrefixesKey, t.prefixes.AppendBinary(nil)); err != nil {
		return err
	}
	if err := db.SetMeta(ctx, topn.SubtreesKey, t.subtrees.AppendBinary(nil)); err != nil {
		return err
	}
	if err := db.SetMeta(ctx, topn.IDTotalsKey, t.ids.AppendBinary(nil)); err != nil {
		return err
	}
	users, err := t.users.write(ctx, db)
	if err != nil {
		return err
	}
	groups, err := t.groups.write(ctx, db)
	if err != nil {
		return err
	}
	return setTopNState(ctx, db, t.newState(users, groups))
}

func (t *topnObserver) newState(users, groups []int64) topn.State {
	return topn.State{
		Valid:         true,
		N:             t.cfg.n,
		Capacity:      t.cfg.capacity,
		PerIDN:        t.cfg.perIDN,
		PerIDCapacity: t.cfg.perIDCapacity,
		Users:         users,
		Groups:        groups,
	}
}

// ErrTopNDisabled is returned by RefreshTopN if the DB was not created
// with WithTopN.
var ErrTopNDisabled = errors.New("top-N rankings are not enabled for this database")

// RefreshTopN rebuilds the recursive totals, and recomputes all of the
// top-N rankings and per-id totals, from the summaries stored in the
// database. It returns ErrStaleSummaries if the summaries are incomplete
// or were computed with a different configuration, ie. if the entire
// filesystem needs to be analyzed first.
func (sdb *DB) RefreshTopN(ctx context.Context) error {
	if !sdb.topn.enabled() {
		return ErrTopNDisabled
	}
	if sdb.rewriteSummaries {
		return ErrStaleSummaries
	}
	if err := CheckSummaryKeys(ctx, sdb.db); err != nil {
		return err
	}
	prev, err := sdb.invalidateTopN(ctx)
	if err != nil {
		return err
	}
	return refreshTopN(ctx, sdb, prev, true)
}

// invalidateTopN marks the stored rankings and recursive totals, if any,
// as invalid and returns the previous state of the rankings.
func (sdb *DB) invalidateTopN(ctx context.Context) (topn.State, error) {
	prev, ok, err := getTopNState(ctx, sdb.db)
	if err != nil {
		ctxlog.Warn(ctx, "failed to read top-N state", "error", err)
	}
	if ok && prev.Valid {
		prev.Valid = false
		if err := setTopNState(ctx, sdb.db, prev); err != nil {
			return prev, err
		}
	}
	state, err := subtreesState(ctx, sdb.db)
	if err != nil {
		ctxlog.Warn(ctx, "failed to read the state of the recursive totals", "error", err)
	}
	if err != nil || state == subtreesValid {
		if err := setSubtreesState(ctx, sdb.db, subtreesDirty); err != nil {
			return prev, err
		}
	}
	return prev, nil
}

// refreshTopN recomputes all of the rankings and per-id totals from the
// summaries, replacing those described by prev, and if rebuild is true,
// also rebuilds the recursive totals. It must only be called when the
// recursive totals are either being rebuilt or are known to be up to date.
func refreshTopN(ctx context.Context, sdb *DB, prev topn.State, rebuild bool) error {
	db := sdb.db
	if rebuild {
		if err := db.Flush(ctx); err != nil {
			return err
		}
		if err := db.ClearSubtrees(ctx); err != nil {
			return err
		}
	}
	t := newTopNObserver(sdb)
	t.reset(prev)
	agg := subtreeAggregator{done: func(key, path string, tot stats.Totals) error {
		t.subtrees.Set(path, tot)
		if rebuild {
			return db.SetSubtree(ctx, key, tot.AppendBinary(nil), true)
		}
		return nil
	}}
	if err := t.scanSummaries(ctx, &agg); err != nil {
		return fmt.Errorf("failed to refresh top-N rankings: %w", err)
	}
	if err := db.Flush(ctx); err != nil {
		return err
	}
	// Delete the rankings for ids that no longer have any usage.
	for _, ir := range []*idRankings{&t.users, &t.groups} {
		for id := range ir.stored {
			if _, ok := ir.loaded[id]; !ok {
				ir.loaded[id] = topn.NewRankings(t.cfg.perIDCapacity)
				ir.modified[id] = true
			}
		}
	}
	if err := t.write(ctx); err != nil {
		return err
	}
	return setSubtreesState(ctx, db, subtreesValid)
}

// scanSummaries computes the rankings, per-id totals and recursive totals
// from all of the summaries stored in the database.
func (t *topnObserver) scanSummaries(ctx context.Context, agg *subtreeAggregator) error {
	set := func(ir *idRankings, path string, ids stats.PerIDTotals) {
		for _, id := range ids {
			r, ok := ir.loaded[id.ID]
			if !ok {
				r = topn.NewRankings(t.cfg.perIDCapacity)
				ir.loaded[id.ID] = r
			}
			r.Set(path, id)
			ir.modified[id.ID] = true
		}
	}
	var ps stats.PrefixSummary
	var visitErr error
	err := t.sdb.db.ScanSummaries(ctx, "", "", func(_ context.Context, key string, val []byte) bool {
		path := stats.DecodeSummaryKey(t.sdb.sep, key)
		if visitErr = ps.UnmarshalBinary(val); visitErr != nil {
			visitErr = fmt.Errorf("%v: %w", path, visitErr)
			return false
		}
		t.prefixes.Set(path, ps.Direct)
		set(&t.users, path, ps.PerUser)
		set(&t.groups, path, ps.PerGroup)
		t.ids.Add(ps.PerUser, ps.PerGroup)
		visitErr = agg.step(key, path, ps.Direct)
		return visitErr == nil
	})
	if err == nil {
		err = visitErr
	}
	if err != nil {
		return err
	}
	return agg.finish()
}

// multiObserver forwards all calls to each of its observers in turn.
type multiObserver []Observer

func (m multiObserver) ScanStarted(ctx context.Context, scan ScanInfo) error {
	for i, o := range m {
		if err := o.ScanStarted(ctx, scan); err != nil {
			// Tell the observers that have started that the scan failed.
			for _, s := range m[:i] {
				_ = s.ScanDone(ctx, err)
			}
			return err
		}
	}
	return nil
}

func (m multiObserver) PrefixChanged(ctx context.Context, path string, old, cur *stats.PrefixSummary) {
	for _, o := range m {
		o.PrefixChanged(ctx, path, old, cur)
	}
}

func (m multiObserver) SubtreeDeleted(ctx context.Context, path string, summaries iter.Seq2[string, *stats.PrefixSummary]) {
	for _, o := range m {
		o.SubtreeDeleted(ctx, path, summaries)
	}
}

func (m multiObserver) ScanDone(ctx context.Context, scanErr error) error {
	var errs []error
	for _, o := range m {
		errs = append(errs, o.ScanDone(ctx, scanErr))
	}
	return errors.Join(errs...)
}
