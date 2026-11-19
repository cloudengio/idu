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

// topnObserver is an Observer that maintains the top-N rankings stored in
// a DB. Changes reported during an analysis are applied to the rankings
// immediately and the rankings are written to the database by ScanDone.
// The rankings are instead recomputed from scratch by ScanDone (see
// refreshTopN) if they are missing, invalid, were created with a
// different configuration, if all of the summaries are being rewritten or
// if incremental maintenance has left any ranking with fewer than N
// entries that are known to be exact, or if any of the summaries replaced
// during the analysis could not be read.
type topnObserver struct {
	sdb *DB
	cfg topnConfig

	mu       sync.Mutex
	refresh  bool
	state    topn.State
	prefixes *topn.Rankings
	users    idRankings
	groups   idRankings
}

func newTopNObserver(sdb *DB) *topnObserver {
	return &topnObserver{sdb: sdb, cfg: sdb.topn}
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

func (t *topnObserver) ScanStarted(ctx context.Context, scan ScanInfo) error {
	db := t.sdb.db
	state, ok, err := getTopNState(ctx, db)
	if err != nil {
		ctxlog.Warn(ctx, "failed to read top-N state, the rankings will be refreshed", "error", err)
	}
	t.state = state
	t.refresh = err != nil || !ok || !state.Valid || !t.cfg.matches(state) || scan.Rewrite
	t.prefixes = topn.NewRankings(t.cfg.capacity)
	t.users = newIDRankings(state.Users, topn.UserKey)
	t.groups = newIDRankings(state.Groups, topn.GroupKey)
	if !t.refresh {
		if _, err := getRankings(ctx, db, topn.PrefixesKey, t.prefixes); err != nil {
			ctxlog.Warn(ctx, "failed to read top-N rankings, they will be refreshed", "error", err)
			t.refresh = true
		}
	}
	// Mark the rankings as invalid until ScanDone has written them.
	state.Valid = false
	return setTopNState(ctx, db, state)
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
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.refresh {
		return
	}
	t.prefixes.Set(path, cur.Direct)
	var oldUsers, oldGroups stats.PerIDTotals
	if old != nil {
		oldUsers, oldGroups = old.PerUser, old.PerGroup
	}
	t.setPerID(ctx, &t.users, path, oldUsers, cur.PerUser)
	if t.refresh {
		return
	}
	t.setPerID(ctx, &t.groups, path, oldGroups, cur.PerGroup)
}

func (t *topnObserver) SubtreeDeleted(ctx context.Context, _ string, summaries iter.Seq2[string, *stats.PrefixSummary]) {
	if t.isRefreshing() {
		return
	}
	for path, ps := range summaries {
		t.mu.Lock()
		if t.refresh {
			t.mu.Unlock()
			return
		}
		t.prefixes.Delete(path)
		for _, u := range ps.PerUser {
			t.deletePerID(ctx, &t.users, path, u.ID)
		}
		for _, g := range ps.PerGroup {
			t.deletePerID(ctx, &t.groups, path, g.ID)
		}
		t.mu.Unlock()
	}
}

func (t *topnObserver) isRefreshing() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.refresh
}

func (t *topnObserver) ScanDone(ctx context.Context, scanErr error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if scanErr != nil || t.sdb.rewriteSummaries {
		// The rankings remain marked as invalid since not all of the
		// changes, or summaries, are known to be complete.
		return nil
	}
	if t.sdb.previousSummaryFailed.Load() {
		// The ids that the prefix was previously attributed to are
		// unknown and hence it may remain, with a stale value, in the
		// rankings of ids that no longer use it.
		ctxlog.Warn(ctx, "previous summaries could not be read, the top-N rankings will be refreshed")
		t.refresh = true
	}
	if !t.refresh {
		t.refresh = t.prefixes.NeedsRefresh(t.cfg.n) ||
			t.users.needsRefresh(t.cfg.perIDN) ||
			t.groups.needsRefresh(t.cfg.perIDN)
	}
	if t.refresh {
		return refreshTopN(ctx, t.sdb, t.state)
	}
	return t.write(ctx)
}

func (t *topnObserver) write(ctx context.Context) error {
	db := t.sdb.db
	if err := db.SetMeta(ctx, topn.PrefixesKey, t.prefixes.AppendBinary(nil)); err != nil {
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

// RefreshTopN recomputes all of the top-N rankings from the summaries
// stored in the database. It returns ErrStaleSummaries if the summaries
// are incomplete or were computed with a different configuration, ie.
// if the entire filesystem needs to be analyzed first.
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
	prev, ok, err := getTopNState(ctx, sdb.db)
	if err != nil {
		ctxlog.Warn(ctx, "failed to read top-N state", "error", err)
	}
	if ok && prev.Valid {
		prev.Valid = false
		if err := setTopNState(ctx, sdb.db, prev); err != nil {
			return err
		}
	}
	return refreshTopN(ctx, sdb, prev)
}

// refreshTopN recomputes all of the rankings from the summaries, replacing
// those described by prev.
func refreshTopN(ctx context.Context, sdb *DB, prev topn.State) error {
	t := newTopNObserver(sdb)
	t.prefixes = topn.NewRankings(t.cfg.capacity)
	t.users = newIDRankings(prev.Users, topn.UserKey)
	t.groups = newIDRankings(prev.Groups, topn.GroupKey)
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
	var decodeErr error
	err := sdb.db.ScanSummaries(ctx, "", "", func(_ context.Context, key string, val []byte) bool {
		path := stats.DecodeSummaryKey(sdb.sep, key)
		if decodeErr = ps.UnmarshalBinary(val); decodeErr != nil {
			decodeErr = fmt.Errorf("%v: %w", path, decodeErr)
			return false
		}
		t.prefixes.Set(path, ps.Direct)
		set(&t.users, path, ps.PerUser)
		set(&t.groups, path, ps.PerGroup)
		return true
	})
	if err == nil {
		err = decodeErr
	}
	if err != nil {
		return fmt.Errorf("failed to refresh top-N rankings: %w", err)
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
	return t.write(ctx)
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
