// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package summary

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"cloudeng.io/cmd/idu/internal/topn"
	"cloudeng.io/cmd/idu/stats"
)

var (
	// ErrNoTopN is returned when the database contains no top-N rankings,
	// ie. when it was not analyzed with them enabled.
	ErrNoTopN = errors.New("the database contains no top-N rankings: rerun analyze")
	// ErrStaleTopN is returned when the stored top-N rankings may be out
	// of date because the most recent analysis was interrupted or failed,
	// or did not cover the entire filesystem after a configuration change.
	ErrStaleTopN = errors.New("the top-N rankings may be out of date: rerun analyze for the entire prefix")
)

// Ranked is a single entry in a ranking.
type Ranked struct {
	Path  string
	Value int64
}

// TopN is a ranking of prefixes by a single stats.Metric, as maintained by
// analyze. Note that hardlinks are not deduplicated.
type TopN struct {
	// Entries are in descending order of Value.
	Entries []Ranked
	// Certified is the number of leading Entries that are known to be
	// ranked exactly. The remaining entries may be out of order with
	// respect to prefixes not included in the ranking. It is normally
	// equal to len(Entries).
	Certified int
	// N is the number of entries that analyze maintains for this ranking,
	// requests for more than N entries may return fewer, uncertified,
	// entries.
	N int
}

func (r *Reader) topNState(ctx context.Context) (topn.State, error) {
	if r.err != nil {
		return topn.State{}, r.err
	}
	var buf bytes.Buffer
	if err := r.db.GetMeta(ctx, topn.StateKey, &buf); err != nil {
		return topn.State{}, err
	}
	if buf.Len() == 0 {
		return topn.State{}, ErrNoTopN
	}
	var s topn.State
	if err := s.UnmarshalBinary(buf.Bytes()); err != nil {
		return topn.State{}, err
	}
	if !s.Valid {
		return s, ErrStaleTopN
	}
	return s, nil
}

func (r *Reader) topN(ctx context.Context, key string, metric stats.Metric, n, maintained int) (TopN, error) {
	if metric < 0 || int(metric) >= stats.NumMetrics {
		return TopN{}, fmt.Errorf("invalid metric: %v", metric)
	}
	res := TopN{N: maintained}
	var buf bytes.Buffer
	if err := r.db.GetMeta(ctx, key, &buf); err != nil {
		return TopN{}, err
	}
	if buf.Len() == 0 {
		return res, nil
	}
	var rk topn.Rankings
	if err := rk.UnmarshalBinary(buf.Bytes()); err != nil {
		return TopN{}, err
	}
	h := rk[metric]
	for _, e := range h.Top(n) {
		res.Entries = append(res.Entries, Ranked{Path: e.Key, Value: e.Value})
	}
	res.Certified = min(h.Certified(), len(res.Entries))
	return res, nil
}

// TopPrefixes returns the top n prefixes by their own, ie. not recursive,
// value for metric. n of zero or less returns all of the stored entries.
// It returns ErrNoTopN or ErrStaleTopN if the rankings are not available
// or may be out of date.
func (r *Reader) TopPrefixes(ctx context.Context, metric stats.Metric, n int) (TopN, error) {
	s, err := r.topNState(ctx)
	if err != nil {
		return TopN{}, err
	}
	return r.topN(ctx, topn.PrefixesKey, metric, n, s.N)
}

// TopPrefixesForUser is like TopPrefixes but ranks the prefixes by the
// value for metric of the files, and the prefixes themselves, owned by uid.
func (r *Reader) TopPrefixesForUser(ctx context.Context, uid int64, metric stats.Metric, n int) (TopN, error) {
	s, err := r.topNState(ctx)
	if err != nil {
		return TopN{}, err
	}
	if !s.HasUser(uid) {
		return TopN{N: s.PerIDN}, nil
	}
	return r.topN(ctx, topn.UserKey(uid), metric, n, s.PerIDN)
}

// TopPrefixesForGroup is like TopPrefixesForUser but for the group gid.
func (r *Reader) TopPrefixesForGroup(ctx context.Context, gid int64, metric stats.Metric, n int) (TopN, error) {
	s, err := r.topNState(ctx)
	if err != nil {
		return TopN{}, err
	}
	if !s.HasGroup(gid) {
		return TopN{N: s.PerIDN}, nil
	}
	return r.topN(ctx, topn.GroupKey(gid), metric, n, s.PerIDN)
}

// TopNIDs returns the users and groups that have rankings, ie. those
// that own any of the files or prefixes in the database, in ascending
// order.
func (r *Reader) TopNIDs(ctx context.Context) (users, groups []int64, err error) {
	s, err := r.topNState(ctx)
	if err != nil {
		return nil, nil, err
	}
	return s.Users, s.Groups, nil
}
