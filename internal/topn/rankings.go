// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package topn

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"

	"cloudeng.io/cmd/idu/stats"
)

// Rankings contains a Heap for each stats.Metric.
type Rankings [stats.NumMetrics]*Heap

// NewRankings returns Rankings whose heaps hold at most capacity entries.
func NewRankings(capacity int) *Rankings {
	var r Rankings
	for i := range r {
		r[i] = New(capacity)
	}
	return &r
}

// Set records that the totals for key are now t.
func (r *Rankings) Set(key string, t stats.Totals) {
	for i, h := range r {
		h.Set(key, stats.Metric(i).Value(t))
	}
}

// Delete removes key from all of the heaps.
func (r *Rankings) Delete(key string) {
	for _, h := range r {
		h.Delete(key)
	}
}

// Empty returns true if none of the heaps contain any entries and none
// has ever excluded a key, ie. if they convey no information.
func (r *Rankings) Empty() bool {
	for _, h := range r {
		if h.Len() > 0 || h.Threshold() > 0 {
			return false
		}
	}
	return true
}

// NeedsRefresh returns true if any of the heaps need to be refreshed for
// a top-n, see Heap.NeedsRefresh.
func (r *Rankings) NeedsRefresh(n int) bool {
	for _, h := range r {
		if h.NeedsRefresh(n) {
			return true
		}
	}
	return false
}

// rankingsVersion is the version of the encoding of Rankings.
const rankingsVersion = 1

// AppendBinary appends the binary encoding of r to data.
func (r *Rankings) AppendBinary(data []byte) []byte {
	data = binary.AppendUvarint(data, rankingsVersion)
	data = binary.AppendUvarint(data, uint64(len(r)))
	for _, h := range r {
		data = h.AppendBinary(data)
	}
	return data
}

// UnmarshalBinary decodes Rankings encoded by AppendBinary.
func (r *Rankings) UnmarshalBinary(data []byte) error {
	ver, data, err := readUvarint(data)
	if err != nil || ver != rankingsVersion {
		return fmt.Errorf("topn.Rankings: unsupported version %d", ver)
	}
	n, data, err := readUvarint(data)
	if err != nil || n != uint64(len(r)) {
		return fmt.Errorf("topn.Rankings: invalid number of metrics %d", n)
	}
	for i := range r {
		r[i] = &Heap{}
		if data, err = r[i].DecodeBinary(data); err != nil {
			return fmt.Errorf("topn.Rankings: %v: %w", stats.Metric(i), err)
		}
	}
	return nil
}

// State records the configuration and validity of the stored rankings.
type State struct {
	// Valid is false while an analysis is updating the rankings, and
	// remains false if that analysis is interrupted or fails, until
	// the rankings are refreshed.
	Valid bool
	// N and Capacity are the number of entries of interest and the
	// number held by each heap of the prefix rankings.
	N, Capacity int
	// PerIDN and PerIDCapacity are the number of entries of interest and
	// the number held by each heap of the per-user and per-group rankings.
	PerIDN, PerIDCapacity int
	// Users and Groups list the ids that have stored rankings, in
	// ascending order.
	Users, Groups []int64
}

// stateVersion is the version of the encoding of State.
const stateVersion = 1

// AppendBinary appends the binary encoding of s to data.
func (s *State) AppendBinary(data []byte) []byte {
	data = binary.AppendUvarint(data, stateVersion)
	valid := uint64(0)
	if s.Valid {
		valid = 1
	}
	data = binary.AppendUvarint(data, valid)
	for _, v := range []int{s.N, s.Capacity, s.PerIDN, s.PerIDCapacity} {
		data = binary.AppendUvarint(data, uint64(v))
	}
	for _, ids := range [][]int64{s.Users, s.Groups} {
		data = binary.AppendUvarint(data, uint64(len(ids)))
		for _, id := range ids {
			data = binary.AppendVarint(data, id)
		}
	}
	return data
}

// UnmarshalBinary decodes a State encoded by AppendBinary.
func (s *State) UnmarshalBinary(data []byte) error {
	ver, data, err := readUvarint(data)
	if err != nil || ver != stateVersion {
		return fmt.Errorf("topn.State: unsupported version %d", ver)
	}
	var valid uint64
	if valid, data, err = readUvarint(data); err != nil {
		return fmt.Errorf("topn.State: %w", err)
	}
	s.Valid = valid == 1
	for _, v := range []*int{&s.N, &s.Capacity, &s.PerIDN, &s.PerIDCapacity} {
		var u uint64
		if u, data, err = readUvarint(data); err != nil {
			return fmt.Errorf("topn.State: %w", err)
		}
		*v = int(u)
	}
	for _, ids := range []*[]int64{&s.Users, &s.Groups} {
		var n uint64
		if n, data, err = readUvarint(data); err != nil {
			return fmt.Errorf("topn.State: %w", err)
		}
		if n > uint64(len(data)) {
			return fmt.Errorf("topn.State: invalid number of ids %d", n)
		}
		*ids = make([]int64, n)
		for i := range *ids {
			if (*ids)[i], data, err = readVarint(data); err != nil {
				return fmt.Errorf("topn.State: %w", err)
			}
		}
	}
	return nil
}

// HasUser returns true if uid has stored rankings.
func (s *State) HasUser(uid int64) bool {
	_, ok := slices.BinarySearch(s.Users, uid)
	return ok
}

// HasGroup returns true if gid has stored rankings.
func (s *State) HasGroup(gid int64) bool {
	_, ok := slices.BinarySearch(s.Groups, gid)
	return ok
}

// The keys, in the database's metadata bucket, used to store the state
// and the rankings.
const (
	keyPrefix   = "topn/v1/"
	StateKey    = keyPrefix + "state"
	PrefixesKey = keyPrefix + "prefixes"
)

// UserKey returns the key for the rankings of the prefixes used by uid.
func UserKey(uid int64) string {
	return keyPrefix + "user/" + strconv.FormatInt(uid, 10)
}

// GroupKey returns the key for the rankings of the prefixes used by gid.
func GroupKey(gid int64) string {
	return keyPrefix + "group/" + strconv.FormatInt(gid, 10)
}
