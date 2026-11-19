// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package topn

import (
	"encoding/binary"
	"fmt"
	"maps"
	"slices"

	"cloudeng.io/cmd/idu/stats"
)

// IDTotals records the totals, across an entire filesystem, for every user
// and group that owns any of its files or prefixes. They are exact, rather
// than bounded rankings, since the number of ids is small compared with
// the number of prefixes.
type IDTotals struct {
	Users, Groups map[int64]stats.Totals
}

// NewIDTotals returns an empty IDTotals.
func NewIDTotals() *IDTotals {
	return &IDTotals{
		Users:  map[int64]stats.Totals{},
		Groups: map[int64]stats.Totals{},
	}
}

func update(m map[int64]stats.Totals, pid stats.PerIDTotals, add bool) {
	for _, t := range pid {
		cur := m[t.ID]
		if add {
			cur.Add(t)
		} else {
			cur.Sub(t)
		}
		cur.ID = t.ID
		if cur.IsZero() {
			delete(m, t.ID)
			continue
		}
		m[t.ID] = cur
	}
}

// Add adds the per-user and per-group totals for a prefix.
func (t *IDTotals) Add(users, groups stats.PerIDTotals) {
	update(t.Users, users, true)
	update(t.Groups, groups, true)
}

// Sub subtracts the per-user and per-group totals for a prefix.
func (t *IDTotals) Sub(users, groups stats.PerIDTotals) {
	update(t.Users, users, false)
	update(t.Groups, groups, false)
}

// IsNegative returns true if any of the totals are negative, ie. if more
// was subtracted than was ever added.
func (t *IDTotals) IsNegative() bool {
	for _, m := range []map[int64]stats.Totals{t.Users, t.Groups} {
		for _, v := range m {
			if v.IsNegative() {
				return true
			}
		}
	}
	return false
}

// Sorted returns the totals in m in descending order of metric, with ties
// broken by ascending id. Ids with a zero value for metric are omitted.
func Sorted(m map[int64]stats.Totals, metric stats.Metric) []stats.Totals {
	out := make([]stats.Totals, 0, len(m))
	for _, id := range slices.Sorted(maps.Keys(m)) {
		if t := m[id]; metric.Value(t) > 0 {
			out = append(out, t)
		}
	}
	slices.SortStableFunc(out, func(a, b stats.Totals) int {
		va, vb := metric.Value(a), metric.Value(b)
		switch {
		case va > vb:
			return -1
		case va < vb:
			return 1
		}
		return 0
	})
	return out
}

// idTotalsVersion is the version of the encoding of IDTotals.
const idTotalsVersion = 1

func flatten(m map[int64]stats.Totals) stats.PerIDTotals {
	out := make(stats.PerIDTotals, 0, len(m))
	for _, id := range slices.Sorted(maps.Keys(m)) {
		out = append(out, m[id])
	}
	return out
}

// AppendBinary appends the binary encoding of t to data.
func (t *IDTotals) AppendBinary(data []byte) []byte {
	data = binary.AppendUvarint(data, idTotalsVersion)
	data = flatten(t.Users).AppendBinary(data)
	return flatten(t.Groups).AppendBinary(data)
}

// UnmarshalBinary decodes IDTotals encoded by AppendBinary.
func (t *IDTotals) UnmarshalBinary(data []byte) error {
	ver, data, err := readUvarint(data)
	if err != nil || ver != idTotalsVersion {
		return fmt.Errorf("topn.IDTotals: unsupported version %d", ver)
	}
	*t = *NewIDTotals()
	for _, m := range []map[int64]stats.Totals{t.Users, t.Groups} {
		var pid stats.PerIDTotals
		if data, err = pid.DecodeBinaryChecked(data); err != nil {
			return fmt.Errorf("topn.IDTotals: %w", err)
		}
		for _, v := range pid {
			m[v.ID] = v
		}
	}
	return nil
}
