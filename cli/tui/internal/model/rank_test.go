// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package model

import (
	"testing"

	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/stats"
)

// ids returns per-id totals where the order by bytes, 1 then 2, is the
// reverse of the order by storage, 2 then 1, eg. since 1's files are
// sparse.
func ids() stats.PerIDTotals {
	return stats.PerIDTotals{
		{ID: 1, Files: 1, Bytes: 1000, StorageBytes: 10},
		{ID: 2, Files: 1, Bytes: 100, StorageBytes: 4096},
	}
}

func TestUsersAndGroupsByStorage(t *testing.T) {
	sdb := report.NewAllStats("/", 10)
	sdb.RecordDirect("/a", stats.Totals{Files: 2, Prefix: 1, Bytes: 1100, StorageBytes: 4106}, ids(), ids())
	sdb.Finalize()
	top := newTopN("/", "test", sdb, 10)
	for name, r := range map[string][]Ranked[int64]{"users": top.Users, "groups": top.Groups} {
		if len(r) != 2 || r[0].Item != 2 || r[0].Value != 4096 || r[1].Item != 1 || r[1].Value != 10 {
			t.Errorf("%v: got %+v, want 2 then 1 by storage", name, r)
		}
	}
	orig := ids()
	sorted := byStorage(orig)
	if sorted[0].ID != 2 || sorted[1].ID != 1 {
		t.Errorf("got %+v, want 2 then 1", sorted)
	}
	// The original is unchanged.
	if orig[0].ID != 1 {
		t.Errorf("byStorage modified its argument")
	}
}
