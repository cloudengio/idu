// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package report_test

import (
	"testing"

	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/stats"
)

func TestHardlinkRankings(t *testing.T) {
	sdb := report.NewAllStats("/", 10)
	for _, p := range []struct {
		path      string
		hardlinks int64
	}{{"/a", 3}, {"/b", 0}, {"/c", 5}} {
		tot := stats.Totals{Files: 5, Prefix: 1, Bytes: 100, Hardlinks: p.hardlinks}
		sdb.RecordDirect(p.path, tot, stats.PerIDTotals{{ID: 1, Files: 5, Prefix: 1, Bytes: 100, Hardlinks: p.hardlinks}}, nil)
	}
	sdb.Finalize()
	// Prefixes without hardlinks are not ranked.
	keys, vals := report.PopN(sdb.Prefix.Hardlinks, 0)
	if len(keys) != 2 || keys[0] != 5 || vals[0] != "/c" || keys[1] != 3 || vals[1] != "/a" {
		t.Errorf("got %v %v", keys, vals)
	}
	if h := sdb.PerUser.ByPrefix[1]; h == nil || h.Hardlinks.Len() != 2 {
		t.Errorf("per-user ranking: %+v", h)
	}
	if sdb.ByUser.Hardlinks.Len() != 1 {
		t.Errorf("by-user ranking: %v", sdb.ByUser.Hardlinks.Len())
	}
	sdb.ClearHardlinkRankings()
	if sdb.Prefix.Hardlinks != nil || sdb.ByUser.Hardlinks != nil || sdb.PerUser.ByPrefix[1].Hardlinks != nil {
		t.Errorf("rankings were not cleared")
	}
}
