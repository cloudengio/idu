// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package reports_test

import (
	"reflect"
	"testing"

	"cloudeng.io/cmd/idu/internal/reports"
	"cloudeng.io/cmd/idu/stats"
)

func TestMerge(t *testing.T) {
	sdb := reports.NewAllStats("/", 10)
	sdb.RecordDirect("/a", stats.Totals{
		Prefix: 1, SubPrefixes: 2, Files: 3, Bytes: 400, StorageBytes: 512,
		PrefixBytes: 96, SparseFiles: 1, SparseBytes: 1 << 20,
	}, nil, nil)
	sdb.RecordDirect("/b", stats.Totals{
		Prefix: 1, SubPrefixes: 0, Files: 1, Bytes: 10, StorageBytes: 4096,
		PrefixBytes: 64,
	}, nil, nil)
	got := sdb.Prefix.Merge(10)
	want := map[string]reports.MergedStats{
		"/a": {Bytes: 400, Storage: 512, Files: 3, Prefixes: 2, PrefixBytes: 96, SparseBytes: 1 << 20},
		"/b": {Bytes: 10, Storage: 4096, Files: 1, Prefixes: 0, PrefixBytes: 64},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
