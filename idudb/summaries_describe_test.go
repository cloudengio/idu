// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/stats"
)

func TestStaleSummariesExplained(t *testing.T) {
	ctx := context.Background()
	calc := newOptions(nil).calc.String()
	db, err := openDatabase(ctx, filepath.Join(t.TempDir(), "db"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	for _, tc := range []struct {
		stored string
		rec    stats.HardlinkRecording
		want   []string
	}{
		{invalidSummariesSpec, stats.RecordNoFiles, []string{"incomplete", "did not finish"}},
		{"other" + summaryFormatSuffix, stats.RecordNoFiles, []string{`"other" calculator`, `"` + calc + `" calculator`}},
		{calc + ";v2;hardlinks", stats.RecordLinkedFiles, []string{"format v2", "format v3"}},
		{summarySpec(calc, stats.RecordNoFiles), stats.RecordLinkedFiles, []string{"recording no hardlink information", "at least the files that may be hardlinks"}},
		{withRecordAll(summarySpec(calc, stats.RecordLinkedFiles), "x"), stats.RecordAllFiles, []string{"hardlinks_record_all", "recording every file"}},
	} {
		if err := db.SetMeta(ctx, metaKeySummaries, []byte(tc.stored)); err != nil {
			t.Fatal(err)
		}
		err := CheckSummaryRecording(ctx, db, calc, tc.rec)
		if !errors.Is(err, ErrStaleSummaries) {
			t.Fatalf("%q: got %v", tc.stored, err)
		}
		for _, w := range tc.want {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("%q: %q does not contain %q", tc.stored, err, w)
			}
		}
	}
	if err := db.SetMeta(ctx, metaKeySummaries, []byte(invalidSummariesSpec)); err != nil {
		t.Fatal(err)
	}
	if err := CheckSummaryKeys(ctx, db); !errors.Is(err, ErrStaleSummaries) || !strings.Contains(err.Error(), "incomplete") {
		t.Errorf("got %v", err)
	}
}
