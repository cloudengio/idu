// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file/localfs"
)

// recordedFiles returns the inodes of the files recorded in the summary
// for each prefix.
func recordedFiles(ctx context.Context, t *testing.T, dbDir string, prefixes ...string) map[string][]uint64 {
	t.Helper()
	db, err := openTestDB(ctx, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	recorded := map[string][]uint64{}
	for _, p := range prefixes {
		exactKey, _ := stats.SummaryKeys(db.sep, p)
		ps, err := db.getSummary(ctx, exactKey)
		if err != nil || ps == nil {
			t.Fatalf("%v: %v %v", p, ps, err)
		}
		var inodes []uint64
		for _, f := range ps.Files {
			inodes = append(inodes, f.FileID)
		}
		slices.Sort(inodes)
		recorded[p] = inodes
	}
	return recorded
}

func inodeOf(ctx context.Context, t *testing.T, name string) uint64 {
	t.Helper()
	fs := localfs.New()
	fi, err := fs.Lstat(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	xattr, err := fs.XAttr(ctx, name, fi)
	if err != nil {
		t.Fatal(err)
	}
	return xattr.FileID
}

func TestHardlinkRecording(t *testing.T) {
	ctx := context.Background()
	root, sub, _ := createRootAndSub(t)
	// root/f0 and sub/f1 have a single link, root/f2 and sub/l2 are links
	// to the same file.
	f2, l2 := filepath.Join(root, "f2"), filepath.Join(sub, "l2")
	if err := os.WriteFile(f2, []byte("c"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(f2, l2); err != nil {
		t.Fatal(err)
	}
	f0, f1 := inodeOf(ctx, t, filepath.Join(root, "f0")), inodeOf(ctx, t, filepath.Join(sub, "f1"))
	linked := inodeOf(ctx, t, f2)

	analyze := func(dbDir string, rec stats.HardlinkRecording) {
		t.Helper()
		db, err := openTestDB(ctx, dbDir, WithHardlinkRecording(rec))
		if err != nil {
			t.Fatal(err)
		}
		if err := NewAnalyzer(db, root).Analyze(ctx, root); err != nil {
			t.Fatal(err)
		}
		if err := db.LogAndClose(ctx, time.Now(), time.Now(), []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct {
		rec  stats.HardlinkRecording
		want map[string][]uint64
	}{
		{stats.RecordNoFiles, map[string][]uint64{root: nil, sub: nil}},
		{stats.RecordLinkedFiles, map[string][]uint64{root: {linked}, sub: {linked}}},
		{stats.RecordAllFiles, map[string][]uint64{
			root: slices.Sorted(slices.Values([]uint64{f0, linked})),
			sub:  slices.Sorted(slices.Values([]uint64{f1, linked}))}},
	} {
		dbDir := filepath.Join(t.TempDir(), "db")
		analyze(dbDir, tc.rec)
		if got := recordedFiles(ctx, t, dbDir, root, sub); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%v: got %v, want %v", tc.rec, got, tc.want)
		}
	}
}

func TestHardlinkRecordingFromConfig(t *testing.T) {
	for _, tc := range []struct {
		cfg  iduconfig.Settings
		want stats.HardlinkRecording
	}{
		{iduconfig.Settings{HardlinkDedup: iduconfig.HardlinkDedupSummaries}, stats.RecordLinkedFiles},
		{iduconfig.Settings{HardlinkDedup: iduconfig.HardlinkDedupSummariesAll}, stats.RecordAllFiles},
		{iduconfig.Settings{HardlinkDedup: iduconfig.HardlinkDedupRescan}, stats.RecordNoFiles},
		{iduconfig.Settings{HardlinkDedup: iduconfig.HardlinkDedupSummariesAll, CountHardlinkAsFiles: true}, stats.RecordNoFiles},
	} {
		if got := HardlinkRecording(&tc.cfg); got != tc.want {
			t.Errorf("%+v: got %v, want %v", tc.cfg, got, tc.want)
		}
	}
}

func TestHardlinkRecordingSpecs(t *testing.T) {
	ctx := context.Background()
	calc := newOptions(nil).calc.String()
	// The spec used before only the linked files were recorded, when every
	// file was recorded, is that used for recording the linked files since
	// such summaries are a superset of them.
	if got, want := summarySpec(calc, stats.RecordLinkedFiles), calc+summaryFormatSuffix+";hardlinks"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	recordings := []stats.HardlinkRecording{stats.RecordNoFiles, stats.RecordLinkedFiles, stats.RecordAllFiles}
	specs := map[stats.HardlinkRecording]string{}
	for _, rec := range recordings {
		specs[rec] = summarySpec(calc, rec)
	}
	if len(slices.Compact(slices.Sorted(maps.Values(specs)))) != 3 {
		t.Errorf("specs are not distinct: %v", specs)
	}
	// The errors expected for each stored recording when the summaries
	// are required to satisfy each of recordings.
	for stored, wantErr := range map[stats.HardlinkRecording][3]error{
		stats.RecordNoFiles:     {nil, ErrStaleSummaries, ErrStaleSummaries},
		stats.RecordLinkedFiles: {nil, nil, ErrStaleSummaries},
		stats.RecordAllFiles:    {nil, nil, nil},
	} {
		dbDir := filepath.Join(t.TempDir(), "db")
		db, err := openDatabase(ctx, dbDir, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.SetMeta(ctx, metaKeySummaries, []byte(specs[stored])); err != nil {
			t.Fatal(err)
		}
		for i, req := range recordings {
			if err := CheckSummaryRecording(ctx, db, calc, req); !errors.Is(err, wantErr[i]) {
				t.Errorf("%v: CheckSummaryRecording(%v): got %v, want %v", stored, req, err, wantErr[i])
			}
		}
		if err := db.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
}
