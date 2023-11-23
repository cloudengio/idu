// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/cmd/idu/internal/database"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/filewalk/localfs"
)

func setupAnalyze(t *testing.T) (tmpDir, config, prefix string, tt *testtree) {
	tmpDir, err := os.MkdirTemp("", "filewalk")
	if err != nil {
		t.Fatalf("failed to create testdir: %v", err)
	}

	tt = newTestTree(tmpDir, 3, 5, 5)

	testTree, dbDir := tt.root, filepath.Join(tmpDir, "database")
	for _, d := range []string{testTree, dbDir} {
		if err := os.Mkdir(d, 0777); err != nil {
			t.Fatal(err)
		}
	}

	if err := tt.createBase(); err != nil {
		t.Fatal(err)
	}

	cfg := fmt.Sprintf(`- prefix: %v
  database: %v/db
  concurrent_scans: 2
  concurrent_stats: 4
  items: 2
  exclusions:
    - d-testtree/d00-01
`, testTree, dbDir)
	err = os.WriteFile(filepath.Join(tmpDir, "config.yaml"), []byte(cfg), 0600)
	if err != nil {
		t.Fatal(err)
	}
	return tmpDir, filepath.Join(tmpDir, "config.yaml"), testTree, tt
}

func removeExclusions(c []string) []string {
	r := []string{}
	for _, s := range c {
		if strings.Contains(s, "testtree/d00-01") {
			continue
		}
		r = append(r, s)
	}
	return r
}

func getExpectedErrors(c []string) []string {
	errors := []string{}
	for _, s := range c {
		if strings.Contains(s, "inaccessible-dir") {
			errors = append(errors, s)
		}
	}
	return errors
}

func scanDB(t *testing.T, ctx context.Context, db database.DB, lfs filewalk.FS, arg0 string) []string {
	scanned := []string{}
	err := db.Scan(ctx, arg0, func(_ context.Context, k string, v []byte) bool {
		var pi prefixinfo.T
		if err := pi.UnmarshalBinary(v); err != nil {
			t.Fatalf("failed to unmarshal value for %v: %v\n", k, err)
			return false
		}
		scanned = append(scanned, k)
		for _, p := range pi.InfoList() {
			path := lfs.Join(k, p.Name())
			info, err := os.Lstat(path)
			if err != nil {
				t.Errorf("%v: %v", path, err)
				continue
			}
			if got, want := p.Size(), info.Size(); got != want {
				t.Errorf("%v: got %v, want %v", path, got, want)
			}
			if got, want := p.ModTime(), info.ModTime(); !got.Equal(want) {
				t.Errorf("%v: got %v, want %v", path, got, want)
			}
			if got, want := p.Mode(), info.Mode(); got != want {
				t.Errorf("%v: got %v, want %v", path, got, want)
			}
			if !p.IsDir() {
				scanned = append(scanned, path)
			}
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(scanned)
	return scanned
}

func scanErrors(t *testing.T, ctx context.Context, db database.DB, fs filewalk.FS, arg0 string) []string {
	errors := []string{}
	err := db.VisitErrors(ctx, arg0,
		func(_ context.Context, key string, when time.Time, detail []byte) bool {
			errors = append(errors, fmt.Sprintf("%s: %s", key, detail))
			return true
		})

	if err != nil {
		t.Fatal(err)
	}
	return errors
}

func getLastLog(t *testing.T, ctx context.Context, db database.DB) (start, stop time.Time, s anaylzeSummary) {
	start, stop, detail, err := db.LastLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(detail, &s); err != nil {
		t.Fatal(err)
	}
	return
}

func verifyDB(t *testing.T, ctx context.Context, cfg config.T, fs filewalk.FS, arg0 string, scannable []string) ([]string, anaylzeSummary) {
	ctx, _, db, err := internal.OpenPrefixAndDatabase(ctx, cfg, arg0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)

	_, _, l, _ := runtime.Caller(1)
	scanned := scanDB(t, ctx, db, fs, arg0)
	if got, want := scanned, scannable; !slices.Equal(got, want) {
		t.Errorf("line %v, got %v, want %v", l, len(got), len(want))
	}

	storedErrors := scanErrors(t, ctx, db, fs, arg0)
	expectedErrors := getExpectedErrors(scannable)
	if got, want := len(storedErrors), len(expectedErrors); got != want {
		t.Errorf("line %v, got %v, want %v", l, got, want)
		fmt.Printf("stored: \n%v\n", strings.Join(storedErrors, "\n"))
		fmt.Printf("\n\nexpected:\n%v\n", strings.Join(expectedErrors, "\n"))
	}
	for _, e := range storedErrors {
		if !strings.Contains(e, "permission denied") {
			t.Errorf("line %v, unexpected error: %v", l, e)
		}
	}

	start, stop, summary := getLastLog(t, ctx, db)

	if took := stop.Sub(start); took > 10*time.Minute {
		t.Errorf("line %v, unexpected duration: %v", l, took)
	}
	if s := time.Since(stop); s > 10*time.Minute {
		t.Errorf("line %v, unexpected duration: %v", l, s)
	}

	return scanned, summary
}

func compareSummary(t *testing.T, got anaylzeSummary,
	prefixesFinished,
	files,
	parentUnchanged,
	childrenUnchanged,
	deletions,
	stats int64) {
	_, _, l, _ := runtime.Caller(1)

	if got, want := got.PrefixesFinished, prefixesFinished; got != want {
		t.Errorf("line %v: PrefixesFinished: got %v, want %v", l, got, want)
	}
	if got, want := got.Files, files; got != want {
		t.Errorf("line %v: Files: got %v, want %v", l, got, want)
	}
	if got, want := got.ParentUnchanged, parentUnchanged; got != want {
		t.Errorf("line %v: ParentUnchanged: got %v, want %v", l, got, want)
	}
	if got, want := got.ChildrenUnchanged, childrenUnchanged; got != want {
		t.Errorf("line %v: ChildrenUnchanged: got %v, want %v", l, got, want)
	}
	if got, want := got.PrefixesDeleted, deletions; got != want {
		t.Errorf("line %v: PrefixesDeleted: got %v, want %v", l, got, want)
	}
	if stats > 0 {
		stats-- // the stat for the top level directory is not included in the summary
	}
	if got, want := got.FSStats, stats; got != want {
		t.Errorf("line %v: FSStats: got %v, want %v", l, got, want)
	}
}

func TestAnalyze(t *testing.T) {
	ctx := context.Background()
	testAnalyze(ctx, t)

}

func testAnalyze(ctx context.Context, t *testing.T) {
	tmpDir, cfgFile, arg0, tt := setupAnalyze(t)

	scannable := slices.Clone(tt.base())
	sort.Strings(scannable)
	scannable = removeExclusions(scannable)

	defer func() {
		if t.Failed() {
			t.Logf("tmpDir: %v\n", tmpDir)
			return
		}
		os.RemoveAll(tmpDir)
	}()

	cfg, err := config.ReadConfig(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	internal.LogDir = filepath.Join(tmpDir, "logs")
	os.MkdirAll(internal.LogDir, 0700)
	globalConfig = cfg

	fs := localfs.New()
	alz := &analyzeCmd{}
	af := analyzeFlags{}
	if err := alz.analyzeFS(ctx, fs, &af, []string{arg0}); err != nil {
		t.Fatal(err)
	}

	scanned, summary := verifyDB(t, ctx, cfg, fs, arg0, scannable)
	nDirs, nFiles := numDirsAndFiles(scanned)

	compareSummary(t, summary, nDirs, nFiles, 0, 0, 0, nDirs+nFiles)

	if got, want := summary.Files+summary.PrefixesFinished, int64(len(scanned)); got < want {
		t.Errorf("unexpected number of prefixes+files: %v, %v", got, want)
	}

	// Run again, make sure all prefixes are reported as unchanged.
	if err := alz.analyzeFS(ctx, fs, &af, []string{arg0}); err != nil {
		t.Fatal(err)
	}

	scannedUnchanged, summaryUnchanged := verifyDB(t, ctx, cfg, fs, arg0, scannable)

	if got, want := scannedUnchanged, scanned; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	compareSummary(t, summaryUnchanged,
		summary.PrefixesFinished,
		summary.Files,
		nDirs,
		nDirs,
		0,
		nDirs)

	// Run again, pick up new files and directories.
	if err := tt.createAdditional(3, 2, 1); err != nil {
		t.Fatal(err)
	}

	if err := tt.createAdditional(4, 2, 0); err != nil {
		t.Fatal(err)
	}

	if err := alz.analyzeFS(ctx, fs, &af, []string{arg0}); err != nil {
		t.Fatal(err)
	}

	expanded := slices.Clone(scannable)
	expanded = append(expanded, tt.additional()...)
	sort.Strings(expanded)
	nAddedDirs, nAddedFiles := numDirsAndFiles(tt.additional())
	scannedAdded, summaryAdded := verifyDB(t, ctx, cfg, fs, arg0, expanded)

	if got, want := scannedAdded, expanded; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	compareSummary(t, summaryAdded,
		nDirs+nAddedDirs,
		nFiles+nAddedFiles,
		nDirs-nAddedDirs,
		nDirs-nAddedDirs-1, // one of the subdirectories has changed.
		0,
		nDirs+nAddedDirs+nAddedFiles)

	// Run again, make sure deletions are detected.
	if err := tt.deletionAdditonal(); err != nil {
		t.Fatal(err)
	}

	if err := alz.analyzeFS(ctx, fs, &af, []string{arg0}); err != nil {
		t.Fatal(err)
	}

	scannedDeleted, summaryDeleted := verifyDB(t, ctx, cfg, fs, arg0, scannable)
	if got, want := scannedDeleted, scanned; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	compareSummary(t, summaryDeleted,
		nDirs,
		nFiles,
		nDirs-nAddedDirs,
		nDirs-nAddedDirs-1, // one of the subdirectories has changed.
		nAddedDirs,
		nDirs)
}
