// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
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
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/localfs"
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

	cfg := fmt.Sprintf(`filesystems:
  - name: testtree
    prefix: %v
    database: %v/db
    concurrent_scans: 2
    concurrent_stats: 4
    scan_size: 2
    exclusions:
      - 'd-testtree[/\\]d00-01'
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
		if strings.Contains(s, "testtree/d00-01") || strings.Contains(s, "testtree\\d00-01") {
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

func scanDB(ctx context.Context, t *testing.T, db database.DB, lfs filewalk.FS, arg0 string) []string {
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
			size := info.Size()
			if size == 0 && ((info.Mode().Type() & fs.ModeSymlink) != 0) {
				l, err := os.Readlink(path)
				if err != nil {
					t.Fatalf("%v: %v", path, err)
				}
				size = int64(len(l))
			}
			if got, want := p.Size(), size; got != want {
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

func scanErrors(ctx context.Context, t *testing.T, db database.DB, _ filewalk.FS, arg0 string) []string {
	errors := []string{}
	err := db.VisitErrors(ctx, arg0,
		func(_ context.Context, key string, _ time.Time, detail []byte) bool {
			errors = append(errors, fmt.Sprintf("%s: %s", key, detail))
			return true
		})

	if err != nil {
		t.Fatal(err)
	}
	return errors
}

func getLastLog(ctx context.Context, t *testing.T, db database.DB) (start, stop time.Time, s anaylzeSummary) {
	start, stop, detail, err := db.LastLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(detail, &s); err != nil {
		t.Fatal(err)
	}
	return
}

func verifyDB(ctx context.Context, t *testing.T, cfg config.T, fs filewalk.FS, arg0 string, scannable []string) ([]string, anaylzeSummary) {
	ctx, _, _, db, err := internal.OpenPrefixAndDatabase(ctx, cfg, arg0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)

	_, _, l, _ := runtime.Caller(1)
	scanned := scanDB(ctx, t, db, fs, arg0)
	if got, want := scanned, scannable; !slices.Equal(got, want) {
		t.Errorf("line %v, got %v, want %v", l, len(got), len(want))
	}

	storedErrors := scanErrors(ctx, t, db, fs, arg0)
	expectedErrors := getExpectedErrors(scannable)
	if got, want := len(storedErrors), len(expectedErrors); got != want {
		t.Errorf("line %v, got %v, want %v", l, got, want)
		fmt.Printf("stored: \n%v\n", strings.Join(storedErrors, "\n"))
		fmt.Printf("\n\nexpected:\n%v\n", strings.Join(expectedErrors, "\n"))
	}
	for _, e := range storedErrors {
		if !strings.Contains(e, "permission denied") && !strings.Contains(e, "Access is denied") {
			t.Errorf("line %v, unexpected error: %v", l, e)
		}
	}

	start, stop, summary := getLastLog(ctx, t, db)

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
	if err := os.MkdirAll(internal.LogDir, 0700); err != nil {
		t.Fatal(err)
	}
	globalConfig = cfg

	fs := localfs.New()
	alz := &analyzeCmd{}
	af := analyzeFlags{}
	if err := alz.analyzeFS(ctx, fs, &af, []string{arg0}); err != nil {
		t.Fatal(err)
	}

	scanned, summary := verifyDB(ctx, t, cfg, fs, arg0, scannable)
	nDirs, nFiles := numDirsAndFiles(scanned)

	compareSummary(t, summary, nDirs, nFiles, 0, 0, 0, nDirs+nFiles)

	if got, want := summary.Files+summary.PrefixesFinished, int64(len(scanned)); got < want {
		t.Errorf("unexpected number of prefixes+files: %v, %v", got, want)
	}

	// Run again, make sure all prefixes are reported as unchanged.
	if err := alz.analyzeFS(ctx, fs, &af, []string{arg0}); err != nil {
		t.Fatal(err)
	}

	scannedUnchanged, summaryUnchanged := verifyDB(ctx, t, cfg, fs, arg0, scannable)

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
	scannedAdded, summaryAdded := verifyDB(ctx, t, cfg, fs, arg0, expanded)

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

	scannedDeleted, summaryDeleted := verifyDB(ctx, t, cfg, fs, arg0, scannable)
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

func TestAnalyzeByName(t *testing.T) {
	ctx := context.Background()
	tmpDir, cfgFile, arg0, tt := setupAnalyze(t)
	defer os.RemoveAll(tmpDir)

	scannable := slices.Clone(tt.base())
	sort.Strings(scannable)
	scannable = removeExclusions(scannable)

	cfg, err := config.ReadConfig(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	internal.LogDir = filepath.Join(tmpDir, "logs")
	if err := os.MkdirAll(internal.LogDir, 0700); err != nil {
		t.Fatal(err)
	}
	globalConfig = cfg

	fs := localfs.New()
	alz := &analyzeCmd{}
	af := analyzeFlags{}
	if err := alz.analyzeFS(ctx, fs, &af, []string{"testtree"}); err != nil {
		t.Fatal(err)
	}

	_, resolvedCfg, target, db, err := internal.OpenPrefixAndDatabase(ctx, cfg, "testtree", true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	if resolvedCfg.Name != "testtree" || target != arg0 {
		t.Fatalf("got name=%q target=%q, want testtree, %q", resolvedCfg.Name, target, arg0)
	}
	scanned := scanDB(ctx, t, db, fs, target)
	if got, want := scanned, scannable; !slices.Equal(got, want) {
		t.Errorf("got %d entries, want %d", len(got), len(want))
	}
}

type faultAnalyzeFS struct {
	filewalk.FS
	scanErrPath  string
	scanErr      error
	lstatErrPath string
	lstatErr     error
	xattrErrPath string
	xattrErr     error
}

type faultLevelScanner struct {
	err error
}

func (s *faultLevelScanner) Scan(context.Context, int) bool { return false }
func (s *faultLevelScanner) Contents() []filewalk.Entry     { return nil }
func (s *faultLevelScanner) Err() error                     { return s.err }

func (f *faultAnalyzeFS) LevelScanner(path string) filewalk.LevelScanner {
	if f.scanErrPath != "" && path == f.scanErrPath {
		return &faultLevelScanner{err: f.scanErr}
	}
	return f.FS.LevelScanner(path)
}

func (f *faultAnalyzeFS) Lstat(ctx context.Context, path string) (file.Info, error) {
	if f.lstatErrPath != "" && path == f.lstatErrPath {
		return file.Info{}, f.lstatErr
	}
	return f.FS.Lstat(ctx, path)
}

func (f *faultAnalyzeFS) XAttr(ctx context.Context, path string, info file.Info) (file.XAttr, error) {
	if f.xattrErrPath != "" && path == f.xattrErrPath {
		return file.XAttr{}, f.xattrErr
	}
	return f.FS.XAttr(ctx, path, info)
}

func TestAnalyzeDefaults(t *testing.T) {
	alz := &analyzeCmd{}
	if err := alz.analyzeFS(context.Background(), localfs.New(), &analyzeFlags{Defaults: true}, nil); err != nil {
		t.Fatalf("analyzeFS defaults: %v", err)
	}
}

func setupAnalyzeTestConfig(t *testing.T, root string) config.T {
	t.Helper()
	tmpDir := filepath.Dir(root)
	cfgText := fmt.Sprintf(`filesystems:
  - name: tree
    prefix: %s
    database: %s
    set_max_threads: 10000
    concurrent_scans: 2
    concurrent_stats: 2
`, root, filepath.Join(tmpDir, "db"))
	cfg, err := config.ParseConfig([]byte(cfgText))
	if err != nil {
		t.Fatal(err)
	}
	origCfg := globalConfig
	origLogDir := internal.LogDir
	globalConfig = cfg
	internal.LogDir = filepath.Join(tmpDir, "logs")
	if err := os.MkdirAll(internal.LogDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		globalConfig = origCfg
		internal.LogDir = origLogDir
	})
	return cfg
}

func TestAnalyzeForceAndSubtree(t *testing.T) {
	ctx := context.Background()
	alz := &analyzeCmd{}
	tmpDir := t.TempDir()
	root := filepath.Join(tmpDir, "root")
	sub := filepath.Join(root, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{filepath.Join(root, "f1"), filepath.Join(sub, "f2")} {
		if err := os.WriteFile(p, []byte("a"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := setupAnalyzeTestConfig(t, root)

	lfs := localfs.New()
	if err := alz.analyzeFS(ctx, lfs, &analyzeFlags{}, []string{root}); err != nil {
		t.Fatalf("initial analyzeFS: %v", err)
	}

	// Force re-analyzes unchanged prefixes.
	if err := alz.analyzeFS(ctx, lfs, &analyzeFlags{Force: true}, []string{root}); err != nil {
		t.Fatalf("force analyzeFS: %v", err)
	}
	_, _, _, db, err := internal.OpenPrefixAndDatabase(ctx, cfg, root, true)
	if err != nil {
		t.Fatal(err)
	}
	_, _, sumForce := getLastLog(ctx, t, db)
	_ = db.Close(ctx)
	if sumForce.ParentUnchanged != 0 {
		t.Errorf("force ParentUnchanged = %d, want 0", sumForce.ParentUnchanged)
	}

	// Modify subdir without modifying root's mtime -> root is parentUnchanged, childrenUnchanged=false.
	if err := os.WriteFile(filepath.Join(sub, "f3"), []byte("c"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := alz.analyzeFS(ctx, lfs, &analyzeFlags{}, []string{root}); err != nil {
		t.Fatalf("analyzeFS after subdir change: %v", err)
	}
	_, _, _, db2, err := internal.OpenPrefixAndDatabase(ctx, cfg, root, true)
	if err != nil {
		t.Fatal(err)
	}
	_, _, sumSub := getLastLog(ctx, t, db2)
	_ = db2.Close(ctx)
	if sumSub.ParentUnchanged != 1 || sumSub.ChildrenUnchanged != 0 {
		t.Errorf("got ParentUnchanged=%d ChildrenUnchanged=%d, want 1, 0", sumSub.ParentUnchanged, sumSub.ChildrenUnchanged)
	}

	// Sub-prefix scan (target != cfg.Prefix).
	if err := alz.analyzeFS(ctx, lfs, &analyzeFlags{}, []string{sub}); err != nil {
		t.Fatalf("sub-prefix analyzeFS: %v", err)
	}
}

func TestAnalyzeSymlinkAndFaults(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	root := filepath.Join(tmpDir, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	f1 := filepath.Join(root, "f1")
	if err := os.WriteFile(f1, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	sym := filepath.Join(tmpDir, "sym-root")
	if err := os.Symlink(root, sym); err != nil {
		t.Fatal(err)
	}

	dbDir := filepath.Join(tmpDir, "db")
	cfgText := fmt.Sprintf(`filesystems:
  - name: tree
    prefix: %s
    database: %s
  - name: sym
    prefix: %s
    database: %s
`, root, dbDir, sym, filepath.Join(tmpDir, "symdb"))
	cfg, err := config.ParseConfig([]byte(cfgText))
	if err != nil {
		t.Fatal(err)
	}
	origCfg := globalConfig
	origLogDir := internal.LogDir
	globalConfig = cfg
	internal.LogDir = filepath.Join(tmpDir, "logs")
	if err := os.MkdirAll(internal.LogDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		globalConfig = origCfg
		internal.LogDir = origLogDir
	})

	alz := &analyzeCmd{}
	lfs := localfs.New()

	t.Run("symlink target ignored cleanly", func(t *testing.T) {
		if err := alz.analyzeFS(ctx, lfs, &analyzeFlags{}, []string{sym}); err != nil {
			t.Fatalf("symlink analyzeFS: %v", err)
		}
	})

	t.Run("xattr error and lstat error logged", func(t *testing.T) {
		ffs := &faultAnalyzeFS{
			FS:           lfs,
			xattrErrPath: root,
			xattrErr:     errors.New("xattr failed"),
			lstatErrPath: f1,
			lstatErr:     errors.New("lstat failed"),
		}
		if err := alz.analyzeFS(ctx, ffs, &analyzeFlags{}, []string{root}); err != nil {
			t.Fatalf("faulty analyzeFS: %v", err)
		}
		_, _, _, db, err := internal.OpenPrefixAndDatabase(ctx, cfg, root, true)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close(ctx)
		errs := scanErrors(ctx, t, db, ffs, root)
		if len(errs) != 2 {
			t.Errorf("logged errors = %v (len %d), want 2", errs, len(errs))
		}
	})

	t.Run("fatal level scanner error logged in Done", func(t *testing.T) {
		fatalErr := errors.New("io hardware failure")
		ffs := &faultAnalyzeFS{
			FS:          lfs,
			scanErrPath: root,
			scanErr:     fatalErr,
		}
		if err := alz.analyzeFS(ctx, ffs, &analyzeFlags{}, []string{root}); err != nil {
			t.Fatalf("analyzeFS: %v", err)
		}
		_, _, _, db, err := internal.OpenPrefixAndDatabase(ctx, cfg, root, true)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close(ctx)
		errs := scanErrors(ctx, t, db, ffs, root)
		if len(errs) != 1 || !strings.Contains(errs[0], "io hardware failure") {
			t.Errorf("logged errors = %v, want [root: io hardware failure]", errs)
		}
	})

	t.Run("unknown prefix returns error", func(t *testing.T) {
		if err := alz.analyzeFS(ctx, lfs, &analyzeFlags{}, []string{"/does/not/exist/cfg"}); err == nil {
			t.Fatal("expected error for unknown prefix")
		}
	})
}
