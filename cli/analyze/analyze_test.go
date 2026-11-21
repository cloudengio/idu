// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package analyze

import (
	"bytes"
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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/localfs"
)

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

func scanDB(ctx context.Context, t *testing.T, db idudb.Reader, lfs filewalk.FS, arg0 string) []string {
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

func scanErrors(ctx context.Context, t *testing.T, db idudb.Reader, _ filewalk.FS, arg0 string) []string {
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

func getLastLog(ctx context.Context, t *testing.T, db idudb.Reader) (start, stop time.Time, s Summary) {
	start, stop, detail, err := db.LastLog(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(detail, &s); err != nil {
		t.Fatal(err)
	}
	return
}

func verifyDB(ctx context.Context, t *testing.T, cfg iduconfig.T, fs filewalk.FS, arg0 string, scannable []string) ([]string, Summary) {
	ctx, _, _, db, err := openPrefixReadOnly(ctx, cfg, arg0)
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

func compareSummary(t *testing.T, got Summary,
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
	tmpDir, cfgFile, arg0, tt := clitest.SetupAnalyze(t)

	scannable := slices.Clone(tt.Base())
	sort.Strings(scannable)
	scannable = removeExclusions(scannable)

	defer func() {
		if t.Failed() {
			t.Logf("tmpDir: %v\n", tmpDir)
			return
		}
		os.RemoveAll(tmpDir)
	}()

	cfg, err := iduconfig.ReadConfig(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	testEnv.Config = cfg

	fs := localfs.New()
	alz := newTestCommands()
	af := Flags{}
	if err := alz.analyzeFS(ctx, fs, &af, []string{arg0}); err != nil {
		t.Fatal(err)
	}

	scanned, summary := verifyDB(ctx, t, cfg, fs, arg0, scannable)
	nDirs, nFiles := clitest.NumDirsAndFiles(scanned)

	compareSummary(t, summary, nDirs, nFiles, 0, 0, 0, nDirs+nFiles-1)

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
		nDirs-1)

	// Run again, pick up new files and directories.
	if err := tt.CreateAdditional(3, 2, 1); err != nil {
		t.Fatal(err)
	}

	if err := tt.CreateAdditional(4, 2, 0); err != nil {
		t.Fatal(err)
	}

	if err := alz.analyzeFS(ctx, fs, &af, []string{arg0}); err != nil {
		t.Fatal(err)
	}

	expanded := slices.Clone(scannable)
	expanded = append(expanded, tt.Additional()...)
	sort.Strings(expanded)
	nAddedDirs, nAddedFiles := clitest.NumDirsAndFiles(tt.Additional())
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
		nDirs+nAddedDirs+nAddedFiles-1)

	// Run again, make sure deletions are detected.
	if err := tt.DeleteAdditional(); err != nil {
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
		nDirs-1)
}

func TestAnalyzeByName(t *testing.T) {
	ctx := context.Background()
	tmpDir, cfgFile, arg0, tt := clitest.SetupAnalyze(t)
	defer os.RemoveAll(tmpDir)

	scannable := slices.Clone(tt.Base())
	sort.Strings(scannable)
	scannable = removeExclusions(scannable)

	cfg, err := iduconfig.ReadConfig(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	testEnv.Config = cfg
	clitest.WithFS(t, testEnv, "testtree")

	fs := localfs.New()
	alz := newTestCommands()
	af := Flags{}
	if err := alz.analyzeFS(ctx, fs, &af, nil); err != nil {
		t.Fatal(err)
	}

	_, resolvedCfg, target, db, err := openPrefixReadOnly(ctx, cfg, "")
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
	alz := newTestCommands()
	if err := alz.analyzeFS(context.Background(), localfs.New(), &Flags{Defaults: true}, nil); err != nil {
		t.Fatalf("analyzeFS defaults: %v", err)
	}
}

func setupAnalyzeTestConfig(t *testing.T, root string) iduconfig.T {
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
	cfg, err := iduconfig.ParseConfig([]byte(cfgText))
	if err != nil {
		t.Fatal(err)
	}
	origCfg := testEnv.Config
	testEnv.Config = cfg
	t.Cleanup(func() {
		testEnv.Config = origCfg
	})
	return cfg
}

func TestAnalyzeForceAndSubtree(t *testing.T) {
	ctx := context.Background()
	alz := newTestCommands()
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
	if err := alz.analyzeFS(ctx, lfs, &Flags{}, []string{root}); err != nil {
		t.Fatalf("initial analyzeFS: %v", err)
	}

	// Force re-analyzes unchanged prefixes.
	if err := alz.analyzeFS(ctx, lfs, &Flags{Force: true}, []string{root}); err != nil {
		t.Fatalf("force analyzeFS: %v", err)
	}
	_, _, _, db, err := openPrefixReadOnly(ctx, cfg, root)
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
	if err := alz.analyzeFS(ctx, lfs, &Flags{}, []string{root}); err != nil {
		t.Fatalf("analyzeFS after subdir change: %v", err)
	}
	_, _, _, db2, err := openPrefixReadOnly(ctx, cfg, root)
	if err != nil {
		t.Fatal(err)
	}
	_, _, sumSub := getLastLog(ctx, t, db2)
	_ = db2.Close(ctx)
	if sumSub.ParentUnchanged != 1 || sumSub.ChildrenUnchanged != 0 {
		t.Errorf("got ParentUnchanged=%d ChildrenUnchanged=%d, want 1, 0", sumSub.ParentUnchanged, sumSub.ChildrenUnchanged)
	}

	// Sub-prefix scan (target != cfg.Prefix).
	if err := alz.analyzeFS(ctx, lfs, &Flags{}, []string{sub}); err != nil {
		t.Fatalf("sub-prefix analyzeFS: %v", err)
	}
}

func TestAnalyzeSymlink(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	root := filepath.Join(tmpDir, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	sym := filepath.Join(tmpDir, "sym-root")
	if err := os.Symlink(root, sym); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlinks unavailable: %v", err)
		}
		t.Fatal(err)
	}
	cfgText := fmt.Sprintf("filesystems:\n  - name: sym\n    prefix: %s\n    database: %s\n", sym, filepath.Join(tmpDir, "symdb"))
	cfg, err := iduconfig.ParseConfig([]byte(cfgText))
	if err != nil {
		t.Fatal(err)
	}
	origCfg := testEnv.Config
	testEnv.Config = cfg
	t.Cleanup(func() { testEnv.Config = origCfg })

	alz := newTestCommands()
	if err := alz.analyzeFS(ctx, localfs.New(), &Flags{}, []string{sym}); err != nil {
		t.Fatalf("symlink analyzeFS: %v", err)
	}
}

func TestAnalyzeFaults(t *testing.T) {
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

	cfg := setupAnalyzeTestConfig(t, root)
	alz := newTestCommands()
	lfs := localfs.New()

	t.Run("xattr error and lstat error logged", func(t *testing.T) {
		ffs := &faultAnalyzeFS{
			FS:           lfs,
			xattrErrPath: root,
			xattrErr:     errors.New("xattr failed"),
			lstatErrPath: f1,
			lstatErr:     errors.New("lstat failed"),
		}
		if err := alz.analyzeFS(ctx, ffs, &Flags{}, []string{root}); err != nil {
			t.Fatalf("faulty analyzeFS: %v", err)
		}
		_, _, _, db, err := openPrefixReadOnly(ctx, cfg, root)
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
		if err := alz.analyzeFS(ctx, ffs, &Flags{}, []string{root}); err != nil {
			t.Fatalf("analyzeFS: %v", err)
		}
		_, _, _, db, err := openPrefixReadOnly(ctx, cfg, root)
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
		if err := alz.analyzeFS(ctx, lfs, &Flags{}, []string{"/does/not/exist/cfg"}); err == nil {
			t.Fatal("expected error for unknown prefix")
		}
	})
}

type interruptFS struct {
	filewalk.FS
	root      string
	onFirst   func()
	once      sync.Once
	triggered chan struct{}
	inWalk    atomic.Int32
}

func (f *interruptFS) XAttr(ctx context.Context, path string, info file.Info) (file.XAttr, error) {
	if path != f.root {
		f.inWalk.Add(1)
		defer f.inWalk.Add(-1)
		f.once.Do(func() {
			f.onFirst()
			close(f.triggered)
		})
		// Wait for ctx cancellation so the signal handler has executed before
		// this walker finishes.
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
		time.Sleep(20 * time.Millisecond)
	}
	return f.FS.XAttr(ctx, path, info)
}

func TestAnalyzeInterruptDoesNotCloseDBEarly(t *testing.T) {
	tmpDir := t.TempDir()
	// localfs.scanner can leave a directory *os.File open until GC when
	// canceled mid-scan; run GC before t.TempDir() cleanup on Windows.
	t.Cleanup(func() {
		runtime.GC()
		runtime.Gosched()
		runtime.GC()
	})
	root := filepath.Join(tmpDir, "root")
	for i := range 12 {
		sub := filepath.Join(root, fmt.Sprintf("d%02d", i))
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		for j := range 5 {
			if err := os.WriteFile(filepath.Join(sub, fmt.Sprintf("f%02d", j)), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}

	cfg := setupAnalyzeTestConfig(t, root)
	lfs := localfs.New()

	runInterrupt := func(t *testing.T, ctx context.Context, onFirst func()) {
		t.Helper()
		var ifs *interruptFS
		var closedWhileWalk atomic.Bool
		var closedCount atomic.Int32
		alz := &Commands{
			env: testEnv,
			beforeClose: func() {
				if ifs.inWalk.Load() > 0 {
					closedWhileWalk.Store(true)
				}
				closedCount.Add(1)
			},
		}
		ifs = &interruptFS{
			FS:        lfs,
			root:      root,
			onFirst:   onFirst,
			triggered: make(chan struct{}),
		}
		err := alz.analyzeFS(ctx, ifs, &Flags{}, []string{root})
		<-ifs.triggered
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("analyzeFS err = %v, want context.Canceled", err)
		}
		if closedWhileWalk.Load() {
			t.Fatal("database was closed while filesystem walk was still active")
		}
		if closedCount.Load() == 0 {
			t.Fatal("expected database Close to be called after walk finished")
		}
	}

	t.Run("context cancel mid-walk", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runInterrupt(t, ctx, cancel)
	})

	t.Run("os.Interrupt mid-walk", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("os.Process.Signal(os.Interrupt) is not supported on Windows")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		runInterrupt(t, ctx, func() {
			p, err := os.FindProcess(os.Getpid())
			if err == nil {
				_ = p.Signal(os.Interrupt)
			}
		})
	})

	// Database must still open cleanly after interrupted runs.
	_, _, _, db, err := openPrefixReadOnly(context.Background(), cfg, root)
	if err != nil {
		t.Fatalf("OpenPrefixAndDatabase after interrupt: %v", err)
	}
	_ = db.Close(context.Background())
}

func setupPopulatedRootAndSub(t *testing.T) (iduconfig.T, string, string) {
	t.Helper()
	ctx := context.Background()
	tmpDir := t.TempDir()
	root := filepath.Join(tmpDir, "root")
	sub := filepath.Join(root, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "f0"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "f1"), []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := setupAnalyzeTestConfig(t, root)
	alz := newTestCommands()
	if err := alz.analyzeFS(ctx, localfs.New(), &Flags{}, []string{root}); err != nil {
		t.Fatalf("initial analyzeFS: %v", err)
	}
	return cfg, root, sub
}

func assertSubtreePreserved(t *testing.T, cfg iduconfig.T, root, sub string) {
	t.Helper()
	ctx := context.Background()
	_, _, _, rodb, err := openPrefixReadOnly(ctx, cfg, root)
	if err != nil {
		t.Fatal(err)
	}
	defer rodb.Close(ctx)

	var buf bytes.Buffer
	if err := rodb.Get(ctx, sub, &buf); err != nil || buf.Len() == 0 {
		t.Errorf("expected %s to be preserved in DB, got (%d bytes, %v)", sub, buf.Len(), err)
	}
	buf.Reset()
	if err := rodb.Get(ctx, root, &buf); err != nil || buf.Len() == 0 {
		t.Fatalf("expected %s in DB, got (%d bytes, %v)", root, buf.Len(), err)
	}
	var pi prefixinfo.T
	if err := pi.UnmarshalBinary(buf.Bytes()); err != nil || len(pi.InfoList()) != 2 {
		t.Errorf("expected %s InfoList len 2 preserved, got err=%v len=%d", root, err, len(pi.InfoList()))
	}
}

func TestAnalyzeLevelScannerErrorPreservesSubtree(t *testing.T) {
	ctx := context.Background()
	cfg, root, sub := setupPopulatedRootAndSub(t)
	alz := newTestCommands()
	ffs := &faultAnalyzeFS{
		FS:          localfs.New(),
		scanErrPath: root,
		scanErr:     errors.New("transient read error"),
	}
	if err := alz.analyzeFS(ctx, ffs, &Flags{}, []string{root}); err != nil {
		t.Fatalf("analyzeFS: %v", err)
	}
	assertSubtreePreserved(t, cfg, root, sub)
}

func setupExclusionTree(t *testing.T) (root, ignoredDir, dbDir string) {
	t.Helper()
	tmpDir := t.TempDir()
	root = filepath.Join(tmpDir, "root")
	sub := filepath.Join(root, "sub")
	ignoredDir = filepath.Join(root, "ignored_dir")
	for _, d := range []string{sub, ignoredDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{
		filepath.Join(root, "keep.txt"),
		filepath.Join(root, "skip.tmp"),
		filepath.Join(sub, "nested.txt"),
		filepath.Join(ignoredDir, "inside.txt"),
	} {
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root, ignoredDir, filepath.Join(tmpDir, "db")
}

func assertRootInfoAndAbsent(ctx context.Context, t *testing.T, cfg iduconfig.T, root, ignoredDir string, wantFiles int64, wantNames []string) {
	t.Helper()
	_, _, _, db, err := openPrefixReadOnly(ctx, cfg, root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close(ctx) }()

	_, _, sum := getLastLog(ctx, t, db)
	if sum.Files != wantFiles {
		t.Errorf("Files = %d, want %d", sum.Files, wantFiles)
	}

	var buf bytes.Buffer
	if err := db.Get(ctx, root, &buf); err != nil {
		t.Fatal(err)
	}
	var rootPI prefixinfo.T
	if err := rootPI.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, fi := range rootPI.InfoList() {
		names = append(names, fi.Name())
	}
	sort.Strings(names)
	if !slices.Equal(names, wantNames) {
		t.Errorf("root InfoList names = %v, want %v", names, wantNames)
	}

	buf.Reset()
	if err := db.Get(ctx, ignoredDir, &buf); err != nil || buf.Len() != 0 {
		t.Errorf("expected ignored_dir absent from DB, got (%d bytes, %v)", buf.Len(), err)
	}
}

func TestAnalyzeExclusions(t *testing.T) {
	ctx := context.Background()
	root, ignoredDir, dbDir := setupExclusionTree(t)

	cfgText := fmt.Sprintf(`filesystems:
  - name: tree
    prefix: %s
    database: %s
    exclusions:
      - '\.tmp$'
      - 'ignored_dir'
`, root, dbDir)
	cfg, err := iduconfig.ParseConfig([]byte(cfgText))
	if err != nil {
		t.Fatal(err)
	}
	origCfg := testEnv.Config
	testEnv.Config = cfg
	t.Cleanup(func() { testEnv.Config = origCfg })

	alz := newTestCommands()
	lfs := localfs.New()
	if err := alz.analyzeFS(ctx, lfs, &Flags{}, []string{root}); err != nil {
		t.Fatalf("analyzeFS: %v", err)
	}
	assertRootInfoAndAbsent(ctx, t, cfg, root, ignoredDir, 2, []string{"keep.txt", "sub"})

	// Re-run with unchanged parent: excluded files must still not be counted.
	if err := alz.analyzeFS(ctx, lfs, &Flags{}, []string{root}); err != nil {
		t.Fatalf("second analyzeFS: %v", err)
	}
	assertRootInfoAndAbsent(ctx, t, cfg, root, ignoredDir, 2, []string{"keep.txt", "sub"})
}

func TestAnalyzeMinorEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("showDefaults formatting and spelling", func(t *testing.T) {
		out, err := clitest.CaptureStdout(t, func() error {
			return (newTestCommands()).analyzeFS(ctx, localfs.New(), &Flags{Defaults: true}, nil)
		})
		if err != nil {
			t.Fatalf("analyzeFS(Defaults): %v", err)
		}
		if !strings.Contains(out, "concurrent_stats_threshold:") {
			t.Errorf("showDefaults output = %q, want concurrent_stats_threshold:", out)
		}
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			if strings.Contains(line, "concurrent_stats:") && strings.Contains(line, "concurrent_stats_threshold:") {
				t.Errorf("missing newline after concurrent_stats in %q", out)
			}
		}
	})

	t.Run("cl quotes single space or tab", func(t *testing.T) {
		origArgs := os.Args
		t.Cleanup(func() { os.Args = origArgs })
		os.Args = []string{"idu", "analyze", "/dir with spaces", "/dir\twith\ttabs"}
		got := cl()
		if !strings.Contains(got, `"/dir with spaces"`) || !strings.Contains(got, `"/dir\twith\ttabs"`) {
			t.Errorf("cl() = %q, expected quoted args with spaces/tabs", got)
		}
	})
}
