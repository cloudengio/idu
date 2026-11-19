// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/hardlinks"
	"cloudeng.io/cmd/idu/stats"
)

// walkSubtrees computes the recursive totals of every prefix from the
// summaries with their hardlinks deduplicated as summary.Walk does, ie.
// the expected value of the stored recursive totals.
func walkSubtrees(ctx context.Context, t *testing.T, dbDir string, opts ...Option) map[string]stats.Totals {
	t.Helper()
	db, err := openTestDB(ctx, dbDir, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	totals := map[string]stats.Totals{}
	agg := subtreeAggregator{done: func(_, path string, tot stats.Totals) error {
		totals[path] = tot
		return nil
	}}
	hl := &hardlinks.Incremental{}
	var ps stats.PrefixSummary
	var visitErr error
	err = db.db.ScanSummaries(ctx, "", "", func(_ context.Context, key string, val []byte) bool {
		if visitErr = ps.UnmarshalBinary(val); visitErr != nil {
			return false
		}
		ps.ApplyHardlinks(hl)
		visitErr = agg.step(key, stats.DecodeSummaryKey(db.sep, key), ps.Direct)
		return visitErr == nil
	})
	if err != nil || visitErr != nil {
		t.Fatal(err, visitErr)
	}
	if err := agg.finish(); err != nil {
		t.Fatal(err)
	}
	return totals
}

// listTree returns the directories and files beneath root.
func listTree(t *testing.T, root string) (dirs, files []string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			dirs = append(dirs, path)
		} else {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return dirs, files
}

// mutateLinks randomly adds and removes files, hardlinks and directories
// beneath root.
func mutateLinks(t *testing.T, rnd *rand.Rand, root string, round int) {
	t.Helper()
	for i := range 12 {
		dirs, files := listTree(t, root)
		parent := dirs[rnd.IntN(len(dirs))]
		name := fmt.Sprintf("r%d-%d", round, i)
		switch op := rnd.IntN(6); {
		case op <= 1 || len(files) == 0:
			writeSized(t, filepath.Join(parent, name), rnd.IntN(10000)+1)
		case op <= 3:
			// Hardlinks are created to files in other directories as well
			// as in the same one.
			target := files[rnd.IntN(len(files))]
			if err := os.Link(target, filepath.Join(parent, name)); err != nil {
				t.Fatal(err)
			}
		case op == 4:
			if err := os.Remove(files[rnd.IntN(len(files))]); err != nil {
				t.Fatal(err)
			}
		default:
			if parent == root || rnd.IntN(2) == 0 {
				writeSized(t, filepath.Join(parent, name, "f"), rnd.IntN(10000)+1)
				continue
			}
			if err := os.RemoveAll(parent); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestLinksRandomized(t *testing.T) {
	for _, rec := range []stats.HardlinkRecording{stats.RecordLinkedFiles, stats.RecordAllFiles} {
		t.Run(rec.String(), func(t *testing.T) {
			testLinksRandomized(t, rec)
		})
	}
}

func testLinksRandomized(t *testing.T, rec stats.HardlinkRecording) {
	ctx := context.Background()
	root, _, dbDir := createRootAndSub(t)
	rnd := rand.New(rand.NewPCG(3, 4)) //nolint:gosec // deterministic test data
	// Small rankings so that some analyses refresh them.
	dbOpts := []Option{WithTopN(2, 3, 1, 2), WithHardlinkRecording(rec)}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	withLinks := 0
	for i := range 10 {
		mutateLinks(t, rnd, root, i)
		// Ensure that directory mtimes change.
		time.Sleep(10 * time.Millisecond)
		injectSubtreeSentinel(ctx, t, dbDir, dbOpts...)
		analyzeTopN(ctx, t, dbDir, root, dbOpts)
		incremental := readSubtrees(ctx, t, dbDir, dbOpts...)
		if !hasSubtreeSentinel(incremental) {
			t.Fatalf("round %v: unexpected rebuild", i)
		}
		delete(incremental.totals, "/sentinel")
		if want := walkSubtrees(ctx, t, dbDir, dbOpts...); !maps.Equal(incremental.totals, want) {
			for _, p := range slices.Sorted(maps.Keys(want)) {
				if incremental.totals[p] != want[p] {
					t.Errorf("round %v: %v: got %+v, want %+v", i, p, incremental.totals[p], want[p])
				}
			}
			t.Fatalf("round %v: incremental totals differ from summary.Walk", i)
		}
		if incremental.totals[root].Hardlinks > 0 {
			withLinks++
		}
		refreshTopNFor(ctx, t, dbDir, dbOpts)
		rebuilt := readSubtrees(ctx, t, dbDir, dbOpts...)
		if !maps.Equal(incremental.totals, rebuilt.totals) {
			t.Errorf("round %v: got %v, want %v", i, incremental.totals, rebuilt.totals)
		}
		for _, ids := range [][2]map[int64]stats.Totals{
			{incremental.ids.Users, rebuilt.ids.Users},
			{incremental.ids.Groups, rebuilt.ids.Groups},
		} {
			if !maps.Equal(ids[0], ids[1]) {
				t.Errorf("round %v: ids: got %v, want %v", i, ids[0], ids[1])
			}
		}
	}
	if withLinks < 5 {
		t.Errorf("only %v rounds deduplicated hardlinks", withLinks)
	}
}

// synthTree generates summaries directly, rather than by analyzing a
// filesystem, so that prefixes can share their device and inode numbers,
// as bind mounts and withLinks directories do.
type synthTree struct {
	rnd     *rand.Rand
	dbDir   string
	opts    []Option
	paths   map[string]bool // the prefixes that exist
	nextIno uint64
}

const (
	synthRoot  = "/r"
	synthP0Ino = 500
	synthP1Ino = 501
)

// synthShared are the prefixes that always exist, with these device and
// inode numbers, which other prefixes may share. There are always at least
// two prefixes with each of them, so that they are found by every rebuild;
// a device and inode number shared by prefixes created after a rebuild is
// not found until the next rebuild.
var synthShared = map[string]uint64{
	"/r/p0": synthP0Ino, "/r/s0": synthP0Ino,
	"/r/p1": synthP1Ino, "/r/s1": synthP1Ino,
}

var synthPaths = []string{
	"/r/a", "/r/a/x", "/r/a/x/y", "/r/b", "/r/b/y", "/r/c", "/r/p0/x",
	"/r/p1/x", "/r/q", "/r/q/z", "/r/s0/x", "/r/z",
}

func (st *synthTree) summary(dirIno uint64) stats.PrefixSummary {
	ps := stats.PrefixSummary{HardlinkInfo: true, Device: 1, FileID: dirIno}
	ps.Direct = stats.Totals{Prefix: 1, Bytes: 10, PrefixBytes: 10}
	users, groups := map[int64]stats.Totals{}, map[int64]stats.Totals{}
	add := func(uid, gid int64, f stats.FileEntry) {
		ps.Direct.Files++
		ps.Direct.Bytes += f.Bytes
		ps.Direct.StorageBytes += f.StorageBytes
		for _, id := range []struct {
			m  map[int64]stats.Totals
			id int64
		}{{users, uid}, {groups, gid}} {
			t := id.m[id.id]
			t.Files++
			t.Bytes += f.Bytes
			t.StorageBytes += f.StorageBytes
			id.m[id.id] = t
		}
	}
	// A file that is not recorded since it has a single link.
	add(1, 1, stats.FileEntry{Bytes: 5, StorageBytes: 8})
	for range st.rnd.IntN(5) {
		ino := uint64(st.rnd.IntN(12) + 1)
		f := stats.FileEntry{FileID: ino, UID: int64(ino%3 + 1), GID: int64(ino%2 + 1), Bytes: int64(ino) * 100, StorageBytes: int64(ino) * 128}
		ps.Files = append(ps.Files, f)
		add(f.UID, f.GID, f)
	}
	for _, id := range []struct {
		m   map[int64]stats.Totals
		pid *stats.PerIDTotals
	}{{users, &ps.PerUser}, {groups, &ps.PerGroup}} {
		for _, k := range slices.Sorted(maps.Keys(id.m)) {
			t := id.m[k]
			t.ID = k
			*id.pid = append(*id.pid, t)
		}
	}
	return ps
}

func (st *synthTree) dirIno(path string) uint64 {
	if ino, ok := synthShared[path]; ok {
		return ino
	}
	if st.rnd.IntN(3) == 0 {
		return []uint64{synthP0Ino, synthP1Ino}[st.rnd.IntN(2)]
	}
	st.nextIno++
	return st.nextIno
}

func (st *synthTree) set(ctx context.Context, t *testing.T, sdb *DB, path string, ps stats.PrefixSummary) {
	t.Helper()
	exactKey, _ := stats.SummaryKeys(sdb.sep, path)
	old := sdb.previousSummary(ctx, path, exactKey)
	if err := sdb.db.SetSummary(ctx, exactKey, ps.AppendBinary(nil), false); err != nil {
		t.Fatal(err)
	}
	sdb.observer.PrefixChanged(ctx, path, old, &ps)
	st.paths[path] = true
}

func (st *synthTree) delete(ctx context.Context, t *testing.T, sdb *DB, path string) {
	t.Helper()
	if err := sdb.deletePrefix(ctx, path); err != nil {
		t.Fatal(err)
	}
	for p := range st.paths {
		if p == path || strings.HasPrefix(p, path+"/") {
			delete(st.paths, p)
		}
	}
}

// scan runs a synthetic analysis that makes n random changes.
func (st *synthTree) scan(ctx context.Context, t *testing.T, n int, initial bool) {
	t.Helper()
	sdb, err := openTestDB(ctx, st.dbDir, st.opts...)
	if err != nil {
		t.Fatal(err)
	}
	obs := newTopNObserver(sdb)
	sdb.observer = obs
	if err := obs.ScanStarted(ctx, ScanInfo{Root: synthRoot, Target: synthRoot}); err != nil {
		t.Fatal(err)
	}
	if initial {
		st.set(ctx, t, sdb, synthRoot, st.summary(st.dirIno(synthRoot)))
		for _, p := range slices.Sorted(maps.Keys(synthShared)) {
			st.set(ctx, t, sdb, p, st.summary(st.dirIno(p)))
		}
	}
	st.mutate(ctx, t, sdb, n)
	if err := sdb.db.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := obs.ScanDone(ctx, nil); err != nil {
		t.Fatal(err)
	}
	sdb.observer = nil
	if err := sdb.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// mutate makes n random changes to the prefixes and then changes the files
// of some of those that share their inodes.
func (st *synthTree) mutate(ctx context.Context, t *testing.T, sdb *DB, n int) {
	t.Helper()
	// As for an analysis, a prefix is never both changed and deleted.
	touched := map[string]bool{}
	isTouched := func(path string) bool {
		for p := range touched {
			if p == path || strings.HasPrefix(p, path+"/") || strings.HasPrefix(path, p+"/") {
				return true
			}
		}
		return false
	}
	for range n {
		path := synthPaths[st.rnd.IntN(len(synthPaths))]
		if isTouched(path) {
			continue
		}
		if _, shared := synthShared[path]; st.paths[path] && !shared && st.rnd.IntN(3) == 0 {
			st.delete(ctx, t, sdb, path)
			touched[path] = true
			continue
		}
		if parent := filepath.Dir(path); !st.paths[parent] {
			continue
		}
		st.set(ctx, t, sdb, path, st.summary(st.dirIno(path)))
		touched[path] = true
	}
	// Change the files of the prefixes that share their inodes.
	for _, p := range slices.Sorted(maps.Keys(synthShared)) {
		if !isTouched(p) && st.rnd.IntN(3) == 0 {
			st.set(ctx, t, sdb, p, st.summary(synthShared[p]))
		}
	}
}

func TestLinksDuplicatePrefixes(t *testing.T) {
	ctx := context.Background()
	st := &synthTree{
		rnd:     rand.New(rand.NewPCG(5, 6)), //nolint:gosec // deterministic test data
		dbDir:   filepath.Join(t.TempDir(), "db"),
		opts:    []Option{WithTopN(2, 3, 1, 2), WithHardlinkRecording(stats.RecordLinkedFiles), WithSeparator("/")},
		paths:   map[string]bool{},
		nextIno: 1000,
	}
	st.scan(ctx, t, 0, true)
	dups := 0
	for i := range 30 {
		injectSubtreeSentinel(ctx, t, st.dbDir, st.opts...)
		st.scan(ctx, t, 6, false)
		incremental := readSubtrees(ctx, t, st.dbDir, st.opts...)
		if !hasSubtreeSentinel(incremental) {
			t.Fatalf("round %v: unexpected rebuild", i)
		}
		delete(incremental.totals, "/sentinel")
		want := walkSubtrees(ctx, t, st.dbDir, st.opts...)
		for _, p := range slices.Sorted(maps.Keys(want)) {
			if incremental.totals[p] != want[p] {
				t.Errorf("round %v: %v: got %+v, want %+v", i, p, incremental.totals[p], want[p])
			}
		}
		if len(incremental.totals) != len(want) {
			t.Errorf("round %v: got %v, want %v", i, incremental.totals, want)
		}
		if t.Failed() {
			t.FailNow()
		}
		if want[synthRoot].HardlinkDirs > 0 {
			dups++
		}
		// Rebuild periodically so that several incremental analyses build
		// upon each other.
		if i%5 == 4 {
			refreshTopNFor(ctx, t, st.dbDir, st.opts)
			rebuilt := readSubtrees(ctx, t, st.dbDir, st.opts...)
			sameSubtrees(t, incremental, rebuilt)
		}
	}
	if dups < 10 {
		t.Errorf("only %v rounds had duplicate prefixes", dups)
	}
}

func TestLinksLegacySubtreesTriggerRebuild(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	dbOpts := []Option{WithTopN(10, 20, 5, 10), WithHardlinkRecording(stats.RecordLinkedFiles)}
	f2, l2 := filepath.Join(root, "f2"), filepath.Join(sub, "l2")
	writeSized(t, f2, 500)
	if err := os.Link(f2, l2); err != nil {
		t.Fatal(err)
	}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	injectSubtreeSentinel(ctx, t, dbDir, dbOpts...)

	// Simulate a database created before hardlink-deduplicated derived state:
	// only subtrees/v1 was written and the link index is empty.
	db, err := openTestDB(ctx, dbDir, dbOpts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.db.SetMeta(ctx, "subtrees/v1", []byte(subtreesValid)); err != nil {
		t.Fatal(err)
	}
	if err := db.db.SetMeta(ctx, metaKeySubtrees, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.db.ClearLinks(ctx); err != nil {
		t.Fatal(err)
	}
	if err := CheckSubtrees(ctx, db.db); !errors.Is(err, ErrStaleSubtrees) {
		t.Fatalf("CheckSubtrees = %v, want %v", err, ErrStaleSubtrees)
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}

	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	st := readSubtrees(ctx, t, dbDir, dbOpts...)
	if hasSubtreeSentinel(st) || st.state != subtreesValid {
		t.Fatalf("expected rebuild to remove sentinel and restore %q: %q %v", subtreesValid, st.state, st.totals)
	}
	if want := walkSubtrees(ctx, t, dbDir, dbOpts...); !maps.Equal(st.totals, want) {
		t.Errorf("got %v, want %v", st.totals, want)
	}
}

func TestLinksErrorHandling(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	dbOpts := []Option{WithTopN(10, 20, 5, 10), WithHardlinkRecording(stats.RecordLinkedFiles)}
	f2, l2 := filepath.Join(root, "f2"), filepath.Join(sub, "l2")
	writeSized(t, f2, 500)
	if err := os.Link(f2, l2); err != nil {
		t.Fatal(err)
	}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)

	db, err := openTestDB(ctx, dbDir, dbOpts...)
	if err != nil {
		t.Fatal(err)
	}
	var dev, ino uint64
	_ = db.db.ScanLinks(ctx, string(fileLinkTag), func(_ context.Context, k string, _ []byte) bool {
		parsed, _, perr := parseLinkKey(k)
		if perr == nil {
			dev, ino = parsed.dev, parsed.ino
			return false
		}
		return true
	})
	if ino == 0 {
		t.Fatal("expected at least one link index entry")
	}

	// Trailing bytes on an occurrence value must return errInconsistentSubtrees.
	key := linkKey(fileLinkTag, inode{dev: dev, ino: ino}, "bad")
	if err := db.db.SetLink(ctx, key, []byte{1, 0}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := readOccurrences(ctx, db.db, inode{dev: dev, ino: ino}); !errors.Is(err, errInconsistentSubtrees) {
		t.Fatalf("readOccurrences trailing bytes = %v, want %v", err, errInconsistentSubtrees)
	}

	// Context cancellation in applyIncremental must propagate rather than triggering a rebuild.
	obs := newTopNObserver(db)
	if err := obs.ScanStarted(ctx, ScanInfo{Root: root, Target: root}); err != nil {
		t.Fatal(err)
	}
	obs.PrefixChanged(ctx, root, nil, &stats.PrefixSummary{
		HardlinkInfo: true,
		Device:       dev,
		FileID:       1,
		Files:        []stats.FileEntry{{FileID: ino}},
	})
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	obs.mu.Lock()
	err = obs.applyIncremental(cctx)
	obs.mu.Unlock()
	if !errors.Is(err, context.Canceled) {
		t.Errorf("applyIncremental(canceled) = %v, want context.Canceled", err)
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Remove sub so an incremental update visits the corrupt link entry and rebuilds.
	injectSubtreeSentinel(ctx, t, dbDir, dbOpts...)
	if err := os.RemoveAll(sub); err != nil {
		t.Fatal(err)
	}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	st := readSubtrees(ctx, t, dbDir, dbOpts...)
	if hasSubtreeSentinel(st) || st.state != subtreesValid {
		t.Errorf("expected rebuild on corrupt link index: %q %v", st.state, st.totals)
	}
}
