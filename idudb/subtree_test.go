// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/topn"
	"cloudeng.io/cmd/idu/stats"
)

// storedSubtrees returns the stored state and recursive totals, keyed by
// path, together with the subtree rankings and per-id totals.
type storedSubtrees struct {
	state    string
	totals   map[string]stats.Totals
	rankings topn.Rankings
	ids      topn.IDTotals
}

func readSubtrees(ctx context.Context, t *testing.T, dbDir string) storedSubtrees {
	t.Helper()
	db, err := openTestDB(ctx, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	var st storedSubtrees
	if st.state, err = subtreesState(ctx, db.db); err != nil {
		t.Fatal(err)
	}
	st.totals = map[string]stats.Totals{}
	var decodeErr error
	err = db.db.ScanSubtrees(ctx, "", "", func(_ context.Context, key string, val []byte) bool {
		path := stats.DecodeSummaryKey(db.sep, key)
		st.totals[path], decodeErr = decodeSubtree(path, val)
		return decodeErr == nil
	})
	if err != nil || decodeErr != nil {
		t.Fatal(err, decodeErr)
	}
	if _, err := getRankings(ctx, db.db, topn.SubtreesKey, &st.rankings); err != nil {
		t.Fatal(err)
	}
	if val, err := getMeta(ctx, db.db, topn.IDTotalsKey); err != nil || len(val) == 0 {
		t.Fatalf("ids: %v", err)
	} else if err := st.ids.UnmarshalBinary(val); err != nil {
		t.Fatal(err)
	}
	return st
}

// sameSubtrees checks that the incrementally maintained recursive totals,
// subtree rankings and per-id totals match those computed from scratch.
func sameSubtrees(t *testing.T, incremental, rebuilt storedSubtrees) {
	t.Helper()
	if incremental.state != subtreesValid || rebuilt.state != subtreesValid {
		t.Errorf("states: %q %q", incremental.state, rebuilt.state)
	}
	if !maps.Equal(incremental.totals, rebuilt.totals) {
		t.Errorf("got %v, want %v", incremental.totals, rebuilt.totals)
	}
	for _, m := range stats.Metrics() {
		if got, want := incremental.rankings[m].Top(0), rebuilt.rankings[m].Top(0); !slices.Equal(got, want) {
			t.Errorf("%v: got %v, want %v", m, got, want)
		}
	}
	if !maps.Equal(incremental.ids.Users, rebuilt.ids.Users) || !maps.Equal(incremental.ids.Groups, rebuilt.ids.Groups) {
		t.Errorf("ids: got %+v, want %+v", incremental.ids, rebuilt.ids)
	}
}

// injectSubtreeSentinel adds recursive totals for a nonexistent prefix
// outside of the analyzed filesystem; they are only removed by a rebuild.
func injectSubtreeSentinel(ctx context.Context, t *testing.T, dbDir string) {
	t.Helper()
	db, err := openTestDB(ctx, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	key, _ := stats.SummaryKeys(db.sep, "/sentinel")
	tot := stats.Totals{Files: 1}
	if err := db.db.SetSubtree(ctx, key, tot.AppendBinary(nil), false); err != nil {
		t.Fatal(err)
	}
}

func hasSubtreeSentinel(st storedSubtrees) bool {
	_, ok := st.totals["/sentinel"]
	return ok
}

// checkRecursive checks the number of files, and their bytes, ie.
// excluding the sizes of the prefixes themselves, in path's subtree.
func checkRecursive(t *testing.T, st storedSubtrees, path string, files, bytes int64) {
	t.Helper()
	tot, ok := st.totals[path]
	if !ok || tot.Files != files || tot.Bytes-tot.PrefixBytes != bytes {
		t.Errorf("%v: got %+v (%v), want %v files, %v bytes", path, tot, ok, files, bytes)
	}
}

func TestSubtreesIncremental(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	a, b := filepath.Join(sub, "a"), filepath.Join(sub, "a", "b")
	writeSized(t, filepath.Join(b, "f"), 1000)
	dbOpts := []Option{WithTopN(10, 20, 5, 10)}

	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	st := readSubtrees(ctx, t, dbDir)
	if st.state != subtreesValid || len(st.totals) != 4 {
		t.Fatalf("state %q, totals %v", st.state, st.totals)
	}
	checkRecursive(t, st, root, 3, 1002)
	checkRecursive(t, st, sub, 2, 1001)
	checkRecursive(t, st, a, 1, 1000)
	checkRecursive(t, st, b, 1, 1000)
	if top := st.rankings[stats.MetricFiles].Top(0); len(top) != 4 || top[0].Key != root || top[0].Value != 3 {
		t.Errorf("got %v", top)
	}

	// The sentinel survives incremental maintenance.
	injectSubtreeSentinel(ctx, t, dbDir)

	// Grow b, add a new subtree beneath a and another beneath root, and
	// shrink sub.
	c, d := filepath.Join(a, "c"), filepath.Join(root, "d", "e")
	writeSized(t, filepath.Join(b, "g"), 500)
	writeSized(t, filepath.Join(c, "f"), 200)
	writeSized(t, filepath.Join(d, "f"), 300)
	if err := os.Remove(filepath.Join(sub, "f1")); err != nil {
		t.Fatal(err)
	}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	st = readSubtrees(ctx, t, dbDir)
	if !hasSubtreeSentinel(st) {
		t.Fatalf("unexpected rebuild")
	}
	checkRecursive(t, st, root, 5, 2001)
	checkRecursive(t, st, sub, 3, 1700)
	checkRecursive(t, st, a, 3, 1700)
	checkRecursive(t, st, c, 1, 200)
	checkRecursive(t, st, filepath.Dir(d), 1, 300)

	// Delete a's subtree.
	if err := os.RemoveAll(a); err != nil {
		t.Fatal(err)
	}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	incremental := readSubtrees(ctx, t, dbDir)
	if !hasSubtreeSentinel(incremental) {
		t.Fatalf("unexpected rebuild")
	}
	checkRecursive(t, incremental, root, 2, 301)
	checkRecursive(t, incremental, sub, 0, 0)
	for _, p := range []string{a, b, c} {
		if _, ok := incremental.totals[p]; ok {
			t.Errorf("%v: not deleted", p)
		}
	}
	delete(incremental.totals, "/sentinel")

	refreshTopNFor(ctx, t, dbDir, dbOpts)
	rebuilt := readSubtrees(ctx, t, dbDir)
	if hasSubtreeSentinel(rebuilt) {
		t.Errorf("sentinel not removed by rebuild")
	}
	sameSubtrees(t, incremental, rebuilt)
}

func TestSubtreesPartialAnalysis(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	dbOpts := []Option{WithTopN(10, 20, 5, 10)}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)

	// Analyzing only sub must still update root's recursive totals.
	writeSized(t, filepath.Join(sub, "x", "f"), 100)
	db, err := openTestDB(ctx, dbDir, dbOpts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewAnalyzer(db, root).Analyze(ctx, sub); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}
	incremental := readSubtrees(ctx, t, dbDir)
	checkRecursive(t, incremental, root, 3, 102)
	checkRecursive(t, incremental, sub, 2, 101)
	refreshTopNFor(ctx, t, dbDir, dbOpts)
	sameSubtrees(t, incremental, readSubtrees(ctx, t, dbDir))
}

// startScan starts, but does not perform, an analysis, returning the
// observer used to maintain the derived state.
func startScan(ctx context.Context, t *testing.T, db *DB, root string) *topnObserver {
	t.Helper()
	obs := newTopNObserver(db)
	if err := obs.ScanStarted(ctx, ScanInfo{Root: root, Target: root}); err != nil {
		t.Fatal(err)
	}
	return obs
}

func TestSubtreesInterrupted(t *testing.T) {
	ctx := context.Background()
	root, _, dbDir := createRootAndSub(t)
	dbOpts := []Option{WithTopN(10, 20, 5, 10)}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)

	db, err := openTestDB(ctx, dbDir, dbOpts...)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	if obs := startScan(ctx, t, db, root); obs.rebuild {
		t.Errorf("unexpected rebuild")
	} else if err := obs.ScanDone(ctx, errors.New("interrupted")); err != nil {
		t.Fatal(err)
	}
	if err := CheckSubtrees(ctx, db.db); !errors.Is(err, ErrStaleSubtrees) {
		t.Errorf("got %v, want %v", err, ErrStaleSubtrees)
	}
	if obs := startScan(ctx, t, db, root); !obs.rebuild || !obs.refresh {
		t.Errorf("expected rebuild and refresh")
	} else if err := obs.ScanDone(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := CheckSubtrees(ctx, db.db); err != nil {
		t.Errorf("got %v", err)
	}
}

func TestSubtreesInconsistent(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	dbOpts := []Option{WithTopN(10, 20, 5, 10)}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	injectSubtreeSentinel(ctx, t, dbDir)

	// Zero root's recursive totals so that deleting sub makes them
	// negative, which must be detected and repaired by a rebuild.
	db, err := openTestDB(ctx, dbDir, dbOpts...)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := stats.SummaryKeys(db.sep, root)
	var zero stats.Totals
	if err := db.db.SetSubtree(ctx, key, zero.AppendBinary(nil), false); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(sub); err != nil {
		t.Fatal(err)
	}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	st := readSubtrees(ctx, t, dbDir)
	if hasSubtreeSentinel(st) || st.state != subtreesValid {
		t.Errorf("expected a rebuild: %q %v", st.state, st.totals)
	}
	checkRecursive(t, st, root, 1, 1)
}

func TestSubtreesInvalidatedWithoutTopN(t *testing.T) {
	ctx := context.Background()
	root, _, dbDir := createRootAndSub(t)
	analyzeTopN(ctx, t, dbDir, root, []Option{WithTopN(10, 20, 5, 10)})
	// An analysis that does not maintain the derived state must mark it
	// as invalid.
	analyzeTopN(ctx, t, dbDir, root, nil)
	db, err := openTestDB(ctx, dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	if err := CheckSubtrees(ctx, db.db); !errors.Is(err, ErrStaleSubtrees) {
		t.Errorf("got %v, want %v", err, ErrStaleSubtrees)
	}
	if s, ok, err := getTopNState(ctx, db.db); err != nil || !ok || s.Valid {
		t.Errorf("state: %+v %v %v", s, ok, err)
	}
}

func TestSubtreeChanges(t *testing.T) {
	key := func(p string) string {
		k, _ := stats.SummaryKeys("/", p)
		return k
	}
	sc := newSubtreeChanges(key("/r"))
	sc.prefixChanged(key("/r/a/b"), nil, stats.Totals{Files: 2, Prefix: 1})
	sc.prefixChanged(key("/r/a"), &stats.Totals{Files: 5}, stats.Totals{Files: 3})
	sc.subtreeDeleted(key("/r/c"), stats.Totals{Files: 7})
	sc.subtreeDeleted(key("/r/a/b/x"), stats.Totals{Files: 1})
	sc.prefixChanged(key("/r/a/b/x/y"), nil, stats.Totals{Files: 100})
	want := map[string]stats.Totals{
		key("/r"):         {Files: 92, Prefix: 1}, // 2 - 2 - 7 - 1 + 100
		key("/r/a"):       {Files: 99, Prefix: 1}, // 2 - 2 - 1 + 100
		key("/r/a/b"):     {Files: 2 - 1 + 100, Prefix: 1},
		key("/r/a/b/x"):   {Files: 100},
		key("/r/a/b/x/y"): {Files: 100},
	}
	if !maps.Equal(sc.changes, want) {
		t.Errorf("got %v, want %v", sc.changes, want)
	}
	for p, deleted := range map[string]bool{
		"/r": false, "/r/a": false, "/r/c": true, "/r/c/d": true,
		"/r/a/b/x": true, "/r/a/b/x/y": true, "/r/a/b": false,
	} {
		if got := sc.isDeleted(key(p)); got != deleted {
			t.Errorf("%v: got %v, want %v", p, got, deleted)
		}
	}
}

// mutateTree randomly adds and removes files and directories beneath root.
func mutateTree(t *testing.T, rnd *rand.Rand, root string, dirs *[]string) {
	t.Helper()
	for range 10 {
		parent := (*dirs)[rnd.IntN(len(*dirs))]
		switch rnd.IntN(4) {
		case 0, 1:
			writeSized(t, filepath.Join(parent, fmt.Sprintf("f%d", rnd.IntN(5))), rnd.IntN(10000)+1)
		case 2:
			d := filepath.Join(parent, fmt.Sprintf("d%d", rnd.IntN(3)))
			writeSized(t, filepath.Join(d, "f"), rnd.IntN(10000)+1)
			if !slices.Contains(*dirs, d) {
				*dirs = append(*dirs, d)
			}
		case 3:
			if parent == root {
				continue
			}
			if err := os.RemoveAll(parent); err != nil {
				t.Fatal(err)
			}
			*dirs = slices.DeleteFunc(*dirs, func(d string) bool {
				return d == parent || strings.HasPrefix(d, parent+string(filepath.Separator))
			})
		}
	}
}

func TestSubtreesRandomized(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	dirs := []string{root, sub}
	rnd := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // deterministic test data
	// Small rankings so that some analyses refresh them.
	dbOpts := []Option{WithTopN(2, 3, 1, 2)}
	analyzeTopN(ctx, t, dbDir, root, dbOpts)
	for i := range 8 {
		mutateTree(t, rnd, root, &dirs)
		// Ensure that directory mtimes change.
		time.Sleep(10 * time.Millisecond)
		injectSubtreeSentinel(ctx, t, dbDir)
		analyzeTopN(ctx, t, dbDir, root, dbOpts)
		incremental := readSubtrees(ctx, t, dbDir)
		if !hasSubtreeSentinel(incremental) {
			t.Fatalf("round %v: unexpected rebuild", i)
		}
		delete(incremental.totals, "/sentinel")
		refreshTopNFor(ctx, t, dbDir, dbOpts)
		rebuilt := readSubtrees(ctx, t, dbDir)
		if !maps.Equal(incremental.totals, rebuilt.totals) {
			t.Errorf("round %v: got %v, want %v", i, incremental.totals, rebuilt.totals)
		}
		if !maps.Equal(incremental.ids.Users, rebuilt.ids.Users) {
			t.Errorf("round %v: ids: got %v, want %v", i, incremental.ids.Users, rebuilt.ids.Users)
		}
	}
}
