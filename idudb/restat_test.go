// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmdutil/cmdyaml"
)

type restatProgress struct {
	nullProgress
	mu               sync.Mutex
	restated, change int
}

func (r *restatProgress) Restated(files, changed int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.restated += files
	r.change += changed
}

// storedFile returns the size and link count stored for prefix/name, and
// the bytes recorded in prefix's summary.
func storedFile(ctx context.Context, t *testing.T, dbDir, prefix, name string, opts ...Option) (size int64, links uint64, summaryBytes int64) {
	t.Helper()
	db, err := openTestDB(ctx, dbDir, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	var pi prefixinfo.T
	if ok, err := db.getPrefixInfo(ctx, prefix, &pi); !ok || err != nil {
		t.Fatalf("%v: %v %v", prefix, ok, err)
	}
	size = -1
	for _, fi := range pi.InfoList() {
		if fi.Name() == name {
			size, links = fi.Size(), pi.XAttrInfo(fi).Hardlinks
		}
	}
	exactKey, _ := stats.SummaryKeys(db.sep, prefix)
	ps, err := db.getSummary(ctx, exactKey)
	if err != nil || ps == nil {
		t.Fatalf("%v: %v %v", prefix, ps, err)
	}
	return size, links, ps.Direct.Bytes
}

func TestRestat(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	f0 := filepath.Join(root, "f0")
	analyzeOnce(ctx, t, dbDir, root)

	rootInfo, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	// Grow f0 in place, which does not change its directory.
	f, err := os.OpenFile(f0, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("more"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if after, err := os.Stat(root); err != nil || !after.ModTime().Equal(rootInfo.ModTime()) {
		t.Fatalf("directory changed: %v", err)
	}

	match := regexp.MustCompile(`/f0$`)
	for i, tc := range []struct {
		restat              func(string) bool
		size                int64
		restated, changed   int
		summaryBytesChanged bool
	}{
		// Without restat the stored size is stale.
		{nil, 1, 0, 0, false},
		{match.MatchString, 5, 1, 1, true},
		// The file is unchanged since it was last re-stat'ed.
		{match.MatchString, 5, 1, 0, false},
	} {
		_, _, before := storedFile(ctx, t, dbDir, root, "f0")
		pt := &restatProgress{}
		analyzeOnce(ctx, t, dbDir, root, WithRestat(tc.restat), WithProgress(pt))
		size, _, after := storedFile(ctx, t, dbDir, root, "f0")
		if size != tc.size {
			t.Errorf("%v: got size %v, want %v", i, size, tc.size)
		}
		if pt.restated != tc.restated || pt.change != tc.changed {
			t.Errorf("%v: got %v restated, %v changed, want %v, %v", i, pt.restated, pt.change, tc.restated, tc.changed)
		}
		if got := before != after; got != tc.summaryBytesChanged {
			t.Errorf("%v: summary bytes %v -> %v", i, before, after)
		}
	}
	// Files in other, unchanged, prefixes are unaffected.
	if size, _, _ := storedFile(ctx, t, dbDir, sub, "f1"); size != 1 {
		t.Errorf("got %v, want 1", size)
	}
}

func TestRecordAllSpec(t *testing.T) {
	ctx := context.Background()
	calc := newOptions(nil).calc.String()
	linked := summarySpec(calc, stats.RecordLinkedFiles)
	if got, want := withRecordAll(linked, "abc"), linked+";record-all=abc"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got := withRecordAll(linked, ""); got != linked {
		t.Errorf("got %q, want %q", got, linked)
	}
	dbDir := filepath.Join(t.TempDir(), "db")
	open := func(opts ...Option) bool {
		t.Helper()
		db, err := openTestDB(ctx, dbDir, opts...)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close(ctx)
		// Record the spec as if an analysis had completed.
		if err := db.db.SetMeta(ctx, metaKeySummaries, []byte(db.spec)); err != nil {
			t.Fatal(err)
		}
		return db.rewriteSummaries
	}
	re := func(p string) func(string) bool { return regexp.MustCompile(p).MatchString }
	rec := WithHardlinkRecording(stats.RecordLinkedFiles)
	open(rec)
	for i, tc := range []struct {
		opts    []Option
		rewrite bool
	}{
		{[]Option{rec, WithRecordAllFiles("a", re("x"))}, true},
		{[]Option{rec, WithRecordAllFiles("a", re("x"))}, false},
		{[]Option{rec, WithRecordAllFiles("b", re("y"))}, true},
		{[]Option{rec}, true},
		// record-all is ignored unless only the linked files are recorded.
		{[]Option{WithHardlinkRecording(stats.RecordAllFiles), WithRecordAllFiles("a", re("x"))}, true},
		{[]Option{WithHardlinkRecording(stats.RecordAllFiles)}, false},
	} {
		if got := open(tc.opts...); got != tc.rewrite {
			t.Errorf("%v: got rewrite %v, want %v", i, got, tc.rewrite)
		}
	}

	// Summaries that record every file for some prefixes satisfy the
	// requirements for those that may be hardlinks, but not for all files.
	raw, err := openDatabase(ctx, filepath.Join(t.TempDir(), "db"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close(ctx)
	if err := raw.SetMeta(ctx, metaKeySummaries, []byte(withRecordAll(linked, "abc"))); err != nil {
		t.Fatal(err)
	}
	for rec, want := range map[stats.HardlinkRecording]error{
		stats.RecordNoFiles:     nil,
		stats.RecordLinkedFiles: nil,
		stats.RecordAllFiles:    ErrStaleSummaries,
	} {
		if err := CheckSummaryRecording(ctx, raw, calc, rec); !errors.Is(err, want) {
			t.Errorf("%v: got %v, want %v", rec, err, want)
		}
	}
}

func TestPatternsID(t *testing.T) {
	list := func(patterns ...string) cmdyaml.RegexpList {
		var rl cmdyaml.RegexpList
		for _, p := range patterns {
			rl = append(rl, cmdyaml.Regexp{Regexp: regexp.MustCompile(p)})
		}
		return rl
	}
	if got := PatternsID(nil); got != "" {
		t.Errorf("got %q", got)
	}
	a, b, c := PatternsID(list("a", "b")), PatternsID(list("a", "b")), PatternsID(list("ab"))
	if a != b || a == c || len(a) != 16 {
		t.Errorf("got %q %q %q", a, b, c)
	}
}

// analyzeLinks analyzes root with the link index enabled.
func analyzeLinks(ctx context.Context, t *testing.T, dbDir, root string, opts ...Option) {
	t.Helper()
	opts = append([]Option{WithTopN(10, 20, 5, 10), WithHardlinkRecording(stats.RecordLinkedFiles)}, opts...)
	db, err := openTestDB(ctx, dbDir, opts...)
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

func missingLinks(ctx context.Context, t *testing.T, dbDir string) []MissingLinksPrefix {
	t.Helper()
	db, err := openTestDB(ctx, dbDir, WithHardlinkRecording(stats.RecordLinkedFiles))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	ml, err := ReadMissingLinks(ctx, db.db)
	if err != nil {
		t.Fatal(err)
	}
	for i := range ml.Prefixes {
		if ml.Prefixes[i].Updated.IsZero() {
			t.Errorf("%v: missing update time", ml.Prefixes[i].Prefix)
		}
		ml.Prefixes[i].Updated = time.Time{}
	}
	return ml.Prefixes
}

func TestMissingLinks(t *testing.T) {
	defer func(n int) { fullScanThreshold = n }(fullScanThreshold)
	for _, threshold := range []int{fullScanThreshold, 0} {
		fullScanThreshold = threshold
		testMissingLinks(t)
	}
}

func testMissingLinks(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	root := filepath.Join(tmp, "root")
	a, b, c := filepath.Join(root, "a"), filepath.Join(root, "b"), filepath.Join(root, "c")
	for _, d := range []string{a, b, c} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	dbDir := filepath.Join(tmp, "db")
	write := func(name string) {
		t.Helper()
		if err := os.WriteFile(name, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	link := func(from, to string) {
		t.Helper()
		if err := os.Link(from, to); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(a, "f"))
	write(filepath.Join(a, "g"))
	// c/h has a link outside of the analyzed filesystem.
	write(filepath.Join(c, "h"))
	link(filepath.Join(c, "h"), filepath.Join(tmp, "outside"))
	analyzeLinks(ctx, t, dbDir, root)
	cMissing := MissingLinksPrefix{Prefix: c, Files: 1, Links: 1, Examples: []string{"h"}}
	if got, want := missingLinks(ctx, t, dbDir), []MissingLinksPrefix{cMissing}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}

	// Links created in b to the files in a, which is unchanged and hence
	// retains its stale link counts.
	link(filepath.Join(a, "f"), filepath.Join(b, "l1"))
	link(filepath.Join(a, "g"), filepath.Join(b, "l2"))
	link(filepath.Join(a, "g"), filepath.Join(b, "l3"))
	analyzeLinks(ctx, t, dbDir, root)
	// g has three links, two of which are in b.
	bMissing := MissingLinksPrefix{Prefix: b, Files: 2, Links: 2, Examples: []string{"l1", "l2", "l3"}}
	if got, want := missingLinks(ctx, t, dbDir), []MissingLinksPrefix{bMissing, cMissing}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if _, links, _ := storedFile(ctx, t, dbDir, a, "f"); links != 1 {
		t.Errorf("got %v, want a stale link count of 1", links)
	}

	// Recording all of the files in a makes the links in b visible.
	recordA := WithRecordAllFiles("a", func(p string) bool { return p == a })
	analyzeLinks(ctx, t, dbDir, root, recordA)
	if got, want := missingLinks(ctx, t, dbDir), []MissingLinksPrefix{cMissing}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}

	// Deleting c removes its entry.
	if err := os.RemoveAll(c); err != nil {
		t.Fatal(err)
	}
	analyzeLinks(ctx, t, dbDir, root, recordA)
	if got := missingLinks(ctx, t, dbDir); len(got) != 0 {
		t.Errorf("got %+v, want none", got)
	}
}

func TestMissingLinksDisabled(t *testing.T) {
	ctx := context.Background()
	root, sub, dbDir := createRootAndSub(t)
	link := filepath.Join(filepath.Dir(root), "outside")
	if err := os.Link(filepath.Join(sub, "f1"), link); err != nil {
		t.Fatal(err)
	}
	// The link index is not maintained without the top-N rankings.
	db, err := openTestDB(ctx, dbDir, WithHardlinkRecording(stats.RecordLinkedFiles))
	if err != nil {
		t.Fatal(err)
	}
	if db.recordsLinks() {
		t.Errorf("links recorded without top-N rankings")
	}
	if err := NewAnalyzer(db, root).Analyze(ctx, root); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := missingLinks(ctx, t, dbDir); len(got) != 0 {
		t.Errorf("got %+v, want none", got)
	}
}

func TestLinkCountsRemoved(t *testing.T) {
	lc := newLinkCounts()
	lc.deleted("/a/b")
	lc.deleted("/")
	for _, tc := range []struct {
		sep, prefix string
		want        bool
	}{
		{"/", "/a/b", true},
		{"/", "/a/b/c", true},
		{"/", "/x", true}, // beneath /
	} {
		if got := lc.isRemoved(tc.sep, tc.prefix); got != tc.want {
			t.Errorf("%v: got %v, want %v", tc.prefix, got, tc.want)
		}
	}
	lc = newLinkCounts()
	lc.deleted("/a/b")
	for prefix, want := range map[string]bool{"/a/b": true, "/a/b/c": true, "/a/bc": false, "/a": false} {
		if got := lc.isRemoved("/", prefix); got != want {
			t.Errorf("%v: got %v, want %v", prefix, got, want)
		}
	}
}
