// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats_test

import (
	"bytes"
	"io/fs"
	"reflect"
	"sort"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/hardlinks"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/file"
	"cloudeng.io/file/diskusage"
)

func TestSummaryKeysAndOrder(t *testing.T) {
	paths := []string{"/a-b", "/a/z", "/a", "/", "/a/b/c", "/a.d", "/a/b"}
	encoded := make([]string, len(paths))
	for i, p := range paths {
		ek, _ := stats.SummaryKeys("/", p)
		encoded[i] = ek
		if dec := stats.DecodeSummaryKey("/", ek); dec != p {
			t.Errorf("DecodeSummaryKey(%q) = %q, want %q", ek, dec, p)
		}
	}
	sort.Strings(encoded)
	got := make([]string, len(encoded))
	for i, ek := range encoded {
		got[i] = stats.DecodeSummaryKey("/", ek)
	}
	want := []string{"/", "/a", "/a/b", "/a/b/c", "/a/z", "/a-b", "/a.d"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("sorted summary order = %v, want %v", got, want)
	}

	// Trailing slash on non-root normalizes cleanly; root preserves separator.
	ekTrailing, cpTrailing := stats.SummaryKeys("/", "/a/b/")
	ekClean, cpClean := stats.SummaryKeys("/", "/a/b")
	if ekTrailing != ekClean || cpTrailing != cpClean {
		t.Errorf("trailing slash mismatch: (%q,%q) vs (%q,%q)", ekTrailing, cpTrailing, ekClean, cpClean)
	}

	ekRoot, cpRoot := stats.SummaryKeys("/", "/")
	if ekRoot != "\x00" || cpRoot != "\x00" {
		t.Errorf("root SummaryKeys = (%q,%q), want (\\x00,\\x00)", ekRoot, cpRoot)
	}

	// Windows drive root & backslash paths.
	ekWinRoot, cpWinRoot := stats.SummaryKeys(`\`, `C:\`)
	ekWinSub, _ := stats.SummaryKeys(`\`, `C:\dir\file`)
	ekWinSib, _ := stats.SummaryKeys(`\`, `C:\dir-2`)
	if ekWinRoot != "C:\x00" || cpWinRoot != "C:\x00" {
		t.Errorf("win root = (%q,%q)", ekWinRoot, cpWinRoot)
	}
	_, cpWinDir := stats.SummaryKeys(`\`, `C:\dir`)
	if cpWinDir != "C:\x00dir\x00" {
		t.Errorf("win child prefix = %q", cpWinDir)
	}
	if ekWinSub >= ekWinSib {
		t.Errorf("expected %q < %q", ekWinSub, ekWinSib)
	}
}

func TestPrefixSummaryBinaryRoundTrip(t *testing.T) {
	in := stats.PrefixSummary{
		CalcID: "block(4096)",
		Direct: stats.Totals{
			Files:        12,
			Prefix:       1,
			SubPrefixes:  3,
			Bytes:        10240,
			StorageBytes: 12288,
			PrefixBytes:  4096,
			Hardlinks:    2,
			HardlinkDirs: 0,
			SparseFiles:  1,
			SparseBytes:  1 << 20,
		},
		PerUser: stats.PerIDTotals{
			{ID: 501, Files: 10, Prefix: 1, SubPrefixes: 3, Bytes: 8192, StorageBytes: 8192, PrefixBytes: 4096, SparseFiles: 1, SparseBytes: 1 << 20},
			{ID: 502, Files: 2, Bytes: 2048, StorageBytes: 4096},
		},
		PerGroup: stats.PerIDTotals{
			{ID: 20, Files: 12, Prefix: 1, SubPrefixes: 3, Bytes: 10240, StorageBytes: 12288, PrefixBytes: 4096},
		},
	}

	data, err := in.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}

	var out stats.PrefixSummary
	if err := out.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round-trip mismatch:\n got: %+v\nwant: %+v", out, in)
	}

	// Empty PerUser/PerGroup also round-trip.
	empty := stats.PrefixSummary{CalcID: "identity", Direct: stats.Totals{Prefix: 1}}
	edata, err := empty.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary(empty): %v", err)
	}
	var eout stats.PrefixSummary
	if err := eout.UnmarshalBinary(edata); err != nil {
		t.Fatalf("UnmarshalBinary(empty): %v", err)
	}
	if !reflect.DeepEqual(empty, eout) {
		t.Errorf("empty round-trip mismatch: %+v vs %+v", eout, empty)
	}

	// Truncated inputs fail cleanly with errors rather than panicking.
	for i := 0; i < len(data); i++ {
		var bad stats.PrefixSummary
		if err := bad.UnmarshalBinary(data[:i]); err == nil {
			t.Errorf("expected error for truncated prefix len %d", i)
		}
	}
}

func TestTotalsAdd(t *testing.T) {
	a := stats.Totals{Files: 1, Prefix: 1, SubPrefixes: 2, Bytes: 100, StorageBytes: 4096, PrefixBytes: 4096, Hardlinks: 1, SparseFiles: 1, SparseBytes: 1000}
	b := stats.Totals{Files: 3, Prefix: 1, SubPrefixes: 0, Bytes: 250, StorageBytes: 8192, PrefixBytes: 4096, HardlinkDirs: 1, SparseFiles: 2, SparseBytes: 500}
	a.Add(b)
	want := stats.Totals{Files: 4, Prefix: 2, SubPrefixes: 2, Bytes: 350, StorageBytes: 12288, PrefixBytes: 8192, Hardlinks: 1, HardlinkDirs: 1, SparseFiles: 3, SparseBytes: 1500}
	if a != want {
		t.Errorf("Add = %+v, want %+v", a, want)
	}
}

func newLinkTestPrefix(dev, ino uint64, files ...file.Info) prefixinfo.T {
	dirInfo := file.NewInfo("dir", 100, 0o755|fs.ModeDir, time.Now(), file.XAttr{UID: 1, GID: 2, Device: dev, FileID: ino})
	pi := prefixinfo.New("dir", dirInfo)
	pi.SetInfoList(files)
	return pi
}

func newLinkTestFile(name string, size int64, ino, nlink uint64) file.Info {
	return file.NewInfo(name, size, 0o644, time.Now(), file.XAttr{UID: 1, GID: 2, FileID: ino, Hardlinks: nlink})
}

func newSparseTestFile(name string, size, blocks int64, ino, nlink uint64) file.Info {
	return file.NewInfo(name, size, 0o644, time.Now(), file.XAttr{UID: 1, GID: 2, FileID: ino, Hardlinks: nlink, Blocks: blocks})
}

func TestSparseBytes(t *testing.T) {
	for _, tc := range []struct {
		size, blocks, want int64
	}{
		{0, 0, 0},
		// Small files stored inline have no blocks.
		{100, 0, 0},
		// Allocated space is rounded up to a whole number of blocks.
		{100, 8, 0},
		{1 << 20, (1 << 20) / 512, 0},
		{stats.SparseThreshold - 1, 0, 0},
		{stats.SparseThreshold, 0, stats.SparseThreshold},
		{1 << 30, 8, 1<<30 - 8*512},
		// Windows derives blocks from the size.
		{1<<30 + 511, (1<<30 + 511) / 512, 0},
	} {
		if got := stats.SparseBytes(tc.size, tc.blocks); got != tc.want {
			t.Errorf("SparseBytes(%v, %v) = %v, want %v", tc.size, tc.blocks, got, tc.want)
		}
	}
}

func TestComputeTotalsSparse(t *testing.T) {
	calc := diskusage.NewBlock(512)
	match := stats.AlwaysMatch()
	pi := newLinkTestPrefix(7, 1,
		newSparseTestFile("dense", 1<<20, (1<<20)/512, 2, 1),
		newSparseTestFile("sparse", 1<<30, 8, 3, 1),
		newSparseTestFile("inline", 100, 0, 4, 1),
	)
	totals, users, groups := stats.ComputeTotals("/dir", &pi, calc, match)
	sparse := int64(1<<30 - 8*512)
	if got, want := totals.SparseFiles, int64(1); got != want {
		t.Errorf("got %v sparse files, want %v", got, want)
	}
	if got, want := totals.SparseBytes, sparse; got != want {
		t.Errorf("got %v sparse bytes, want %v", got, want)
	}
	// Bytes is the size of the files whereas StorageBytes is what's allocated.
	if got, want := totals.Bytes-totals.StorageBytes, sparse+100+100; got != want {
		t.Errorf("got bytes-storage %v, want %v", got, want)
	}
	for _, pid := range []stats.PerIDTotals{users, groups} {
		if len(pid) != 1 || pid[0].SparseFiles != 1 || pid[0].SparseBytes != sparse {
			t.Errorf("per id: got %+v", pid)
		}
	}
}

func TestComputeSummaryRecordsFiles(t *testing.T) {
	calc := diskusage.NewIdentity()
	match := stats.AlwaysMatch()
	pi := newLinkTestPrefix(7, 1,
		newLinkTestFile("single", 10, 2, 1),
		newLinkTestFile("linked", 20, 3, 2),
		newLinkTestFile("unknown", 30, 4, 0),
	)
	for _, rec := range []stats.HardlinkRecording{stats.RecordNoFiles, stats.HardlinkRecording(99)} {
		ps := stats.ComputeSummary("/dir", &pi, calc, match, rec)
		if ps.HardlinkInfo || ps.Files != nil {
			t.Errorf("%v: hardlink info recorded when not requested: %+v", rec, ps)
		}
	}
	// nil Calculator must not panic when recording file entries.
	ps := stats.ComputeSummary("/dir", &pi, nil, match, stats.RecordAllFiles)
	if !ps.HardlinkInfo || ps.CalcID != "" || len(ps.Files) != 3 {
		t.Errorf("nil calculator summary: %+v", ps)
	}
	for _, tc := range []struct {
		rec  stats.HardlinkRecording
		want []uint64
	}{
		// Files with a single link cannot be hardlinks, but those whose
		// link count is unknown may be.
		{stats.RecordLinkedFiles, []uint64{3, 4}},
		{stats.RecordAllFiles, []uint64{2, 3, 4}},
	} {
		ps = stats.ComputeSummary("/dir", &pi, calc, match, tc.rec)
		if !ps.HardlinkInfo || ps.Device != 7 || ps.FileID != 1 {
			t.Fatalf("%v: unexpected prefix identity: %+v", tc.rec, ps)
		}
		var inodes []uint64
		for _, f := range ps.Files {
			inodes = append(inodes, f.FileID)
		}
		if got := inodes; !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%v: got %v, want %v", tc.rec, got, tc.want)
		}
		if got, want := ps.Direct.Files, int64(3); got != want {
			t.Errorf("%v: got %v, want %v", tc.rec, got, want)
		}
	}
}

func TestApplyHardlinks(t *testing.T) {
	calc := diskusage.NewIdentity()
	match := stats.AlwaysMatch()
	first := newLinkTestPrefix(7, 1, newLinkTestFile("f", 20, 3, 2), newLinkTestFile("g", 5, 4, 1),
		newSparseTestFile("s", 1<<30, 8, 7, 2))
	second := newLinkTestPrefix(7, 2, newLinkTestFile("l", 20, 3, 2), newLinkTestFile("h", 7, 5, 1),
		newSparseTestFile("sl", 1<<30, 8, 7, 2))
	dupDir := newLinkTestPrefix(7, 1, newLinkTestFile("x", 9, 6, 2))

	for _, rec := range []stats.HardlinkRecording{stats.RecordLinkedFiles, stats.RecordAllFiles} {
		hl := &hardlinks.Incremental{}
		dedup := hardlinkMatcher(t)
		for _, pi := range []prefixinfo.T{first, second, dupDir} {
			ps := stats.ComputeSummary("/dir", &pi, calc, match, rec)
			ps.ApplyHardlinks(hl)
			// ApplyHardlinks must match ComputeTotals with hardlink handling.
			totals, users, groups := stats.ComputeTotals("/dir", &pi, calc, dedup)
			if !reflect.DeepEqual(ps.Direct, totals) {
				t.Errorf("%v: direct: got %+v, want %+v", rec, ps.Direct, totals)
			}
			if !reflect.DeepEqual(ps.PerUser, users) || !reflect.DeepEqual(ps.PerGroup, groups) {
				t.Errorf("%v: per id: got %+v %+v, want %+v %+v", rec, ps.PerUser, ps.PerGroup, users, groups)
			}
		}
	}
}

func hardlinkMatcher(t *testing.T) stats.Matcher {
	t.Helper()
	m, err := stats.NewMatcher(nil, stats.WithEmptyValue(true), stats.WithHardlinkHandling(true))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestPrefixSummaryFilesRoundTrip(t *testing.T) {
	in := stats.PrefixSummary{
		CalcID:       "identity",
		Direct:       stats.Totals{Prefix: 1, Files: 2, Bytes: 30},
		HardlinkInfo: true,
		Device:       7,
		FileID:       1,
		Files: []stats.FileEntry{
			{FileID: 3, UID: 1, GID: 2, Bytes: 20, StorageBytes: 4096},
			{FileID: 4, UID: -1, GID: -1, Bytes: 10, StorageBytes: 512},
			{FileID: 5, UID: 1, GID: 2, Bytes: 1 << 30, StorageBytes: 4096, SparseBytes: 1<<30 - 4096},
		},
	}
	data := in.AppendBinary(nil)
	var out stats.PrefixSummary
	if err := out.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("got %+v, want %+v", out, in)
	}
	for i := 0; i < len(data); i++ {
		var bad stats.PrefixSummary
		if err := bad.UnmarshalBinary(data[:i]); err == nil {
			t.Errorf("expected error for truncated prefix len %d", i)
		}
	}
}

func TestParentSummaryKey(t *testing.T) {
	for _, tc := range []struct {
		sep, path, parent string
	}{
		{"/", "/", ""},
		{"/", "/a", "/"},
		{"/", "/a/b", "/a"},
		{"/", "/a/b/c", "/a/b"},
		{`\`, `C:\`, ""},
		{`\`, `C:\a`, `C:\`},
		{`\`, `C:\a\b`, `C:\a`},
		{"/", "data", ""},
		{"/", "data/sub", "data"},
		{"/", "data/sub/leaf", "data/sub"},
	} {
		key, _ := stats.SummaryKeys(tc.sep, tc.path)
		parent, ok := stats.ParentSummaryKey(key)
		if tc.parent == "" {
			if ok {
				t.Errorf("%q: unexpected parent %q", tc.path, parent)
			}
			continue
		}
		want, _ := stats.SummaryKeys(tc.sep, tc.parent)
		if !ok || parent != want {
			t.Errorf("%q: got %q %v, want %q", tc.path, parent, ok, want)
		}
		if !stats.IsSummaryDescendant(parent, key) {
			t.Errorf("%q is not a descendant of %q", key, parent)
		}
		if stats.IsSummaryDescendant(key, parent) {
			t.Errorf("%q is a descendant of %q", parent, key)
		}
	}
}

func TestTotalsArithmetic(t *testing.T) {
	a := stats.Totals{ID: 1, Files: 3, Bytes: 10, Prefix: 1}
	b := stats.Totals{ID: 2, Files: 1, Bytes: 4}
	c := a
	c.Add(b)
	c.Sub(b)
	if c != a {
		t.Errorf("got %+v, want %+v", c, a)
	}
	c.Sub(a)
	if !c.IsZero() || c.IsNegative() {
		t.Errorf("got %+v", c)
	}
	c.Sub(b)
	if c.IsZero() || !c.IsNegative() {
		t.Errorf("got %+v", c)
	}
	var d stats.Totals
	if err := d.UnmarshalBinary([]byte{0x7f}); err == nil {
		t.Errorf("expected an error")
	}
}

func TestEffective(t *testing.T) {
	calc := diskusage.NewIdentity()
	match := stats.AlwaysMatch()
	// f and l are the same file, s has a single link and u's inode is unknown.
	pi := newLinkTestPrefix(7, 1,
		newLinkTestFile("f", 20, 3, 2),
		newLinkTestFile("l", 20, 3, 2),
		newLinkTestFile("g", 30, 4, 2),
		newLinkTestFile("s", 5, 5, 1),
		newLinkTestFile("u", 7, 0, 0),
	)
	ps := stats.ComputeSummary("/dir", &pi, calc, match, stats.RecordLinkedFiles)
	orig := ps.AppendBinary(nil)

	var asked []uint64
	eff := ps.Effective(false, func(ino uint64) bool {
		asked = append(asked, ino)
		return ino == 3
	})
	// owns is called once per file, other than those with unknown inodes.
	if got, want := asked, []uint64{3, 4}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	// l repeats f, and g is owned by another prefix.
	if got, want := [3]int64{eff.Direct.Files, eff.Direct.Bytes - eff.Direct.PrefixBytes, eff.Direct.Hardlinks}, [3]int64{3, 20 + 5 + 7, 2}; got != want {
		t.Errorf("direct: got %v, want %v", got, want)
	}
	if len(eff.PerUser) != 1 || eff.PerUser[0].Files != 3 || eff.PerUser[0].Hardlinks != 2 {
		t.Errorf("per user: got %+v", eff.PerUser)
	}
	// ps is not modified.
	if !bytes.Equal(ps.AppendBinary(nil), orig) {
		t.Errorf("Effective modified its receiver")
	}

	dup := ps.Effective(true, func(uint64) bool { t.Error("owns called for a duplicate"); return true })
	if want := (stats.Totals{Prefix: 1, HardlinkDirs: 1}); dup.Direct != want || dup.PerUser != nil || dup.Files != nil {
		t.Errorf("dup: got %+v", dup)
	}

	// Without hardlink information the summary is returned unchanged.
	raw := stats.ComputeSummary("/dir", &pi, calc, match, stats.RecordNoFiles)
	if got := raw.Effective(true, nil); !reflect.DeepEqual(got, raw) {
		t.Errorf("got %+v, want %+v", got, raw)
	}
}

func TestApplyHardlinksUnknownInodes(t *testing.T) {
	calc := diskusage.NewIdentity()
	match := stats.AlwaysMatch()
	dedup := hardlinkMatcher(t)
	hl := &hardlinks.Incremental{}
	// Prefixes and files whose inode numbers are unknown are never
	// hardlinks, and the summaries agree with the matcher.
	for _, pi := range []prefixinfo.T{
		newLinkTestPrefix(7, 0, newLinkTestFile("a", 10, 0, 0), newLinkTestFile("b", 10, 0, 0)),
		newLinkTestPrefix(7, 0, newLinkTestFile("c", 10, 0, 0)),
	} {
		ps := stats.ComputeSummary("/dir", &pi, calc, match, stats.RecordAllFiles)
		ps.ApplyHardlinks(hl)
		totals, _, _ := stats.ComputeTotals("/dir", &pi, calc, dedup)
		if ps.Direct != totals {
			t.Errorf("got %+v, want %+v", ps.Direct, totals)
		}
		if ps.Direct.Hardlinks != 0 || ps.Direct.HardlinkDirs != 0 {
			t.Errorf("unexpected hardlinks: %+v", ps.Direct)
		}
	}
}
