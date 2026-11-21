// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package stats

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/cli/clitest"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/snapshot"
	"cloudeng.io/cmd/idu/stats"
)

func writeSelectionSnapshot(t *testing.T, withHardlinks bool) string {
	t.Helper()
	sdb := report.NewAllStats("/root", 10)
	totals := stats.Totals{Files: 3, Prefix: 1, SubPrefixes: 1, Bytes: 3000, StorageBytes: 4096, Hardlinks: 2}
	sdb.RecordDirect("/root/a", totals,
		stats.PerIDTotals{{ID: 99993, Files: 3, Prefix: 1, SubPrefixes: 1, Bytes: 3000, StorageBytes: 4096, Hardlinks: 2}}, nil)
	sdb.Finalize()
	if !withHardlinks {
		sdb.ClearHardlinkRankings()
	}
	var buf bytes.Buffer
	if err := (snapshot.File{Prefix: "/root", Date: time.Now(), Stats: sdb, Source: snapshot.FromSummariesSource}).Encode(&buf); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "test.idustats")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestViewSelection(t *testing.T) {
	ctx := context.Background()
	path := writeSelectionSnapshot(t, true)
	const (
		totals   = "Totals\n"
		bytes    = "Bytes used\n"
		storage  = "Bytes used on underlying filesystem\n"
		files    = "Number of Files\n"
		prefixes = "Number of Prefixes/Directories\n"
		users    = "Usage by top 10 users"
	)
	all := []string{totals, bytes, storage, files, prefixes, users}
	for _, tc := range []struct {
		name  string
		flags ViewFlags
		want  []string
	}{
		{"all", ViewFlags{}, all},
		{"summary", ViewFlags{Summary: true}, []string{totals}},
		{"bytes", ViewFlags{Bytes: true}, []string{bytes, users}},
		{"storage", ViewFlags{Storage: true}, []string{storage, users}},
		{"files", ViewFlags{FileCounts: true}, []string{files, users}},
		{"summary+files", ViewFlags{Summary: true, FileCounts: true}, []string{totals, files, users}},
		{"user", ViewFlags{User: "99993"}, []string{bytes, storage, files, prefixes}},
		{"user summary", ViewFlags{User: "99993", Summary: true}, []string{totals}},
		{"user storage", ViewFlags{User: "99993", Storage: true}, []string{storage}},
	} {
		tc.flags.DisplayN = 10
		out, err := clitest.CaptureStdout(t, func() error {
			return New(testEnv).View(ctx, &tc.flags, []string{path})
		})
		if err != nil {
			t.Fatalf("%v: %v", tc.name, err)
		}
		for _, section := range all {
			wanted := false
			for _, w := range tc.want {
				wanted = wanted || w == section
			}
			if got := strings.Contains(out, section); got != wanted {
				t.Errorf("%v: %q displayed: %v, want %v\n%s", tc.name, section, got, wanted, out)
			}
		}
		if tc.flags.User != "" && !strings.Contains(out, "/root/a") && tc.flags.Summary == false {
			t.Errorf("%v: missing per-user prefix\n%s", tc.name, out)
		}
	}
}

func viewOutput(ctx context.Context, t *testing.T, path string, flags ViewFlags) string {
	t.Helper()
	flags.DisplayN = 10
	out, err := clitest.CaptureStdout(t, func() error {
		return New(testEnv).View(ctx, &flags, []string{path})
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// checkOutput checks that out contains all of want and none of notWant.
func checkOutput(t *testing.T, name, out string, want, notWant []string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(out, w) {
			t.Errorf("%v: missing %q:\n%s", name, w, out)
		}
	}
	for _, w := range notWant {
		if strings.Contains(out, w) {
			t.Errorf("%v: unexpected %q:\n%s", name, w, out)
		}
	}
}

func TestViewHotspotsAndHardlinks(t *testing.T) {
	ctx := context.Background()
	path := writeSelectionSnapshot(t, true)
	const (
		hardlinks = "Hardlinks as of"
		ranking   = "Top 10 prefixes by number of hardlinks\n"
		usage     = "Usage by top"
		totals    = "Totals\n"
	)
	// The snapshot has no hotspots.
	checkOutput(t, "--hotspots", viewOutput(ctx, t, path, ViewFlags{Hotspots: true}),
		[]string{"No distributed subtree hotspots were found"}, []string{usage, totals})
	checkOutput(t, "--hardlinks", viewOutput(ctx, t, path, ViewFlags{Hardlinks: true}),
		[]string{hardlinks, ranking, "/root/a", "Top 10 users by number of hardlinks"}, []string{usage})
	checkOutput(t, "--hardlinks --summary", viewOutput(ctx, t, path, ViewFlags{Hardlinks: true, Summary: true}),
		[]string{totals, ranking}, nil)
	checkOutput(t, "--hardlinks --user", viewOutput(ctx, t, path, ViewFlags{Hardlinks: true, User: "99993"}),
		[]string{ranking, "/root/a"}, []string{"Bytes used\n"})
	// Without the rankings, eg. from the stored state.
	checkOutput(t, "--hardlinks without rankings", viewOutput(ctx, t, writeSelectionSnapshot(t, false), ViewFlags{Hardlinks: true}),
		[]string{hardlinks, "not included in this snapshot"}, []string{ranking})
	// The hardlinks section is not part of the default output.
	checkOutput(t, "default", viewOutput(ctx, t, path, ViewFlags{}), nil, []string{hardlinks})
}
