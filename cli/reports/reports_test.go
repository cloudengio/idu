// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package reports

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/stats"
)

func writeTestStatsFile(t *testing.T, dir string, when time.Time) string {
	t.Helper()
	sdb := report.NewAllStats("/root", 10)
	sdb.RecordDirect(
		"/root/sub",
		stats.Totals{Files: 2, Prefix: 1, SubPrefixes: 1, Bytes: 1200, StorageBytes: 4096, PrefixBytes: 98765},
		stats.PerIDTotals{{ID: 99991, Files: 2, Prefix: 1, SubPrefixes: 1, Bytes: 1200, StorageBytes: 4096, PrefixBytes: 98765}},
		stats.PerIDTotals{{ID: 99992, Files: 2, Prefix: 1, SubPrefixes: 1, Bytes: 1200, StorageBytes: 4096, PrefixBytes: 98765}},
	)
	sdb.Finalize()
	sf := cli.StatsFile{
		Prefix: "/root",
		Date:   when,
		Stats:  sdb,
		Source: cli.StatsFromSummaries,
	}
	var buf bytes.Buffer
	if err := sf.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	statsPath := filepath.Join(dir, "test.idustats")
	if err := os.WriteFile(statsPath, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return statsPath
}

func TestMarkdownPerIDPrefixBytes(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	when := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	statsPath := writeTestStatsFile(t, tmpDir, when)
	reportDir := filepath.Join(tmpDir, "reports")

	rc := New(cli.NewEnv())
	if err := rc.Generate(ctx, &GenerateFlags{ReportDir: reportDir, Markdown: 10}, []string{statsPath}); err != nil {
		t.Fatalf("Generate: %v", err)
	}

	mdPath := filepath.Join(reportDir, when.Format(cli.ReportDirLayout), "usage-summary.md")
	data, err := os.ReadFile(mdPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	marker := "prefixes by prefix bytes"
	idx := strings.Index(text, marker)
	if idx < 0 {
		t.Fatalf("missing %q in markdown:\n%s", marker, text)
	}
	section := text[idx:]
	if !strings.Contains(section, "98.765 KB") {
		t.Errorf("expected prefix bytes 98.765 KB in section:\n%s", section)
	}
}

func TestGenerateReportsPreservesErrors(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	when := time.Date(2026, 11, 19, 11, 0, 0, 0, time.UTC)
	statsPath := writeTestStatsFile(t, tmpDir, when)
	reportDir := filepath.Join(tmpDir, "reports")

	// Pre-create prefixes.tsv as a directory so writing TSV fails while JSON succeeds.
	blocker := filepath.Join(reportDir, when.Format(cli.ReportDirLayout), "prefixes.tsv")
	if err := os.MkdirAll(blocker, 0o750); err != nil {
		t.Fatal(err)
	}

	rc := New(cli.NewEnv())
	err := rc.Generate(ctx, &GenerateFlags{ReportDir: reportDir, TSV: 10, JSON: 10}, []string{statsPath})
	if err == nil {
		t.Fatal("expected error from failing TSV generation even though JSON succeeded")
	}
	if _, statErr := os.Stat(filepath.Join(reportDir, "latest")); statErr == nil {
		t.Error("expected latest symlink not to be created when a format fails")
	}
}
