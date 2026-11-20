// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package reports

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
)

func TestReportsDir(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	configured := filepath.Join(tmpDir, "configured")
	cfg, err := iduconfig.ParseConfig([]byte(fmt.Sprintf(
		"filesystems:\n  - name: a\n    root: /a\n    database: %s\n    reports_dir: %s\n  - name: b\n    root: /b\n    database: %s\n",
		filepath.Join(tmpDir, "db-a"), configured, filepath.Join(tmpDir, "db-b"))))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Filesystems[0].Config().ReportsDir; got != configured {
		t.Fatalf("got %q, want %q", got, configured)
	}
	env := cli.NewEnv()
	clitest.WithConfig(t, env, cfg)
	rc := New(env)
	for _, tc := range []struct {
		fs, flag, want string
	}{
		{"a", "", configured},
		{"a", "flag", "flag"},
		// b has no reports_dir.
		{"b", "", DefaultReportsDir},
		// The filesystem is ambiguous.
		{"", "", DefaultReportsDir},
	} {
		if got := rc.reportsDirFor(tc.fs, tc.flag); got != tc.want {
			t.Errorf("%q %q: got %q, want %q", tc.fs, tc.flag, got, tc.want)
		}
	}

	// generate writes to, and locate looks in, reports_dir by default.
	when := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	statsPath := writeTestStatsFile(t, tmpDir, when)
	if err := rc.Generate(ctx, &GenerateFlags{FSFlag: cli.FSFlag{FS: "a"}, Markdown: 10}, []string{statsPath}); err != nil {
		t.Fatal(err)
	}
	out, err := clitest.CaptureStdout(t, func() error {
		return rc.Locate(ctx, &LocateFlags{FSFlag: cli.FSFlag{FS: "a"}, N: 2}, nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	var located []struct {
		ReportDir string `json:"report_dir"`
	}
	if err := json.Unmarshal([]byte(out), &located); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if len(located) != 1 || located[0].ReportDir != when.Local().Format(cli.ReportDirLayout) {
		t.Errorf("got %+v", located)
	}
	// locate requires a filesystem when none is given.
	if err := rc.Locate(ctx, &LocateFlags{N: 2}, nil); err == nil {
		t.Errorf("expected an error")
	}
}
