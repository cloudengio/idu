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

	"cloudeng.io/cmd/idu/cli/internal/snapshot"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
	"cloudeng.io/cmd/idu/internal/usernames"
	"cloudeng.io/cmd/idu/report"
	"cloudeng.io/cmd/idu/stats"
)

func TestViewUserAndGroup(t *testing.T) {
	ctx := context.Background()
	sdb := report.NewAllStats("/root", 10)
	// Give UID 99992 and GID 99992 distinct prefixes so a lookup bug
	// querying PerUser for --group=99992 is immediately detected.
	sdb.RecordDirect(
		"/root/user-only",
		stats.Totals{Files: 1, Prefix: 1, SubPrefixes: 1, Bytes: 1111},
		stats.PerIDTotals{{ID: 99992, Files: 1, Prefix: 1, SubPrefixes: 1, Bytes: 1111}},
		nil,
	)
	sdb.RecordDirect(
		"/root/group-only",
		stats.Totals{Files: 1, Prefix: 1, SubPrefixes: 1, Bytes: 2222},
		nil,
		stats.PerIDTotals{{ID: 99992, Files: 1, Prefix: 1, SubPrefixes: 1, Bytes: 2222}},
	)
	sdb.Finalize()

	sf := snapshot.File{
		Prefix: "/root",
		Date:   time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC),
		Stats:  sdb,
		Source: snapshot.FromSummariesSource,
	}
	var buf bytes.Buffer
	if err := sf.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	statsPath := filepath.Join(t.TempDir(), "test.idustats")
	if err := os.WriteFile(statsPath, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	st := New(testEnv)
	groupOut, err := clitest.CaptureStdout(t, func() error {
		return st.View(ctx, &ViewFlags{DisplayFlags: DisplayFlags{DisplayN: 10}, Group: "99992"}, []string{statsPath})
	})
	if err != nil {
		t.Fatalf("View --group: %v", err)
	}
	groupName := usernames.Manager.NameForGID(99992)
	if !strings.Contains(groupOut, "/root/group-only") || !strings.Contains(groupOut, groupName) || strings.Contains(groupOut, "/root/user-only") {
		t.Errorf("unexpected --group output:\n%s", groupOut)
	}

	userOut, err := clitest.CaptureStdout(t, func() error {
		return st.View(ctx, &ViewFlags{DisplayFlags: DisplayFlags{DisplayN: 10}, User: "99992"}, []string{statsPath})
	})
	if err != nil {
		t.Fatalf("View --user: %v", err)
	}
	userName := usernames.Manager.NameForUID(99992)
	if !strings.Contains(userOut, "/root/user-only") || !strings.Contains(userOut, userName) || strings.Contains(userOut, "/root/group-only") {
		t.Errorf("unexpected --user output:\n%s", userOut)
	}
}
