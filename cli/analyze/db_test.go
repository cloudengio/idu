// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package analyze

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli/clitest"
	"cloudeng.io/file/diskusage"
)

func TestDBLocate(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	existingDB := filepath.Join(tmpDir, "existing-db")
	if err := os.MkdirAll(existingDB, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(existingDB, "data.sst"), make([]byte, 5000), 0o600); err != nil {
		t.Fatal(err)
	}
	missingDB := filepath.Join(tmpDir, "missing-db")

	cfg, err := iduconfig.ParseConfig([]byte(
		"filesystems:\n" +
			"  - name: present\n    prefix: /p1\n    database: " + existingDB + "\n" +
			"  - name: absent\n    prefix: /p2\n    database: " + missingDB + "\n",
	))
	if err != nil {
		t.Fatal(err)
	}

	origCfg := testEnv.Config
	origPrinter := testEnv.BytesPrinter
	testEnv.Config = cfg
	testEnv.BytesPrinter = func(size int64) (float64, string) {
		return diskusage.Decimal(size).Standardize()
	}
	t.Cleanup(func() {
		testEnv.Config = origCfg
		testEnv.BytesPrinter = origPrinter
	})

	cmd := newTestCommands()
	for _, tc := range []struct {
		name         string
		flags        LocateFlags
		arg          string
		wantErr      bool
		wantContains string
	}{
		{name: "non-verbose", flags: LocateFlags{Verbose: false}, arg: "present"},
		{name: "verbose existing", flags: LocateFlags{Verbose: true}, arg: "present", wantContains: "5.000 KB"},
		{name: "verbose missing", flags: LocateFlags{Verbose: true}, arg: "absent", wantContains: "database for /p2"},
		{name: "unknown prefix", flags: LocateFlags{Verbose: false}, arg: "nope", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clitest.WithFS(t, testEnv, tc.arg)
			out, err := clitest.CaptureStdout(t, func() error {
				return cmd.Locate(ctx, &tc.flags, nil)
			})
			if (err != nil) != tc.wantErr {
				t.Errorf("locate(%q) err = %v, wantErr=%v", tc.arg, err, tc.wantErr)
			}
			if tc.wantContains != "" && !strings.Contains(out, tc.wantContains) {
				t.Errorf("locate(%q) output = %q, want substring %q", tc.arg, out, tc.wantContains)
			}
		})
	}
}

func TestOpenPrefixReadOnly(t *testing.T) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")
	allCfg, err := iduconfig.ParseConfig([]byte("filesystems:\n  - name: testfs\n    prefix: /test/root\n    database: " + dbDir + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	// Create the database.
	fs, _ := allCfg.ForName("testfs")
	wdb, err := testEnv.OpenWritableDB(ctx, fs.Config())
	if err != nil {
		t.Fatal(err)
	}
	if err := wdb.Close(ctx); err != nil {
		t.Fatal(err)
	}

	_, cfg, target, rdb, err := openPrefixReadOnly(ctx, allCfg, "")
	if err != nil {
		t.Fatalf("openPrefixReadOnly by name: %v", err)
	}
	_ = rdb.Close(ctx)
	if cfg.Name != "testfs" || target != "/test/root" {
		t.Fatalf("got (%q, %q), want (testfs, /test/root)", cfg.Name, target)
	}

	_, _, target, rdb, err = openPrefixReadOnly(ctx, allCfg, "/test/root/sub")
	if err != nil {
		t.Fatalf("openPrefixReadOnly by path: %v", err)
	}
	_ = rdb.Close(ctx)
	if target != "/test/root/sub" {
		t.Fatalf("target = %q, want /test/root/sub", target)
	}

	if _, _, _, _, err := openPrefixReadOnly(ctx, allCfg, "/other/path"); err == nil {
		t.Fatal("expected error for unknown prefix")
	}
	if runtime.GOOS != "windows" {
		badCfg, err := iduconfig.ParseConfig([]byte("filesystems:\n  - name: bad\n    prefix: /bad\n    database: " + filepath.Join(t.TempDir(), "missing") + "\n"))
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, db, err := openPrefixReadOnly(ctx, badCfg, "bad"); err == nil {
			_ = db.Close(ctx)
			t.Fatal("expected error opening missing readonly database")
		}
	}
}
