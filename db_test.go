// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"cloudeng.io/cmd/idu/internal/config"
	"cloudeng.io/file/diskusage"
)

func TestDBLocate(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	existingDB := filepath.Join(tmpDir, "existing-db")
	if err := os.MkdirAll(existingDB, 0o750); err != nil {
		t.Fatal(err)
	}
	missingDB := filepath.Join(tmpDir, "missing-db")

	cfg, err := config.ParseConfig([]byte(
		"filesystems:\n" +
			"  - name: present\n    prefix: /p1\n    database: " + existingDB + "\n" +
			"  - name: absent\n    prefix: /p2\n    database: " + missingDB + "\n",
	))
	if err != nil {
		t.Fatal(err)
	}

	origCfg := globalConfig
	origPrinter := bytesPrinter
	globalConfig = cfg
	bytesPrinter = func(size int64) (float64, string) {
		return diskusage.Decimal(size).Standardize()
	}
	t.Cleanup(func() {
		globalConfig = origCfg
		bytesPrinter = origPrinter
	})

	cmd := &dbCmd{}
	for _, tc := range []struct {
		name    string
		flags   locateFlags
		arg     string
		wantErr bool
	}{
		{name: "non-verbose", flags: locateFlags{Verbose: false}, arg: "present"},
		{name: "verbose existing", flags: locateFlags{Verbose: true}, arg: "present"},
		{name: "verbose missing", flags: locateFlags{Verbose: true}, arg: "absent"},
		{name: "unknown prefix", flags: locateFlags{Verbose: false}, arg: "nope", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := cmd.locate(ctx, &tc.flags, []string{tc.arg})
			if (err != nil) != tc.wantErr {
				t.Errorf("locate(%q) err = %v, wantErr=%v", tc.arg, err, tc.wantErr)
			}
		})
	}
}
