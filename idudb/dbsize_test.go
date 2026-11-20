// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package idudb_test

import (
	"os"
	"path/filepath"
	"testing"

	"cloudeng.io/cmd/idu/idudb"
)

func TestDatabaseSize(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, n := range map[string]int{"a.sst": 100, "sub/b.sst": 23, "empty": 0} {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, n), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := idudb.DatabaseSize(dir)
	if err != nil || got != 123 {
		t.Errorf("got %v, %v, want 123", got, err)
	}
	if _, err := idudb.DatabaseSize(filepath.Join(dir, "missing")); err == nil {
		t.Error("expected an error for a missing directory")
	}
}
