// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal_test

import (
	"io/fs"
	"testing"
	"time"

	"cloudeng.io/cmd/idu/internal"
	"cloudeng.io/cmd/idu/internal/prefixinfo"
	"cloudeng.io/file"
)

func TestPrefixInfoAsFSInfo(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)

	tests := []struct {
		name      string
		mode      fs.FileMode
		wantIsDir bool
	}{
		{name: "directory", mode: fs.ModeDir | 0o755, wantIsDir: true},
		{name: "regular file", mode: 0o644, wantIsDir: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := file.NewInfo("ignored", 42, tc.mode, now, file.XAttr{UID: 1, GID: 2})
			pi := prefixinfo.New("/some/dir", raw)
			fi := internal.PrefixInfoAsFSInfo(pi, "/some/dir")
			if fi.Name() != "/some/dir" {
				t.Errorf("Name() = %q, want /some/dir", fi.Name())
			}
			if fi.IsDir() != tc.wantIsDir {
				t.Errorf("IsDir() = %v, want %v", fi.IsDir(), tc.wantIsDir)
			}
			if fi.Size() != 42 || fi.Mode() != tc.mode || !fi.ModTime().Equal(now) {
				t.Errorf("unexpected metadata: size=%d mode=%v modtime=%v", fi.Size(), fi.Mode(), fi.ModTime())
			}
			if fi.Sys() != nil {
				t.Errorf("Sys() = %v, want nil", fi.Sys())
			}
		})
	}
}
