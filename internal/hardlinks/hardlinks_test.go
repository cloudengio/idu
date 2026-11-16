// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package hardlinks_test

import (
	"testing"

	"cloudeng.io/cmd/idu/internal/hardlinks"
)

func TestIncremental_Ref(t *testing.T) {
	t.Parallel()

	var hl hardlinks.Incremental
	tests := []struct {
		name string
		dev  uint64
		ino  uint64
		want bool
	}{
		{name: "first dev1 ino10", dev: 1, ino: 10, want: false},
		{name: "first dev1 ino20", dev: 1, ino: 20, want: false},
		{name: "duplicate dev1 ino10", dev: 1, ino: 10, want: true},
		{name: "same ino on different dev2", dev: 2, ino: 10, want: false},
		{name: "duplicate dev2 ino10", dev: 2, ino: 10, want: true},
		{name: "third dev3 ino30", dev: 3, ino: 30, want: false},
		{name: "duplicate dev1 ino20", dev: 1, ino: 20, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := hl.Ref(tc.dev, tc.ino); got != tc.want {
				t.Errorf("Ref(%d, %d) = %v, want %v", tc.dev, tc.ino, got, tc.want)
			}
		})
	}
}
