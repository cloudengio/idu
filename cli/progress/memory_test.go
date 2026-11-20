// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package progress_test

import (
	"runtime"
	"testing"

	"cloudeng.io/cmd/idu/cli/progress"
)

func TestReadMemory(t *testing.T) {
	m := progress.ReadMemory()
	if m.HeapBytes == 0 || m.TotalBytes < m.HeapBytes || m.Goroutines == 0 {
		t.Errorf("implausible memory stats: %+v", m)
	}
	switch runtime.GOOS {
	case "windows":
		if m.MaxRSSBytes >= 0 {
			t.Errorf("expected an unknown RSS: %+v", m)
		}
	default:
		// The peak RSS must be at least the heap in use, ie. it is in
		// bytes on all systems.
		if m.MaxRSSBytes < int64(m.HeapBytes) {
			t.Errorf("implausible peak RSS: %+v", m)
		}
	}
}
