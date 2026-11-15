// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package config

import (
	"errors"
	"runtime"
	"testing"
)

func TestRoundUp(t *testing.T) {
	for _, tc := range []struct {
		bytes, blockSize, want int64
	}{
		{0, 4096, 0},
		{1, 4096, 4096},
		{4095, 4096, 4096},
		{4096, 4096, 4096},
		{4097, 4096, 8192},
	} {
		if got := roundUp(tc.bytes, tc.blockSize); got != tc.want {
			t.Errorf("roundUp(%v, %v) = %v, want %v", tc.bytes, tc.blockSize, got, tc.want)
		}
	}
}

func TestChooseBlockSize(t *testing.T) {
	failed := errors.New("failed")
	for _, tc := range []struct {
		detected   int64
		err        error
		configured int64
		want       int64
	}{
		// The filesystem's block size is preferred to the configured one.
		{8192, nil, 4096, 8192},
		{0, failed, 16384, 16384},
		{0, nil, 16384, 16384},
		{0, failed, 0, defaultBlockSize},
	} {
		if got := chooseBlockSize(tc.detected, tc.err, tc.configured); got != tc.want {
			t.Errorf("chooseBlockSize(%v, %v, %v) = %v, want %v", tc.detected, tc.err, tc.configured, got, tc.want)
		}
	}
}

func TestAllocated(t *testing.T) {
	bs, err := filesystemBlockSize(t.TempDir())
	if runtime.GOOS == "windows" || blocksReportAllocation {
		if err != nil || bs <= 0 || bs&(bs-1) != 0 {
			t.Errorf("filesystemBlockSize: got %v, %v, want a power of two", bs, err)
		}
	}
	calc := newAllocated(t.TempDir(), 1<<20)
	if blocksReportAllocation {
		// The configured block size and the file's size are ignored.
		if got, want := calc.Calculate(100, 8), int64(8*512); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := calc.Calculate(1<<30, 8), int64(8*512); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := calc.String(), "allocated"; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		return
	}
	// The detected block size is used in preference to the configured one.
	if got, want := calc.Calculate(100, 1), roundUp(100, chooseBlockSize(bs, err, 1<<20)); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
