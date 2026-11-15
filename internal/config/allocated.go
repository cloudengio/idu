// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package config

import (
	"strconv"
	"sync"

	"cloudeng.io/file/diskusage"
)

// statBlockSize is the unit of file.XAttr.Blocks, i.e. of st_blocks,
// which is 512 bytes regardless of the filesystem's block size.
const statBlockSize = 512

// defaultBlockSize is used to round up file sizes when neither the
// filesystem's block size can be determined nor one is configured.
const defaultBlockSize = 4096

// allocated is a disk usage calculator that uses the space allocated to
// a file on disk. Where the number of blocks allocated to a file is
// reported by the operating system, as it is by all POSIX systems via
// st_blocks, that is used directly since it already accounts for the
// filesystem's block size, holes in sparse files and compression.
// Otherwise, a file's size is rounded up to the filesystem's block size,
// which is determined from the filesystem if possible, or failing that,
// the configured block size.
type allocated struct {
	path       string
	configured int64

	once      sync.Once
	blockSize int64
}

func newAllocated(path string, configured int64) diskusage.Calculator {
	return &allocated{path: path, configured: configured}
}

// Calculate implements diskusage.Calculator.
func (a *allocated) Calculate(bytes, blocks int64) int64 {
	if blocksReportAllocation {
		return blocks * statBlockSize
	}
	return roundUp(bytes, a.roundUpSize())
}

// String implements diskusage.Calculator. It identifies how the space
// used was calculated and hence includes the block size when it is
// used.
func (a *allocated) String() string {
	if blocksReportAllocation {
		return "allocated"
	}
	return "allocated: " + strconv.FormatInt(a.roundUpSize(), 10)
}

func (a *allocated) roundUpSize() int64 {
	a.once.Do(func() {
		detected, err := filesystemBlockSize(a.path)
		a.blockSize = chooseBlockSize(detected, err, a.configured)
	})
	return a.blockSize
}

// chooseBlockSize prefers the block size determined from the filesystem
// over the configured one.
func chooseBlockSize(detected int64, err error, configured int64) int64 {
	switch {
	case err == nil && detected > 0:
		return detected
	case configured > 0:
		return configured
	default:
		return defaultBlockSize
	}
}

func roundUp(bytes, blockSize int64) int64 {
	if bytes <= 0 {
		return 0
	}
	return ((bytes + blockSize - 1) / blockSize) * blockSize
}
