// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build unix

package iduconfig

import "golang.org/x/sys/unix"

// blocksReportAllocation is true since st_blocks reports the number of
// 512 byte blocks allocated to a file.
const blocksReportAllocation = true

// filesystemBlockSize returns the block size of the filesystem
// containing path.
func filesystemBlockSize(path string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, err
	}
	return int64(st.Bsize), nil //nolint:unconvert // the type of Bsize varies by platform.
}
