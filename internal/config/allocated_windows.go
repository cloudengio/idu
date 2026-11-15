// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build windows

package config

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

// blocksReportAllocation is false since the number of blocks reported
// for a file on Windows is derived from its size.
const blocksReportAllocation = false

var procGetDiskFreeSpaceW = windows.NewLazySystemDLL("kernel32.dll").NewProc("GetDiskFreeSpaceW")

// filesystemBlockSize returns the cluster size of the volume containing
// path.
func filesystemBlockSize(path string) (int64, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	volume := make([]uint16, windows.MAX_PATH+1)
	if err := windows.GetVolumePathName(p, &volume[0], uint32(len(volume))); err != nil {
		return 0, err
	}
	var sectorsPerCluster, bytesPerSector, freeClusters, totalClusters uint32
	r, _, err := procGetDiskFreeSpaceW.Call(
		uintptr(unsafe.Pointer(&volume[0])),
		uintptr(unsafe.Pointer(&sectorsPerCluster)),
		uintptr(unsafe.Pointer(&bytesPerSector)),
		uintptr(unsafe.Pointer(&freeClusters)),
		uintptr(unsafe.Pointer(&totalClusters)))
	if r == 0 {
		return 0, err
	}
	return int64(sectorsPerCluster) * int64(bytesPerSector), nil
}
