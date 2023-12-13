// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build windows

package prefixinfo

import (
	"syscall"

	"cloudeng.io/file"
	"golang.org/x/sys/windows"
)

type sysinfo struct {
	uid, gid uint32
	dev      uint64
	ino      uint64
}

func packFileIndices(hi, low uint32) uint64 {
	return uint64(hi)<<32 | uint64(low)
}

func GetSysInfo(pathname string, fi file.Info) (uid, gid uint32, dev, ino uint64, err error) {
	si := fi.Sys()
	if si == nil {
		return getSysInfo(pathname)
	}
	switch s := si.(type) {
	case *sysinfo:
		return s.uid, s.gid, s.dev, s.ino, nil
	}
	return getSysInfo(pathname)
}

func getSysInfo(pathname string) (uid, gid uint32, dev, ino uint64, err error) {
	// taken from loadFileId in types_windows.go
	pathp, err := syscall.UTF16PtrFromString(pathname)
	if err != nil {
		return
	}
	attrs := uint32(syscall.FILE_FLAG_BACKUP_SEMANTICS | syscall.FILE_FLAG_OPEN_REPARSE_POINT)
	h, err := windows.CreateFile(pathp, 0, 0, nil, syscall.OPEN_EXISTING, attrs, 0)
	if err != nil {
		return
	}
	defer windows.CloseHandle(h)
	var d windows.ByHandleFileInformation
	if err = windows.GetFileInformationByHandle(h, &d); err != nil {
		return
	}
	return 0, 0, uint64(d.VolumeSerialNumber), packFileIndices(d.FileIndexHigh, d.FileIndexLow), nil
}

// NewSysInfo is intended to be used by tests.
func NewSysInfo(uid, gid uint32, dev, ino uint64) any {
	return &sysinfo{uid: uid, gid: gid, dev: dev, ino: ino}
}

func (pi *T) SysInfo(fi file.Info) (userID, groupID uint32, dev, ino uint64) {
	if fi.Sys() == nil {
		return pi.userID, pi.groupID, pi.device, 0
	}
	switch s := fi.Sys().(type) {
	case inoOnly:
		return pi.userID, pi.groupID, pi.device, uint64(s)
	case idAndIno:
		return s.uid, s.gid, pi.device, s.ino
	case *sysinfo:
		return s.uid, s.gid, s.dev, s.ino
	}
	return pi.userID, pi.groupID, 0, 0
}
