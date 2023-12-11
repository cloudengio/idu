// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build linux

package prefixinfo

import (
	"syscall"

	"cloudeng.io/file"
)

func getSysInfo(fi file.Info) (uid, gid uint32, dev, ino uint64) {
	si := fi.Sys()
	if si == nil {
		return 0, 0, 0, 0
	}
	if s, ok := si.(*syscall.Stat_t); ok {
		return s.Uid, s.Gid, s.Dev, s.Ino
	}
	return 0, 0, 0, 0
}

// NewSysInfo is intended to be used by tests.
func NewSysInfo(uid, gid uint32, dev, ino uint64) any {
	return &syscall.Stat_t{Uid: uid, Gid: gid, Dev: dev, Ino: ino}
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
	case *syscall.Stat_t:
		return s.Uid, s.Gid, s.Dev, s.Ino
	}
	return pi.userID, pi.groupID, 0, 0
}
