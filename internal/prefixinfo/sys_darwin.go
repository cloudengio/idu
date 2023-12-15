// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build darwin

package prefixinfo

import (
	"fmt"
	"syscall"

	"cloudeng.io/file"
)

func GetSysInfo(pathname string, fi file.Info) (uid, gid uint32, dev, ino uint64, blocks int64, err error) {
	si := fi.Sys()
	if si == nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("no system set for %v", pathname)
	}
	if s, ok := si.(*syscall.Stat_t); ok {
		return s.Uid, s.Gid, uint64(s.Dev), s.Ino, s.Blocks, nil
	}
	return 0, 0, 0, 0, 0, fmt.Errorf("unrecognised system information %T for %v", si, pathname)
}

// NewSysInfo is intended to be used by tests.
func NewSysInfo(uid, gid uint32, dev, ino uint64, blocks int64) any {
	return &syscall.Stat_t{Uid: uid, Gid: gid, Dev: int32(dev), Ino: ino, Blocks: blocks}
}

func (pi *T) SysInfo(fi file.Info) (userID, groupID uint32, dev, ino uint64, blocks int64) {
	if fi.Sys() == nil {
		return pi.userID, pi.groupID, pi.device, 0, 0
	}
	switch s := fi.Sys().(type) {
	case fsOnly:
		return pi.userID, pi.groupID, pi.device, s.ino, s.blocks
	case idAndFS:
		return s.uid, s.gid, pi.device, s.ino, s.blocks
	case *syscall.Stat_t:
		return s.Uid, s.Gid, uint64(s.Dev), s.Ino, s.Blocks
	}
	return pi.userID, pi.groupID, 0, 0, 0
}
