// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build darwin

package prefixinfo

import (
	"syscall"

	"cloudeng.io/file"
)

func devino(fi file.Info) (uint64, uint64, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}
	return uint64(st.Dev), st.Ino, true
}

func SysInfo(uid, gid uint32, dev, ino uint64) any {
	return &syscall.Stat_t{Uid: uid, Gid: gid, Dev: int32(dev), Ino: ino}
}
