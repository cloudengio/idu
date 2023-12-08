// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build windows

package prefixinfo

import (
	"cloudeng.io/file"
)

func devino(fi file.Info) (uint64, uint64) {
	return 0, 0, false
}

func SysInfo(uid, gid uint32, dev, ino uint64) any {
	return &sysinfo{uid: uid, gid: gid, dev: dev, ino: ino}
}
