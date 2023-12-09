// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

type inoOnly uint64

type idAndIno struct {
	uid, gid uint32
	ino      uint64
}

/*func (pi *T) SetSysInfo(fi *file.Info, uid, gid uint32, dev, ino uint64) {
	if pi.userID == uid && pi.groupID == gid {
		fi.SetSys(inoOnly(ino))
	}
	fi.SetSys(idAndIno{uid: uid, gid: gid, ino: ino})
}*/
