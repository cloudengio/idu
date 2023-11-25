// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build windows

package prefixinfo

import (
	"cloudeng.io/file"
)

func userGroupID(fi file.Info) (userID, groupID uint32, ok bool) {
	if u, ok := fi.Sys().(*userInfo); ok {
		return u.Uid, u.Gid, true
	}
	return 0, 0, false
}

type userInfo struct {
	uid, gid uint32
}

func sysUserGroupID(uid, gid uint32) any {
	return &userInfo{Uid: uid, Gid: gid}
}
