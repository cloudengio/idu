// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build windows

package prefixinfo

import (
	"cloudeng.io/file"
)

type userInfo struct {
	uid, gid uint32
}

func userGroupID(fi file.Info) (userID, groupID uint32, ok bool) {
	if u, ok := fi.Sys().(*userInfo); ok {
		return u.uid, u.gid, true
	}
	// TODO: implement user/group in some form for windows, for now just
	// pretend that the user and group are 0
	return 0, 0, true
}
