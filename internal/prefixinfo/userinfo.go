// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package prefixinfo

import (
	"cloudeng.io/file"
)

type userinfo struct {
	uid, gid uint32
}

func (pi *T) GetUserGroupFile(fi file.Info) (userID, groupID uint32) {
	if fi.Sys() == nil {
		return pi.userID, pi.groupID
	}
	if ui, ok := fi.Sys().(userinfo); ok {
		return ui.uid, ui.gid
	}
	u, g, ok := userGroupID(fi)
	if !ok {
		return pi.userID, pi.groupID
	}
	return u, g
}

func (pi *T) SetUserGroupFile(fi *file.Info, userID, groupID uint32) {
	if pi.userID == userID && pi.groupID == groupID {
		fi.SetSys(nil)
	}
	fi.SetSys(userinfo{userID, groupID})
}

func UserGroup(fi file.Info) (userID, groupID uint32, ok bool) {
	if ui, ok := fi.Sys().(userinfo); ok {
		return ui.uid, ui.gid, true
	}
	return userGroupID(fi)
}
