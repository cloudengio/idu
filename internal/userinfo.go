// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"cloudeng.io/file"
)

type userinfo struct {
	uid, gid uint32
}

func (pi *PrefixInfo) GetUserGroup(fi file.Info) (userID, groupID uint32) {
	if fi.Sys() == nil {
		return pi.UserID, pi.GroupID
	}
	if ui, ok := fi.Sys().(userinfo); ok {
		return ui.uid, ui.gid
	}
	u, g, ok := userGroupID(fi)
	if !ok {
		return pi.UserID, pi.GroupID
	}
	return u, g
}

func (pi *PrefixInfo) SetUserGroup(fi *file.Info, userID, groupID uint32) {
	if pi.UserID == userID && pi.GroupID == groupID {
		fi.SetSys(nil)
	}
	fi.SetSys(userinfo{userID, groupID})
}

func UserGroup(fi file.Info) (userID, groupID uint32, ok bool) {
	return userGroupID(fi)
}
