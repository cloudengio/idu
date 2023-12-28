// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package usernames

import (
	"fmt"
	"strconv"

	"cloudeng.io/os/userid"
)

var IDM *userid.IDManager

func init() {
	IDM = userid.NewIDManager()
}

var Manager = IDManager{
	idmanager: IDM,
}

type IDManager struct {
	idmanager *userid.IDManager
}

func (um *IDManager) NameForUID(uid int64) string {
	u := fmt.Sprintf("%d", uid)
	info, err := um.idmanager.LookupUser(u)
	if err == nil {
		return info.Username
	}
	return u
}

func (um *IDManager) UIDForName(name string) (int64, error) {
	info, err := um.idmanager.LookupUser(name)
	if err == nil {
		name = info.UID
	}
	id, err := strconv.ParseInt(name, 10, 32)
	return id, err
}

func (um *IDManager) GIDForName(name string) (int64, error) {
	grp, err := um.idmanager.LookupGroup(name)
	if err == nil {
		name = grp.Gid
	}
	id, err := strconv.ParseInt(name, 10, 32)
	return id, err
}

func (um *IDManager) NameForGID(gid int64) string {
	g := fmt.Sprintf("%d", gid)
	grp, err := um.idmanager.LookupGroup(g)
	if err == nil {
		return grp.Name
	}
	return g
}
