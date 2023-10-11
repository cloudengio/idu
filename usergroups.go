// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"strconv"

	"cloudeng.io/os/userid"
)

type userManager struct {
	idmanager *userid.IDManager
}

var globalUserManager = userManager{
	idmanager: userid.NewIDManager(),
}

func (um *userManager) nameForUID(uid uint32) string {
	u := fmt.Sprintf("%d", uid)
	info, err := um.idmanager.LookupUser(u)
	if err == nil {
		return info.Username
	}
	return u
}

func (um *userManager) uidForName(name string) (uint32, error) {
	info, err := um.idmanager.LookupUser(name)
	if err == nil {
		name = info.UID
	}
	id, err := strconv.ParseUint(name, 10, 32)
	return uint32(id), err
}

func (um *userManager) gidForName(name string) (uint32, error) {
	grp, err := um.idmanager.LookupGroup(name)
	if err == nil {
		name = grp.Gid
	}
	id, err := strconv.ParseUint(name, 10, 32)
	return uint32(id), err
}

func (um *userManager) nameForGID(gid uint32) string {
	g := fmt.Sprintf("%d", gid)
	grp, err := um.idmanager.LookupGroup(g)
	if err == nil {
		return grp.Name
	}
	return g
}