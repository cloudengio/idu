// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package usernames maps user and group ids, as recorded by idu and used
// in its statistics, to and from their names, caching the results of the
// lookups.
package usernames

import (
	"fmt"
	"strconv"

	"cloudeng.io/os/userid"
)

// Manager is the IDManager used by idu.
var Manager IDManager

// IDM is the userid.IDManager used by Manager.
var IDM *userid.IDManager

func init() {
	IDM = userid.NewIDManager()
	Manager = IDManager{IDM}
}

// IDManager maps user and group ids to and from their names; ids that
// cannot be looked up are mapped to their decimal representations.
type IDManager struct {
	idmanager *userid.IDManager
}

// NameForUID returns the name of the user with id uid.
func (um *IDManager) NameForUID(uid int64) string {
	u := fmt.Sprintf("%d", uid)
	info, err := um.idmanager.LookupUser(u)
	if err == nil {
		return info.Username
	}
	return u
}

// UIDForName returns the id of the user named name, which may be a
// decimal id.
func (um *IDManager) UIDForName(name string) (int64, error) {
	info, err := um.idmanager.LookupUser(name)
	if err == nil {
		name = info.UID
	}
	id, err := strconv.ParseInt(name, 10, 64)
	return id, err
}

// GIDForName returns the id of the group named name, which may be a
// decimal id.
func (um *IDManager) GIDForName(name string) (int64, error) {
	grp, err := um.idmanager.LookupGroup(name)
	if err == nil {
		name = grp.Gid
	}
	id, err := strconv.ParseInt(name, 10, 64)
	return id, err
}

// NameForGID returns the name of the group with id gid.
func (um *IDManager) NameForGID(gid int64) string {
	g := fmt.Sprintf("%d", gid)
	grp, err := um.idmanager.LookupGroup(g)
	if err == nil {
		return grp.Name
	}
	return g
}
