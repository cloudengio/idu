// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"syscall"

	"cloudeng.io/file"
)

func (db *linkDB) addLink(prefix string, info file.Info) (bool, []string) {
	db.Lock()
	defer db.Unlock()
	sys, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false, nil
	}
	fsid, ino := sys.Dev, sys.Ino
	if _, ok := db.links[fsid]; !ok {
		db.links[fsid] = make(map[uint64][]string)
	}
	filename := prefix + "/" + info.Name()
	_, ok = db.links[fsid][ino]
	db.links[fsid][ino] = append(db.links[fsid][ino], filename)
	if ok {
		return ok, db.links[fsid][ino]
	}
	return ok, nil
}
