// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build darwin

package main

import (
	"cloudeng.io/file"
)

type linkDB struct{}

func newLinkDB() *linkDB {
	return &linkDB{}
}

func (db *linkDB) addLink(prefix string, info file.Info) (bool, []string) {
	return false, nil
}
