// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build linux

package main

import "sync"

type linkDB struct {
	sync.Mutex
	links map[uint64]map[uint64][]string
}

func newLinkDB() *linkDB {
	return &linkDB{
		links: make(map[uint64]map[uint64][]string),
	}
}
