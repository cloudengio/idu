// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build windows

package main

type sysMemstats struct{}

func (s *sysMemstats) update() {
}

func (s *sysMemstats) MaxRSSGiB() float64 {
	return -1
}
