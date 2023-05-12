// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"syscall"
)

type sysMemstats struct {
	syscall.Rusage
}

func (s *sysMemstats) update() {
	syscall.Getrusage(0, &s.Rusage)
}

func (s *sysMemstats) MaxRSSGiB() float64 {
	return float64(s.Rusage.Maxrss) / (1024 * 1024)
}
