// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build unix

package progress

import (
	"runtime"
	"syscall"
)

type sysMemstats struct {
	syscall.Rusage
}

func (s *sysMemstats) update() {
	_ = syscall.Getrusage(0, &s.Rusage)
}

// maxRSSBytes returns the peak resident set size, Maxrss is in bytes on
// macOS and in kilobytes on other systems.
func (s *sysMemstats) maxRSSBytes() int64 {
	if runtime.GOOS == "darwin" || runtime.GOOS == "ios" {
		return s.Maxrss
	}
	return s.Maxrss * 1024
}

func (s *sysMemstats) MaxRSSGiB() float64 {
	return float64(s.maxRSSBytes()) / (1024 * 1024 * 1024)
}
