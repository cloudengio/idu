// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build unix

package main

import (
	"context"

	"cloudeng.io/logging/ctxlog"
	"golang.org/x/sys/unix"
)

func useMaxProcs(ctx context.Context) error {
	var nproc unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NPROC, &nproc); err != nil {
		return err
	}
	if nproc.Cur == nproc.Max {
		ctxlog.Info(ctx, "max procs already at max", "nprocs", nproc.Max)
		return nil
	}
	ctxlog.Info(ctx, "setting max procs", "cur", nproc.Cur, "max", nproc.Max)
	nproc.Cur = nproc.Max
	return unix.Setrlimit(unix.RLIMIT_NPROC, &nproc)
}
