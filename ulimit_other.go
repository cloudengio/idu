// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build !unix

package main

import (
	"context"

	"cloudeng.io/cmd/idu/internal"
)

func useMaxProcs(ctx context.Context) error {
	internal.Log(ctx, internal.LogProgress, "max procs unchanged")
	return nil
}
