// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"strings"

	"cloudeng.io/cmd/idu/tui"
)

type browseFlags struct{}

func browse(ctx context.Context, _ any, args []string) error {
	ctx, cfg, target, db, err := openPrefixReadOnly(ctx, globalConfig, args[0])
	if err != nil {
		return err
	}
	defer db.Close(context.WithoutCancel(ctx))
	return tui.Run(ctx, tui.NewSummarySource(db, &cfg), target, tui.Options{
		FormatBytes: func(n int64) string { return strings.TrimSpace(fmtSize(n)) },
		FormatCount: func(n int64) string { return strings.TrimSpace(fmtCount(n)) },
	})
}
