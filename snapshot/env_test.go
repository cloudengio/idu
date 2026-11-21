// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package snapshot

import (
	"context"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
	"cloudeng.io/cmd/idu/internal/cli"
)

// testEnv is the environment used by the tests.
var testEnv = cli.NewEnv()

// openPrefixReadOnly is like cli.Env.OpenPrefixReadOnly but uses cfg.
func openPrefixReadOnly(ctx context.Context, cfg iduconfig.T, prefix string) (context.Context, *iduconfig.Settings, string, idudb.Reader, error) {
	env := *testEnv
	env.Config = cfg
	return env.OpenPrefixReadOnly(ctx, prefix)
}
