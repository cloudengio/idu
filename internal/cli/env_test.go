// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cli

import (
	"context"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/idudb"
)

// testEnv is the environment used by the tests.
var testEnv = NewEnv()

// openPrefixReadOnly is like Env.OpenPrefixReadOnly but uses cfg.
func openPrefixReadOnly(ctx context.Context, cfg iduconfig.T, prefix string) (context.Context, *iduconfig.Settings, string, idudb.Reader, error) {
	env := *testEnv
	env.Config = cfg
	return env.OpenPrefixReadOnly(ctx, prefix)
}
