// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmdutil/cmdconfig"
	"cloudeng.io/cmdutil/subcmd"
)

// newConfigExtension returns the extension that implements the config
// commands. The configuration is loaded by configHook, rather than by a
// cmdconfig.ConfigHook.
func newConfigExtension() subcmd.Extension {
	return cmdconfig.ConfigExtension(iduconfig.ConfigCommands())
}
