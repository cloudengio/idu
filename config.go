// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"io"
	"os"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmdutil"
	"cloudeng.io/cmdutil/cmdconfig"
	"cloudeng.io/cmdutil/subcmd"
)

type configLoadError struct {
	err error
}

func configCommands() cmdconfig.ConfigCommands[iduconfig.T] {
	return cmdconfig.ConfigCommands[iduconfig.T]{
		Summary:    "YAML configuration file options\n",
		Describers: []func(io.Writer) error{iduconfig.DescribeLayouts, iduconfig.DescribeHardlinkDedup},
	}
}

func newConfigExtension() subcmd.Extension {
	return cmdconfig.ConfigExtension(configCommands())
}

func configShow(ctx context.Context, flagValues any, args []string) error {
	if loadErr, ok := cmdutil.ContextValue[configLoadError](ctx); ok && loadErr.err != nil {
		if loc, ok := cmdutil.ContextValue[*cmdconfig.Located](ctx); ok {
			iduconfig.PrintLocated(os.Stdout, loc)
		}
		return loadErr.err
	}
	return configCommands().Show(ctx, flagValues, args)
}
