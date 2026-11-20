// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"io"
	"os"

	"cloudeng.io/cmd/idu/iduconfig"
	"cloudeng.io/cmd/idu/internal/cli"
	"cloudeng.io/cmdutil"
	"cloudeng.io/cmdutil/cmdconfig"
	"cloudeng.io/cmdutil/subcmd"
)

func configCommands() cmdconfig.ConfigCommands[iduconfig.T] {
	return cmdconfig.ConfigCommands[iduconfig.T]{
		Summary:    "YAML configuration file options\n",
		Describers: []func(io.Writer) error{iduconfig.DescribeLayouts, iduconfig.DescribeHardlinkDedup},
	}
}

func newConfigExtension() subcmd.Extension {
	return cmdconfig.ConfigExtension(configCommands())
}

// configDescribe documents the configuration file format. It is used
// in place of cmdconfig's generic implementation since the filesystem
// entries are represented by an interface that cannot be described
// via reflection.
func configDescribe(_ context.Context, _ any, _ []string) error {
	_, err := io.WriteString(os.Stdout, "YAML configuration file options\n"+iduconfig.Documentation())
	return err
}

func configShow(ctx context.Context, flagValues any, args []string) error {
	if loadErr := cli.ConfigLoadError(ctx); loadErr != nil {
		if loc, ok := cmdutil.ContextValue[*cmdconfig.Located](ctx); ok {
			iduconfig.PrintLocated(os.Stdout, loc)
		}
		return loadErr
	}
	return configCommands().Show(ctx, flagValues, args)
}
