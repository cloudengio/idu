// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io/ioutil"

	"cloudeng.io/cmd/idu/internal/config"
)

type configFlags struct {
	Documentation bool `subcmd:"document,false,documentation for the configuration file"`
}

func configManager(ctx context.Context, values interface{}, args []string) error {
	flagValues := values.(*configFlags)
	if flagValues.Documentation {
		fmt.Println(config.Documentation())
		return nil
	}
	_, err := config.ReadConfig(globalFlags.ConfigFile)
	if err != nil {
		return fmt.Errorf("failed to parse file %v: %v", globalFlags.ConfigFile, err)
	}
	buf, _ := ioutil.ReadFile(globalFlags.ConfigFile)
	fmt.Println(string(buf))
	return err
}
