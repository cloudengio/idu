// Copyright 2020 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
)

type queryFlags struct {
}

func query(ctx context.Context, values interface{}, args []string) error {
	flagValues := values.(*queryFlags)
	_ = flagValues
	return fmt.Errorf("not yet implemented")
}
