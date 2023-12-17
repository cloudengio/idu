// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"

	"cloudeng.io/cmd/idu/internal/boolexpr"
)

type exprCmd struct{}

func (ec *exprCmd) explain(ctx context.Context, values interface{}, args []string) error {
	p := boolexpr.NewParser(ctx, nil)
	fmt.Printf("idu commands accept boolean expressions using || && and ( and ) to combine any of the following operands:\n\n")
	for _, op := range p.ListOperands() {
		fmt.Printf("  %v\n", op.Document())
	}
	fmt.Printf("\nNote that directories are evaluated both using their full path name as well as their name within a parent, whereas files use evaluated just using their name within a directory.\n")

	fmt.Printf("\nThe expression may span multiple arguments which are concatenated together using spaces. Operand values may be quoted using single quotes or may contain escaped characters using \\. For example re='a b.pdf' or re=a\\ b.pdf\n")

	return nil
}
