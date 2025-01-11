// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"strings"

	"cloudeng.io/cmd/idu/internal/boolexpr"
	"cloudeng.io/text/linewrap"
)

type exprCmd struct{}

func (ec *exprCmd) explain(ctx context.Context, _ interface{}, _ []string) error {
	p := boolexpr.NewParser(ctx, nil)
	var out strings.Builder
	out.WriteString("idu commands accept boolean expressions using || && ! and ( and ) to combine any of the following operands:\n\n")

	for _, op := range p.ListOperands() {
		out.WriteString("  ")
		out.WriteString(op.Document())
		out.WriteRune('\n')
		out.WriteRune('\n')
	}

	out.WriteString(`
Note that the name operand evaluates both the name of a file or directory
within the directory that contains it as well as its full path name. The re
(regexp) operand evaluates the full path name of a file or directory.

For example 'name=bar' will match a file named 'bar' in directory '/foo',
as will 'name=/foo/bar'. Since name uses glob matching all directory
levels must be specified, i.e. 'name=/*/*/baz' is required to match
/foo/bar/baz. The re (regexp) operator can be used to match any level,
 for example 're=bar' will match '/foo/bar/baz' as will 're=bar/baz.
`)

	out.WriteString(`
The expression may span multiple arguments which are concatenated together using spaces. Operand values may be quoted using single quotes or may contain escaped characters using. For example re='a b.pdf' or re=a\\ b.pdf\n
`)

	fmt.Println(linewrap.Block(4, 80, out.String()))
	return nil
}
