// Copyright 2026 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package testutil

import (
	"context"
	"strconv"

	"cloudeng.io/cmd/idu/stats"
	"cloudeng.io/cmdutil/boolexpr"
	"cloudeng.io/file"
	"cloudeng.io/file/filewalk"
	"cloudeng.io/file/matcher"
)

// NewParser returns a stats.NewParser whose user and group operands accept
// any uid/gid rather than requiring that they exist on the local system.
func NewParser(ctx context.Context, fs filewalk.FS) *boolexpr.Parser {
	return stats.NewParser(ctx, fs,
		stats.WithOperand("user", func(n, v string) boolexpr.Operand {
			return matcher.NewUser(n, v, func(text string) (file.XAttr, error) {
				id, err := strconv.ParseInt(text, 10, 64)
				if err != nil {
					return file.XAttr{UID: -1, User: text}, nil
				}
				return file.XAttr{UID: id}, nil
			})
		}),
		stats.WithOperand("group", func(n, v string) boolexpr.Operand {
			return matcher.NewGroup(n, v, func(text string) (file.XAttr, error) {
				id, err := strconv.ParseInt(text, 10, 64)
				if err != nil {
					return file.XAttr{GID: -1, Group: text}, nil
				}
				return file.XAttr{GID: id}, nil
			})
		}))
}
