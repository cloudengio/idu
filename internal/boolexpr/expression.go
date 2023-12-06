// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package boolexpr provides a wrapper for cloudeng.io/cmdutil/boolexpr
// for use with idu.
package boolexpr

import (
	"fmt"
	"strings"

	"cloudeng.io/cmd/idu/internal/usernames"
	"cloudeng.io/cmdutil/boolexpr"
	"cloudeng.io/file"
	"cloudeng.io/file/matcher"
)

func NewParser() *boolexpr.Parser {
	parser := matcher.New()
	parser.RegisterOperand("user",
		func(_, v string) boolexpr.Operand {
			return NewUID("user", v, usernames.Manager.UIDForName)
		})
	parser.RegisterOperand("group", func(_, v string) boolexpr.Operand {
		return NewGID("group", v, usernames.Manager.GIDForName)
	})
	return parser
}

type T struct {
	s bool
	e boolexpr.T
}

func (e T) Eval(value any) bool {
	if !e.s {
		return true
	}
	return e.e.Eval(value)
}

func (e T) String() string {
	if !e.s {
		return ""
	}
	return e.e.String()
}

func CreateExpr(parser *boolexpr.Parser, args []string) (T, error) {
	if len(args) == 0 {
		// If no expression is specified, then always return true.
		return T{}, nil
	}
	input := strings.Join(args, " ")
	expr, err := parser.Parse(input)
	if err != nil {
		return T{}, fmt.Errorf("failed to parse expression: %v: %v\n", input, err)
	}
	return T{e: expr, s: true}, nil
}

type FileInfoUserGroup struct {
	file.Info
	uid, gid uint32
}

func NewFileInfoUserGroup(info file.Info, uid, gid uint32) FileInfoUserGroup {
	return FileInfoUserGroup{Info: info, uid: uid, gid: gid}
}

func (fwid FileInfoUserGroup) UserGroup() (uint32, uint32) {
	return fwid.uid, fwid.gid
}
