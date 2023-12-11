// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package boolexpr provides a wrapper for cloudeng.io/cmdutil/boolexpr
// for use with idu.
package boolexpr

import (
	"fmt"
	"strings"

	"cloudeng.io/cmd/idu/internal/prefixinfo"
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

func CreateMatcher(parser *boolexpr.Parser, args []string) (Matcher, error) {
	if len(args) == 0 {
		// If no expression is specified, then always return true.
		return Matcher{}, nil
	}
	input := strings.Join(args, " ")
	expr, err := parser.Parse(input)
	if err != nil {
		return Matcher{}, fmt.Errorf("failed to parse expression: %v: %v\n", input, err)
	}
	return Matcher{T: expr}, nil
}

type Matcher struct {
	boolexpr.T
}

func (m Matcher) Prefix(prefix string, info *prefixinfo.T) bool {
	named := prefixinfo.NewNamed(prefix, info)
	return m.Eval(named)
}

type withsys struct {
	*prefixinfo.T
	fi file.Info
}

func (w withsys) UserGroup() (uid, gid uint32) {
	uid, gid, _, _ = w.SysInfo(w.fi)
	return
}

func (w withsys) DevIno() (dev, ino uint64) {
	_, _, dev, ino = w.SysInfo(w.fi)
	return
}

func (m Matcher) Entry(prefix string, info *prefixinfo.T, fi file.Info) bool {
	return m.Eval(withsys{info, fi})
}

type AlwaysTrue struct{}

func (AlwaysTrue) Prefix(prefix string, info *prefixinfo.T) bool {
	return true
}

func (AlwaysTrue) Entry(prefix string, info *prefixinfo.T, fi file.Info) bool {
	return true
}
