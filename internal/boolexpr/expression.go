// Copyright 2023 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package boolexpr provides a wrapper for cloudeng.io/cmdutil/boolexpr
// for use with idu.
package boolexpr

import (
	"fmt"
	"strings"

	"cloudeng.io/cmd/idu/internal/hardlinks"
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

type Option func(o *options)

func WithExpression(expr ...string) Option {
	return func(o *options) {
		o.args = append(o.args, expr...)
	}
}

// WithHardlinkHandling enables incrmental detection of hardlinks so as to
// avoid visiting the second and subsequent file system entries that
// represent the same file. This is primarily useful for avoiding overcounting
// the resources shared by hardlinks. With this option enabled, the matcher's
// Entry method will return false for any file that has already been seen
// (based on its device and inode numbers).
func WithHardlinkHandling(v bool) Option {
	return func(o *options) {
		o.hardlinks = v
	}
}

type options struct {
	args      []string
	hardlinks bool
}

func CreateMatcher(parser *boolexpr.Parser, opts ...Option) (Matcher, error) {
	options := &options{}
	for _, fn := range opts {
		fn(options)
	}
	m := Matcher{}
	if options.hardlinks {
		m.hl = &hardlinks.Incremental{}
	}
	input := strings.Join(options.args, " ")
	input = strings.TrimSpace(input)
	if len(input) == 0 {
		// If no expression is specified, then always return true.
		return m, nil
	}
	expr, err := parser.Parse(input)
	if err != nil {
		return Matcher{}, fmt.Errorf("failed to parse expression: %v: %v\n", input, err)
	}
	m.set = true
	m.expr = expr
	return m, nil
}

type Matcher struct {
	set  bool
	expr boolexpr.T
	hl   *hardlinks.Incremental
}

func (m Matcher) Prefix(prefix string, info *prefixinfo.T) bool {
	if !m.set {
		return true
	}
	named := prefixinfo.NewNamed(prefix, info)
	return m.expr.Eval(named)
}

func (m Matcher) Entry(prefix string, info *prefixinfo.T, fi file.Info) bool {
	if m.hl != nil {
		_, _, dev, ino := info.SysInfo(fi)
		if m.hl.Ref(dev, ino) {
			// seen before.
			return false
		}
	}
	if !m.set {
		return true
	}
	return m.expr.Eval(withsys{info, fi})
}

func (m Matcher) String() string {
	if m.hl != nil {
		return fmt.Sprintf("[hardlink handling enabled]: %v", m.expr.String())
	}
	return fmt.Sprintf("[hardlink handling disabled]: %v", m.expr.String())
}

type withsys struct {
	pi *prefixinfo.T
	fi file.Info
}

func (w withsys) UserGroup() (uid, gid uint32) {
	uid, gid, _, _ = w.pi.SysInfo(w.fi)
	return
}

func (w withsys) DevIno() (dev, ino uint64) {
	_, _, dev, ino = w.pi.SysInfo(w.fi)
	return
}

func (w withsys) Name() string {
	return w.fi.Name()
}

type AlwaysTrue struct{}

func (AlwaysTrue) Prefix(prefix string, info *prefixinfo.T) bool {
	return true
}

func (AlwaysTrue) Entry(prefix string, info *prefixinfo.T, fi file.Info) bool {
	return true
}
